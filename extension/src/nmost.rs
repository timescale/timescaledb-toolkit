use pgx::*;

use serde::{Deserialize, Serialize};

use crate::{
    aggregate_utils::in_aggregate_context,
    datum_utils::{deep_copy_datum, free_datum, DatumStore},
    palloc::{Inner, Internal, InternalAsValue},
};

use std::collections::BinaryHeap;

mod max_float;
mod max_int;
mod max_time;
mod min_float;
mod min_int;
mod min_time;

mod max_by_float;
mod max_by_int;
mod max_by_time;
mod min_by_float;
mod min_by_int;
mod min_by_time;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NMostTransState<T: Ord> {
    capacity: usize,
    heap: BinaryHeap<T>,
}

impl<T: Ord> NMostTransState<T> {
    fn new(capacity: usize, first_val: T) -> NMostTransState<T> {
        let mut new_heap = NMostTransState {
            capacity,
            heap: BinaryHeap::with_capacity(capacity),
        };

        new_heap.new_entry(first_val);

        new_heap
    }

    fn new_entry(&mut self, new_val: T) {
        // If at capacity see if we need to replace something
        if self.heap.len() == self.capacity {
            if !self.belongs_in_heap(&new_val) {
                return;
            }

            self.heap.pop();
        }

        self.heap.push(new_val)
    }

    fn belongs_in_heap(&self, val: &T) -> bool {
        // Note that this will actually be '>' if T is a Reverse<...> type
        val < self.heap.peek().unwrap()
    }
}

impl<T: Ord + Copy> From<(&[T], usize)> for NMostTransState<T> {
    fn from(input: (&[T], usize)) -> Self {
        let (vals, capacity) = input;
        let mut state = Self::new(capacity, vals[0]);
        for val in vals[1..].iter() {
            state.new_entry(*val);
        }
        state
    }
}

fn nmost_trans_function<T: Ord>(
    state: Option<Inner<NMostTransState<T>>>,
    val: T,
    capacity: usize,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<NMostTransState<T>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            if state.is_none() {
                return Internal::new(NMostTransState::<T>::new(capacity, val)).to_inner();
            }

            let mut state = state.unwrap();
            state.new_entry(val);
            Some(state)
        })
    }
}

fn nmost_rollup_trans_function<T: Ord + Copy>(
    state: Option<Inner<NMostTransState<T>>>,
    sorted_vals: &[T],
    capacity: usize,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<NMostTransState<T>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            if let Some(mut state) = state {
                for val in sorted_vals {
                    // The values are sorted, so as soon as we find one that shouldn't be added, we're done
                    if !state.belongs_in_heap(val) {
                        return Some(state);
                    }
                    state.new_entry(*val);
                }

                Some(state)
            } else {
                Internal::new::<NMostTransState<T>>((sorted_vals, capacity).into()).to_inner()
            }
        })
    }
}

fn nmost_trans_combine<T: Clone + Ord + Copy>(
    first: Option<Inner<NMostTransState<T>>>,
    second: Option<Inner<NMostTransState<T>>>,
) -> Option<Inner<NMostTransState<T>>> {
    match (first, second) {
        (None, None) => None,
        (None, Some(only)) | (Some(only), None) => unsafe {
            Internal::new(only.clone()).to_inner()
        },
        (Some(a), Some(b)) => {
            let mut a = a.clone();
            // This could be made more efficient by iterating in the appropriate order with an early exit, but would requiring ordering the other heap
            for entry in b.heap.iter() {
                a.new_entry(*entry);
            }
            unsafe { Internal::new(a).to_inner() }
        }
    }
}

// TODO: serialize and deserialize will need to be implemented with Datum handling code
#[derive(Clone, Debug)]
pub struct NMostByTransState<T: Ord> {
    values: NMostTransState<(T, usize)>,
    data: Vec<pg_sys::Datum>,
    oid: pg_sys::Oid,
}

impl<T: Clone + Ord> NMostByTransState<T> {
    fn new(capacity: usize, first_val: T, first_element: pgx::AnyElement) -> NMostByTransState<T> {
        // first entry will always have index 0
        let first_val = (first_val, 0);
        NMostByTransState {
            values: NMostTransState::new(capacity, first_val),
            data: vec![unsafe { deep_copy_datum(first_element.datum(), first_element.oid()) }],
            oid: first_element.oid(),
        }
    }

    fn new_entry(&mut self, new_val: T, new_element: pgx::AnyElement) {
        assert!(new_element.oid() == self.oid);
        if self.data.len() < self.values.capacity {
            // Not yet full, easy case
            self.values.new_entry((new_val, self.data.len()));
            self.data
                .push(unsafe { deep_copy_datum(new_element.datum(), new_element.oid()) });
        } else if self
            .values
            .belongs_in_heap(&(new_val.clone(), self.data.len()))
        {
            // Full and value belongs in the heap (using len() for this check just keeps us from
            // succeeding if we tie the max heap element)

            let (_, index_to_replace) = *self
                .values
                .heap
                .peek()
                .expect("Can't be empty in this case");
            let old_datum = std::mem::replace(&mut self.data[index_to_replace], unsafe {
                deep_copy_datum(new_element.datum(), new_element.oid())
            });
            unsafe { free_datum(old_datum, new_element.oid()) };
            self.values.new_entry((new_val, index_to_replace));
        }
    }

    // Sort the trans state and break it into a tuple of (capacity, values array, datum_store)
    fn into_sorted_parts(self) -> (usize, Vec<T>, DatumStore<'static>) {
        let values = self.values;
        let heap = values.heap;
        let (val_ary, idx_ary): (Vec<T>, Vec<usize>) = heap.into_sorted_vec().into_iter().unzip();

        let mut mapped_data = vec![];
        for i in idx_ary {
            mapped_data.push(self.data[i]);
        }

        (
            values.capacity,
            val_ary,
            DatumStore::from((self.oid, mapped_data)),
        )
    }
}

impl<T: Ord + Copy> From<(&[T], &DatumStore<'_>, usize)> for NMostByTransState<T> {
    fn from(in_tuple: (&[T], &DatumStore, usize)) -> Self {
        let (vals, data, capacity) = in_tuple;
        let mut elements = data.clone().into_anyelement_iter();
        let mut state = Self::new(capacity, vals[0], elements.next().unwrap());
        for val in vals[1..].iter() {
            state.new_entry(*val, elements.next().unwrap());
        }
        state
    }
}

fn nmost_by_trans_function<T: Ord + Clone>(
    state: Option<Inner<NMostByTransState<T>>>,
    val: T,
    data: pgx::AnyElement,
    capacity: usize,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<NMostByTransState<T>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            if state.is_none() {
                return Internal::new(NMostByTransState::<T>::new(capacity, val, data)).to_inner();
            }

            let mut state = state.unwrap();
            state.new_entry(val, data);
            Some(state)
        })
    }
}

fn nmost_by_rollup_trans_function<T: Ord + Copy>(
    state: Option<Inner<NMostByTransState<T>>>,
    sorted_vals: &[T],
    datum_store: &DatumStore,
    capacity: usize,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<NMostByTransState<T>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            if let Some(mut state) = state {
                for (val, element) in sorted_vals
                    .iter()
                    .zip(datum_store.clone().into_anyelement_iter())
                {
                    // The values are sorted, so as soon as we find one that shouldn't be added, we're done
                    if !state.values.belongs_in_heap(&(*val, state.values.capacity)) {
                        return Some(state);
                    }
                    state.new_entry(*val, element);
                }

                Some(state)
            } else {
                Internal::new::<NMostByTransState<T>>((sorted_vals, datum_store, capacity).into())
                    .to_inner()
            }
        })
    }
}
