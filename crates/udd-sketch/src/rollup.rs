//! UDDSketch implementation in rust.
//! Based on the paper: https://arxiv.org/abs/2004.08604

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::compactor::{compact_from_iter, ArrayCompactor};
use crate::SketchHashKey::{Invalid, Zero};
#[cfg(test)]
use ordered_float::OrderedFloat;
#[cfg(test)]
use std::collections::HashSet;
use crate::{estimate_quantile, estimate_quantile_at_value, gamma, key, SketchHashEntry, SketchHashIterator, SketchHashKey, SketchHashMap};

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UDDSketchRollup {
    buckets: SketchHashMap,
    alpha: f64,
    gamma: f64,
    compactions: u32, // should always be smaller than 64
    max_buckets: u64,
    num_values: u64,
    values_sum: f64,
}

impl UDDSketchRollup {
    pub fn new(max_buckets: u64, initial_error: f64) -> Self {
        assert!((1e-12..1.0).contains(&initial_error));
        UDDSketchRollup {
            buckets: SketchHashMap::new(),
            alpha: initial_error,
            gamma: (1.0 + initial_error) / (1.0 - initial_error),
            compactions: 0,
            max_buckets,
            num_values: 0,
            values_sum: 0.0,
        }
    }

    /// This constructor is used to recreate a UddSketch from its component data
    /// it assumes the provided keys/counts are ordered by the keys.
    pub fn new_from_data(
        max_buckets: u64,
        current_error: f64,
        compactions: u64,
        num_buckets: u32,
        values: u64,
        sum: f64,
        keys: impl Iterator<Item = SketchHashKey>,
        counts: impl Iterator<Item = u64>,
    ) -> Self {
        let mut sketch = UDDSketchRollup {
            buckets: SketchHashMap::with_capacity(num_buckets as usize),
            alpha: current_error,
            gamma: gamma(current_error),
            compactions: compactions as u32,
            max_buckets,
            num_values: values,
            values_sum: sum,
        };

        let mut iter = keys.into_iter().zip(counts.into_iter()).peekable();
        sketch.buckets.head = iter.peek().map(|p| p.0).unwrap_or(Invalid);

        while let Some((key, count)) = iter.next() {
            sketch
                .buckets
                .map
                .entry(key)
                .and_modify(|e| e.count += count)
                .or_insert(SketchHashEntry {
                    count,
                    next: iter.peek().map(|p| p.0).unwrap_or(SketchHashKey::Invalid),
                });
        }

        //assert_eq!(num_buckets as usize, sketch.buckets.map.len());

        sketch
    }
}

impl UDDSketchRollup {
    // For a given value return the index of it's bucket in the current sketch.
    fn key(&self, value: f64) -> SketchHashKey {
        key(value, self.gamma)
    }

    pub fn compact_buckets(&mut self, swap: &mut Vec<(SketchHashKey, u64)>) {
        self.buckets.compact_with_swap(swap);

        self.compactions += 1;
        self.gamma *= self.gamma; // See https://arxiv.org/pdf/2004.08604.pdf Equation 3
        self.alpha = 2.0 * self.alpha / (1.0 + self.alpha.powi(2)); // See https://arxiv.org/pdf/2004.08604.pdf Equation 4
    }

    pub fn bucket_iter(&self) -> SketchHashIterator {
        self.buckets.iter()
    }
}

impl UDDSketchRollup {
    pub fn add_value(&mut self, value: f64) {
        self.buckets.increment(self.key(value));

        while self.buckets.len() > self.max_buckets as usize {
            self.compact_buckets(&mut Vec::new());
        }

        self.num_values += 1;
        self.values_sum += value;
    }

    pub fn merge_sketch(&mut self, other: &UDDSketchRollup) {
        // Require matching initial parameters
        assert!(
            (self
                .gamma
                .powf(1.0 / f64::powi(2.0, self.compactions as i32))
                - other
                .gamma
                .powf(1.0 / f64::powi(2.0, other.compactions as i32)))
                .abs()
                < 1e-9 // f64::EPSILON too small, see issue #396
        );
        assert!(self.max_buckets == other.max_buckets);

        if other.num_values == 0 {
            return;
        }
        if self.num_values == 0 {
            *self = other.clone();
            return;
        }

        // We can reuse this Heap allocated Vec every time
        // we compact.
        let mut swap = Vec::new();

        let mut tmp: UDDSketchRollup;
        // We only need to fully clone the other sketch
        // if we need to compact it. Not doing it
        // is useful, as it doesn't require us to
        // allocate any more memory.
        // We optimize here, as this is code is called frequently
        let other = if self.compactions > other.compactions {
            tmp = other.clone();
            while self.compactions > tmp.compactions {
                tmp.compact_buckets(&mut swap);
            }
            &tmp
        } else {
            while other.compactions > self.compactions {
                self.compact_buckets(&mut swap);
            }
            other
        };

        for entry in other.buckets.iter() {
            let (key, value) = entry;
            self.buckets.entry(key).count += value;
        }

        while self.buckets.len() > self.max_buckets as usize {
            self.compact_buckets(&mut swap);
        }

        self.num_values += other.num_values;
        self.values_sum += other.values_sum;
    }

    pub fn max_allowed_buckets(&self) -> u64 {
        self.max_buckets
    }

    pub fn times_compacted(&self) -> u32 {
        self.compactions
    }

    pub fn current_buckets_count(&self) -> usize {
        self.buckets.map.len()
    }
}

impl UDDSketchRollup {
    #[inline]
    pub fn mean(&self) -> f64 {
        if self.num_values == 0 {
            0.0
        } else {
            self.values_sum / self.num_values as f64
        }
    }

    #[inline]
    pub fn sum(&self) -> f64 {
        self.values_sum
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.num_values
    }

    #[inline]
    pub fn max_error(&self) -> f64 {
        self.alpha
    }

    pub fn estimate_quantile(&self, quantile: f64) -> f64 {
        estimate_quantile(
            quantile,
            self.alpha,
            self.gamma,
            self.num_values,
            self.buckets.iter(),
        )
    }

    pub fn estimate_quantile_at_value(&self, value: f64) -> f64 {
        estimate_quantile_at_value(value, self.gamma, self.num_values, self.buckets.iter())
    }
}
