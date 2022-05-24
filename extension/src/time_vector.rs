#![allow(clippy::identity_op)] // clippy gets confused by pg_type! enums

use pgx::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    build,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

use tspoint::TSPoint;

pub use iter::Iter;

use flat_serialize::*;

mod iter;
mod pipeline;

use crate::raw::bytea;

pub use toolkit_experimental::{Timevector, TimevectorData};

// Bit flags stored in Timevector flags
pub const FLAG_IS_SORTED : u8 = 0x01;
pub const FLAG_HAS_NULLS : u8 = 0x01 << 1;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;
    pg_type! {
        #[derive(Debug)]
        struct Timevector<'input> {
            num_points: u32,
            flags: u8,         // extra information about the stored data
            internal_padding: [u8; 3],  // required to be aligned
            points: [TSPoint; self.num_points],
            null_val: [u8; (self.num_points + 7)/ 8], // bit vector, must be last element for alignment purposes
        }
    }

    ron_inout_funcs!(Timevector);
}

impl<'input> Timevector<'input> {
    pub fn num_points(&self) -> usize {
        self.num_points as usize
    }

    // Gets the nth point of a timevector
    // Differs from normal vector get in that it returns a copy rather than a reference (as the point may have to be constructed)
    pub fn get(&self, index: usize) -> Option<TSPoint> {
        if index >= self.num_points() {
            return None;
        }

        Some(self.points.as_slice()[index])
    }

    #[inline]
    pub fn is_sorted(&self) -> bool {
        self.flags & FLAG_IS_SORTED != 0
    }

    #[inline]
    pub fn has_nulls(&self) -> bool {
        self.flags & FLAG_HAS_NULLS != 0
    }

    pub fn is_null_val(&self, index: usize) -> bool {
        assert!(index < self.num_points()); // should we handle this better

        let byte_id = index / 8;
        let byte_idx = index % 8;

        self.null_val.as_slice()[byte_id] & (1 << byte_idx) != 0
    }

    fn clone_owned(&self) -> Timevector<'static> {
        TimevectorData::clone(&*self).into_owned().into()
    }
}

impl<'a> Timevector<'a> {
    pub fn iter(&self) -> Iter<'_> {
        Iter::Slice {
            iter: self.points.iter(),
        }
    }

    pub fn num_vals(&self) -> usize {
        self.num_points()
    }
}

impl<'a> IntoIterator for Timevector<'a> {
    type Item = TSPoint;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        #[allow(clippy::unnecessary_to_owned)] // Pretty sure clippy's wrong about this
        Iter::Slice {
            iter: self.points.to_owned().into_iter(),
        }
    }
}

pub static TIMEVECTOR_OID: once_cell::sync::Lazy<pg_sys::Oid> =
    once_cell::sync::Lazy::new(Timevector::type_oid);

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn unnest(
    series: toolkit_experimental::Timevector<'_>,
) -> impl std::iter::Iterator<Item = (name!(time, crate::raw::TimestampTz), name!(value, f64))> + '_
{
    series
        .into_iter()
        .map(|points| (points.ts.into(), points.val))
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe, strict)]
pub fn timevector_serialize(state: Internal) -> bytea {
    // FIXME: This might duplicate the version and padding bits
    let state: &TimevectorData = unsafe { state.get().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(schema = "toolkit_experimental", strict, immutable, parallel_safe)]
pub fn timevector_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let data: Timevector<'static> = crate::do_deserialize!(bytes, TimevectorData);
    Inner::from(data).internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_trans(
    state: Internal,
    time: Option<crate::raw::TimestampTz>,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { timevector_trans_inner(state.to_inner(), time, value, fcinfo).internal() }
}

pub fn timevector_trans_inner(
    state: Option<Inner<Timevector<'_>>>,
    time: Option<crate::raw::TimestampTz>,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector<'_>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let time: pg_sys::TimestampTz = match time {
                None => return state,
                Some(time) => time.into(),
            };
            let mut state = match state {
                None => Inner::from(build! {
                    Timevector {
                        num_points: 0,
                        flags: FLAG_IS_SORTED,
                        internal_padding: [0; 3],
                        points: vec![].into(),
                        null_val: vec![].into(),
                    }
                }),
                Some(state) => state,
            };
            if let Some(last_point) = state.points.as_slice().last() {
                if state.is_sorted() && last_point.ts > time {
                    state.flags ^= FLAG_IS_SORTED;
                }
            }
            if state.num_points % 8 == 0 {
                state.null_val.as_owned().push(0);
            }
            match value {
                None => {
                    state.flags |= FLAG_HAS_NULLS;
                    state.points.as_owned().push(TSPoint {
                        ts: time,
                        val: f64::NAN,
                    });
                    let byte_idx = state.num_points % 8;  // off by 1, but num_points isn't yet incremented
                    *state.null_val.as_owned().last_mut().unwrap() |= 1 << byte_idx;
                },
                Some(val) => state.points.as_owned().push(TSPoint {
                    ts: time,
                    val,
                }),
            };
            state.num_points += 1;
            Some(state)
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_compound_trans(
    state: Internal,
    series: Option<toolkit_experimental::Timevector>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    inner_compound_trans(unsafe { state.to_inner() }, series, fcinfo).internal()
}

pub fn inner_compound_trans<'b>(
    state: Option<Inner<Timevector<'static>>>,
    series: Option<toolkit_experimental::Timevector<'b>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector<'static>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, series) {
            (None, None) => None,
            (Some(state), None) => Some(state),
            (None, Some(series)) => Some(series.clone_owned().into()),
            (Some(mut state), Some(series)) => {
                if state.is_sorted() {
                    if !series.is_sorted() 
                    || state.points.as_slice().last().unwrap().ts > series.points.as_slice().first().unwrap().ts {
                        state.flags ^= FLAG_IS_SORTED
                    }
                }
                state
                    .points
                    .as_owned()
                    .extend_from_slice(series.points.slice());
                Some(state)
            }
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { inner_combine(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}

pub fn inner_combine<'a, 'b>(
    state1: Option<Inner<Timevector<'a>>>,
    state2: Option<Inner<Timevector<'b>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector<'static>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(state2)) => Some(state2.clone_owned().into()),
            (Some(state1), None) => Some(state1.clone_owned().into()),
            (Some(state1), Some(state2)) => Some(combine(state1.clone(), state2.clone()).into()),
        })
    }
}

pub fn combine(first: Timevector<'_>, second: Timevector<'_>) -> Timevector<'static> {
    if first.num_vals() == 0 {
        return second.clone_owned();
    }
    if second.num_vals() == 0 {
        return first.clone_owned();
    }

    let is_sorted = first.is_sorted()
        && second.is_sorted()
        && first.points.as_slice().last().unwrap().ts
            <= second.points.as_slice().first().unwrap().ts;
    let points: Vec<_> = first.iter().chain(second.iter()).collect();

    let mut flags = (first.flags | FLAG_HAS_NULLS) | (second.flags | FLAG_HAS_NULLS);
    if is_sorted {
        flags |= FLAG_IS_SORTED;
    }

    let null_val = if flags & FLAG_HAS_NULLS == 0 {
        std::vec::from_elem(0 as u8, (points.len() + 7) / 8)
    } else {
        let mut v = first.null_val.as_slice().to_vec();
        v.resize((points.len() + 7) / 8, 0);
        if second.has_nulls() {
            for i in 0..second.num_points {
                if second.is_null_val(i as usize) {
                    let idx = i + first.num_points;
                    let byte_id = idx / 8;
                    let byte_idx = idx % 8;
                    v[byte_id as usize] |= 1 << byte_idx;
                }
            }
        }
        v
    };

    build! {
        Timevector {
            num_points: points.len() as _,
            flags,
            internal_padding: [0; 3],
            points: points.into(),
            null_val: null_val.into(),
        }
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_vector::toolkit_experimental::Timevector<'static>> {
    unsafe { timevector_final_inner(state.to_inner(), fcinfo) }
}

pub fn timevector_final_inner<'a>(
    state: Option<Inner<Timevector<'a>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_vector::toolkit_experimental::Timevector<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };
            Some(state.in_current_context())
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.timevector(ts TIMESTAMPTZ, value DOUBLE PRECISION) (\n\
        sfunc = toolkit_experimental.timevector_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.timevector_final,\n\
        combinefunc = toolkit_experimental.timevector_combine,\n\
        serialfunc = toolkit_experimental.timevector_serialize,\n\
        deserialfunc = toolkit_experimental.timevector_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "timevector_agg",
    requires = [
        timevector_trans,
        timevector_final,
        timevector_combine,
        timevector_serialize,
        timevector_deserialize
    ],
);

extension_sql!(
    "\n\
CREATE AGGREGATE toolkit_experimental.rollup(\n\
    toolkit_experimental.timevector\n\
) (\n\
    sfunc = toolkit_experimental.timevector_compound_trans,\n\
    stype = internal,\n\
    finalfunc = toolkit_experimental.timevector_final,\n\
    combinefunc = toolkit_experimental.timevector_combine,\n\
    serialfunc = toolkit_experimental.timevector_serialize,\n\
    deserialfunc = toolkit_experimental.timevector_deserialize,\n\
    parallel = safe\n\
);\n\
",
    name = "timevector_rollup",
    requires = [
        timevector_compound_trans,
        timevector_final,
        timevector_combine,
        timevector_serialize,
        timevector_deserialize
    ],
);
