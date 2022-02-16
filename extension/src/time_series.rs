#![allow(clippy::identity_op)] // clippy gets confused by pg_type! enums

use pgx::*;

use crate::{
    aggregate_utils::in_aggregate_context, pg_type, build, flatten, palloc::{Internal, InternalAsValue, Inner, ToInternal},
};

use time_series::{
    TSPoint,
};

pub use iter::Iter;

use flat_serialize::*;

mod pipeline;
mod iter;

use crate::raw::bytea;

pub use toolkit_experimental::{Timevector, TimevectorData, SeriesType};

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;
    pg_type! {
        #[derive(Debug)]
        struct Timevector<'input> {
            series: enum SeriesType<'input> {
                type_id: u64,
                Sorted: 1 {
                    num_points: u64,  // required to be aligned
                    points: [TSPoint; self.num_points],
                },
                Normal: 2 {
                    start_ts: i64,
                    step_interval: i64,
                    num_vals: u64,  // required to be aligned
                    values: [f64; self.num_vals],
                },
                // Explicit is assumed to be unordered
                Explicit: 3 {
                    num_points: u64,  // required to be aligned
                    points: [TSPoint; self.num_points],
                },
                GappyNormal: 4 {
                    start_ts: i64,
                    step_interval: i64,
                    num_vals: u64,  // required to be aligned
                    count: u64,
                    values: [f64; self.num_vals],
                    present: [u64; (self.count + 63) / 64]
                },
            },
        }
    }

    impl<'input> InOutFuncs for Timevector<'input> {
        fn output(&self, buffer: &mut StringInfo) {
            use crate::serialization::{EncodedStr::*, str_to_db_encoding};

            // TODO remove extra allocation
            // FIXME print timestamps as times, not integers
            let serializer: Vec<_> = self.iter().collect();

            // Extra & in the to_string call due to ron not supporting ?Sized, shouldn't affect output
            let stringified = ron::to_string(&&*serializer).unwrap();
            match str_to_db_encoding(&stringified) {
                Utf8(s) => buffer.push_str(s),
                Other(s) => buffer.push_bytes(s.to_bytes()),
            }
        }

        fn input(input: &std::ffi::CStr) -> Self
        where
            Self: Sized,
        {
            use crate::serialization::str_from_db_encoding;

            // TODO reduce allocation?
            let input = str_from_db_encoding(input);
            let series: Vec<TSPoint> = ron::from_str(input).unwrap();
            unsafe {
                flatten! {
                    Timevector {
                        series: SeriesType::Explicit {
                            num_points: series.len() as u64,
                            points: series.into(),
                        }
                    }
                }
            }
        }
    }
}

impl<'input> Timevector<'input> {
    pub fn num_points(&self) -> usize {
        match &self.series {
            SeriesType::Sorted{points, ..} =>
                points.len(),
            SeriesType::Explicit{points, ..} =>
                points.len(),
            SeriesType::Normal{values, ..} =>
                values.len(),
            SeriesType::GappyNormal{values, ..} =>
                values.len(),
        }
    }

    // Gets the nth point of a timevector
    // Differs from normal vector get in that it returns a copy rather than a reference (as the point may have to be constructed)
    pub fn get(&self, index: usize) -> Option<TSPoint> {
        if index >= self.num_points() {
            return None;
        }

        match &self.series {
            SeriesType::Sorted{points, ..} =>
                Some(points.as_slice()[index]),
            SeriesType::Explicit{points, ..} =>
                Some(points.as_slice()[index]),
            SeriesType::Normal{start_ts, step_interval, values, ..} =>
                Some(TSPoint{ts: start_ts + index as i64 * step_interval, val: values.as_slice()[index]}),
            SeriesType::GappyNormal{..} =>
                panic!("Can not efficient index into the middle of a normalized timevector with gaps"),
        }
    }

    pub fn is_sorted(&self) -> bool {
        match self.series {
            SeriesType::Sorted{..} =>
                true,
            SeriesType::Explicit{..} =>
                false, // a sorted Explicit is written out as a SortedSeries
            SeriesType::Normal{..} =>
                true,
            SeriesType::GappyNormal{..} =>
                true,
        }
    }

    fn clone_owned(&self) -> Timevector<'static> {
        TimevectorData::clone(&*self).into_owned().into()
    }
}

impl<'a> Timevector<'a> {
    pub fn iter(&self) -> Iter<'_> {
        match &self.series {
            SeriesType::Sorted{points, ..} =>
                Iter::Slice{iter: points.iter()},
            SeriesType::Explicit{points, ..} =>
                Iter::Slice{iter: points.iter()},
            SeriesType::Normal{start_ts, step_interval, values, ..} =>
                Iter::Normal{idx: 0, start: *start_ts, step: *step_interval, vals: values.iter()},
            SeriesType::GappyNormal{count, start_ts, step_interval, present, values, ..} =>
                Iter::GappyNormal{idx: 0, count: *count, start: *start_ts, step: *step_interval, present: present.as_slice(), vals: values.iter()},
        }
    }

    pub fn num_vals(&self) -> usize {
        match &self.series {
            SeriesType::Sorted { num_points, .. } => *num_points as _,
            SeriesType::Normal { num_vals, .. } => *num_vals as _,
            SeriesType::Explicit { num_points, ..} => *num_points as _,
            SeriesType::GappyNormal { num_vals, .. } => *num_vals as _,
        }
    }
}

impl<'a> IntoIterator for Timevector<'a> {
    type Item = TSPoint;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self.0.series {
            SeriesType::Sorted{points, ..} =>
                Iter::Slice{iter: points.into_iter()},
            SeriesType::Explicit{points, ..} =>
                Iter::Slice{iter: points.into_iter()},
            SeriesType::Normal{start_ts, step_interval, values, ..} =>
                Iter::Normal{idx: 0, start: start_ts, step: step_interval, vals: values.into_iter()},
            SeriesType::GappyNormal{count, start_ts, step_interval, present, values, ..} =>
                Iter::GappyNormal{idx: 0, count, start: start_ts, step: step_interval, present: present.slice(), vals: values.into_iter()},
        }
    }
}

pub static TIMEVECTOR_OID: once_cell::sync::Lazy<pg_sys::Oid> = once_cell::sync::Lazy::new(|| {
    Timevector::type_oid()
});

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn unnest(
    series: toolkit_experimental::Timevector<'_>,
) -> impl std::iter::Iterator<Item = (name!(time,crate::raw::TimestampTz),name!(value,f64))> + '_ {
    series.into_iter().map(|points| (points.ts.into(), points.val))
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_serialize(
    state: Internal,
) -> bytea {
    let state: &Timevector = unsafe { state.get().unwrap() };
    let series = &state.series;
    crate::do_serialize!(series)
}

#[pg_extern(schema = "toolkit_experimental",strict, immutable, parallel_safe)]
pub fn timevector_deserialize(
    bytes: bytea,
    _internal: Internal,
) -> Internal {
    let data: Timevector<'static> = crate::do_deserialize!(bytes, TimevectorData);
    Inner::from(data).internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_trans(
    state: Internal,
    time: Option<crate::raw::TimestampTz>,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    unsafe {
        timevector_trans_inner(state.to_inner(), time, value, fcinfo).internal()
    }
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
            let value = match value {
                None => return state,   // Should we support NULL values?
                Some(value) => value,
            };
            let mut state = match state {
                None => Inner::from(build!{
                    Timevector {
                        series: SeriesType::Sorted{
                            num_points: 0,
                            points: vec![].into(),
                        }
                    }
                }),
                Some(state) => state,
            };
            match &mut state.series {
                SeriesType::Explicit { num_points, points } => {
                    points.as_owned().push(TSPoint{ts: time, val:value});
                    *num_points = points.len() as _;
                },
                SeriesType::Sorted { num_points, points } => {
                    points.as_owned().push(TSPoint{ts: time, val:value});
                    *num_points = points.len() as _;
                    if let Some(slice) = points.as_slice().windows(2).last() {
                        if slice[0].ts > slice[1].ts {
                            let points = std::mem::replace(points, vec![].into());
                            *state = build!{
                                Timevector {
                                    series: SeriesType::Explicit{
                                        num_points: points.len() as _,
                                        points,
                                    }
                                }
                            };
                        }
                    }
                },
                _ => unreachable!(),
            }
            Some(state)
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_compound_trans(
    state: Internal,
    series: Option<toolkit_experimental::Timevector>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    inner_compound_trans(unsafe { state.to_inner() }, series, fcinfo).internal()
}

pub fn inner_compound_trans<'b>(
    state: Option<Inner<Timevector<'static>>>,
    series: Option<toolkit_experimental::Timevector<'b>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector<'static>>> {
    use SeriesType::{Sorted, Explicit};
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, series) {
                (None, None) => None,
                (Some(state), None) => Some(state),
                (None, Some(series)) => Some(series.clone_owned().into()),
                (Some(mut state), Some(series)) =>
                    match &mut state.series {
                        Explicit { num_points, points } => {
                            points.as_owned().extend(series.iter());
                            *num_points = points.len() as _;
                            Some(state)
                        },
                        Sorted { num_points, points } => {
                            if let Sorted { points: other_points, ..} = &series.series {
                                let is_ordered = || {
                                    let second = other_points.slice().get(0)?;
                                    let first = points.slice().last()?;
                                    Some(second.ts >= first.ts)
                                };
                                if is_ordered().unwrap_or(true) {
                                    points.as_owned().extend_from_slice(other_points.slice());
                                    *num_points = points.len() as _;
                                    return Some(state)
                                }
                            }
                            points.as_owned().extend(series.iter());
                            let points = std::mem::replace(points, vec![].into());
                            *state = build!{
                                Timevector {
                                    series: SeriesType::Explicit{
                                        num_points: points.len() as _,
                                        points,
                                    }
                                }
                            };
                            Some(state)
                        },
                        _ => unreachable!(),

                    }
            }
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_combine (
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    unsafe {
        inner_combine(state1.to_inner(), state2.to_inner(), fcinfo).internal()
    }
}

pub fn inner_combine<'a, 'b> (
    state1: Option<Inner<Timevector<'a>>>,
    state2: Option<Inner<Timevector<'b>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector<'static>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone_owned().into()),
                (Some(state1), None) => Some(state1.clone_owned().into()),
                (Some(state1), Some(state2)) =>
                    Some(combine(state1.clone(), state2.clone()).into())
            }
        })
    }
}

pub fn combine(first: Timevector<'_>, second: Timevector<'_>) -> Timevector<'static> {
    use SeriesType::*;
    if first.num_vals() == 0 {
        return second.clone_owned();
    }
    if second.num_vals() == 0 {
        return first.clone_owned();
    }

    // If two explicit series are sorted and disjoint, return a sorted explicit series
    if let (
        Sorted{ num_points: _, points: first_points },
        Sorted{ num_points: _, points: second_points }) = (&first.series, &second.series) {
        if first_points.slice().last().unwrap().ts <= second_points.slice()[0].ts {
            let mut new_points = first_points.clone().into_owned();
            new_points.as_owned().extend(second_points.iter());
            return build! { Timevector {
                series: Sorted {
                    num_points: new_points.len() as _,
                    points: new_points,
                }
            }}
        }

        if second_points.slice().last().unwrap().ts < first_points.slice()[0].ts {
            let mut new_points = second_points.clone().into_owned();
            new_points.as_owned().extend(first_points.iter());
            return build! { Timevector {
                series: Sorted {
                    num_points: new_points.len() as _,
                    points: new_points,
                }
            }}
        }
    };

    // If the series are adjacent normal series, combine them into a larger normal series
    let mut ordered = false;
    if let (
        Normal {
            start_ts: start_ts_1,
            step_interval: step_interval_1,
            num_vals: _,
            values: values_1
        },
        Normal {
            start_ts: start_ts_2,
            step_interval: step_interval_2,
            num_vals: _,
            values: values_2
        }
    ) = (&first.series, &second.series) {
        if step_interval_1 == step_interval_2 {
            if *start_ts_2 == start_ts_1 + values_1.len() as i64 * step_interval_1 {
                let mut new_values = values_1.clone().into_owned();
                new_values.as_owned().extend(values_2.iter());
                return build!{ Timevector {
                    series: Normal {
                        start_ts: *start_ts_1,
                        step_interval: *step_interval_1,
                        num_vals: new_values.len() as _,
                        values: new_values,
                    }
                }};
            }

            if *start_ts_1 == start_ts_2 + values_2.len() as i64 * step_interval_2 {
                let mut new_values = values_2.clone().into_owned();
                new_values.as_owned().extend(values_1.iter());
                return build!{ Timevector {
                    series: Normal {
                        start_ts: *start_ts_2,
                        step_interval: *step_interval_2,
                        num_vals: new_values.len() as _,
                        values: new_values,
                    }
                }};
            }
        }

        ordered = start_ts_1 + (values_1.len() - 1) as i64 * step_interval_1 < *start_ts_2
    };

    // In all other cases, just return a new explicit series containing all the points from both series
    let points: Vec<_> = first.iter().chain(second.iter()).collect();
    if ordered {
        build!{ Timevector {
            series: Sorted {
                num_points: points.len() as _,
                points: points.into(),
            }
        }}
    } else {
        build!{ Timevector {
            series: Explicit {
                num_points: points.len() as _,
                points: points.into(),
            }
        }}
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timevector_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_series::toolkit_experimental::Timevector<'static>> {
    unsafe {
        timevector_final_inner(state.to_inner(), fcinfo)
    }
}

pub fn timevector_final_inner<'a>(
    state: Option<Inner<Timevector<'a>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_series::toolkit_experimental::Timevector<'static>> {
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

extension_sql!("\n\
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
name="timevector_agg",
requires= [timevector_trans, timevector_final, timevector_combine, timevector_serialize, timevector_deserialize],
);

extension_sql!("\n\
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
name="timevector_rollup",
requires= [timevector_compound_trans, timevector_final, timevector_combine, timevector_serialize, timevector_deserialize],
);
