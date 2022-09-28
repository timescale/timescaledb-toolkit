use serde::{Deserialize, Serialize};

use pgx::*;

use crate::{
    accessors::{
        AccessorCorr, AccessorCounterZeroTime, AccessorDelta, AccessorExtrapolatedDelta,
        AccessorExtrapolatedRate, AccessorFirstTime, AccessorFirstVal, AccessorIdeltaLeft,
        AccessorIdeltaRight, AccessorIntercept, AccessorIrateLeft, AccessorIrateRight,
        AccessorLastTime, AccessorLastVal, AccessorNumChanges, AccessorNumElements,
        AccessorNumResets, AccessorRate, AccessorSlope, AccessorTimeDelta, AccessorWithBounds,
    },
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    range::*,
    ron_inout_funcs,
};

use tspoint::TSPoint;

use counter_agg::{range::I64Range, CounterSummaryBuilder, MetricSummary};
use stats_agg::stats2d::StatsSummary2D;

use self::Method::*;

use crate::raw::tstzrange;

use crate::raw::bytea;

// TODO wrap FlatSummary a la GaugeSummary - requires serialization version bump
pg_type! {
    #[derive(Debug, PartialEq)]
    struct CounterSummary {
        stats: StatsSummary2D,
        first: TSPoint,
        second: TSPoint,
        penultimate:TSPoint,
        last: TSPoint,
        reset_sum: f64,
        num_resets: u64,
        num_changes: u64,
        #[flat_serialize::flatten]
        bounds: I64RangeWrapper,
    }
}

ron_inout_funcs!(CounterSummary);

impl<'input> CounterSummary<'input> {
    pub fn to_internal_counter_summary(&self) -> MetricSummary {
        MetricSummary {
            first: self.first,
            second: self.second,
            penultimate: self.penultimate,
            last: self.last,
            reset_sum: self.reset_sum,
            num_resets: self.num_resets,
            num_changes: self.num_changes,
            stats: self.stats,
            bounds: self.bounds.to_i64range(),
        }
    }
    pub fn from_internal_counter_summary(st: MetricSummary) -> Self {
        unsafe {
            flatten!(CounterSummary {
                stats: st.stats,
                first: st.first,
                second: st.second,
                penultimate: st.penultimate,
                last: st.last,
                reset_sum: st.reset_sum,
                num_resets: st.num_resets,
                num_changes: st.num_changes,
                bounds: I64RangeWrapper::from_i64range(st.bounds)
            })
        }
    }
    // fn set_bounds(&mut self, bounds: Option<I64Range>){
    //     self.bounds = &I64RangeWrapper::from_i64range(bounds);
    // }
    fn interpolate(
        &self,
        interval_start: i64,
        interval_len: i64,
        prev: Option<CounterSummary>,
        next: Option<CounterSummary>,
    ) -> CounterSummary<'static> {
        let prev = if self.first.ts > interval_start {
            prev.map(|summary| {
                let first = if summary.last.val > self.first.val {
                    TSPoint {
                        ts: summary.last.ts,
                        val: 0.,
                    }
                } else {
                    summary.last
                };
                time_weighted_average::TimeWeightMethod::Linear
                    .interpolate(first, Some(self.first), interval_start)
                    .expect("unable to interpolate lower bound")
            })
        } else {
            None
        };

        let next = next.map(|summary| {
            let last = if self.last.val > summary.first.val {
                TSPoint {
                    ts: self.last.ts,
                    val: 0.,
                }
            } else {
                self.last
            };
            time_weighted_average::TimeWeightMethod::Linear
                .interpolate(last, Some(summary.first), interval_start + interval_len)
                .expect("unable to interpolate upper bound")
        });

        let builder = prev.map(|pt| CounterSummaryBuilder::new(&pt, None));
        let mut builder = builder.map_or_else(
            || {
                let mut summary = self.clone();
                summary.bounds = I64RangeWrapper::from_i64range(None);
                summary.to_internal_counter_summary().into()
            },
            |mut builder| {
                builder
                    .combine(&self.to_internal_counter_summary())
                    .expect("unable to add data to interpolation");
                builder
            },
        );

        if let Some(next) = next {
            builder
                .add_point(&next)
                .expect("unable to add final interpolated point");
        }

        CounterSummary::from_internal_counter_summary(builder.build())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CounterSummaryTransState {
    #[serde(skip)]
    point_buffer: Vec<TSPoint>,
    #[serde(skip)]
    bounds: Option<I64Range>, // stores bounds until we combine points, after which, the bounds are stored in each summary
    // We have a summary buffer here in order to deal with the fact that when the cmobine function gets called it
    // must first build up a buffer of InternalMetricSummaries, then sort them, then call the combine function in
    // the correct order.
    summary_buffer: Vec<MetricSummary>,
}

impl CounterSummaryTransState {
    fn new() -> Self {
        Self {
            point_buffer: vec![],
            bounds: None,
            summary_buffer: vec![],
        }
    }

    fn push_point(&mut self, value: TSPoint) {
        self.point_buffer.push(value);
    }

    // fn set_bounds(&mut self, bounds: Option<I64Range>){
    //     self.bounds = bounds;
    // }

    fn combine_points(&mut self) {
        if self.point_buffer.is_empty() {
            return;
        }
        self.point_buffer.sort_unstable_by_key(|p| p.ts);
        let mut iter = self.point_buffer.iter();
        let mut summary = CounterSummaryBuilder::new(iter.next().unwrap(), self.bounds);
        for p in iter {
            summary
                .add_point(p)
                .unwrap_or_else(|e| pgx::error!("{}", e));
        }
        self.point_buffer.clear();
        // TODO build method should check validity
        // check bounds only after we've combined all the points, so we aren't doing it all the time.
        if !summary.bounds_valid() {
            panic!("counter bounds invalid")
        }
        self.summary_buffer.push(summary.build());
    }

    fn push_summary(&mut self, other: &CounterSummaryTransState) {
        let sum_iter = other.summary_buffer.iter();
        for sum in sum_iter {
            self.summary_buffer.push(sum.clone());
        }
    }

    fn combine_summaries(&mut self) {
        self.combine_points();

        if self.summary_buffer.len() <= 1 {
            return;
        }
        // TODO move much of this method to crate?
        self.summary_buffer.sort_unstable_by_key(|s| s.first.ts);
        let mut sum_iter = self.summary_buffer.iter();
        let mut new_summary = CounterSummaryBuilder::from(sum_iter.next().unwrap().clone());
        for sum in sum_iter {
            new_summary
                .combine(sum)
                .unwrap_or_else(|e| pgx::error!("{}", e));
        }
        self.summary_buffer = vec![new_summary.build()];
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn counter_summary_trans_serialize(state: Internal) -> bytea {
    let state: &mut CounterSummaryTransState = unsafe { state.get_mut().unwrap() };
    state.combine_summaries();
    crate::do_serialize!(state)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn counter_summary_trans_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    counter_summary_trans_deserialize_inner(bytes).internal()
}
pub fn counter_summary_trans_deserialize_inner(bytes: bytea) -> Inner<CounterSummaryTransState> {
    let c: CounterSummaryTransState = crate::do_deserialize!(bytes, CounterSummaryTransState);
    c.into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn counter_agg_trans(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    bounds: Option<tstzrange>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    counter_agg_trans_inner(unsafe { state.to_inner() }, ts, val, bounds, fcinfo).internal()
}
pub fn counter_agg_trans_inner(
    state: Option<Inner<CounterSummaryTransState>>,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    bounds: Option<tstzrange>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<CounterSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint { ts: ts.into(), val },
            };
            match state {
                None => {
                    let mut s = CounterSummaryTransState::new();
                    if let Some(r) = bounds {
                        s.bounds = get_range(r.0.cast_mut_ptr());
                    }
                    s.push_point(p);
                    Some(s.into())
                }
                Some(mut s) => {
                    s.push_point(p);
                    Some(s)
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn counter_agg_trans_no_bounds(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    counter_agg_trans_inner(unsafe { state.to_inner() }, ts, val, None, fcinfo).internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn counter_agg_summary_trans(
    state: Internal,
    value: Option<CounterSummary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    counter_agg_summary_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn counter_agg_summary_trans_inner(
    state: Option<Inner<CounterSummaryTransState>>,
    value: Option<CounterSummary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<CounterSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (state, None) => state,
            (None, Some(value)) => {
                let mut state = CounterSummaryTransState::new();
                state
                    .summary_buffer
                    .push(value.to_internal_counter_summary());
                Some(state.into())
            }
            (Some(mut state), Some(value)) => {
                state
                    .summary_buffer
                    .push(value.to_internal_counter_summary());
                Some(state)
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn counter_agg_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { counter_agg_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}
pub fn counter_agg_combine_inner(
    state1: Option<Inner<CounterSummaryTransState>>,
    state2: Option<Inner<CounterSummaryTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<CounterSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => {
                    let mut s = state2.clone();
                    s.combine_points();
                    Some(s.into())
                }
                (Some(state1), None) => {
                    let mut s = state1.clone();
                    s.combine_points();
                    Some(s.into())
                } //should I make these return themselves?
                (Some(state1), Some(state2)) => {
                    let mut s1 = state1.clone(); // is there a way to avoid if it doesn't need it?
                    s1.combine_points();
                    let mut s2 = state2.clone();
                    s2.combine_points();
                    s2.push_summary(&s1);
                    Some(s2.into())
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn counter_agg_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<CounterSummary<'static>> {
    counter_agg_final_inner(unsafe { state.to_inner() }, fcinfo)
}
fn counter_agg_final_inner(
    state: Option<Inner<CounterSummaryTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<CounterSummary<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state.clone(),
            };
            state.combine_summaries();
            debug_assert!(state.summary_buffer.len() <= 1);
            match state.summary_buffer.pop() {
                None => None,
                Some(st) => {
                    // there are some edge cases that this should prevent, but I'm not sure it's necessary, we do check the bounds in the functions that use them.
                    if !st.bounds_valid() {
                        panic!("counter bounds invalid")
                    }
                    Some(CounterSummary::from_internal_counter_summary(st))
                }
            }
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE counter_agg( ts timestamptz, value DOUBLE PRECISION, bounds tstzrange )\n\
    (\n\
        sfunc = counter_agg_trans,\n\
        stype = internal,\n\
        finalfunc = counter_agg_final,\n\
        combinefunc = counter_agg_combine,\n\
        serialfunc = counter_summary_trans_serialize,\n\
        deserialfunc = counter_summary_trans_deserialize,\n\
        parallel = restricted\n\
    );\n",
    name = "counter_agg",
    requires = [
        counter_agg_trans,
        counter_agg_final,
        counter_agg_combine,
        counter_summary_trans_serialize,
        counter_summary_trans_deserialize
    ],
);

// allow calling counter agg without bounds provided.
extension_sql!(
    "\n\
    CREATE AGGREGATE counter_agg( ts timestamptz, value DOUBLE PRECISION )\n\
    (\n\
        sfunc = counter_agg_trans_no_bounds,\n\
        stype = internal,\n\
        finalfunc = counter_agg_final,\n\
        combinefunc = counter_agg_combine,\n\
        serialfunc = counter_summary_trans_serialize,\n\
        deserialfunc = counter_summary_trans_deserialize,\n\
        parallel = restricted\n\
    );\n\
",
    name = "counter_agg2",
    requires = [
        counter_agg_trans_no_bounds,
        counter_agg_final,
        counter_agg_combine,
        counter_summary_trans_serialize,
        counter_summary_trans_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(cs CounterSummary)\n\
    (\n\
        sfunc = counter_agg_summary_trans,\n\
        stype = internal,\n\
        finalfunc = counter_agg_final,\n\
        combinefunc = counter_agg_combine,\n\
        serialfunc = counter_summary_trans_serialize,\n\
        deserialfunc = counter_summary_trans_deserialize,\n\
        parallel = restricted\n\
    );\n\
",
    name = "counter_rollup",
    requires = [
        counter_agg_summary_trans,
        counter_agg_final,
        counter_agg_combine,
        counter_summary_trans_serialize,
        counter_summary_trans_deserialize
    ],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_delta(sketch: CounterSummary, _accessor: AccessorDelta) -> f64 {
    counter_agg_delta(sketch)
}

#[pg_extern(name = "delta", strict, immutable, parallel_safe)]
fn counter_agg_delta(summary: CounterSummary) -> f64 {
    summary.to_internal_counter_summary().delta()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_rate(sketch: CounterSummary, _accessor: AccessorRate) -> Option<f64> {
    counter_agg_rate(sketch)
}

#[pg_extern(name = "rate", strict, immutable, parallel_safe)]
fn counter_agg_rate(summary: CounterSummary) -> Option<f64> {
    summary.to_internal_counter_summary().rate()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_time_delta(sketch: CounterSummary, _accessor: AccessorTimeDelta) -> f64 {
    counter_agg_time_delta(sketch)
}

#[pg_extern(name = "time_delta", strict, immutable, parallel_safe)]
fn counter_agg_time_delta(summary: CounterSummary) -> f64 {
    summary.to_internal_counter_summary().time_delta()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_irate_left(
    sketch: CounterSummary,
    _accessor: AccessorIrateLeft,
) -> Option<f64> {
    counter_agg_irate_left(sketch)
}

#[pg_extern(name = "irate_left", strict, immutable, parallel_safe)]
fn counter_agg_irate_left(summary: CounterSummary) -> Option<f64> {
    summary.to_internal_counter_summary().irate_left()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_irate_right(
    sketch: CounterSummary,
    _accessor: AccessorIrateRight,
) -> Option<f64> {
    counter_agg_irate_right(sketch)
}

#[pg_extern(name = "irate_right", strict, immutable, parallel_safe)]
fn counter_agg_irate_right(summary: CounterSummary) -> Option<f64> {
    summary.to_internal_counter_summary().irate_right()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_idelta_left(sketch: CounterSummary, _accessor: AccessorIdeltaLeft) -> f64 {
    counter_agg_idelta_left(sketch)
}

#[pg_extern(name = "idelta_left", strict, immutable, parallel_safe)]
fn counter_agg_idelta_left(summary: CounterSummary) -> f64 {
    summary.to_internal_counter_summary().idelta_left()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_idelta_right(
    sketch: CounterSummary,
    _accessor: AccessorIdeltaRight,
) -> f64 {
    counter_agg_idelta_right(sketch)
}

#[pg_extern(name = "idelta_right", strict, immutable, parallel_safe)]
fn counter_agg_idelta_right(summary: CounterSummary) -> f64 {
    summary.to_internal_counter_summary().idelta_right()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_with_bounds(
    sketch: CounterSummary,
    accessor: AccessorWithBounds,
) -> CounterSummary<'static> {
    let mut builder = CounterSummaryBuilder::from(sketch.to_internal_counter_summary());
    builder.set_bounds(accessor.bounds());
    CounterSummary::from_internal_counter_summary(builder.build())
}

#[pg_extern(name = "with_bounds", strict, immutable, parallel_safe)]
fn counter_agg_with_bounds(summary: CounterSummary, bounds: tstzrange) -> CounterSummary {
    // TODO dedup with previous by using apply_bounds
    unsafe {
        let ptr = bounds.0.cast_mut_ptr();
        let mut builder = CounterSummaryBuilder::from(summary.to_internal_counter_summary());
        builder.set_bounds(get_range(ptr));
        CounterSummary::from_internal_counter_summary(builder.build())
    }
}

// TODO MetricSummary::with_bounds ?
//     fn with_bounds(mut self, bounds: Option<I64Range>) -> Self {
//         self.bounds = bounds;
//         self
//     }
// fn apply_bounds(summary: MetricSummary, bounds: Option<I64Range>) -> MetricSummary {
//     let mut builder = CounterSummaryBuilder::from(summary.to_internal_counter_summary());
//     builder.set_bounds(bounds);
//     CounterSummary::from_internal_counter_summary(builder.build())
// }

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_extrapolated_delta(
    sketch: CounterSummary,
    accessor: AccessorExtrapolatedDelta,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    counter_agg_extrapolated_delta(sketch, &*method)
}

#[pg_extern(name = "extrapolated_delta", strict, immutable, parallel_safe)]
fn counter_agg_extrapolated_delta(summary: CounterSummary, method: &str) -> Option<f64> {
    match method_kind(method) {
        Prometheus => summary
            .to_internal_counter_summary()
            .prometheus_delta()
            .unwrap(),
    }
}

#[pg_extern(
    name = "interpolated_delta",
    immutable,
    parallel_safe,
    schema = "toolkit_experimental"
)]
fn counter_agg_interpolated_delta(
    summary: CounterSummary,
    start: crate::raw::TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<CounterSummary>,
    next: Option<CounterSummary>,
) -> f64 {
    let interval = crate::datum_utils::interval_to_ms(&start, &interval);
    summary
        .interpolate(start.into(), interval, prev, next)
        .to_internal_counter_summary()
        .delta()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_extrapolated_rate(
    sketch: CounterSummary,
    accessor: AccessorExtrapolatedRate,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    counter_agg_extrapolated_rate(sketch, &*method)
}

#[pg_extern(name = "extrapolated_rate", strict, immutable, parallel_safe)]
fn counter_agg_extrapolated_rate(summary: CounterSummary, method: &str) -> Option<f64> {
    match method_kind(method) {
        Prometheus => summary
            .to_internal_counter_summary()
            .prometheus_rate()
            .unwrap(),
    }
}

#[pg_extern(
    name = "interpolated_rate",
    immutable,
    parallel_safe,
    schema = "toolkit_experimental"
)]
fn counter_agg_interpolated_rate(
    summary: CounterSummary,
    start: crate::raw::TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<CounterSummary>,
    next: Option<CounterSummary>,
) -> Option<f64> {
    let interval = crate::datum_utils::interval_to_ms(&start, &interval);
    summary
        .interpolate(start.into(), interval, prev, next)
        .to_internal_counter_summary()
        .rate()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_num_elements(
    sketch: CounterSummary,
    _accessor: AccessorNumElements,
) -> i64 {
    counter_agg_num_elements(sketch)
}

#[pg_extern(name = "num_elements", strict, immutable, parallel_safe)]
fn counter_agg_num_elements(summary: CounterSummary) -> i64 {
    summary.to_internal_counter_summary().stats.n as i64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_num_changes(sketch: CounterSummary, _accessor: AccessorNumChanges) -> i64 {
    counter_agg_num_changes(sketch)
}

#[pg_extern(name = "num_changes", strict, immutable, parallel_safe)]
fn counter_agg_num_changes(summary: CounterSummary) -> i64 {
    summary.to_internal_counter_summary().num_changes as i64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_num_resets(sketch: CounterSummary, _accessor: AccessorNumResets) -> i64 {
    counter_agg_num_resets(sketch)
}

#[pg_extern(name = "num_resets", strict, immutable, parallel_safe)]
fn counter_agg_num_resets(summary: CounterSummary) -> i64 {
    summary.to_internal_counter_summary().num_resets as i64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_slope(sketch: CounterSummary, _accessor: AccessorSlope) -> Option<f64> {
    counter_agg_slope(sketch)
}

#[pg_extern(name = "slope", strict, immutable, parallel_safe)]
fn counter_agg_slope(summary: CounterSummary) -> Option<f64> {
    summary.to_internal_counter_summary().stats.slope()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_intercept(
    sketch: CounterSummary,
    _accessor: AccessorIntercept,
) -> Option<f64> {
    counter_agg_intercept(sketch)
}

#[pg_extern(name = "intercept", strict, immutable, parallel_safe)]
fn counter_agg_intercept(summary: CounterSummary) -> Option<f64> {
    summary.to_internal_counter_summary().stats.intercept()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_corr(sketch: CounterSummary, _accessor: AccessorCorr) -> Option<f64> {
    counter_agg_corr(sketch)
}

#[pg_extern(name = "corr", strict, immutable, parallel_safe)]
fn counter_agg_corr(summary: CounterSummary) -> Option<f64> {
    summary.to_internal_counter_summary().stats.corr()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_zero_time(
    sketch: CounterSummary,
    _accessor: AccessorCounterZeroTime,
) -> Option<crate::raw::TimestampTz> {
    counter_agg_counter_zero_time(sketch)
}

#[pg_extern(name = "counter_zero_time", strict, immutable, parallel_safe)]
fn counter_agg_counter_zero_time(summary: CounterSummary) -> Option<crate::raw::TimestampTz> {
    Some(((summary.to_internal_counter_summary().stats.x_intercept()? * 1_000_000.0) as i64).into())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_first_val(sketch: CounterSummary, _accessor: AccessorFirstVal) -> f64 {
    counter_agg_first_val(sketch)
}

#[pg_extern(name = "first_val", strict, immutable, parallel_safe)]
fn counter_agg_first_val(summary: CounterSummary) -> f64 {
    summary.to_internal_counter_summary().first.val
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_last_val(sketch: CounterSummary, _accessor: AccessorLastVal) -> f64 {
    counter_agg_last_val(sketch)
}

#[pg_extern(name = "last_val", strict, immutable, parallel_safe)]
fn counter_agg_last_val(summary: CounterSummary) -> f64 {
    summary.to_internal_counter_summary().last.val
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_first_time(
    sketch: CounterSummary,
    _accessor: AccessorFirstTime,
) -> crate::raw::TimestampTz {
    counter_agg_first_time(sketch)
}

#[pg_extern(name = "first_time", strict, immutable, parallel_safe)]
fn counter_agg_first_time(summary: CounterSummary) -> crate::raw::TimestampTz {
    summary.to_internal_counter_summary().first.ts.into()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_counter_agg_last_time(
    sketch: CounterSummary,
    _accessor: AccessorLastTime,
) -> crate::raw::TimestampTz {
    counter_agg_last_time(sketch)
}

#[pg_extern(name = "last_time", strict, immutable, parallel_safe)]
fn counter_agg_last_time(summary: CounterSummary) -> crate::raw::TimestampTz {
    summary.to_internal_counter_summary().last.ts.into()
}

#[derive(Clone, Copy)]
pub enum Method {
    Prometheus,
}

#[track_caller]
pub fn method_kind(method: &str) -> Method {
    match as_method(method) {
        Some(method) => method,
        None => pgx::error!("unknown analysis method. Valid methods are 'prometheus'"),
    }
}

pub fn as_method(method: &str) -> Option<Method> {
    match method.trim().to_lowercase().as_str() {
        "prometheus" => Some(Method::Prometheus),
        _ => None,
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {

    use super::testing::*;
    use super::*;
    use approx::assert_relative_eq;

    macro_rules! select_one {
        ($client:expr, $stmt:expr, $type:ty) => {
            $client
                .select($stmt, None, None)
                .first()
                .get_one::<$type>()
                .unwrap()
        };
    }

    macro_rules! select_and_check_one {
        ($client:expr, $stmt:expr, $type:ty) => {{
            let (a, b) = $client
                .select($stmt, None, None)
                .first()
                .get_two::<$type, $type>();
            assert_eq!(a, b);
            a.unwrap()
        }};
    }

    //do proper numerical comparisons on the values where that matters, use exact where it should be exact.
    // copied from counter_agg crate
    #[track_caller]
    fn assert_close_enough(p1: &MetricSummary, p2: &MetricSummary) {
        assert_eq!(p1.first, p2.first, "first");
        assert_eq!(p1.second, p2.second, "second");
        assert_eq!(p1.penultimate, p2.penultimate, "penultimate");
        assert_eq!(p1.last, p2.last, "last");
        assert_eq!(p1.num_changes, p2.num_changes, "num_changes");
        assert_eq!(p1.num_resets, p2.num_resets, "num_resets");
        assert_eq!(p1.stats.n, p2.stats.n, "n");
        assert_relative_eq!(p1.stats.sx, p2.stats.sx);
        assert_relative_eq!(p1.stats.sx2, p2.stats.sx2);
        assert_relative_eq!(p1.stats.sy, p2.stats.sy);
        assert_relative_eq!(p1.stats.sy2, p2.stats.sy2);
        assert_relative_eq!(p1.stats.sxy, p2.stats.sxy);
    }

    #[pg_test]
    fn test_counter_aggregate() {
        Spi::execute(|client| {
            // set search_path after defining our table so we don't pollute the wrong schema
            let stmt = "SELECT format('toolkit_experimental, %s',current_setting('search_path'))";
            let search_path = select_one!(client, stmt, String);
            client.select(
                &format!("SET LOCAL search_path TO {}", search_path),
                None,
                None,
            );
            make_test_table(&client, "test");

            // NULL bounds are equivalent to none provided
            let stmt = "SELECT counter_agg(ts, val) FROM test";
            let a = select_one!(client, stmt, CounterSummary);
            let stmt = "SELECT counter_agg(ts, val, NULL::tstzrange) FROM test";
            let b = select_one!(client, stmt, CounterSummary);
            assert_close_enough(
                &a.to_internal_counter_summary(),
                &b.to_internal_counter_summary(),
            );

            let stmt = "SELECT \
                delta(counter_agg(ts, val)), \
                counter_agg(ts, val)->delta() \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), 10.0);

            let stmt = "SELECT \
                time_delta(counter_agg(ts, val)), \
                counter_agg(ts, val)->time_delta() \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), 60.0);

            // have to add 1 ms to right bounds to get full range and simple values because prometheus subtracts a ms
            let stmt = "SELECT \
                extrapolated_delta(counter_agg(ts, val, '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00.001+00)'), 'prometheus'), \
                counter_agg(ts, val, '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00.001+00)') -> extrapolated_delta('prometheus')  \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), 20.0);
            // doesn't matter if we set the bounds before or after
            let stmt = "SELECT \
                extrapolated_delta(with_bounds(counter_agg(ts, val), '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00.001+00)'), 'prometheus'), \
                counter_agg(ts, val)->with_bounds('[2020-01-01 00:00:00+00, 2020-01-01 00:02:00.001+00)')-> extrapolated_delta('prometheus') \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), 20.0);

            let stmt = "SELECT \
                extrapolated_rate(counter_agg(ts, val, '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00.001+00)'), 'prometheus'), \
                counter_agg(ts, val, '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00.001+00)')->extrapolated_rate('prometheus') \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), 20.0 / 120.0);

            let stmt = "INSERT INTO test VALUES('2020-01-01 00:02:00+00', 10.0), ('2020-01-01 00:03:00+00', 20.0), ('2020-01-01 00:04:00+00', 10.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT \
                slope(counter_agg(ts, val)), \
                counter_agg(ts, val)->slope() \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), 10.0 / 60.0);

            let stmt = "SELECT \
                intercept(counter_agg(ts, val)), \
                counter_agg(ts, val)->intercept() \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), -105191990.0);

            let stmt = "SELECT \
                corr(counter_agg(ts, val)), \
                counter_agg(ts, val)->corr() \
            FROM test";
            assert_relative_eq!(select_and_check_one!(client, stmt, f64), 1.0);

            let stmt = "SELECT \
                counter_zero_time(counter_agg(ts, val)), \
                counter_agg(ts, val)->counter_zero_time() \
            FROM test";
            let zp = select_and_check_one!(client, stmt, i64);
            let real_zp = select_one!(client, "SELECT '2019-12-31 23:59:00+00'::timestamptz", i64);
            assert_eq!(zp, real_zp);

            let stmt = "INSERT INTO test VALUES('2020-01-01 00:08:00+00', 30.0), ('2020-01-01 00:10:00+00', 30.0), ('2020-01-01 00:10:30+00', 10.0), ('2020-01-01 00:20:00+00', 40.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT \
                num_elements(counter_agg(ts, val)), \
                counter_agg(ts, val)->num_elements() \
            FROM test";
            assert_eq!(select_and_check_one!(client, stmt, i64), 9);

            let stmt = "SELECT \
                num_resets(counter_agg(ts, val)), \
                counter_agg(ts, val)->num_resets() \
            FROM test";
            assert_eq!(select_and_check_one!(client, stmt, i64), 3);

            let stmt = "SELECT \
                num_changes(counter_agg(ts, val)), \
                counter_agg(ts, val)->num_changes() \
            FROM test";
            assert_eq!(select_and_check_one!(client, stmt, i64), 7);

            //combine function works as expected
            let stmt = "SELECT counter_agg(ts, val) FROM test";
            let a = select_one!(client, stmt, CounterSummary);
            let stmt = "WITH t as (SELECT date_trunc('minute', ts), counter_agg(ts, val) as agg FROM test group by 1 ) SELECT rollup(agg) FROM t";
            let b = select_one!(client, stmt, CounterSummary);
            assert_close_enough(
                &a.to_internal_counter_summary(),
                &b.to_internal_counter_summary(),
            );
        });
    }

    #[pg_test]
    fn test_counter_io() {
        Spi::execute(|client| {
            client.select(
                "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
                None,
                None,
            );
            client.select("SET TIME ZONE 'UTC'", None, None);
            let stmt = "INSERT INTO test VALUES\
                ('2020-01-01 00:00:00+00', 10.0),\
                ('2020-01-01 00:01:00+00', 20.0),\
                ('2020-01-01 00:02:00+00', 30.0),\
                ('2020-01-01 00:03:00+00', 20.0),\
                ('2020-01-01 00:04:00+00', 10.0),\
                ('2020-01-01 00:05:00+00', 20.0),\
                ('2020-01-01 00:06:00+00', 10.0),\
                ('2020-01-01 00:07:00+00', 30.0),\
                ('2020-01-01 00:08:00+00', 10.0)";
            client.select(stmt, None, None);

            let expected = "(\
                version:1,\
                stats:(\
                    n:9,\
                    sx:5680370160,\
                    sx2:216000,\
                    sx3:0,\
                    sx4:9175680000,\
                    sy:530,\
                    sy2:9688.888888888889,\
                    sy3:13308.641975308623,\
                    sy4:18597366.255144034,\
                    sxy:45600\
                ),\
                first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                second:(ts:\"2020-01-01 00:01:00+00\",val:20),\
                penultimate:(ts:\"2020-01-01 00:07:00+00\",val:30),\
                last:(ts:\"2020-01-01 00:08:00+00\",val:10),\
                reset_sum:100,\
                num_resets:4,\
                num_changes:8,\
                bounds:(\
                    is_present:0,\
                    has_left:0,\
                    has_right:0,\
                    padding:(0,0,0,0,0),\
                    left:None,\
                    right:None\
                )\
            )";

            let stmt = "SELECT counter_agg(ts, val)::TEXT FROM test";
            let test = select_one!(client, stmt, String);
            assert_eq!(test, expected);

            let stmt = format!("SELECT '{}'::CounterSummary::TEXT", expected);
            let round_trip = select_one!(client, &stmt, String);
            assert_eq!(expected, round_trip);

            let stmt = "SELECT delta(counter_agg(ts, val)) FROM test";
            let delta = select_one!(client, stmt, f64);
            assert!((delta - 100.).abs() < f64::EPSILON);
            let stmt = format!("SELECT delta('{}')", expected);
            let delta_test = select_one!(client, &stmt, f64);
            assert!((delta - delta_test).abs() < f64::EPSILON);

            let stmt = "SELECT num_resets(counter_agg(ts, val)) FROM test";
            let resets = select_one!(client, stmt, i64);
            assert_eq!(resets, 4);
            let stmt = format!("SELECT num_resets('{}')", expected);
            let resets_test = select_one!(client, &stmt, i64);
            assert_eq!(resets, resets_test);
        });
    }

    #[pg_test]
    fn test_counter_byte_io() {
        unsafe {
            use std::ptr;
            const BASE: i64 = 631152000000000;
            const MIN: i64 = 60000000;
            let state =
                counter_agg_trans_inner(None, Some(BASE.into()), Some(10.0), None, ptr::null_mut());
            let state = counter_agg_trans_inner(
                state,
                Some((BASE + MIN).into()),
                Some(20.0),
                None,
                ptr::null_mut(),
            );
            let state = counter_agg_trans_inner(
                state,
                Some((BASE + 2 * MIN).into()),
                Some(30.0),
                None,
                ptr::null_mut(),
            );
            let state = counter_agg_trans_inner(
                state,
                Some((BASE + 3 * MIN).into()),
                Some(10.0),
                None,
                ptr::null_mut(),
            );
            let state = counter_agg_trans_inner(
                state,
                Some((BASE + 4 * MIN).into()),
                Some(20.0),
                None,
                ptr::null_mut(),
            );
            let state = counter_agg_trans_inner(
                state,
                Some((BASE + 5 * MIN).into()),
                Some(30.0),
                None,
                ptr::null_mut(),
            );

            let mut control = state.unwrap();
            let buffer =
                counter_summary_trans_serialize(Inner::from(control.clone()).internal().unwrap());
            let buffer = pgx::varlena::varlena_to_byte_slice(buffer.0.cast_mut_ptr());

            let expected = [
                1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 96, 194, 134, 7, 62, 2, 0, 0, 0, 0, 0, 0, 0, 36,
                64, 0, 231, 85, 138, 7, 62, 2, 0, 0, 0, 0, 0, 0, 0, 52, 64, 0, 124, 16, 149, 7, 62,
                2, 0, 0, 0, 0, 0, 0, 0, 52, 64, 0, 3, 164, 152, 7, 62, 2, 0, 0, 0, 0, 0, 0, 0, 62,
                64, 0, 0, 0, 0, 0, 0, 62, 64, 1, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 128, 144, 246, 54, 236, 65, 0, 0, 0, 0, 0, 195, 238, 64, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 32, 17, 209, 65, 0, 0, 0, 0, 0, 64, 106, 64, 0,
                0, 0, 0, 0, 88, 155, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 76, 248, 42, 65, 0, 0,
                0, 0, 0, 130, 196, 64, 0,
            ];
            assert_eq!(buffer, expected);

            let expected = pgx::varlena::rust_byte_slice_to_bytea(&expected);
            let new_state = counter_summary_trans_deserialize_inner(bytea(
                pgx::Datum::from(&*expected.cast_mut_ptr()),
            ));

            control.combine_summaries(); // Serialized form is always combined
            assert_eq!(&*new_state, &*control);
        }
    }

    #[pg_test]
    fn delta_after_counter_decrease() {
        Spi::execute(|client| {
            decrease(&client);
            let stmt = "SELECT delta(counter_agg(ts, val)) FROM test";
            // 10 after 30 means there was a reset so we add 30 + 10 = 40.
            // Delta from 30 to 40 => 10
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_counter_increase() {
        Spi::execute(|client| {
            increase(&client);
            let stmt = "SELECT delta(counter_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_counter_decrease_then_increase_to_same_value() {
        Spi::execute(|client| {
            decrease_then_increase_to_same_value(&client);
            let stmt = "SELECT delta(counter_agg(ts, val)) FROM test";
            // 10 after 30 means there was a reset so we add 30 + 10 + 30 = 70.
            // Delta from 30 to 70 => 30
            assert_eq!(30.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_counter_increase_then_decrease_to_same_value() {
        Spi::execute(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT delta(counter_agg(ts, val)) FROM test";
            // In this case, counter goes 10, 30, 40 (reset + 10).
            // Delta from 10 to 40 => 30
            assert_eq!(30.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_counter_decrease() {
        Spi::execute(|client| {
            decrease(&client);
            let stmt = "SELECT idelta_left(counter_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_counter_increase() {
        Spi::execute(|client| {
            increase(&client);
            let stmt = "SELECT idelta_left(counter_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_counter_increase_then_decrease_to_same_value() {
        Spi::execute(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT idelta_left(counter_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_counter_decrease_then_increase_to_same_value() {
        Spi::execute(|client| {
            decrease_then_increase_to_same_value(&client);

            let stmt = "SELECT idelta_left(counter_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_counter_decrease() {
        Spi::execute(|client| {
            decrease(&client);
            let stmt = "SELECT idelta_right(counter_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_counter_increase() {
        Spi::execute(|client| {
            increase(&client);
            let stmt = "SELECT idelta_right(counter_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_counter_increase_then_decrease_to_same_value() {
        Spi::execute(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT idelta_right(counter_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_counter_decrease_then_increase_to_same_value() {
        Spi::execute(|client| {
            decrease_then_increase_to_same_value(&client);
            let stmt = "SELECT idelta_right(counter_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn counter_agg_interpolation() {
        Spi::execute(|client| {
            client.select(
                "CREATE TABLE test(time timestamptz, value double precision, bucket timestamptz)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO test VALUES
                ('2020-1-1 10:00'::timestamptz, 10.0, '2020-1-1'::timestamptz),
                ('2020-1-1 12:00'::timestamptz, 40.0, '2020-1-1'::timestamptz),
                ('2020-1-1 16:00'::timestamptz, 20.0, '2020-1-1'::timestamptz),
                ('2020-1-2 4:00'::timestamptz, 15.0, '2020-1-2'::timestamptz),
                ('2020-1-2 12:00'::timestamptz, 50.0, '2020-1-2'::timestamptz),
                ('2020-1-2 20:00'::timestamptz, 25.0, '2020-1-2'::timestamptz),
                ('2020-1-3 4:00'::timestamptz, 30.0, '2020-1-3'::timestamptz),
                ('2020-1-3 12:00'::timestamptz, 0.0, '2020-1-3'::timestamptz), 
                ('2020-1-3 16:00'::timestamptz, 35.0, '2020-1-3'::timestamptz)"#,
                None,
                None,
            );

            let mut deltas = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_delta(
                    agg,
                    bucket,
                    '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket), 
                    LEAD(agg) OVER (ORDER BY bucket)
                ) FROM (
                    SELECT bucket, counter_agg(time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            // Day 1, start at 10, interpolated end of day is 10 (after reset), reset at 40 and 20
            assert_eq!(
                deltas.next().unwrap()[1].value(),
                Some(10. + 40. + 20. - 10.)
            );
            // Day 2, interpolated start is 10, interpolated end is 27.5, reset at 50
            assert_eq!(deltas.next().unwrap()[1].value(), Some(27.5 + 50. - 10.));
            // Day 3, interpolated start is 27.5, end is 35, reset at 30
            assert_eq!(deltas.next().unwrap()[1].value(), Some(35. + 30. - 27.5));
            assert!(deltas.next().is_none());

            let mut rates = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_rate(
                    agg,
                    bucket,
                    '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket), 
                    LEAD(agg) OVER (ORDER BY bucket)
                ) FROM (
                    SELECT bucket, counter_agg(time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            // Day 1, 14 hours (rate is per second)
            assert_eq!(
                rates.next().unwrap()[1].value(),
                Some((10. + 40. + 20. - 10.) / (14. * 60. * 60.))
            );
            // Day 2, 24 hours
            assert_eq!(
                rates.next().unwrap()[1].value(),
                Some((27.5 + 50. - 10.) / (24. * 60. * 60.))
            );
            // Day 3, 16 hours
            assert_eq!(
                rates.next().unwrap()[1].value(),
                Some((35. + 30. - 27.5) / (16. * 60. * 60.))
            );
            assert!(rates.next().is_none());
        });
    }

    #[pg_test]
    fn interpolated_delta_with_aligned_point() {
        Spi::execute(|client| {
            client.select(
                "CREATE TABLE test(time timestamptz, value double precision, bucket timestamptz)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO test VALUES
                ('2020-1-1 10:00'::timestamptz, 10.0, '2020-1-1'::timestamptz),
                ('2020-1-1 12:00'::timestamptz, 40.0, '2020-1-1'::timestamptz),
                ('2020-1-1 16:00'::timestamptz, 20.0, '2020-1-1'::timestamptz),
                ('2020-1-2 0:00'::timestamptz, 15.0, '2020-1-2'::timestamptz),
                ('2020-1-2 12:00'::timestamptz, 50.0, '2020-1-2'::timestamptz),
                ('2020-1-2 20:00'::timestamptz, 25.0, '2020-1-2'::timestamptz)"#,
                None,
                None,
            );

            let mut deltas = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_delta(
                    agg,
                    bucket,
                    '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket), 
                    LEAD(agg) OVER (ORDER BY bucket)
                ) FROM (
                    SELECT bucket, counter_agg(time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );
            // Day 1, start at 10, interpolated end of day is 15 (after reset), reset at 40 and 20
            assert_eq!(
                deltas.next().unwrap()[1].value(),
                Some(15. + 40. + 20. - 10.)
            );
            // Day 2, start is 15, end is 25, reset at 50
            assert_eq!(deltas.next().unwrap()[1].value(), Some(25. + 50. - 15.));
            assert!(deltas.next().is_none());
        });
    }

    #[pg_test]
    fn irate_left_arrow_match() {
        Spi::execute(|client| {
            make_test_table(&client, "test");

            assert_relative_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       irate_left(counter_agg(ts, val)), \
                       counter_agg(ts, val) -> irate_left() \
                     FROM test",
                    f64
                ),
                0.16666666666666666,
            );
        });
    }

    #[pg_test]
    fn irate_right_arrow_match() {
        Spi::execute(|client| {
            make_test_table(&client, "test");

            assert_relative_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       irate_right(counter_agg(ts, val)), \
                       counter_agg(ts, val) -> irate_right() \
                     FROM test",
                    f64
                ),
                0.16666666666666666,
            );
        });
    }

    #[pg_test]
    fn idelta_left_arrow_match() {
        Spi::execute(|client| {
            make_test_table(&client, "test");

            assert_relative_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       idelta_left(counter_agg(ts, val)), \
                       counter_agg(ts, val) -> idelta_left() \
                     FROM test",
                    f64
                ),
                10.0,
            );
        });
    }

    #[pg_test]
    fn idelta_right_arrow_match() {
        Spi::execute(|client| {
            make_test_table(&client, "test");

            assert_relative_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       idelta_right(counter_agg(ts, val)), \
                       counter_agg(ts, val) -> idelta_right() \
                     FROM test",
                    f64
                ),
                10.0,
            );
        });
    }

    #[pg_test]
    fn num_resets_arrow_match() {
        Spi::execute(|client| {
            make_test_table(&client, "test");

            assert_relative_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       num_resets(counter_agg(ts, val)), \
                       counter_agg(ts, val) -> num_resets() \
                     FROM test",
                    f64
                ),
                0.0,
            );
        });
    }

    #[pg_test]
    fn first_and_last_val() {
        Spi::execute(|client| {
            make_test_table(&client, "test");

            assert_relative_eq!(
                select_one!(
                    client,
                    "SELECT \
                       first_val(counter_agg(ts, val)) \
                     FROM test",
                    f64
                ),
                10.0,
            );

            assert_relative_eq!(
                select_one!(
                    client,
                    "SELECT \
                       last_val(counter_agg(ts, val)) \
                     FROM test",
                    f64
                ),
                20.0,
            );
        });
    }

    #[pg_test]
    fn first_and_last_val_arrow_match() {
        Spi::execute(|client| {
            make_test_table(&client, "test");

            assert_relative_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       first_val(counter_agg(ts, val)), \
                       counter_agg(ts, val) -> first_val() \
                     FROM test",
                    f64
                ),
                10.0,
            );

            assert_relative_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       last_val(counter_agg(ts, val)), \
                       counter_agg(ts, val) -> last_val() \
                     FROM test",
                    f64
                ),
                20.0,
            );
        });
    }

    #[pg_test]
    fn first_and_last_time() {
        Spi::execute(|client| {
            make_test_table(&client, "test");
            client.select("SET TIME ZONE 'UTC'", None, None);

            assert_eq!(
                select_one!(
                    client,
                    "SELECT \
                       first_time(counter_agg(ts, val))::text \
                     FROM test",
                    &str
                ),
                "2020-01-01 00:00:00+00",
            );

            assert_eq!(
                select_one!(
                    client,
                    "SELECT \
                       last_time(counter_agg(ts, val))::text \
                     FROM test",
                    &str
                ),
                "2020-01-01 00:01:00+00",
            );
        });
    }

    #[pg_test]
    fn first_and_last_time_arrow_match() {
        Spi::execute(|client| {
            make_test_table(&client, "test");
            client.select("SET TIME ZONE 'UTC'", None, None);

            assert_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       first_time(counter_agg(ts, val))::text, \
                       (counter_agg(ts, val) -> first_time())::text \
                     FROM test",
                    &str
                ),
                "2020-01-01 00:00:00+00",
            );

            assert_eq!(
                select_and_check_one!(
                    client,
                    "SELECT \
                       last_time(counter_agg(ts, val))::text, \
                       (counter_agg(ts, val) -> last_time())::text \
                     FROM test",
                    &str
                ),
                "2020-01-01 00:01:00+00",
            );
        });
    }

    // #[pg_test]
    // fn test_combine_aggregate(){
    //     Spi::execute(|client| {

    //     });
    // }
}

#[cfg(any(test, feature = "pg_test"))]
pub(crate) mod testing {
    pub fn decrease(client: &pgx::SpiClient) {
        client.select(
            "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
            None,
            None,
        );
        client.select("SET TIME ZONE 'UTC'", None, None);
        client.select(
            r#"INSERT INTO test VALUES
                ('2020-01-01 00:00:00+00', 30.0),
                ('2020-01-01 00:07:00+00', 10.0)"#,
            None,
            None,
        );
    }

    pub fn increase(client: &pgx::SpiClient) {
        client.select(
            "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
            None,
            None,
        );
        client.select("SET TIME ZONE 'UTC'", None, None);
        client.select(
            r#"INSERT INTO test VALUES
                ('2020-01-01 00:00:00+00', 10.0),
                ('2020-01-01 00:07:00+00', 30.0)"#,
            None,
            None,
        );
    }

    pub fn decrease_then_increase_to_same_value(client: &pgx::SpiClient) {
        client.select(
            "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
            None,
            None,
        );
        client.select("SET TIME ZONE 'UTC'", None, None);
        client.select(
            r#"INSERT INTO test VALUES
                ('2020-01-01 00:00:00+00', 30.0),
                ('2020-01-01 00:07:00+00', 10.0),
                ('2020-01-01 00:08:00+00', 30.0)"#,
            None,
            None,
        );
    }

    pub fn increase_then_decrease_to_same_value(client: &pgx::SpiClient) {
        client.select(
            "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
            None,
            None,
        );
        client.select("SET TIME ZONE 'UTC'", None, None);
        client.select(
            r#"INSERT INTO test VALUES
                ('2020-01-01 00:00:00+00', 10.0),
                ('2020-01-01 00:07:00+00', 30.0),
                ('2020-01-01 00:08:00+00', 10.0)"#,
            None,
            None,
        );
    }

    pub fn make_test_table(client: &pgx::SpiClient, name: &str) {
        client.select(
            &format!(
                "CREATE TABLE {}(ts timestamptz, val DOUBLE PRECISION)",
                name
            ),
            None,
            None,
        );
        client.select(
                &format!("INSERT INTO {} VALUES('2020-01-01 00:00:00+00', 10.0), ('2020-01-01 00:01:00+00', 20.0)", name),
                None,
                None,
            );
    }
}
