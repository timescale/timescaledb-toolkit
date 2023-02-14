use pgx::*;

use serde::{Deserialize, Serialize};

use counter_agg::{range::I64Range, GaugeSummaryBuilder, MetricSummary};
use flat_serialize_macro::FlatSerializable;
use stats_agg::stats2d::StatsSummary2D;
use tspoint::TSPoint;

use crate::{
    accessors::{
        AccessorCorr, AccessorCounterZeroTime, AccessorDelta, AccessorExtrapolatedDelta,
        AccessorExtrapolatedRate, AccessorIdeltaLeft, AccessorIdeltaRight, AccessorIntercept,
        AccessorIrateLeft, AccessorIrateRight, AccessorNumChanges, AccessorNumElements,
        AccessorRate, AccessorSlope, AccessorTimeDelta, AccessorWithBounds,
    },
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    range::{get_range, I64RangeWrapper},
    raw::{bytea, tstzrange},
    ron_inout_funcs,
};

// TODO move to share with counter_agg
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, FlatSerializable)]
#[repr(C)]
pub struct FlatSummary {
    stats: StatsSummary2D<f64>,
    first: TSPoint,
    second: TSPoint,
    penultimate: TSPoint,
    last: TSPoint,
    reset_sum: f64,
    num_resets: u64,
    num_changes: u64,
    bounds: I64RangeWrapper,
}

#[pg_schema]
mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug, PartialEq)]
        struct GaugeSummary {
            #[flat_serialize::flatten]
            summary: FlatSummary,
        }
    }

    impl<'input> GaugeSummary<'input> {
        pub(super) fn interpolate(
            &self,
            interval_start: i64,
            interval_len: i64,
            prev: Option<GaugeSummary>,
            next: Option<GaugeSummary>,
        ) -> GaugeSummary<'static> {
            let this = MetricSummary::from(self.clone());
            let prev = prev.map(MetricSummary::from);
            let next = next.map(MetricSummary::from);

            let prev = if this.first.ts > interval_start {
                prev.map(|summary| {
                    time_weighted_average::TimeWeightMethod::Linear
                        .interpolate(summary.last, Some(this.first), interval_start)
                        .expect("unable to interpolate lower bound")
                })
            } else {
                None
            };

            let next = next.map(|summary| {
                time_weighted_average::TimeWeightMethod::Linear
                    .interpolate(
                        this.last,
                        Some(summary.first),
                        interval_start + interval_len,
                    )
                    .expect("unable to interpolate upper bound")
            });

            let builder = prev.map(|pt| GaugeSummaryBuilder::new(&pt, None));
            let mut builder = builder.map_or_else(
                || {
                    let mut summary = this.clone();
                    summary.bounds = None;
                    summary.into()
                },
                |mut builder| {
                    builder
                        .combine(&this)
                        .expect("unable to add data to interpolation");
                    builder
                },
            );

            if let Some(next) = next {
                builder
                    .add_point(&next)
                    .expect("unable to add final interpolated point");
            }

            builder.build().into()
        }
    }

    ron_inout_funcs!(GaugeSummary);
}

use toolkit_experimental::*;

// TODO reunify with crate::counter_agg::CounterSummaryTransSate
// TODO move to crate::metrics::TransState (taking FnOnce()->MetricSummaryBuilder to support both)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct GaugeSummaryTransState {
    #[serde(skip)]
    point_buffer: Vec<TSPoint>,
    #[serde(skip)]
    bounds: Option<I64Range>, // stores bounds until we combine points, after which, the bounds are stored in each summary
    // We have a summary buffer here in order to deal with the fact that when the cmobine function gets called it
    // must first build up a buffer of InternalMetricSummaries, then sort them, then call the combine function in
    // the correct order.
    summary_buffer: Vec<MetricSummary>,
}

impl GaugeSummaryTransState {
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

    fn combine_points(&mut self) {
        if self.point_buffer.is_empty() {
            return;
        }
        self.point_buffer.sort_unstable_by_key(|p| p.ts);
        let mut iter = self.point_buffer.iter();
        let mut summary = GaugeSummaryBuilder::new(iter.next().unwrap(), self.bounds);
        for p in iter {
            summary
                .add_point(p)
                .unwrap_or_else(|e| pgx::error!("{}", e));
        }
        self.point_buffer.clear();
        // TODO build method should check validity
        // check bounds only after we've combined all the points, so we aren't doing it all the time.
        if !summary.bounds_valid() {
            panic!("Metric bounds invalid")
        }
        self.summary_buffer.push(summary.build());
    }

    fn push_summary(&mut self, other: &Self) {
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
        self.summary_buffer.sort_unstable_by_key(|s| s.first.ts);
        let mut sum_iter = self.summary_buffer.drain(..);
        let first = sum_iter.next().expect("already handled empty case");
        let mut new_summary = GaugeSummaryBuilder::from(first);
        for sum in sum_iter {
            new_summary
                .combine(&sum)
                .unwrap_or_else(|e| pgx::error!("{}", e));
        }
        self.summary_buffer.push(new_summary.build());
    }
}

#[pg_extern(immutable, parallel_safe, strict, schema = "toolkit_experimental")]
fn gauge_summary_trans_serialize(state: Internal) -> bytea {
    let state: &mut GaugeSummaryTransState = unsafe { state.get_mut().unwrap() };
    state.combine_summaries();
    crate::do_serialize!(state)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn gauge_summary_trans_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    gauge_summary_trans_deserialize_inner(bytes).internal()
}
fn gauge_summary_trans_deserialize_inner(bytes: bytea) -> Inner<GaugeSummaryTransState> {
    let c: GaugeSummaryTransState = crate::do_deserialize!(bytes, GaugeSummaryTransState);
    c.into()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn gauge_agg_trans(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    bounds: Option<tstzrange>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    gauge_agg_trans_inner(unsafe { state.to_inner() }, ts, val, bounds, fcinfo).internal()
}
fn gauge_agg_trans_inner(
    state: Option<Inner<GaugeSummaryTransState>>,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    bounds: Option<tstzrange>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<GaugeSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint { ts: ts.into(), val },
            };
            match state {
                None => {
                    let mut s = GaugeSummaryTransState::new();
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

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn gauge_agg_trans_no_bounds(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    gauge_agg_trans_inner(unsafe { state.to_inner() }, ts, val, None, fcinfo).internal()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn gauge_agg_summary_trans<'a>(
    state: Internal,
    value: Option<GaugeSummary<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    gauge_agg_summary_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
fn gauge_agg_summary_trans_inner(
    state: Option<Inner<GaugeSummaryTransState>>,
    value: Option<GaugeSummary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<GaugeSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (state, None) => state,
            (None, Some(value)) => {
                let mut state = GaugeSummaryTransState::new();
                state.summary_buffer.push(value.into());
                Some(state.into())
            }
            (Some(mut state), Some(value)) => {
                state.summary_buffer.push(value.into());
                Some(state)
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn gauge_agg_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { gauge_agg_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}
fn gauge_agg_combine_inner(
    state1: Option<Inner<GaugeSummaryTransState>>,
    state2: Option<Inner<GaugeSummaryTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<GaugeSummaryTransState>> {
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

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn gauge_agg_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<GaugeSummary<'static>> {
    gauge_agg_final_inner(unsafe { state.to_inner() }, fcinfo)
}
fn gauge_agg_final_inner(
    state: Option<Inner<GaugeSummaryTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<GaugeSummary<'static>> {
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
                        panic!("Metric bounds invalid")
                    }
                    Some(GaugeSummary::from(st))
                }
            }
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.gauge_agg( ts timestamptz, value DOUBLE PRECISION, bounds tstzrange )\n\
    (\n\
        sfunc = toolkit_experimental.gauge_agg_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.gauge_agg_final,\n\
        combinefunc = toolkit_experimental.gauge_agg_combine,\n\
        serialfunc = toolkit_experimental.gauge_summary_trans_serialize,\n\
        deserialfunc = toolkit_experimental.gauge_summary_trans_deserialize,\n\
        parallel = restricted\n\
    );\n",
    name = "gauge_agg",
    requires = [
        gauge_agg_trans,
        gauge_agg_final,
        gauge_agg_combine,
        gauge_summary_trans_serialize,
        gauge_summary_trans_deserialize
    ],
);

// allow calling gauge agg without bounds provided.
extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.gauge_agg( ts timestamptz, value DOUBLE PRECISION )\n\
    (\n\
        sfunc = toolkit_experimental.gauge_agg_trans_no_bounds,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.gauge_agg_final,\n\
        combinefunc = toolkit_experimental.gauge_agg_combine,\n\
        serialfunc = toolkit_experimental.gauge_summary_trans_serialize,\n\
        deserialfunc = toolkit_experimental.gauge_summary_trans_deserialize,\n\
        parallel = restricted\n\
    );\n\
",
    name = "gauge_agg2",
    requires = [
        gauge_agg_trans_no_bounds,
        gauge_agg_final,
        gauge_agg_combine,
        gauge_summary_trans_serialize,
        gauge_summary_trans_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.rollup(gs toolkit_experimental.GaugeSummary)\n\
    (\n\
        sfunc = toolkit_experimental.gauge_agg_summary_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.gauge_agg_final,\n\
        combinefunc = toolkit_experimental.gauge_agg_combine,\n\
        serialfunc = toolkit_experimental.gauge_summary_trans_serialize,\n\
        deserialfunc = toolkit_experimental.gauge_summary_trans_deserialize,\n\
        parallel = restricted\n\
    );\n\
",
    name = "gauge_rollup",
    requires = [
        gauge_agg_summary_trans,
        gauge_agg_final,
        gauge_agg_combine,
        gauge_summary_trans_serialize,
        gauge_summary_trans_deserialize
    ],
);

// TODO Reconsider using the same pg_type for counter and gauge aggregates to avoid duplicating all these functions.

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_delta<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorDelta<'a>) -> f64 {
    delta(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn delta<'a>(summary: GaugeSummary<'a>) -> f64 {
    MetricSummary::from(summary).delta()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_gauge_agg_rate<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorRate<'a>) -> Option<f64> {
    rate(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn rate<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).rate()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_time_delta<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorTimeDelta<'a>) -> f64 {
    time_delta(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn time_delta<'a>(summary: GaugeSummary<'a>) -> f64 {
    MetricSummary::from(summary).time_delta()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_irate_left<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorIrateLeft<'a>) -> Option<f64> {
    irate_left(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn irate_left<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).irate_left()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_irate_right<'a>(
    sketch: GaugeSummary<'a>,
    _accessor: AccessorIrateRight<'a>,
) -> Option<f64> {
    irate_right(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn irate_right<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).irate_right()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_idelta_left<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorIdeltaLeft<'a>) -> f64 {
    idelta_left(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn idelta_left<'a>(summary: GaugeSummary<'a>) -> f64 {
    MetricSummary::from(summary).idelta_left()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_idelta_right<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorIdeltaRight<'a>) -> f64 {
    idelta_right(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn idelta_right<'a>(summary: GaugeSummary<'a>) -> f64 {
    MetricSummary::from(summary).idelta_right()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_with_bounds<'a>(
    sketch: GaugeSummary<'a>,
    accessor: AccessorWithBounds<'a>,
) -> GaugeSummary<'static> {
    let mut builder = GaugeSummaryBuilder::from(MetricSummary::from(sketch));
    builder.set_bounds(accessor.bounds());
    builder.build().into()
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn with_bounds<'a>(summary: GaugeSummary<'a>, bounds: tstzrange) -> GaugeSummary {
    // TODO dedup with previous by using apply_bounds
    unsafe {
        let ptr = bounds.0.cast_mut_ptr();
        let mut builder = GaugeSummaryBuilder::from(MetricSummary::from(summary));
        builder.set_bounds(get_range(ptr));
        builder.build().into()
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_extrapolated_delta<'a>(
    sketch: GaugeSummary<'a>,
    _accessor: AccessorExtrapolatedDelta<'a>,
) -> Option<f64> {
    extrapolated_delta(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn extrapolated_delta<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).prometheus_delta().unwrap()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn interpolated_delta<'a>(
    summary: GaugeSummary<'a>,
    start: crate::raw::TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<GaugeSummary<'a>>,
    next: Option<GaugeSummary<'a>>,
) -> f64 {
    let interval = crate::datum_utils::interval_to_ms(&start, &interval);
    MetricSummary::from(summary.interpolate(start.into(), interval, prev, next)).delta()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_extrapolated_rate<'a>(
    sketch: GaugeSummary<'a>,
    _accessor: AccessorExtrapolatedRate<'a>,
) -> Option<f64> {
    extrapolated_rate(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn extrapolated_rate<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).prometheus_rate().unwrap()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn interpolated_rate<'a>(
    summary: GaugeSummary<'a>,
    start: crate::raw::TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<GaugeSummary<'a>>,
    next: Option<GaugeSummary<'a>>,
) -> Option<f64> {
    let interval = crate::datum_utils::interval_to_ms(&start, &interval);
    MetricSummary::from(summary.interpolate(start.into(), interval, prev, next)).rate()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_num_elements<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorNumElements<'a>) -> i64 {
    num_elements(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn num_elements<'a>(summary: GaugeSummary<'a>) -> i64 {
    MetricSummary::from(summary).stats.n as i64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_num_changes<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorNumChanges<'a>) -> i64 {
    num_changes(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn num_changes<'a>(summary: GaugeSummary<'a>) -> i64 {
    MetricSummary::from(summary).num_changes as i64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_slope<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorSlope<'a>) -> Option<f64> {
    slope(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn slope<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).stats.slope()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_intercept<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorIntercept<'a>) -> Option<f64> {
    intercept(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn intercept<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).stats.intercept()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_corr<'a>(sketch: GaugeSummary<'a>, _accessor: AccessorCorr<'a>) -> Option<f64> {
    corr(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn corr<'a>(summary: GaugeSummary<'a>) -> Option<f64> {
    MetricSummary::from(summary).stats.corr()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
fn arrow_zero_time<'a>(
    sketch: GaugeSummary<'a>,
    __accessor: AccessorCounterZeroTime<'a>,
) -> Option<crate::raw::TimestampTz> {
    gauge_zero_time(sketch)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
fn gauge_zero_time<'a>(summary: GaugeSummary<'a>) -> Option<crate::raw::TimestampTz> {
    Some(((MetricSummary::from(summary).stats.x_intercept()? * 1_000_000.0) as i64).into())
}

impl From<GaugeSummary<'_>> for MetricSummary {
    fn from(pg: GaugeSummary<'_>) -> Self {
        Self {
            first: pg.summary.first,
            second: pg.summary.second,
            penultimate: pg.summary.penultimate,
            last: pg.summary.last,
            reset_sum: pg.summary.reset_sum,
            num_resets: pg.summary.num_resets,
            num_changes: pg.summary.num_changes,
            stats: pg.summary.stats,
            bounds: pg.summary.bounds.to_i64range(),
        }
    }
}

impl From<MetricSummary> for GaugeSummary<'_> {
    fn from(internal: MetricSummary) -> Self {
        unsafe {
            flatten!(GaugeSummary {
                summary: FlatSummary {
                    stats: internal.stats,
                    first: internal.first,
                    second: internal.second,
                    penultimate: internal.penultimate,
                    last: internal.last,
                    reset_sum: internal.reset_sum,
                    num_resets: internal.num_resets,
                    num_changes: internal.num_changes,
                    bounds: I64RangeWrapper::from_i64range(internal.bounds)
                }
            })
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx_macros::pg_test;

    use crate::counter_agg::testing::*;

    use super::*;

    macro_rules! select_one {
        ($client:expr, $stmt:expr, $type:ty) => {
            $client
                .select($stmt, None, None)
                .first()
                .get_one::<$type>()
                .unwrap()
        };
    }

    #[pg_test]
    fn round_trip() {
        Spi::connect(|client| {
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
                summary:(\
                    stats:(\
                        n:9,\
                        sx:5680370160,\
                        sx2:216000,\
                        sx3:0,\
                        sx4:9175680000,\
                        sy:160,\
                        sy2:555.5555555555555,\
                        sy3:1802.4691358024695,\
                        sy4:59341.563786008235,\
                        sxy:-600\
                    ),\
                    first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                    second:(ts:\"2020-01-01 00:01:00+00\",val:20),\
                    penultimate:(ts:\"2020-01-01 00:07:00+00\",val:30),\
                    last:(ts:\"2020-01-01 00:08:00+00\",val:10),\
                    reset_sum:0,\
                    num_resets:0,\
                    num_changes:8,\
                    bounds:(\
                        is_present:0,\
                        has_left:0,\
                        has_right:0,\
                        padding:(0,0,0,0,0),\
                        left:None,\
                        right:None\
                    )\
                )\
            )";

            assert_eq!(
                expected,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.gauge_agg(ts, val)::TEXT FROM test",
                    String
                )
            );

            assert_eq!(
                expected,
                select_one!(
                    client,
                    &format!(
                        "SELECT '{}'::toolkit_experimental.GaugeSummary::TEXT",
                        expected
                    ),
                    String
                )
            );
        });
    }

    #[pg_test]
    fn delta_after_gauge_decrease() {
        Spi::connect(|client| {
            decrease(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(-20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_gauge_increase() {
        Spi::connect(|client| {
            increase(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_gauge_decrease_then_increase_to_same_value() {
        Spi::connect(|client| {
            decrease_then_increase_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(0.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_gauge_increase_then_decrease_to_same_value() {
        Spi::connect(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(0.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_decrease() {
        Spi::connect(|client| {
            decrease(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_increase() {
        Spi::connect(|client| {
            increase(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_increase_then_decrease_to_same_value() {
        Spi::connect(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_decrease_then_increase_to_same_value() {
        Spi::connect(|client| {
            decrease_then_increase_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_decrease() {
        Spi::connect(|client| {
            decrease(&client);
            let stmt = "SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_increase() {
        Spi::connect(|client| {
            increase(&client);
            let stmt = "SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_increase_then_decrease_to_same_value() {
        Spi::connect(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_decrease_then_increase_to_same_value() {
        Spi::connect(|client| {
            decrease_then_increase_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    // TODO 3rd copy of this...
    #[track_caller]
    fn assert_close_enough(p1: &MetricSummary, p2: &MetricSummary) {
        assert_eq!(p1.first, p2.first, "first");
        assert_eq!(p1.second, p2.second, "second");
        assert_eq!(p1.penultimate, p2.penultimate, "penultimate");
        assert_eq!(p1.last, p2.last, "last");
        assert_eq!(p1.num_changes, p2.num_changes, "num_changes");
        assert_eq!(p1.num_resets, p2.num_resets, "num_resets");
        assert_eq!(p1.stats.n, p2.stats.n, "n");
        use approx::assert_relative_eq;
        assert_relative_eq!(p1.stats.sx, p2.stats.sx);
        assert_relative_eq!(p1.stats.sx2, p2.stats.sx2);
        assert_relative_eq!(p1.stats.sy, p2.stats.sy);
        assert_relative_eq!(p1.stats.sy2, p2.stats.sy2);
        assert_relative_eq!(p1.stats.sxy, p2.stats.sxy);
    }

    #[pg_test]
    fn rollup() {
        Spi::connect(|client| {
            client.select(
                "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
                None,
                None,
            );

            // This tests GaugeSummary::single_value - the old first == last
            // check erroneously saw 21.0 == 21.0 and called it a single value.
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:00:00+00', 10.0), ('2020-01-01 00:01:00+00', 21.0), ('2020-01-01 00:01:00+00', 22.0), ('2020-01-01 00:01:00+00', 21.0)";
            client.select(stmt, None, None);

            let stmt = "INSERT INTO test VALUES('2020-01-01 00:02:00+00', 10.0), ('2020-01-01 00:03:00+00', 20.0), ('2020-01-01 00:04:00+00', 10.0)";
            client.select(stmt, None, None);

            let stmt = "INSERT INTO test VALUES('2020-01-01 00:08:00+00', 30.0), ('2020-01-01 00:10:00+00', 30.0), ('2020-01-01 00:10:30+00', 10.0), ('2020-01-01 00:20:00+00', 40.0)";
            client.select(stmt, None, None);

            //combine function works as expected
            let stmt = "SELECT toolkit_experimental.gauge_agg(ts, val) FROM test";
            let a = select_one!(client, stmt, GaugeSummary);
            let stmt = "WITH t as (SELECT date_trunc('minute', ts), toolkit_experimental.gauge_agg(ts, val) as agg FROM test group by 1 ) SELECT toolkit_experimental.rollup(agg) FROM t";
            let b = select_one!(client, stmt, GaugeSummary);
            assert_close_enough(&a.into(), &b.into());
        });
    }

    #[pg_test]
    fn gauge_agg_interpolation() {
        Spi::connect(|client| {
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
                ('2020-1-2 2:00'::timestamptz, 15.0, '2020-1-2'::timestamptz),
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
                    SELECT bucket, toolkit_experimental.gauge_agg(time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            // Day 1, start at 10, interpolated end of day is 16
            assert_eq!(deltas.next().unwrap()[1].value(), Some(16. - 10.));
            // Day 2, interpolated start is 16, interpolated end is 27.5
            assert_eq!(deltas.next().unwrap()[1].value(), Some(27.5 - 16.));
            // Day 3, interpolated start is 27.5, end is 35
            assert_eq!(deltas.next().unwrap()[1].value(), Some(35. - 27.5));

            let mut rates = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_rate(
                    agg,
                    bucket,
                    '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket), 
                    LEAD(agg) OVER (ORDER BY bucket)
                ) FROM (
                    SELECT bucket, toolkit_experimental.gauge_agg(time, value) as agg 
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
                Some((16. - 10.) / (14. * 60. * 60.))
            );
            // Day 2, 24 hours
            assert_eq!(
                rates.next().unwrap()[1].value(),
                Some((27.5 - 16.) / (24. * 60. * 60.))
            );
            // Day 3, 16 hours
            assert_eq!(
                rates.next().unwrap()[1].value(),
                Some((35. - 27.5) / (16. * 60. * 60.))
            );
        });
    }

    #[pg_test]
    fn guage_agg_interpolated_delta_with_aligned_point() {
        Spi::connect(|client| {
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
                    SELECT bucket, toolkit_experimental.gauge_agg(time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );
            // Day 1, start at 10, interpolated end of day is 15 (after reset)
            assert_eq!(deltas.next().unwrap()[1].value(), Some(15. - 10.));
            // Day 2, start is 15, end is 25
            assert_eq!(deltas.next().unwrap()[1].value(), Some(25. - 15.));
            assert!(deltas.next().is_none());
        });
    }

    #[pg_test]
    fn no_results_on_null_input() {
        Spi::connect(|client| {
            client.select(
                "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
                None,
                None,
            );

            let stmt = "INSERT INTO test VALUES (NULL, NULL)";
            client.select(stmt, None, None);

            let stmt = "SELECT toolkit_experimental.gauge_agg(ts, val) FROM test";
            assert!(client
                .select(stmt, None, None)
                .first()
                .get_one::<GaugeSummary>()
                .is_none());
        });
    }
}
