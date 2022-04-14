use pgx::*;
use serde::{Deserialize, Serialize};

use counter_agg::{range::I64Range, stable, GaugeSummaryBuilder, MetricSummary};
use flat_serialize::FlatSerializable;
use flat_serialize_macro::FlatSerializable;
use stats_agg::stats2d::StatsSummary2D;
use time_series::TSPoint;

use crate::{
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
    stats: StatsSummary2D,
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
    summary_buffer: Vec<stable::MetricSummary>,
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
        let mut summary = GaugeSummaryBuilder::new(
            iter.next().unwrap(),
            self.bounds.clone().unwrap_or_else(I64Range::infinite),
        );
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
        self.summary_buffer.push(summary.build().into());
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
        // TODO move much of this method to crate?
        self.summary_buffer.sort_unstable_by_key(|s| s.first.ts);
        let mut sum_iter = self.summary_buffer.drain(..);
        let first = sum_iter.next().expect("already handled empty case");
        let mut new_summary = GaugeSummaryBuilder::from(MetricSummary::from(first));
        for sum in sum_iter {
            new_summary
                .combine(&sum.into())
                .unwrap_or_else(|e| pgx::error!("{}", e));
        }
        self.summary_buffer.push(new_summary.build().into());
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
                        s.bounds = get_range(r.0 as *mut pg_sys::varlena);
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
fn gauge_agg_summary_trans(
    state: Internal,
    value: Option<GaugeSummary>,
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
                state
                    .summary_buffer
                    .push(stable::MetricSummary::from(MetricSummary::from(value)));
                Some(state.into())
            }
            (Some(mut state), Some(value)) => {
                state
                    .summary_buffer
                    .push(stable::MetricSummary::from(MetricSummary::from(value)));
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
                    let st = MetricSummary::from(st);
                    // there are some edge cases that this should prevent, but I'm not sure it's necessary, we do check the bounds in the functions that use them.
                    if !st.bounds_valid() {
                        panic!("counter bounds invalid")
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

#[pg_extern(
    name = "delta",
    strict,
    immutable,
    parallel_safe,
    schema = "toolkit_experimental"
)]
fn gauge_agg_delta(summary: GaugeSummary) -> f64 {
    MetricSummary::from(summary).delta()
}

#[pg_extern(
    name = "idelta_left",
    strict,
    immutable,
    parallel_safe,
    schema = "toolkit_experimental"
)]
fn gauge_agg_idelta_left(summary: GaugeSummary) -> f64 {
    MetricSummary::from(summary).idelta_left()
}

#[pg_extern(
    name = "idelta_right",
    strict,
    immutable,
    parallel_safe,
    schema = "toolkit_experimental"
)]
fn gauge_agg_idelta_right(summary: GaugeSummary) -> f64 {
    MetricSummary::from(summary).idelta_right()
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
        Spi::execute(|client| {
            decrease(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(-20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_gauge_increase() {
        Spi::execute(|client| {
            increase(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_gauge_decrease_then_increase_to_same_value() {
        Spi::execute(|client| {
            decrease_then_increase_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(0.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn delta_after_gauge_increase_then_decrease_to_same_value() {
        Spi::execute(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(0.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_decrease() {
        Spi::execute(|client| {
            decrease(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_increase() {
        Spi::execute(|client| {
            increase(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_increase_then_decrease_to_same_value() {
        Spi::execute(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_left_after_gauge_decrease_then_increase_to_same_value() {
        Spi::execute(|client| {
            decrease_then_increase_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_decrease() {
        Spi::execute(|client| {
            decrease(&client);
            let stmt = "SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_increase() {
        Spi::execute(|client| {
            increase(&client);
            let stmt = "SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(20.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_increase_then_decrease_to_same_value() {
        Spi::execute(|client| {
            increase_then_decrease_to_same_value(&client);
            let stmt = "SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM test";
            assert_eq!(10.0, select_one!(client, stmt, f64));
        });
    }

    #[pg_test]
    fn idelta_right_after_gauge_decrease_then_increase_to_same_value() {
        Spi::execute(|client| {
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
        Spi::execute(|client| {
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

    // TODO https://github.com/timescale/timescaledb-toolkit/issues/362
    // TODO why doesn't this catch the error under github actions?
    // #[pg_test(error = "returned Datum was NULL")]
    #[allow(dead_code)]
    fn nulls() {
        Spi::execute(|client| {
            client.select(
                "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)",
                None,
                None,
            );

            let stmt = "INSERT INTO test VALUES (NULL, NULL)";
            client.select(stmt, None, None);

            let stmt = "SELECT toolkit_experimental.gauge_agg(ts, val) FROM test";
            let _ = select_one!(client, stmt, GaugeSummary);
        });
    }
}
