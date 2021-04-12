use serde::{Serialize, Deserialize};

use std::{
    slice,
};

use pgx::*;
use pg_sys::Datum;

use flat_serialize::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    json_inout_funcs,
    flatten,
    palloc::Internal,
    pg_type,
    range::*,
};

use time_weighted_average::{
    tspoint::TSPoint,
};

use counter_agg::{
    CounterSummary as InternalCounterSummary,
    regression::RegressionSummary,
    range::I64Range,
};

#[allow(non_camel_case_types)]
type tstzrange = Datum;
// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
mod timescale_analytics_experimental {
    pub(crate) use super::*;

    varlena_type!(CounterSummary);
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

pg_type! {
    #[derive(Debug, PartialEq)]
    struct CounterSummary {
        regress: RegressionSummary,
        first: TSPoint,
        second: TSPoint,
        penultimate:TSPoint,
        last: TSPoint,
        reset_sum: f64,
        num_resets: u64,
        num_changes: u64,
        bounds: I64RangeWrapper,
    }
}

json_inout_funcs!(CounterSummary);

impl<'input> CounterSummary<'input> {
    fn to_internal_counter_summary(&self) -> InternalCounterSummary {
        InternalCounterSummary{
            first: *self.first,
            second: *self.second,
            penultimate: *self.penultimate,
            last: *self.last,
            reset_sum: *self.reset_sum,
            num_resets: *self.num_resets,
            num_changes: *self.num_changes,
            regress: *self.regress,
            bounds: self.bounds.to_i64range(),
        }
    }
    fn from_internal_counter_summary(st: InternalCounterSummary) -> Self {
        unsafe{
            flatten!(
            CounterSummary {
                regress: &st.regress,
                first: &st.first,
                second: &st.second,
                penultimate: &st.penultimate,
                last: &st.last,
                reset_sum: &st.reset_sum,
                num_resets: &st.num_resets,
                num_changes: &st.num_changes,
                bounds: &I64RangeWrapper::from_i64range(st.bounds)
            })
        }
    }
    // fn set_bounds(&mut self, bounds: Option<I64Range>){
    //     self.bounds = &I64RangeWrapper::from_i64range(bounds);
    // }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CounterSummaryTransState {
    #[serde(skip)]
    point_buffer: Vec<TSPoint>,
    #[serde(skip)]
    bounds: Option<I64Range>, // stores bounds until we combine points, after which, the bounds are stored in each summary
    // We have a summary buffer here in order to deal with the fact that when the cmobine function gets called it
    // must first build up a buffer of InternalMetricSummaries, then sort them, then call the combine function in
    // the correct order.
    summary_buffer: Vec<InternalCounterSummary>,
}

impl CounterSummaryTransState {
    fn push_point(&mut self, value: TSPoint) {
        self.point_buffer.push(value);
    }

    // fn set_bounds(&mut self, bounds: Option<I64Range>){
    //     self.bounds = bounds;
    // }

    fn combine_points(&mut self) {
        if self.point_buffer.is_empty() {
            return
        }
        self.point_buffer.sort_unstable_by_key(|p| p.ts);
        let mut iter = self.point_buffer.iter();
        let mut summary = InternalCounterSummary::new( iter.next().unwrap(), self.bounds);
        for p in iter {
            summary.add_point(p).unwrap();
        }
        self.point_buffer.clear();
        // check bounds only after we've combined all the points, so we aren't doing it all the time.
        if !summary.bounds_valid() {
            panic!("counter bounds invalid")
        }
        self.summary_buffer.push(summary);
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
            return
        }
        self.summary_buffer.sort_unstable_by_key(|s| s.first.ts);
        let mut sum_iter = self.summary_buffer.iter();
        let mut new_summary = sum_iter.next().unwrap().clone();
        for sum in sum_iter {
            new_summary.combine(sum).unwrap();
        }
        self.summary_buffer = vec![new_summary];
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn counter_summary_trans_serialize(
    mut state: Internal<CounterSummaryTransState>,
) -> bytea {
    state.combine_summaries();
    crate::do_serialize!(state)
}

#[pg_extern(schema = "timescale_analytics_experimental", strict)]
pub fn counter_summary_trans_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<CounterSummaryTransState> {
    crate::do_deserialize!(bytes, CounterSummaryTransState)
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn counter_agg_trans(
    state: Option<Internal<CounterSummaryTransState>>,
    ts: Option<pg_sys::TimestampTz>,
    val: Option<f64>,
    bounds: Option<tstzrange>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<CounterSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint{ts, val},
            };
            match state {
                None => {
                    let mut s = CounterSummaryTransState{point_buffer: vec![], bounds: None, summary_buffer: vec![]};
                    if let Some(r) = bounds {
                        s.bounds = get_range(r as *mut pg_sys::varlena);
                    }
                    s.push_point(p);
                    Some(s.into())
                },
                Some(mut s) => {s.push_point(p); Some(s)},
            }
        })
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn counter_agg_trans_no_bounds(
    state: Option<Internal<CounterSummaryTransState>>,
    ts: Option<pg_sys::TimestampTz>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<CounterSummaryTransState>> {
    counter_agg_trans(state, ts, val, None, fcinfo)
}


#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn counter_agg_summary_trans(
    state: Option<Internal<CounterSummaryTransState>>,
    value: Option<timescale_analytics_experimental::CounterSummary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<CounterSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (state, None) => state,
                (None, Some(value)) => Some(
                    CounterSummaryTransState{point_buffer: vec![], bounds: None, summary_buffer: vec![value.to_internal_counter_summary()]}.into()),
                (Some(mut state), Some(value)) => {
                    state.summary_buffer.push(value.to_internal_counter_summary());
                    Some(state)
                }
            }
        })
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn counter_agg_combine(
    state1: Option<Internal<CounterSummaryTransState>>,
    state2: Option<Internal<CounterSummaryTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
)  -> Option<Internal<CounterSummaryTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => {let mut s = state2.clone(); s.combine_points(); Some(s.into())},
                (Some(state1), None) => {let mut s = state1.clone(); s.combine_points(); Some(s.into())}, //should I make these return themselves?
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

#[pg_extern(schema = "timescale_analytics_experimental")]
fn counter_agg_final(
    state: Option<Internal<CounterSummaryTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<timescale_analytics_experimental::CounterSummary<'static>> {
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
                    Some(CounterSummary::from_internal_counter_summary(st).into())
                }
            }
        })
    }
}


extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.counter_agg( ts timestamptz, value DOUBLE PRECISION, bounds tstzrange )
(
    sfunc = timescale_analytics_experimental.counter_agg_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.counter_agg_final,
    combinefunc = timescale_analytics_experimental.counter_agg_combine,
    serialfunc = timescale_analytics_experimental.counter_summary_trans_serialize,
    deserialfunc = timescale_analytics_experimental.counter_summary_trans_deserialize,
    parallel = restricted
);
"#);

// allow calling counter agg without bounds provided.
extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.counter_agg( ts timestamptz, value DOUBLE PRECISION )
(
    sfunc = timescale_analytics_experimental.counter_agg_trans_no_bounds,
    stype = internal,
    finalfunc = timescale_analytics_experimental.counter_agg_final,
    combinefunc = timescale_analytics_experimental.counter_agg_combine,
    serialfunc = timescale_analytics_experimental.counter_summary_trans_serialize,
    deserialfunc = timescale_analytics_experimental.counter_summary_trans_deserialize,
    parallel = restricted
);
"#);

extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.counter_agg(counter_agg timescale_analytics_experimental.CounterSummary)
(
    sfunc = timescale_analytics_experimental.counter_agg_summary_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.counter_agg_final,
    combinefunc = timescale_analytics_experimental.counter_agg_combine,
    serialfunc = timescale_analytics_experimental.counter_summary_trans_serialize,
    deserialfunc = timescale_analytics_experimental.counter_summary_trans_deserialize,
    parallel = restricted
);
"#);

#[pg_extern(name="delta", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_delta(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> f64 {
    summary.to_internal_counter_summary().delta()
}

#[pg_extern(name="rate", schema = "timescale_analytics_experimental", strict, immutable )]
fn counter_agg_rate(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal_counter_summary().rate()
}

#[pg_extern(name="time_delta", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_time_delta(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> f64 {
    summary.to_internal_counter_summary().time_delta()
}

#[pg_extern(name="irate_left", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_irate_left(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal_counter_summary().irate_left()
}

#[pg_extern(name="irate_right", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_irate_right(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal_counter_summary().irate_right()
}

#[pg_extern(name="idelta_left", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_idelta_left(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> f64 {
    summary.to_internal_counter_summary().idelta_left()
}

#[pg_extern(name="idelta_right", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_idelta_right(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> f64 {
    summary.to_internal_counter_summary().idelta_right()
}

#[pg_extern(name="with_bounds", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_with_bounds(
    summary: timescale_analytics_experimental::CounterSummary,
    bounds: tstzrange,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> timescale_analytics_experimental::CounterSummary{
    unsafe{
        let ptr = bounds as *mut pg_sys::varlena;
        let mut summary = summary.to_internal_counter_summary();
        summary.bounds = get_range(ptr);
        CounterSummary::from_internal_counter_summary(summary)
    }
}

#[pg_extern(name="extrapolated_delta", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_extrapolated_delta(
    summary: timescale_analytics_experimental::CounterSummary,
    method: String,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.to_lowercase().as_str() {
        "prometheus" => {
            summary.to_internal_counter_summary().prometheus_delta().unwrap()
        },
        _ => panic!("unknown method"),
    }
}

#[pg_extern(name="extrapolated_rate", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_extrapolated_rate(
    summary: timescale_analytics_experimental::CounterSummary,
    method: String,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.to_lowercase().as_str() {
        "prometheus" => {
            summary.to_internal_counter_summary().prometheus_rate().unwrap()
        },
        _ => panic!("unknown method"),
    }
}

#[pg_extern(name="num_elements", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_num_elements(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> i64 {
    summary.to_internal_counter_summary().regress.n as i64
}

#[pg_extern(name="num_changes", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_num_changes(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> i64 {
    summary.to_internal_counter_summary().num_changes as i64
}

#[pg_extern(name="num_resets", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_num_resets(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> i64 {
    summary.to_internal_counter_summary().num_resets as i64
}

#[pg_extern(name="slope", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_slope(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal_counter_summary().regress.slope()
}

#[pg_extern(name="intercept", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_intercept(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal_counter_summary().regress.intercept()
}

#[pg_extern(name="corr", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_corr(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal_counter_summary().regress.corr()
}

#[pg_extern(name="counter_zero_time", schema = "timescale_analytics_experimental", strict, immutable)]
fn counter_agg_counter_zero_time(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<pg_sys::TimestampTz> {
    Some((summary.to_internal_counter_summary().regress.x_intercept()? * 1_000_000.0) as i64)
}




#[cfg(any(test, feature = "pg_test"))]
mod tests {

    use approx::assert_relative_eq;
    use pgx::*;
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

    //do proper numerical comparisons on the values where that matters, use exact where it should be exact.
    // copied from counter_agg crate
    #[track_caller]
    fn assert_close_enough(p1:&InternalCounterSummary, p2:&InternalCounterSummary) {
        assert_eq!(p1.first, p2.first, "first");
        assert_eq!(p1.second, p2.second, "second");
        assert_eq!(p1.penultimate, p2.penultimate, "penultimate");
        assert_eq!(p1.last, p2.last, "last");
        assert_eq!(p1.num_changes, p2.num_changes, "num_changes");
        assert_eq!(p1.num_resets, p2.num_resets, "num_resets");
        assert_eq!(p1.regress.n, p2.regress.n, "n");
        assert_relative_eq!(p1.regress.sx, p2.regress.sx);
        assert_relative_eq!(p1.regress.sxx, p2.regress.sxx);
        assert_relative_eq!(p1.regress.sy, p2.regress.sy);
        assert_relative_eq!(p1.regress.syy, p2.regress.syy);
        assert_relative_eq!(p1.regress.sxy, p2.regress.sxy);
    }

    #[pg_test]
    fn test_counter_aggregate() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)", None, None);
            // set search_path after defining our table so we don't pollute the wrong schema
            let stmt = "SELECT format('timescale_analytics_experimental, %s',current_setting('search_path'))";
            let search_path = select_one!(client, stmt, String);
            client.select(&format!("SET LOCAL search_path TO {}", search_path), None, None);
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:00:00+00', 10.0), ('2020-01-01 00:01:00+00', 20.0)";
            client.select(stmt, None, None);

            // NULL bounds are equivalent to none provided
            let stmt = "SELECT counter_agg(ts, val) FROM test";
            let a = select_one!(client,stmt, timescale_analytics_experimental::CounterSummary);
            let stmt = "SELECT counter_agg(ts, val, NULL::tstzrange) FROM test";
            let b = select_one!(client,stmt, timescale_analytics_experimental::CounterSummary);
            assert_eq!(a, b);

            let stmt = "SELECT delta(counter_agg(ts, val)) FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), 10.0);

            let stmt = "SELECT time_delta(counter_agg(ts, val)) FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), 60.0);

            let stmt = "SELECT extrapolated_delta(counter_agg(ts, val, '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00+00)'), 'prometheus') FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), 20.0);
            // doesn't matter if we set the bounds before or after
            let stmt = "SELECT extrapolated_delta(with_bounds(counter_agg(ts, val), '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00+00)'), 'prometheus') FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), 20.0);

            let stmt = "SELECT extrapolated_rate(counter_agg(ts, val, '[2020-01-01 00:00:00+00, 2020-01-01 00:02:00+00)'), 'prometheus') FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), 20.0 / 120.0);

            let stmt = "INSERT INTO test VALUES('2020-01-01 00:02:00+00', 10.0), ('2020-01-01 00:03:00+00', 20.0), ('2020-01-01 00:04:00+00', 10.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT slope(counter_agg(ts, val)) FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), 10.0 / 60.0);

            let stmt = "SELECT intercept(counter_agg(ts, val)) FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), -105191990.0);

            let stmt = "SELECT corr(counter_agg(ts, val)) FROM test";
            assert_relative_eq!(select_one!(client, stmt, f64), 1.0);

            let stmt = "SELECT counter_zero_time(counter_agg(ts, val)) FROM test";
            let zp = select_one!(client, stmt, i64);
            let real_zp = select_one!(client, "SELECT '2019-12-31 23:59:00+00'::timestamptz", i64);
            assert_eq!(zp, real_zp);

            let stmt = "INSERT INTO test VALUES('2020-01-01 00:08:00+00', 30.0), ('2020-01-01 00:10:00+00', 30.0), ('2020-01-01 00:10:30+00', 10.0), ('2020-01-01 00:20:00+00', 40.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT num_elements(counter_agg(ts, val)) FROM test";
            assert_eq!(select_one!(client, stmt, i64), 9);

            let stmt = "SELECT num_resets(counter_agg(ts, val)) FROM test";
            assert_eq!(select_one!(client, stmt, i64), 3);

            let stmt = "SELECT num_changes(counter_agg(ts, val)) FROM test";
            assert_eq!(select_one!(client, stmt, i64), 7);

            //combine function works as expected
            let stmt = "SELECT counter_agg(ts, val) FROM test";
            let a = select_one!(client,stmt, timescale_analytics_experimental::CounterSummary);
            let stmt = "WITH t as (SELECT date_trunc('minute', ts), counter_agg(ts, val) as agg FROM test group by 1 ) SELECT counter_agg(agg) FROM t";
            let b = select_one!(client,stmt, timescale_analytics_experimental::CounterSummary);
            assert_close_enough(&a.to_internal_counter_summary(), &b.to_internal_counter_summary());
        });
    }

    // #[pg_test]
    // fn test_combine_aggregate(){
    //     Spi::execute(|client| {

    //     });
    // }
}