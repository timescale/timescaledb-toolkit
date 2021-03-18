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
    palloc::Internal, pg_type
};

use time_weighted_average::{
    tspoint::TSPoint,
};

use counter_agg::{
    CounterSummary as InternalCounterSummary,
};

// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
mod timescale_analytics_experimental {
    pub(crate) use super::*;
    extension_sql!(r#"
        CREATE SCHEMA IF NOT EXISTS timescale_analytics_experimental;
    "#);
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

extension_sql!(r#"
CREATE TYPE timescale_analytics_experimental.CounterSummary;
"#);

pg_type! {
    #[derive(Debug)]
    struct CounterSummary {
        first: TSPoint,
        second: TSPoint,
        penultimate:TSPoint,
        last: TSPoint,
        reset_sum: f64,
        num_resets: u64,
        num_changes: u64,
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
        }
    }
}

extension_sql!(r#"
CREATE OR REPLACE FUNCTION timescale_analytics_experimental.counter_summary_in(cstring) RETURNS timescale_analytics_experimental.CounterSummary IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'countersummary_in_wrapper';
CREATE OR REPLACE FUNCTION timescale_analytics_experimental.counter_summary_out(timescale_analytics_experimental.CounterSummary) RETURNS CString IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'countersummary_out_wrapper';

CREATE TYPE timescale_analytics_experimental.CounterSummary (
    INTERNALLENGTH = variable,
    INPUT = timescale_analytics_experimental.counter_summary_in,
    OUTPUT = timescale_analytics_experimental.counter_summary_out,
    STORAGE = extended
);
"#);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CounterSummaryTransState {
    #[serde(skip)]
    point_buffer: Vec<TSPoint>,
    // We have a summary buffer here in order to deal with the fact that when the cmobine function gets called it
    // must first build up a buffer of InternalMetricSummaries, then sort them, then call the combine function in 
    // the correct order. 
    summary_buffer: Vec<InternalCounterSummary>,
}

impl CounterSummaryTransState {
    fn push_point(&mut self, value: TSPoint) {
        self.point_buffer.push(value);
    }

    fn combine_points(&mut self) {
        if self.point_buffer.is_empty() {
            return
        }
        self.point_buffer.sort_unstable_by_key(|p| p.ts);
        let mut iter = self.point_buffer.iter();
        let mut summary = InternalCounterSummary::new( iter.next().unwrap());
        for p in iter {
            summary.add_point(p).unwrap();
        }
        self.point_buffer.clear();
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
        for sum in sum_iter.next() {
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
                    let mut s = CounterSummaryTransState{point_buffer: vec![], summary_buffer: vec![]};
                    s.push_point(p);
                    Some(s.into())
                },
                Some(mut s) => {s.push_point(p); Some(s)},
            }
        })
    }
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
                (None, Some(value)) => Some(CounterSummaryTransState{point_buffer: vec![], summary_buffer: vec![value.to_internal_counter_summary()]}.into()),
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
                Some(st) => Some(
                    flatten!(
                        CounterSummary {
                            first: &st.first,
                            second: &st.second,
                            penultimate: &st.penultimate,
                            last: &st.last,
                            reset_sum: &st.reset_sum,
                            num_resets: &st.num_resets,
                            num_changes: &st.num_changes,
                    }
                ).into())
            }
        })
    }
}


extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.counter_agg( ts timestamptz, value DOUBLE PRECISION)
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

#[pg_extern(name="delta", schema = "timescale_analytics_experimental")]
fn counter_agg_delta(
    summary: timescale_analytics_experimental::CounterSummary,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> f64 {
    summary.to_internal_counter_summary().delta()
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;
 

    #[pg_test]
    fn test_counter_aggregate(){
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)", None, None);
            // set search_path after defining our table so we don't pollute the wrong schema
            let search_path = client.select("SELECT format('timescale_analytics_experimental, %s',current_setting('search_path'))", None, None).first().get_one::<String>();
            client.select(&format!("SET LOCAL search_path TO {}", search_path.unwrap()), None, None);
            client.select("INSERT INTO test VALUES('2020-01-01 00:00:00+00', 10.0), ('2020-01-01 00:01:00+00', 20.0)", None, None);

            client.select("SELECT counter_agg(ts, val) FROM test", None, None).first().get_one::<String>().unwrap();
            let result = client.select("SELECT delta(counter_agg(ts, val)) FROM test", None, None).first().get_one::<f64>().unwrap();
            assert_eq!(result, 10.0);
        });
    }

    // #[pg_test]
    // fn test_combine_aggregate(){
    //     Spi::execute(|client| {
            
    //     });
    // }
}