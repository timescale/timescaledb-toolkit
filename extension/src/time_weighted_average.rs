#![allow(non_camel_case_types)]

use serde::{Serialize, Deserialize};
use pg_sys::Datum;
use std::slice;

use flat_serialize::*;
use pgx::*;
use crate::{
    aggregate_utils::{ in_aggregate_context},
    flatten,
    json_inout_funcs,
    palloc::{Internal}, pg_type
};

use time_weighted_average::{
    tspoint::TSPoint, 
    TimeWeightSummary,
    TimeWeightError,
    TimeWeightMethod,
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
CREATE TYPE timescale_analytics_experimental.time_weight_summary;
"#);

pg_type! {
    #[derive(Debug)]
    struct time_weight_summary {
        first: TSPoint,
        last: TSPoint,
        w_sum: f64,
        method: TimeWeightMethod,
    }
}

json_inout_funcs!(time_weight_summary);

impl<'input> time_weight_summary<'input> {
    #[allow(non_snake_case)]
    fn to_TimeWeightSummary(&self) -> TimeWeightSummary {
        TimeWeightSummary{
            method: *self.method,
            first: *self.first,
            last: *self.last,
            w_sum: *self.w_sum,
        }
    }
}

extension_sql!(r#"
CREATE OR REPLACE FUNCTION timescale_analytics_experimental.time_weight_summary_in(cstring) RETURNS timescale_analytics_experimental.time_weight_summary IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'time_weight_summary_in_wrapper';
CREATE OR REPLACE FUNCTION timescale_analytics_experimental.time_weight_summary_out(timescale_analytics_experimental.time_weight_summary) RETURNS CString IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'time_weight_summary_out_wrapper';

CREATE TYPE timescale_analytics_experimental.time_weight_summary (
    INTERNALLENGTH = variable,
    INPUT = timescale_analytics_experimental.time_weight_summary_in,
    OUTPUT = timescale_analytics_experimental.time_weight_summary_out,
    STORAGE = extended
);
"#);


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeWeightTransState {
    #[serde(skip)]
    point_buffer: Vec<TSPoint>,
    method: Option<TimeWeightMethod>,
    summary_buffer: Vec<TimeWeightSummary>,
}

impl TimeWeightTransState {
    fn push_point(&mut self, value: TSPoint, method: TimeWeightMethod) {
        match self.method {
            None => {self.method = Some(method)},
            Some(m)=> {if m != method {panic!("Mismatched methods")}}
        }
        self.point_buffer.push(value);
    }

    fn combine_points(&mut self) {
        if self.point_buffer.is_empty() {
            return
        }
        self.point_buffer.sort_unstable_by_key(|p| p.ts);
        match self.method {
            None => panic!("invalid state"), // this shouldn't be None if the point_buffer is not empty
            Some(m)=>self.summary_buffer.push(TimeWeightSummary::new_from_sorted_iter(&self.point_buffer, m).unwrap()),
        };
        self.point_buffer.clear();
    }
    fn push_summary(&mut self, other: &TimeWeightTransState) {
        let cb = other.summary_buffer.clone();
        for val in cb.into_iter(){
            self.summary_buffer.push(val);
        };
    }
    fn combine_summaries(&mut self) {
        if self.summary_buffer.len() <= 1 {
            return
        }
        self.summary_buffer.sort_unstable_by_key(|s| s.first.ts);
        self.summary_buffer = vec![TimeWeightSummary::combine_sorted_iter(&self.summary_buffer).unwrap()];
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_trans_serialize(
    mut state: Internal<TimeWeightTransState>,
) -> bytea {
    state.combine_points();
    state.combine_summaries();
    crate::do_serialize!(state)
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_trans_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TimeWeightTransState> {
    crate::do_deserialize!(bytes, TimeWeightTransState)
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_trans(
    state: Option<Internal<TimeWeightTransState>>,
    method: String,
    ts: Option<pg_sys::TimestampTz>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimeWeightTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state, 
                (Some(ts), Some(val)) => TSPoint{ts, val},
            };
            let method = match method.to_lowercase().as_str() {
                "linear"=> TimeWeightMethod::Linear,
                "locf" => TimeWeightMethod::LOCF,
                &_ => panic!("unknown method")
            };
            match state {
                None => {
                    let mut s = TimeWeightTransState{point_buffer: vec![], method: None, summary_buffer: vec![]}; 
                    s.push_point(p, method); 
                    Some(s.into())
                },
                Some(mut s) => {s.push_point(p, method); Some(s)},
            }
        })
    }
}


#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_summary_trans(
    state: Option<Internal<TimeWeightTransState>>,
    next: Option<timescale_analytics_experimental::time_weight_summary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimeWeightTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, next) {
                (None, None) => None,
                (None, Some(next)) => Some(TimeWeightTransState{summary_buffer:vec![next.to_TimeWeightSummary()], point_buffer: vec![], method: Some(*next.method)}.into()),
                (Some(state), None) => Some(state),
                (Some(mut state), Some(next)) =>  {
                    let next = TimeWeightTransState{summary_buffer:vec![next.to_TimeWeightSummary()], point_buffer: vec![], method: Some(*next.method)};
                    state.push_summary(&next); 
                    Some(state.into())
                },
            }
        })
    }
}


#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_combine(
    state1: Option<Internal<TimeWeightTransState>>,
    state2: Option<Internal<TimeWeightTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
)  -> Option<Internal<TimeWeightTransState>> {
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
fn time_weight_final(
    state: Option<Internal<TimeWeightTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<timescale_analytics_experimental::time_weight_summary<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state.clone(),
            };
            state.combine_points();
            state.combine_summaries();
            debug_assert!(state.summary_buffer.len() <= 1);
            match state.summary_buffer.pop() {
                None => None,
                Some(st) => Some(
                    flatten!(
                        time_weight_summary {
                            method: &st.method,
                            first: &st.first,
                            last: &st.last,
                            w_sum: &st.w_sum,
                    }
                ).into())
            }
        })
    }
}

extension_sql!(r#"


CREATE AGGREGATE timescale_analytics_experimental.time_weight(method text, ts timestamptz, value DOUBLE PRECISION)
(
    sfunc = timescale_analytics_experimental.time_weight_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.time_weight_final,
    combinefunc = timescale_analytics_experimental.time_weight_combine,
    serialfunc = timescale_analytics_experimental.time_weight_trans_serialize,
    deserialfunc = timescale_analytics_experimental.time_weight_trans_deserialize,
    parallel = restricted
);

CREATE AGGREGATE timescale_analytics_experimental.time_weight(tws timescale_analytics_experimental.time_weight_summary)
(
    sfunc = timescale_analytics_experimental.time_weight_summary_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.time_weight_final,
    combinefunc = timescale_analytics_experimental.time_weight_combine,
    serialfunc = timescale_analytics_experimental.time_weight_trans_serialize,
    deserialfunc = timescale_analytics_experimental.time_weight_trans_deserialize,
    parallel = restricted
);
"#);

#[pg_extern(name="average", schema = "timescale_analytics_experimental")]
pub fn time_weighted_average_average(
    tws: Option<timescale_analytics_experimental::time_weight_summary>,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<f64> {
    match tws {
        None => None,
        Some(tws) => 
            match tws.to_TimeWeightSummary().time_weighted_average(None, None) {
                Ok(a) => Some(a),
                //without bounds, the average for a single value is undefined, but it probably shouldn't throw an error, we'll return null for now. 
                Err(e) => if e == TimeWeightError::ZeroDuration {None} else {Err(e).unwrap()}
            }
    }
}




#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    
    #[pg_test]
    fn test_time_weight_aggregate(){
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)", None, None);
            // set search_path after defining our table so we don't pollute the wrong schema
            let search_path = client.select("SELECT format('timescale_analytics_experimental, %s',current_setting('search_path'))", None, None).first().get_one::<String>();
            client.select(&format!("SET LOCAL search_path TO {}", search_path.unwrap()), None, None);
            client.select("INSERT INTO test VALUES('2020-01-01 00:00:00+00', 10.0), ('2020-01-01 00:01:00+00', 20.0)", None, None);
            // test basic with 2 points
            let simple = client.select("SELECT average(time_weight('Linear', ts, val)) FROM test", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 15.0);
            let simple = client.select("SELECT average(time_weight('LOCF', ts, val)) FROM test", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 10.0);
            
            // more values evenly spaced
            client.select("INSERT INTO test VALUES('2020-01-01 00:02:00+00', 10.0), ('2020-01-01 00:03:00+00', 20.0), ('2020-01-01 00:04:00+00', 10.0)", None, None);
            let simple = client.select("SELECT average(time_weight('Linear', ts, val)) FROM test", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 15.0);
            let simple = client.select("SELECT average(time_weight('LOCF', ts, val)) FROM test", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 15.0);

            //non-evenly spaced values
            client.select("INSERT INTO test VALUES('2020-01-01 00:08:00+00', 30.0), ('2020-01-01 00:10:00+00', 10.0), ('2020-01-01 00:10:30+00', 20.0), ('2020-01-01 00:20:00+00', 30.0)", None, None);
            let simple = client.select("SELECT average(time_weight('Linear', ts, val)) FROM test", None, None).first().get_one::<f64>();
            // expected =(15 +15 +15 +15 + 20*4 + 20*2 +15*.5 + 25*9.5) / 20 = 21.25 just taking the midpoints between each point and multiplying by minutes and dividing by total
            assert_eq!(simple.unwrap(), 21.25);
            let simple = client.select("SELECT average(time_weight('LOCF', ts, val)) FROM test", None, None).first().get_one::<f64>();
            // expected = (10 + 20 + 10 + 20 + 10*4 + 30*2 +10*.5 + 20*9.5) / 20 = 17.75 using last value and carrying for each point
            assert_eq!(simple.unwrap(), 17.75);

            //make sure this works with whatever ordering we throw at it
            let simple = client.select("SELECT average(time_weight('Linear', ts, val ORDER BY random())) FROM test", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 21.25);
            let simple = client.select("SELECT average(time_weight('LOCF', ts, val ORDER BY random())) FROM test", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 17.75);

            // make sure we get the same result if we do multi-level aggregation (though these will only have )
            let simple = client.select("WITH t AS (SELECT date_trunc('minute', ts), time_weight('Linear', ts, val) AS tws FROM test GROUP BY 1) SELECT average(time_weight(tws)) FROM t", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 21.25);
            let simple = client.select("WITH t AS (SELECT date_trunc('minute', ts), time_weight('LOCF', ts, val) AS tws FROM test GROUP BY 1) SELECT average(time_weight(tws)) FROM t", None, None).first().get_one::<f64>();
            assert_eq!(simple.unwrap(), 17.75);
    });
        
    }
    
}

