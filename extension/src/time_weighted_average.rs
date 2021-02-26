
use serde::{Serialize, Deserialize};
use pg_sys::TimestampTz;
use pg_sys::Datum;
use std::slice;

use flat_serialize::*;
use pgx::*;
use crate::{
    aggregate_utils::{aggregate_mctx, in_aggregate_context},
    flatten,
    json_inout_funcs,
    palloc::{Internal, in_memory_context}, pg_type
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
extension_sql!(r#"set search_path to 'timescale_analytics_experimental', 'public';"#);

// This assumes ordered input and will not sort interally, will error if input is unsorted.
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_ordered_trans(
    state: Option<Internal<TimeWeightSummary>>,
    method: String,
    ts: Option<pg_sys::TimestampTz>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimeWeightSummary>> {
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
                &_ => panic!()
            };
            let state = match state {
                None => TimeWeightSummary::new(p, method).into(),
                Some(state) => {state.clone().accum(p).unwrap(); state},
            };
            Some(state)
        })
    }
}

// requires moderately ordered states, not for this function call as either can be first or second, but for multiple it will need to be ordered or it'll error
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_ordered_combine(
    state1: Option<Internal<TimeWeightSummary>>,
    state2: Option<Internal<TimeWeightSummary>>,
    fcinfo: pg_sys::FunctionCallInfo,
)  -> Option<Internal<TimeWeightSummary>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => {
                    if state1.first.ts < state2.first.ts {
                        Some(state1.combine(&state2).unwrap().clone().into())
                    } else {
                        Some(state2.combine(&state1).unwrap().clone().into())
                    }
                }
            }
        })
    }
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_summary_serialize(
    state: Internal<TimeWeightSummary>,
) -> bytea {
    crate::do_serialize!(state)
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_summary_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TimeWeightSummary> {
    crate::do_deserialize!(bytes, TimeWeightSummary)
}

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
    fn to_TimeWeightSummary(&self) -> TimeWeightSummary {
        TimeWeightSummary{
            method: *self.method,
            first: *self.first,
            last: *self.last,
            w_sum: *self.w_sum,
        }
    }
}


// trans function for the aggregate over the exposed summary
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_summary_ordered_trans(
    state: Option<Internal<TimeWeightSummary>>,
    next: Option<time_weight_summary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimeWeightSummary>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, next) {
                (None, None) => None,
                (None, Some(next)) => Some(next.to_TimeWeightSummary().clone().into()),
                (Some(state), None) => Some(state),
                (Some(state), Some(next)) =>  Some(state.combine(&next.to_TimeWeightSummary()).unwrap().into())
            }
        })
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
fn time_weight_ordered_final(
    state: Option<Internal<TimeWeightSummary>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<time_weight_summary<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };

            flatten!(
                time_weight_summary {
                    method: &state.method,
                    first: &state.first,
                    last: &state.last,
                    w_sum: &state.w_sum,
                }
            ).into()
        })
    }
}

extension_sql!(r#"
CREATE OR REPLACE FUNCTION time_weight_summary_in(cstring) RETURNS time_weight_summary IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'time_weight_summary_in_wrapper';
CREATE OR REPLACE FUNCTION time_weight_summary_out(time_weight_summary) RETURNS CString IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'time_weight_summary_out_wrapper';

CREATE TYPE time_weight_summary (
    INTERNALLENGTH = variable,
    INPUT = time_weight_summary_in,
    OUTPUT = time_weight_summary_out,
    STORAGE = extended
);

CREATE AGGREGATE time_weight_ordered(method text, ts timestamptz, value DOUBLE PRECISION)
(
    sfunc = time_weight_ordered_trans,
    stype = internal,
    finalfunc = time_weight_ordered_final,
    combinefunc = time_weight_ordered_combine,
    serialfunc = time_weight_summary_serialize,
    deserialfunc = time_weight_summary_deserialize,
    parallel = restricted
);

CREATE AGGREGATE time_weight_ordered(tws time_weight_summary)
(
    sfunc = time_weight_summary_ordered_trans,
    stype = internal,
    finalfunc = time_weight_ordered_final,
    combinefunc = time_weight_ordered_combine,
    serialfunc = time_weight_summary_serialize,
    deserialfunc = time_weight_summary_deserialize,
    parallel = restricted
);
"#);


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeWeightTransState {
    #[serde(skip)]
    buffer: Vec<TSPoint>,
    method: Option<TimeWeightMethod>,
    combine_buffer: Vec<TimeWeightSummary>,
}

impl TimeWeightTransState {
    fn push(&mut self, value: TSPoint, method: TimeWeightMethod) {
        match self.method {
            None => {self.method = Some(method)},
            Some(m)=> {if m != method {panic!("Mismatched methods")}}
        }
        self.buffer.push(value);
    }

    fn sort_and_calc(&mut self) {
        if self.buffer.is_empty() {
            return
        }
        self.buffer.sort_unstable_by_key(|p| p.ts);
        match self.method {
            None => panic!("invalid state"), // this shouldn't be None if the buffer is not empty
            Some(m)=>self.combine_buffer.push(TimeWeightSummary::new_from_sorted_iter(&self.buffer, m).unwrap()),
        };
        self.buffer.clear();
    }
    fn combine_push(&mut self, other: &TimeWeightTransState) {
        let cb = other.combine_buffer.clone();
        for val in cb.into_iter(){
            self.combine_buffer.push(val);
        };
    }
    fn combine_sort_and_calc(&mut self) {
        if self.combine_buffer.len() <= 1 {
            return
        }
        self.combine_buffer.sort_unstable_by_key(|s| s.first.ts);
        self.combine_buffer = vec![TimeWeightSummary::combine_sorted_iter(&self.combine_buffer).unwrap()];
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_trans_serialize(
    mut state: Internal<TimeWeightTransState>,
) -> bytea {
    state.sort_and_calc();
    state.combine_sort_and_calc();
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
                None => {let mut s = TimeWeightTransState{buffer: vec![], method: None, combine_buffer: vec![] }; s.push(p, method); Some(s.into())},
                Some(mut s) => {s.push(p, method); Some(s)},
            }
        })
    }
}


#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn time_weight_summary_trans(
    state: Option<Internal<TimeWeightTransState>>,
    next: Option<time_weight_summary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimeWeightTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, next) {
                (None, None) => None,
                (None, Some(next)) => Some(TimeWeightTransState{combine_buffer:vec![next.to_TimeWeightSummary()], buffer: vec![], method: Some(*next.method)}.into()),
                (Some(state), None) => Some(state),
                (Some(mut state), Some(next)) =>  {
                    let next = TimeWeightTransState{combine_buffer:vec![next.to_TimeWeightSummary()], buffer: vec![], method: Some(*next.method)};
                    state.combine_push(&next); 
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
                (None, Some(state2)) => {let mut s = state2.clone(); s.sort_and_calc(); Some(s.into())},
                (Some(state1), None) => {let mut s = state1.clone(); s.sort_and_calc(); Some(s.into())}, //should I make these return themselves?
                (Some(state1), Some(state2)) => {
                    let mut s1 = state1.clone(); // is there a way to avoid if it doesn't need it?
                    s1.sort_and_calc(); 
                    let mut s2 = state2.clone();
                    s2.sort_and_calc();
                    s2.combine_push(&s1);
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
) -> Option<time_weight_summary<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state.clone(),
            };
            state.sort_and_calc();
            state.combine_sort_and_calc();
            debug_assert!(state.combine_buffer.len() <= 1);
            match state.combine_buffer.pop() {
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


CREATE AGGREGATE time_weight(method text, ts timestamptz, value DOUBLE PRECISION)
(
    sfunc = time_weight_trans,
    stype = internal,
    finalfunc = time_weight_final,
    combinefunc = time_weight_combine,
    serialfunc = time_weight_trans_serialize,
    deserialfunc = time_weight_trans_deserialize,
    parallel = restricted
);

CREATE AGGREGATE time_weight(tws time_weight_summary)
(
    sfunc = time_weight_summary_trans,
    stype = internal,
    finalfunc = time_weight_final,
    combinefunc = time_weight_combine,
    serialfunc = time_weight_trans_serialize,
    deserialfunc = time_weight_trans_deserialize,
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


extension_sql!(r#"reset search_path;"#);
