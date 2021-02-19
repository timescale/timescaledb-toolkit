
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
//
//
// time_weight_ordered()
// time_weight()
// WITH t as (SELECT time_bucket('5 min', ts) as bucket, id,  time_weight(ts, value, method=>'locf') as tw
// FROM foo
// WHERE ts > '2020-10-01' and ts <= '2020-10-02'
// [AND id IN ('foo', 'bar', 'baz')]
// GROUP BY 1, 2 )
// SELECT bucket, id, average(with_bounds(tw, bounds => time_bucket_range(bucket, '5 min'), prev=>(SELECT tspoint(ts, value) FROM foo f WHERE f.id = t.id AND f.ts < '2020-10-01' ORDER BY ts DESC LIMIT 1)) OVER (PARTITION BY id ORDER BY bucket ASC ))
// FROM t;
// with_bounds(time_weight(ts, val, 'linear'), bounds=> tstzrange, prev => tspoint(), next => tspoint())
// SELECT average(time_weight_ordered(time, value, 'linear'))
// SELECT time_weight_ordered(time, value, 'linear') |> with_bounds(bounds)

// This assumes ordered input and will nott sort interally, will error if input is unsorted.
#[pg_extern]
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
                (None, None) => return state,
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
#[pg_extern]
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

#[pg_extern]
pub fn time_weight_serialize(
    mut state: Internal<TimeWeightSummary>,
) -> bytea {
    crate::do_serialize!(state)
}

#[pg_extern]
pub fn time_weight_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TimeWeightSummary> {
    crate::do_deserialize!(bytes, TimeWeightSummary)
}

extension_sql!(r#"
CREATE TYPE time_weight_summary;
"#);

// PG object for the digest.
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
    fn to_time_weight_summary(&self) -> TimeWeightSummary {
        TimeWeightSummary{
            method: *self.method,
            first: *self.first,
            last: *self.last,
            w_sum: *self.w_sum,
        }
    }
}

#[pg_extern]
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
    sfunc=time_weight_ordered_trans,
    stype=internal,
    finalfunc=time_weight_ordered_final,
    combinefunc=time_weight_ordered_combine,
    serialfunc = time_weight_serialize,
    deserialfunc = time_weight_deserialize,
    parallel = restricted
);
"#);


#[derive(Debug, Serialize, Deserialize)]
pub struct TimeWeightTransState {
    #[serde(skip_serializing)]
    buffer: Vec<TSPoint>,
    computed: TimeWeightSummary,
}

