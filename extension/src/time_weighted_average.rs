#![allow(non_camel_case_types)]

use serde::{Deserialize, Serialize};
use std::slice;

use crate::{
    aggregate_utils::in_aggregate_context, flatten, json_inout_funcs, palloc::Internal, pg_type,
};
use flat_serialize::*;
use pgx::*;

use time_weighted_average::{
    tspoint::TSPoint, TimeWeightError, TimeWeightMethod,
    TimeWeightSummary as TimeWeightSummaryInternal,
};

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

pg_type! {
    #[derive(Debug)]
    struct TimeWeightSummary {
        first: TSPoint,
        last: TSPoint,
        w_sum: f64,
        method: TimeWeightMethod,
    }
}

json_inout_funcs!(TimeWeightSummary);

varlena_type!(TimeWeightSummary);

impl<'input> TimeWeightSummary<'input> {
    #[allow(non_snake_case)]
    fn to_internal(&self) -> TimeWeightSummaryInternal {
        TimeWeightSummaryInternal {
            method: *self.method,
            first: *self.first,
            last: *self.last,
            w_sum: *self.w_sum,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeWeightTransState {
    #[serde(skip)]
    point_buffer: Vec<TSPoint>,
    method: TimeWeightMethod,
    summary_buffer: Vec<TimeWeightSummaryInternal>,
}

impl TimeWeightTransState {
    fn push_point(&mut self, value: TSPoint) {
        self.point_buffer.push(value);
    }

    fn combine_points(&mut self) {
        if self.point_buffer.is_empty() {
            return;
        }
        self.point_buffer.sort_unstable_by_key(|p| p.ts);
        self.summary_buffer.push(
            TimeWeightSummaryInternal::new_from_sorted_iter(&self.point_buffer, self.method)
                .unwrap(),
        );
        self.point_buffer.clear();
    }

    fn push_summary(&mut self, other: &TimeWeightTransState) {
        let cb = other.summary_buffer.clone();
        for val in cb.into_iter() {
            self.summary_buffer.push(val);
        }
    }

    fn combine_summaries(&mut self) {
        self.combine_points();
        if self.summary_buffer.len() <= 1 {
            return;
        }
        self.summary_buffer.sort_unstable_by_key(|s| s.first.ts);
        self.summary_buffer =
            vec![TimeWeightSummaryInternal::combine_sorted_iter(&self.summary_buffer).unwrap()];
    }
}

#[pg_extern()]
pub fn time_weight_trans_serialize(mut state: Internal<TimeWeightTransState>) -> bytea {
    state.combine_summaries();
    crate::do_serialize!(state)
}

#[pg_extern(strict)]
pub fn time_weight_trans_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TimeWeightTransState> {
    crate::do_deserialize!(bytes, TimeWeightTransState)
}

#[pg_extern()]
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
                (Some(ts), Some(val)) => TSPoint { ts, val },
            };

            match state {
                None => {
                    let mut s = TimeWeightTransState {
                        point_buffer: vec![],
                        // TODO technically not portable to ASCII-compatible charsets
                        method: match method.to_lowercase().as_str() {
                            "linear" => TimeWeightMethod::Linear,
                            "locf" => TimeWeightMethod::LOCF,
                            _ => panic!("unknown method"),
                        },
                        summary_buffer: vec![],
                    };
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

#[pg_extern()]
pub fn time_weight_summary_trans(
    state: Option<Internal<TimeWeightTransState>>,
    next: Option<TimeWeightSummary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimeWeightTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, next) {
            (None, None) => None,
            (None, Some(next)) => Some(
                TimeWeightTransState {
                    summary_buffer: vec![next.to_internal()],
                    point_buffer: vec![],
                    method: *next.method,
                }
                .into(),
            ),
            (Some(state), None) => Some(state),
            (Some(mut state), Some(next)) => {
                let next = TimeWeightTransState {
                    summary_buffer: vec![next.to_internal()],
                    point_buffer: vec![],
                    method: *next.method,
                };
                state.push_summary(&next);
                Some(state.into())
            }
        })
    }
}

#[pg_extern()]
pub fn time_weight_combine(
    state1: Option<Internal<TimeWeightTransState>>,
    state2: Option<Internal<TimeWeightTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimeWeightTransState>> {
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
                }
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

#[pg_extern()]
fn time_weight_final(
    state: Option<Internal<TimeWeightTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TimeWeightSummary<'static>> {
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
                    flatten!(TimeWeightSummary {
                        method: &st.method,
                        first: &st.first,
                        last: &st.last,
                        w_sum: &st.w_sum,
                    })
                    .into(),
                ),
            }
        })
    }
}

extension_sql!(
    r#"
CREATE AGGREGATE time_weight(method text, ts timestamptz, value DOUBLE PRECISION)
(
    sfunc = time_weight_trans,
    stype = internal,
    finalfunc = time_weight_final,
    combinefunc = time_weight_combine,
    serialfunc = time_weight_trans_serialize,
    deserialfunc = time_weight_trans_deserialize,
    parallel = restricted,
    finalfunc_modify = shareable
);

CREATE AGGREGATE time_weight(tws TimeWeightSummary)
(
    sfunc = time_weight_summary_trans,
    stype = internal,
    finalfunc = time_weight_final,
    combinefunc = time_weight_combine,
    serialfunc = time_weight_trans_serialize,
    deserialfunc = time_weight_trans_deserialize,
    parallel = restricted
);
"#
);

#[pg_extern(immutable, parallel_safe, name = "average")]
pub fn time_weighted_average_average(
    tws: Option<TimeWeightSummary>,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<f64> {
    match tws {
        None => None,
        Some(tws) => match tws.to_internal().time_weighted_average() {
            Ok(a) => Some(a),
            //without bounds, the average for a single value is undefined, but it probably shouldn't throw an error, we'll return null for now.
            Err(e) => {
                if e == TimeWeightError::ZeroDuration {
                    None
                } else {
                    Err(e).unwrap()
                }
            }
        },
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;
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
    fn test_time_weight_aggregate() {
        Spi::execute(|client| {
            let stmt = "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)";
            client.select(stmt, None, None);

            // add a couple points
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:00:00+00', 10.0), ('2020-01-01 00:01:00+00', 20.0)";
            client.select(stmt, None, None);

            // test basic with 2 points
            let stmt = "SELECT average(time_weight('Linear', ts, val)) FROM test";
            assert_eq!(select_one!(client, stmt, f64), 15.0);
            let stmt = "SELECT average(time_weight('LOCF', ts, val)) FROM test";
            assert_eq!(select_one!(client, stmt, f64), 10.0);

            // more values evenly spaced
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:02:00+00', 10.0), ('2020-01-01 00:03:00+00', 20.0), ('2020-01-01 00:04:00+00', 10.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT average(time_weight('Linear', ts, val)) FROM test";
            assert_eq!(select_one!(client, stmt, f64), 15.0);
            let stmt = "SELECT average(time_weight('LOCF', ts, val)) FROM test";
            assert_eq!(select_one!(client, stmt, f64), 15.0);

            //non-evenly spaced values
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:08:00+00', 30.0), ('2020-01-01 00:10:00+00', 10.0), ('2020-01-01 00:10:30+00', 20.0), ('2020-01-01 00:20:00+00', 30.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT average(time_weight('Linear', ts, val)) FROM test";
            // expected =(15 +15 +15 +15 + 20*4 + 20*2 +15*.5 + 25*9.5) / 20 = 21.25 just taking the midpoints between each point and multiplying by minutes and dividing by total
            assert_eq!(select_one!(client, stmt, f64), 21.25);
            let stmt = "SELECT average(time_weight('LOCF', ts, val)) FROM test";
            // expected = (10 + 20 + 10 + 20 + 10*4 + 30*2 +10*.5 + 20*9.5) / 20 = 17.75 using last value and carrying for each point
            assert_eq!(select_one!(client, stmt, f64), 17.75);

            //make sure this works with whatever ordering we throw at it
            let stmt = "SELECT average(time_weight('Linear', ts, val ORDER BY random())) FROM test";
            assert_eq!(select_one!(client, stmt, f64), 21.25);
            let stmt = "SELECT average(time_weight('LOCF', ts, val ORDER BY random())) FROM test";
            assert_eq!(select_one!(client, stmt, f64), 17.75);

            // make sure we get the same result if we do multi-level aggregation
            let stmt = "WITH t AS (SELECT date_trunc('minute', ts), time_weight('Linear', ts, val) AS tws FROM test GROUP BY 1) SELECT average(time_weight(tws)) FROM t";
            assert_eq!(select_one!(client, stmt, f64), 21.25);
            let stmt = "WITH t AS (SELECT date_trunc('minute', ts), time_weight('LOCF', ts, val) AS tws FROM test GROUP BY 1) SELECT average(time_weight(tws)) FROM t";
            assert_eq!(select_one!(client, stmt, f64), 17.75);
        });
    }
}
