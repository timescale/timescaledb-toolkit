#![allow(non_camel_case_types)]

use pgx::*;
use serde::{Deserialize, Serialize};

use crate::{
    accessors::{
        toolkit_experimental, AccessorAverage, AccessorFirstTime, AccessorFirstVal,
        AccessorLastTime, AccessorLastVal,
    },
    aggregate_utils::in_aggregate_context,
    duration::DurationUnit,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

use tspoint::TSPoint;

use time_weighted_average::{
    TimeWeightError, TimeWeightMethod, TimeWeightSummary as TimeWeightSummaryInternal,
};

use crate::raw::bytea;

pg_type! {
    #[derive(Debug)]
    struct TimeWeightSummary {
        first: TSPoint,
        last: TSPoint,
        weighted_sum: f64,
        method: TimeWeightMethod,
    }
}
ron_inout_funcs!(TimeWeightSummary);

impl<'input> TimeWeightSummary<'input> {
    fn internal(&self) -> TimeWeightSummaryInternal {
        TimeWeightSummaryInternal {
            method: self.method,
            first: self.first,
            last: self.last,
            w_sum: self.weighted_sum,
        }
    }

    pub(super) fn interpolate(
        &self,
        interval_start: i64,
        interval_len: i64,
        prev: Option<TimeWeightSummary>,
        next: Option<TimeWeightSummary>,
    ) -> TimeWeightSummary<'static> {
        assert!(
            interval_start <= self.first.ts,
            "Interval start ({}) must be at or before first timestamp ({})",
            interval_start,
            self.first.ts
        );
        let end = interval_start + interval_len;
        assert!(
            end > self.last.ts,
            "Interval end ({}) must be after last timestamp ({})",
            end,
            self.last.ts
        );
        let mut new_sum = self.weighted_sum;
        let new_start = match prev {
            Some(prev) if interval_start < self.first.ts => {
                let new_start = self
                    .method
                    .interpolate(prev.last, Some(self.first), interval_start)
                    .expect("unable to interpolate start of interval");
                new_sum += self.method.weighted_sum(new_start, self.first);
                new_start
            }
            _ => self.first,
        };
        let new_end = match next {
            Some(next) => {
                let new_end = self
                    .method
                    .interpolate(self.last, Some(next.first), end)
                    .expect("unable to interpolate end of interval");
                new_sum += self.method.weighted_sum(self.last, new_end);
                new_end
            }
            _ => self.last,
        };

        unsafe {
            crate::flatten!(TimeWeightSummary {
                first: new_start,
                last: new_end,
                weighted_sum: new_sum,
                method: self.method,
            })
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

#[pg_extern(immutable, parallel_safe, strict)]
pub fn time_weight_trans_serialize(state: Internal) -> bytea {
    let mut state: Inner<TimeWeightTransState> = unsafe { state.to_inner().unwrap() };
    state.combine_summaries();
    crate::do_serialize!(state)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn time_weight_trans_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    time_weight_trans_deserialize_inner(bytes).internal()
}
pub fn time_weight_trans_deserialize_inner(bytes: bytea) -> Inner<TimeWeightTransState> {
    let t: TimeWeightTransState = crate::do_deserialize!(bytes, TimeWeightTransState);
    t.into()
}

// these are technically parallel_safe (as in they can be called in a parallel context) even though the aggregate itself is parallel restricted.
#[pg_extern(immutable, parallel_safe)]
pub fn time_weight_trans(
    state: Internal,
    method: String,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { time_weight_trans_inner(state.to_inner(), method, ts, val, fcinfo).internal() }
}

pub fn time_weight_trans_inner(
    state: Option<Inner<TimeWeightTransState>>,
    method: String,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<TimeWeightTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint { ts: ts.into(), val },
            };

            match state {
                None => {
                    let mut s = TimeWeightTransState {
                        point_buffer: vec![],
                        // TODO technically not portable to ASCII-compatible charsets
                        method: match method.trim().to_lowercase().as_str() {
                            "linear" | "trapezoidal" => TimeWeightMethod::Linear,
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

#[pg_extern(immutable, parallel_safe)]
pub fn time_weight_summary_trans<'a>(
    state: Internal,
    next: Option<TimeWeightSummary<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    time_weight_summary_trans_inner(unsafe { state.to_inner() }, next, fcinfo).internal()
}

pub fn time_weight_summary_trans_inner(
    state: Option<Inner<TimeWeightTransState>>,
    next: Option<TimeWeightSummary>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<TimeWeightTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, next) {
            (None, None) => None,
            (None, Some(next)) => Some(
                TimeWeightTransState {
                    summary_buffer: vec![next.internal()],
                    point_buffer: vec![],
                    method: next.method,
                }
                .into(),
            ),
            (Some(state), None) => Some(state),
            (Some(mut state), Some(next)) => {
                let next = TimeWeightTransState {
                    summary_buffer: vec![next.internal()],
                    point_buffer: vec![],
                    method: next.method,
                };
                state.push_summary(&next);
                Some(state)
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn time_weight_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { time_weight_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}

pub fn time_weight_combine_inner(
    state1: Option<Inner<TimeWeightTransState>>,
    state2: Option<Inner<TimeWeightTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<TimeWeightTransState>> {
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

#[pg_extern(immutable, parallel_safe)]
fn time_weight_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TimeWeightSummary<'static>> {
    time_weight_final_inner(unsafe { state.to_inner() }, fcinfo)
}

fn time_weight_final_inner(
    state: Option<Inner<TimeWeightTransState>>,
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
            state.summary_buffer.pop().map(|st| {
                flatten!(TimeWeightSummary {
                    method: st.method,
                    first: st.first,
                    last: st.last,
                    weighted_sum: st.w_sum,
                })
            })
        })
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_time_weight_first_val<'a>(
    sketch: TimeWeightSummary<'a>,
    _accessor: AccessorFirstVal<'a>,
) -> f64 {
    time_weight_first_val(sketch)
}

#[pg_extern(name = "first_val", strict, immutable, parallel_safe)]
fn time_weight_first_val<'a>(summary: TimeWeightSummary<'a>) -> f64 {
    summary.first.val
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_time_weight_last_val<'a>(
    sketch: TimeWeightSummary<'a>,
    _accessor: AccessorLastVal<'a>,
) -> f64 {
    time_weight_last_val(sketch)
}

#[pg_extern(name = "last_val", strict, immutable, parallel_safe)]
fn time_weight_last_val<'a>(summary: TimeWeightSummary<'a>) -> f64 {
    summary.last.val
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_time_weight_first_time<'a>(
    sketch: TimeWeightSummary<'a>,
    _accessor: AccessorFirstTime<'a>,
) -> crate::raw::TimestampTz {
    time_weight_first_time(sketch)
}

#[pg_extern(name = "first_time", strict, immutable, parallel_safe)]
fn time_weight_first_time<'a>(summary: TimeWeightSummary<'a>) -> crate::raw::TimestampTz {
    summary.first.ts.into()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_time_weight_last_time<'a>(
    sketch: TimeWeightSummary<'a>,
    _accessor: AccessorLastTime<'a>,
) -> crate::raw::TimestampTz {
    time_weight_last_time(sketch)
}

#[pg_extern(name = "last_time", strict, immutable, parallel_safe)]
fn time_weight_last_time<'a>(summary: TimeWeightSummary<'a>) -> crate::raw::TimestampTz {
    summary.last.ts.into()
}

extension_sql!(
    "\n\
    CREATE AGGREGATE time_weight(method text, ts timestamptz, value DOUBLE PRECISION)\n\
    (\n\
        sfunc = time_weight_trans,\n\
        stype = internal,\n\
        finalfunc = time_weight_final,\n\
        combinefunc = time_weight_combine,\n\
        serialfunc = time_weight_trans_serialize,\n\
        deserialfunc = time_weight_trans_deserialize,\n\
        parallel = restricted\n\
    );\n\
\n\
    CREATE AGGREGATE rollup(tws TimeWeightSummary)\n\
    (\n\
        sfunc = time_weight_summary_trans,\n\
        stype = internal,\n\
        finalfunc = time_weight_final,\n\
        combinefunc = time_weight_combine,\n\
        serialfunc = time_weight_trans_serialize,\n\
        deserialfunc = time_weight_trans_deserialize,\n\
        parallel = restricted\n\
    );\n\
",
    name = "time_weight_agg",
    requires = [
        time_weight_trans,
        time_weight_final,
        time_weight_combine,
        time_weight_trans_serialize,
        time_weight_trans_deserialize,
        time_weight_summary_trans
    ],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_time_weighted_average_average<'a>(
    sketch: Option<TimeWeightSummary<'a>>,
    _accessor: AccessorAverage<'a>,
) -> Option<f64> {
    time_weighted_average_average(sketch)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_time_weighted_average_integral<'a>(
    tws: Option<TimeWeightSummary<'a>>,
    accessor: toolkit_experimental::AccessorIntegral<'a>,
) -> Option<f64> {
    time_weighted_average_integral(
        tws,
        String::from_utf8_lossy(accessor.bytes.as_slice()).to_string(),
    )
}

#[pg_extern(immutable, parallel_safe, name = "average")]
pub fn time_weighted_average_average<'a>(tws: Option<TimeWeightSummary<'a>>) -> Option<f64> {
    match tws {
        None => None,
        Some(tws) => match tws.internal().time_weighted_average() {
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

#[pg_extern(
    immutable,
    parallel_safe,
    name = "integral",
    schema = "toolkit_experimental"
)]
pub fn time_weighted_average_integral<'a>(
    tws: Option<TimeWeightSummary<'a>>,
    unit: default!(String, "'second'"),
) -> Option<f64> {
    let unit = match DurationUnit::from_str(&unit) {
        Some(unit) => unit,
        None => pgx::error!(
            "Unrecognized duration unit: {}. Valid units are: usecond, msecond, second, minute, hour",
            unit,
        ),
    };
    let integral_microsecs = tws?.internal().time_weighted_integral();
    Some(DurationUnit::Microsec.convert_unit(integral_microsecs, unit))
}

fn interpolate<'a>(
    tws: Option<TimeWeightSummary>,
    start: crate::raw::TimestampTz,
    duration: crate::raw::Interval,
    prev: Option<TimeWeightSummary>,
    next: Option<TimeWeightSummary>,
) -> Option<TimeWeightSummary<'a>> {
    match tws {
        None => None,
        Some(tws) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &duration);
            Some(tws.interpolate(start.into(), interval, prev, next))
        }
    }
}

// Public facing interpolated_average
extension_sql!(
    "\n\
     CREATE FUNCTION toolkit_experimental.interpolated_average(tws timeweightsummary,\n\
          start timestamptz,\n\
          duration interval,\n\
          prev timeweightsummary,\n\
          next timeweightsummary) RETURNS DOUBLE PRECISION\n\
     AS $$\n\
          SELECT interpolated_average(tws,start,duration,prev,next) $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;\n\
",
    name = "experimental_interpolated_average", requires = [time_weighted_average_interpolated_average]
);

#[pg_extern(immutable, parallel_safe, name = "interpolated_average")]
pub fn time_weighted_average_interpolated_average<'a>(
    tws: Option<TimeWeightSummary<'a>>,
    start: crate::raw::TimestampTz,
    duration: crate::raw::Interval,
    prev: Option<TimeWeightSummary<'a>>,
    next: Option<TimeWeightSummary<'a>>,
) -> Option<f64> {
    let target = interpolate(tws, start, duration, prev, next);
    time_weighted_average_average(target)
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "interpolated_integral",
    schema = "toolkit_experimental"
)]
pub fn time_weighted_average_interpolated_integral<'a>(
    tws: Option<TimeWeightSummary<'a>>,
    start: crate::raw::TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimeWeightSummary<'a>>,
    next: Option<TimeWeightSummary<'a>>,
    unit: default!(String, "'second'"),
) -> Option<f64> {
    let target = interpolate(tws, start, interval, prev, next);
    time_weighted_average_integral(target, unit)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use pgx_macros::pg_test;
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
            let stmt =
                "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION); SET TIME ZONE 'UTC'";
            client.select(stmt, None, None);

            // add a point
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:00:00+00', 10.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT toolkit_experimental.integral(time_weight('Trapezoidal', ts, val), 'hrs') FROM test";
            assert_eq!(select_one!(client, stmt, f64), 0.0);
            let stmt = "SELECT toolkit_experimental.integral(time_weight('LOCF', ts, val), 'msecond') FROM test";
            assert_eq!(select_one!(client, stmt, f64), 0.0);

            // add another point
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:01:00+00', 20.0)";
            client.select(stmt, None, None);

            // test basic with 2 points
            let stmt = "SELECT average(time_weight('Linear', ts, val)) FROM test";
            assert!((select_one!(client, stmt, f64) - 15.0).abs() < f64::EPSILON);
            let stmt = "SELECT average(time_weight('LOCF', ts, val)) FROM test";
            assert!((select_one!(client, stmt, f64) - 10.0).abs() < f64::EPSILON);

            let stmt = "SELECT first_val(time_weight('LOCF', ts, val)) FROM test";
            assert!((select_one!(client, stmt, f64) - 10.0).abs() < f64::EPSILON);
            let stmt = "SELECT last_val(time_weight('LOCF', ts, val)) FROM test";
            assert!((select_one!(client, stmt, f64) - 20.0).abs() < f64::EPSILON);

            // arrow syntax should be the same
            let stmt = "SELECT time_weight('LOCF', ts, val) -> first_val() FROM test";
            assert!((select_one!(client, stmt, f64) - 10.0).abs() < f64::EPSILON);
            let stmt = "SELECT time_weight('LOCF', ts, val) -> last_val() FROM test";
            assert!((select_one!(client, stmt, f64) - 20.0).abs() < f64::EPSILON);

            let stmt = "SELECT first_time(time_weight('LOCF', ts, val))::text FROM test";
            assert_eq!(select_one!(client, stmt, &str), "2020-01-01 00:00:00+00");
            let stmt = "SELECT last_time(time_weight('LOCF', ts, val))::text FROM test";
            assert_eq!(select_one!(client, stmt, &str), "2020-01-01 00:01:00+00");

            // arrow syntax should be the same
            let stmt = "SELECT (time_weight('LOCF', ts, val) -> first_time())::text FROM test";
            assert_eq!(select_one!(client, stmt, &str), "2020-01-01 00:00:00+00");
            let stmt = "SELECT (time_weight('LOCF', ts, val) -> last_time())::text FROM test";
            assert_eq!(select_one!(client, stmt, &str), "2020-01-01 00:01:00+00");

            // more values evenly spaced
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:02:00+00', 10.0), ('2020-01-01 00:03:00+00', 20.0), ('2020-01-01 00:04:00+00', 10.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT average(time_weight('Linear', ts, val)) FROM test";
            assert!((select_one!(client, stmt, f64) - 15.0).abs() < f64::EPSILON);
            let stmt = "SELECT average(time_weight('LOCF', ts, val)) FROM test";
            assert!((select_one!(client, stmt, f64) - 15.0).abs() < f64::EPSILON);

            let stmt = "SELECT toolkit_experimental.integral(time_weight('Linear', ts, val), 'mins') FROM test";
            assert!((select_one!(client, stmt, f64) - 60.0).abs() < f64::EPSILON);
            let stmt = "SELECT toolkit_experimental.integral(time_weight('LOCF', ts, val), 'hour') FROM test";
            assert!((select_one!(client, stmt, f64) - 1.0).abs() < f64::EPSILON);

            //non-evenly spaced values
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:08:00+00', 30.0), ('2020-01-01 00:10:00+00', 10.0), ('2020-01-01 00:10:30+00', 20.0), ('2020-01-01 00:20:00+00', 30.0)";
            client.select(stmt, None, None);

            let stmt = "SELECT average(time_weight('Linear', ts, val)) FROM test";
            // expected =(15 +15 +15 +15 + 20*4 + 20*2 +15*.5 + 25*9.5) / 20 = 21.25 just taking the midpoints between each point and multiplying by minutes and dividing by total
            assert!((select_one!(client, stmt, f64) - 21.25).abs() < f64::EPSILON);
            let stmt = "SELECT time_weight('Linear', ts, val) \
                ->average() \
            FROM test";
            // arrow syntax should be the same
            assert!((select_one!(client, stmt, f64) - 21.25).abs() < f64::EPSILON);

            let stmt = "SELECT toolkit_experimental.integral(time_weight('Linear', ts, val), 'microseconds') FROM test";
            assert!((select_one!(client, stmt, f64) - 25500000000.00).abs() < f64::EPSILON);
            let stmt = "SELECT time_weight('Linear', ts, val) \
                ->toolkit_experimental.integral('microseconds') \
            FROM test";
            // arrow syntax should be the same
            assert!((select_one!(client, stmt, f64) - 25500000000.00).abs() < f64::EPSILON);
            let stmt = "SELECT time_weight('Linear', ts, val) \
                ->toolkit_experimental.integral() \
            FROM test";
            assert!((select_one!(client, stmt, f64) - 25500.00).abs() < f64::EPSILON);

            let stmt = "SELECT average(time_weight('LOCF', ts, val)) FROM test";
            // expected = (10 + 20 + 10 + 20 + 10*4 + 30*2 +10*.5 + 20*9.5) / 20 = 17.75 using last value and carrying for each point
            assert!((select_one!(client, stmt, f64) - 17.75).abs() < f64::EPSILON);

            let stmt = "SELECT toolkit_experimental.integral(time_weight('LOCF', ts, val), 'milliseconds') FROM test";
            assert!((select_one!(client, stmt, f64) - 21300000.0).abs() < f64::EPSILON);

            //make sure this works with whatever ordering we throw at it
            let stmt = "SELECT average(time_weight('Linear', ts, val ORDER BY random())) FROM test";
            assert!((select_one!(client, stmt, f64) - 21.25).abs() < f64::EPSILON);
            let stmt = "SELECT average(time_weight('LOCF', ts, val ORDER BY random())) FROM test";
            assert!((select_one!(client, stmt, f64) - 17.75).abs() < f64::EPSILON);

            let stmt = "SELECT toolkit_experimental.integral(time_weight('Linear', ts, val ORDER BY random()), 'seconds') FROM test";
            assert!((select_one!(client, stmt, f64) - 25500.0).abs() < f64::EPSILON);
            let stmt = "SELECT toolkit_experimental.integral(time_weight('LOCF', ts, val ORDER BY random())) FROM test";
            assert!((select_one!(client, stmt, f64) - 21300.0).abs() < f64::EPSILON);

            // make sure we get the same result if we do multi-level aggregation
            let stmt = "WITH t AS (SELECT date_trunc('minute', ts), time_weight('Linear', ts, val) AS tws FROM test GROUP BY 1) SELECT average(rollup(tws)) FROM t";
            assert!((select_one!(client, stmt, f64) - 21.25).abs() < f64::EPSILON);
            let stmt = "WITH t AS (SELECT date_trunc('minute', ts), time_weight('LOCF', ts, val) AS tws FROM test GROUP BY 1) SELECT average(rollup(tws)) FROM t";
            assert!((select_one!(client, stmt, f64) - 17.75).abs() < f64::EPSILON);
        });
    }

    #[pg_test]
    fn test_time_weight_io() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            let stmt = "CREATE TABLE test(ts timestamptz, val DOUBLE PRECISION)";
            client.select(stmt, None, None);

            let linear_time_weight = "SELECT time_weight('Linear', ts, val)::TEXT FROM test";
            let locf_time_weight = "SELECT time_weight('LOCF', ts, val)::TEXT FROM test";
            let avg = |text: &str| format!("SELECT average('{}'::TimeWeightSummary)", text);

            // add a couple points
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:00:00+00', 10.0), ('2020-01-01 00:01:00+00', 20.0)";
            client.select(stmt, None, None);

            // test basic with 2 points
            let expected = "(\
                version:1,\
                first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                last:(ts:\"2020-01-01 00:01:00+00\",val:20),\
                weighted_sum:900000000,\
                method:Linear\
            )";
            assert_eq!(select_one!(client, linear_time_weight, String), expected);
            assert!((select_one!(client, &*avg(expected), f64) - 15.0).abs() < f64::EPSILON);

            let expected = "(\
                version:1,\
                first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                last:(ts:\"2020-01-01 00:01:00+00\",val:20),\
                weighted_sum:600000000,\
                method:LOCF\
            )";
            assert_eq!(select_one!(client, locf_time_weight, String), expected);
            assert!((select_one!(client, &*avg(expected), f64) - 10.0).abs() < f64::EPSILON);

            // more values evenly spaced
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:02:00+00', 10.0), ('2020-01-01 00:03:00+00', 20.0), ('2020-01-01 00:04:00+00', 10.0)";
            client.select(stmt, None, None);

            let expected = "(\
                version:1,\
                first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                last:(ts:\"2020-01-01 00:04:00+00\",val:10),\
                weighted_sum:3600000000,\
                method:Linear\
            )";
            assert_eq!(select_one!(client, linear_time_weight, String), expected);
            assert!((select_one!(client, &*avg(expected), f64) - 15.0).abs() < f64::EPSILON);
            let expected = "(\
                version:1,\
                first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                last:(ts:\"2020-01-01 00:04:00+00\",val:10),\
                weighted_sum:3600000000,\
                method:LOCF\
            )";
            assert_eq!(select_one!(client, locf_time_weight, String), expected);
            assert!((select_one!(client, &*avg(expected), f64) - 15.0).abs() < f64::EPSILON);

            //non-evenly spaced values
            let stmt = "INSERT INTO test VALUES('2020-01-01 00:08:00+00', 30.0), ('2020-01-01 00:10:00+00', 10.0), ('2020-01-01 00:10:30+00', 20.0), ('2020-01-01 00:20:00+00', 30.0)";
            client.select(stmt, None, None);

            let expected = "(\
                version:1,\
                first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                last:(ts:\"2020-01-01 00:20:00+00\",val:30),\
                weighted_sum:25500000000,\
                method:Linear\
            )";
            assert_eq!(select_one!(client, linear_time_weight, String), expected);
            assert!((select_one!(client, &*avg(expected), f64) - 21.25).abs() < f64::EPSILON);
            let expected = "(\
                version:1,\
                first:(ts:\"2020-01-01 00:00:00+00\",val:10),\
                last:(ts:\"2020-01-01 00:20:00+00\",val:30),\
                weighted_sum:21300000000,\
                method:LOCF\
            )";
            assert_eq!(select_one!(client, locf_time_weight, String), expected);
            assert!((select_one!(client, &*avg(expected), f64) - 17.75).abs() < f64::EPSILON);
        });
    }

    #[pg_test]
    fn test_time_weight_byte_io() {
        unsafe {
            use std::ptr;
            const BASE: i64 = 631152000000000;
            const MIN: i64 = 60000000;
            let state = time_weight_trans_inner(
                None,
                "linear".to_string(),
                Some(BASE.into()),
                Some(10.0),
                ptr::null_mut(),
            );
            let state = time_weight_trans_inner(
                state,
                "linear".to_string(),
                Some((BASE + MIN).into()),
                Some(20.0),
                ptr::null_mut(),
            );
            let state = time_weight_trans_inner(
                state,
                "linear".to_string(),
                Some((BASE + 2 * MIN).into()),
                Some(30.0),
                ptr::null_mut(),
            );
            let state = time_weight_trans_inner(
                state,
                "linear".to_string(),
                Some((BASE + 3 * MIN).into()),
                Some(10.0),
                ptr::null_mut(),
            );
            let state = time_weight_trans_inner(
                state,
                "linear".to_string(),
                Some((BASE + 4 * MIN).into()),
                Some(20.0),
                ptr::null_mut(),
            );
            let state = time_weight_trans_inner(
                state,
                "linear".to_string(),
                Some((BASE + 5 * MIN).into()),
                Some(30.0),
                ptr::null_mut(),
            );

            let mut control = state.unwrap();
            let buffer =
                time_weight_trans_serialize(Inner::from(control.clone()).internal().unwrap());
            let buffer = pgx::varlena::varlena_to_byte_slice(buffer.0.cast_mut_ptr());

            let expected = [
                1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 96, 194, 134, 7, 62, 2, 0,
                0, 0, 0, 0, 0, 0, 36, 64, 0, 3, 164, 152, 7, 62, 2, 0, 0, 0, 0, 0, 0, 0, 62, 64, 0,
                0, 0, 192, 11, 90, 246, 65,
            ];
            assert_eq!(buffer, expected);

            let expected = pgx::varlena::rust_byte_slice_to_bytea(&expected);
            let new_state =
                time_weight_trans_deserialize_inner(bytea(pg_sys::Datum::from(expected.as_ptr())));

            control.combine_summaries(); // Serialized form is always combined
            assert_eq!(&*new_state, &*control);
        }
    }

    #[pg_test]
    fn test_time_weight_interpolation() {
        Spi::execute(|client| {
            client.select(
                "CREATE TABLE test(time timestamptz, value double precision, bucket timestamptz)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO test VALUES
                ('2020-1-1 8:00'::timestamptz, 10.0, '2020-1-1'::timestamptz),
                ('2020-1-1 12:00'::timestamptz, 40.0, '2020-1-1'::timestamptz),
                ('2020-1-1 16:00'::timestamptz, 20.0, '2020-1-1'::timestamptz),
                ('2020-1-2 2:00'::timestamptz, 15.0, '2020-1-2'::timestamptz),
                ('2020-1-2 12:00'::timestamptz, 50.0, '2020-1-2'::timestamptz),
                ('2020-1-2 20:00'::timestamptz, 25.0, '2020-1-2'::timestamptz),
                ('2020-1-3 10:00'::timestamptz, 30.0, '2020-1-3'::timestamptz),
                ('2020-1-3 12:00'::timestamptz, 0.0, '2020-1-3'::timestamptz), 
                ('2020-1-3 16:00'::timestamptz, 35.0, '2020-1-3'::timestamptz)"#,
                None,
                None,
            );
            // test experimental version
            let mut experimental_averages = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_average(
                    agg,
                    bucket,
                    '1 day'::interval,
                    LAG(agg) OVER (ORDER BY bucket),
                    LEAD(agg) OVER (ORDER BY bucket)
                ) FROM (
                    SELECT bucket, time_weight('LOCF', time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );
            // test non_experimental version
            let mut averages = client.select(
                r#"SELECT
                interpolated_average(
                    agg,
                    bucket,
                    '1 day'::interval,
                    LAG(agg) OVER (ORDER BY bucket),
                    LEAD(agg) OVER (ORDER BY bucket)
                ) FROM (
                    SELECT bucket, time_weight('LOCF', time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );
            let mut integrals = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_integral(
                    agg,
                    bucket,
                    '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket),
                    LEAD(agg) OVER (ORDER BY bucket),
                    'hours'
                ) FROM (
                    SELECT bucket, time_weight('LOCF', time, value) as agg 
                    FROM test 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );
            // verify that default value works
            client.select(
                r#"SELECT
                toolkit_experimental.interpolated_integral(
                    agg,
                    bucket,
                    '1 day'::interval,
                    LAG(agg) OVER (ORDER BY bucket),
                    LEAD(agg) OVER (ORDER BY bucket)
                ) FROM (
                    SELECT bucket, time_weight('LOCF', time, value) as agg
                    FROM test
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            // Day 1, 4 hours @ 10, 4 @ 40, 8 @ 20
            let result = experimental_averages.next().unwrap()[1].value();
            assert_eq!(result, Some((4. * 10. + 4. * 40. + 8. * 20.) / 16.));
            assert_eq!(result, averages.next().unwrap()[1].value());

            assert_eq!(
                integrals.next().unwrap()[1].value(),
                Some(4. * 10. + 4. * 40. + 8. * 20.)
            );
            // Day 2, 2 hours @ 20, 10 @ 15, 8 @ 50, 4 @ 25
            let result = experimental_averages.next().unwrap()[1].value();
            assert_eq!(
                result,
                Some((2. * 20. + 10. * 15. + 8. * 50. + 4. * 25.) / 24.)
            );
            assert_eq!(result, averages.next().unwrap()[1].value());
            assert_eq!(
                integrals.next().unwrap()[1].value(),
                Some(2. * 20. + 10. * 15. + 8. * 50. + 4. * 25.)
            );
            // Day 3, 10 hours @ 25, 2 @ 30, 4 @ 0
            let result = experimental_averages.next().unwrap()[1].value();
            assert_eq!(result, Some((10. * 25. + 2. * 30.) / 16.));
            assert_eq!(result, averages.next().unwrap()[1].value());
            assert_eq!(
                integrals.next().unwrap()[1].value(),
                Some(10. * 25. + 2. * 30.)
            );
            assert!(experimental_averages.next().is_none());
            assert!(averages.next().is_none());
            assert!(integrals.next().is_none());
        });
    }
}
