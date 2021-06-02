use std::{
    slice,
};

use pgx::*;

use flat_serialize::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    json_inout_funcs,
    flatten,
    palloc::Internal,
    pg_type,
};

use time_weighted_average::{
    tspoint::TSPoint,
};

use stats_agg::stats1d::StatsSummary1D as InternalStatsSummary1D;
use stats_agg::stats2d::StatsSummary2D as IntneralStatsSummary2D;



#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

pg_type! {
    #[derive(Debug, PartialEq)]
    struct StatsSummary1D {
        n: u64,
        sx: f64,
        sxx: f64,
    }
}

pg_type! {
    #[derive(Debug, PartialEq)]
    struct StatsSummary2D {
        n: u64,
        sx: f64,
        sxx: f64,
        sy: f64,
        syy: f64,
        sxy: f64,
    }
}

json_inout_funcs!(StatsSummary1D);
json_inout_funcs!(StatsSummary2D);


// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
mod timescale_analytics_experimental {
    pub(crate) use super::*;

    varlena_type!(StatsSummary1D);
    varlena_type!(StatsSummary2D);

}

impl<'input> StatsSummary1D<'input> {
    fn to_internal(&self) -> InternalStatsSummary1D {
        InternalStatsSummary1D{
            n: *self.n,
            sx: *self.sx,
            sxx: *self.sxx,
        }
    }
    fn from_internal(st: InternalStatsSummary1D) -> Self {
        unsafe{
            flatten!(
            StatsSummary1D {
                n: &st.n,
                sx: &st.sx,
                sxx: &st.sxx,
            })
        }
    }
}



#[pg_extern(schema = "timescale_analytics_experimental", strict)]
pub fn stats1d_trans_serialize<'s>(
    state: Internal<StatsSummary1D<'s>>,
) -> bytea {
    let ser: &StatsSummary1DData = &*state;
    crate::do_serialize!(ser)
}

#[pg_extern(schema = "timescale_analytics_experimental", strict)]
pub fn stats1d_trans_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<StatsSummary1D<'static>> {
    let de: StatsSummary1D = crate::do_deserialize!(bytes, StatsSummary1DData);
    de.into()
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn stats1d_trans<'s>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, val) {
                (a, None) => a,
                (None, Some(val)) => {
                    let mut s = InternalStatsSummary1D::new();
                    s.accum(val).unwrap();
                    Some(StatsSummary1D::from_internal(s).into())
                },
                (Some(mut state), Some(val)) => {
                    let mut s: InternalStatsSummary1D = state.to_internal(); 
                    s.accum(val).unwrap();
                    *state = StatsSummary1D::from_internal(s);
                    Some(state)
                },
            }
        })
    }
}


#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn stats1d_inv_trans<'s>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, val) {
                (None, _) => panic!("Inverse function should never be called with NULL state"),
                (Some(state), None) => Some(state),
                (Some(state), Some(val)) => {
                    let s: InternalStatsSummary1D = state.to_internal(); 
                    let s = s.remove(val);
                    match s {
                        None => None,
                        Some(s) => Some(StatsSummary1D::from_internal(s).into())
                    }
                },
            }
        })
    }
}


#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn stats1d_summary_trans<'s, 'v>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    value: Option<timescale_analytics_experimental::StatsSummary1D<'v>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (state, None) => state,
                (None, Some(value)) =>  Some(value.in_current_context().into()),
                (Some(mut state), Some(value)) => {
                    let s = state.to_internal();
                    let v = value.to_internal();
                    let s = s.combine(v).unwrap();
                    let s = StatsSummary1D::from_internal(s);
                    Some(s.into())
                }
            }
        })
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn stats1d_summary_inv_trans<'s, 'v>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    value: Option<timescale_analytics_experimental::StatsSummary1D<'v>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, &value) {
                (None, _) => panic!("Inverse function should never be called with NULL state"),
                (Some(state), None) => Some(state),
                (Some(state), Some(value)) => {
                    let s = state.to_internal();
                    let v = value.to_internal();
                    let s = s.remove_combined(v);
                    match s {
                        None => None,
                        Some(s) => Some(StatsSummary1D::from_internal(s).into()),
                    }
                }
            }
        })
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn stats1d_combine<'s, 'v>(
    state1: Option<Internal<StatsSummary1D<'s>>>,
    state2: Option<Internal<StatsSummary1D<'v>>>,
    fcinfo: pg_sys::FunctionCallInfo,
)  -> Option<Internal<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => {
                    let s = state2.in_current_context();
                    Some(s.into())
                },
                (Some(state1), None) => {
                    let s = state1.in_current_context();
                    Some(s.into())
                },
                (Some(state1), Some(state2)) => {
                    let s1 = state1.to_internal(); 
                    let s2 = state2.to_internal();
                    let s1 = s1.combine(s2).unwrap();
                    Some(StatsSummary1D::from_internal(s1).into())
                }
            }
        })
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
fn stats1d_final<'s>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<timescale_analytics_experimental::StatsSummary1D<'s>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match state {
                None => None,
                Some(state) => Some(state.in_current_context()),
            }
        })
    }
}


extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.stats_agg( value DOUBLE PRECISION )
(
    sfunc = timescale_analytics_experimental.stats1d_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats1d_final,
    combinefunc = timescale_analytics_experimental.stats1d_combine,
    serialfunc = timescale_analytics_experimental.stats1d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats1d_trans_deserialize,
    msfunc = timescale_analytics_experimental.stats1d_trans,
    minvfunc = timescale_analytics_experimental.stats1d_inv_trans,
    mstype = internal,
    mfinalfunc = timescale_analytics_experimental.stats1d_final,
    parallel = safe
);
"#);


extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.rollup(ss timescale_analytics_experimental.statssummary1d)
(
    sfunc = timescale_analytics_experimental.stats1d_summary_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats1d_final,
    combinefunc = timescale_analytics_experimental.stats1d_combine,
    serialfunc = timescale_analytics_experimental.stats1d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats1d_trans_deserialize,
    msfunc = timescale_analytics_experimental.stats1d_summary_trans,
    minvfunc = timescale_analytics_experimental.stats1d_summary_inv_trans,
    mstype = internal,
    mfinalfunc = timescale_analytics_experimental.stats1d_final,
    parallel = safe
);
"#);

// these are the same, but for UI, we decided to have slightly differently named functions for the windowed context and not, so that it reads better.
extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.rolling(ss timescale_analytics_experimental.statssummary1d)
(
    sfunc = timescale_analytics_experimental.stats1d_summary_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats1d_final,
    combinefunc = timescale_analytics_experimental.stats1d_combine,
    serialfunc = timescale_analytics_experimental.stats1d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats1d_trans_deserialize,
    msfunc = timescale_analytics_experimental.stats1d_summary_trans,
    minvfunc = timescale_analytics_experimental.stats1d_summary_inv_trans,
    mstype = internal,
    mfinalfunc = timescale_analytics_experimental.stats1d_final,
    parallel = safe
);
"#);




#[pg_extern(name="average", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats1d_average(
    summary: timescale_analytics_experimental::StatsSummary1D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().avg()
}

#[pg_extern(name="sum_vals", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats1d_sum_vals(
    summary: timescale_analytics_experimental::StatsSummary1D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().sum()
}

#[pg_extern(name="stddev", schema = "timescale_analytics_experimental", immutable)]
fn stats1d_stddev(
    summary: timescale_analytics_experimental::StatsSummary1D,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => summary.to_internal().stddev_pop(),
        "sample" | "samp" => summary.to_internal().stddev_samp(),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="variance", schema = "timescale_analytics_experimental", immutable)]
fn stats1d_variance(
    summary: timescale_analytics_experimental::StatsSummary1D,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => summary.to_internal().var_pop(),
        "sample" | "samp" => summary.to_internal().var_samp(),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="num_vals", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats1d_num_vals(
    summary: timescale_analytics_experimental::StatsSummary1D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> i64 {
    summary.to_internal().count()
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
        assert_eq!(p1.stats.n, p2.stats.n, "n");
        assert_relative_eq!(p1.stats.sx, p2.stats.sx);
        assert_relative_eq!(p1.stats.sxx, p2.stats.sxx);
        assert_relative_eq!(p1.stats.sy, p2.stats.sy);
        assert_relative_eq!(p1.stats.syy, p2.stats.syy);
        assert_relative_eq!(p1.stats.sxy, p2.stats.sxy);
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
            assert_close_enough(&a.to_internal(), &b.to_internal());

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
            assert_close_enough(&a.to_internal(), &b.to_internal());
        });
    }

    // #[pg_test]
    // fn test_combine_aggregate(){
    //     Spi::execute(|client| {

    //     });
    // }
}