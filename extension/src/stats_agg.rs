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

use stats_agg::XYPair;
use stats_agg::stats1d::StatsSummary1D as InternalStatsSummary1D;
use stats_agg::stats2d::StatsSummary2D as InternalStatsSummary2D;



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
            n: self.n,
            sx: self.sx,
            sxx: self.sxx,
        }
    }
    fn from_internal(st: InternalStatsSummary1D) -> Self {
        unsafe{
            flatten!(StatsSummary1D {
                n: st.n,
                sx: st.sx,
                sxx: st.sxx,
            })
        }
    }
}

impl<'input> StatsSummary2D<'input> {
    fn to_internal(&self) -> InternalStatsSummary2D {
        InternalStatsSummary2D{
            n: self.n,
            sx: self.sx,
            sxx: self.sxx,
            sy: self.sy,
            syy: self.syy,
            sxy: self.sxy,
        }
    }
    fn from_internal(st: InternalStatsSummary2D) -> Self {
        unsafe{
            flatten!(
            StatsSummary2D {
                n: st.n,
                sx: st.sx,
                sxx: st.sxx,
                sy: st.sy,
                syy: st.syy,
                sxy: st.sxy,
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

#[pg_extern(schema = "timescale_analytics_experimental", strict)]
pub fn stats2d_trans_serialize<'s>(
    state: Internal<StatsSummary2D<'s>>,
) -> bytea {
    let ser: &StatsSummary2DData = &*state;
    crate::do_serialize!(ser)
}

#[pg_extern(schema = "timescale_analytics_experimental", strict)]
pub fn stats2d_trans_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<StatsSummary2D<'static>> {
    let de: StatsSummary2D = crate::do_deserialize!(bytes, StatsSummary2DData);
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
                (None, None) => Some(StatsSummary1D::from_internal(InternalStatsSummary1D::new()).into()), // return an empty one from the trans function because otherwise it breaks in the window context
                (Some(state), None) => Some(state),
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
// Note that in general, for all stats2d cases, if either the y or x value is missing, we disregard the entire point as the n is shared between them
// if the user wants us to treat nulls as a particular value (ie zero), they can use COALESCE to do so
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn stats2d_trans<'s>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary2D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let val: Option<XYPair> = match (y, x) {
                (None, _) => None,
                (_, None) => None,
                (Some(y), Some(x)) => Some(XYPair{y, x})
            };
            match (state, val) {
                (None, None) => Some(StatsSummary2D::from_internal(InternalStatsSummary2D::new()).into()), // return an empty one from the trans function because otherwise it breaks in the window context
                (Some(state), None) => Some(state),
                (None, Some(val)) => {
                    let mut s = InternalStatsSummary2D::new();
                    s.accum(val).unwrap();
                    Some(StatsSummary2D::from_internal(s).into())
                },
                (Some(mut state), Some(val)) => {
                    let mut s: InternalStatsSummary2D = state.to_internal();
                    s.accum(val).unwrap();
                    *state = StatsSummary2D::from_internal(s);
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
pub fn stats2d_inv_trans<'s>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary2D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let val: Option<XYPair> = match (y, x) {
                (None, _) => None,
                (_, None) => None,
                (Some(y), Some(x)) => Some(XYPair{y, x})
            };
            match (state, val) {
                (None, _) => panic!("Inverse function should never be called with NULL state"),
                (Some(state), None) => Some(state),
                (Some(state), Some(val)) => {
                    let s: InternalStatsSummary2D = state.to_internal();
                    let s = s.remove(val);
                    match s {
                        None => None,
                        Some(s) => Some(StatsSummary2D::from_internal(s).into())
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
                (Some(state), Some(value)) => {
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
pub fn stats2d_summary_trans<'s, 'v>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    value: Option<timescale_analytics_experimental::StatsSummary2D<'v>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary2D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (state, None) => state,
                (None, Some(value)) =>  Some(value.in_current_context().into()),
                (Some(state), Some(value)) => {
                    let s = state.to_internal();
                    let v = value.to_internal();
                    let s = s.combine(v).unwrap();
                    let s = StatsSummary2D::from_internal(s);
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
pub fn stats2d_summary_inv_trans<'s, 'v>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    value: Option<timescale_analytics_experimental::StatsSummary2D<'v>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<StatsSummary2D<'s>>> {
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
                        Some(s) => Some(StatsSummary2D::from_internal(s).into()),
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
pub fn stats2d_combine<'s, 'v>(
    state1: Option<Internal<StatsSummary2D<'s>>>,
    state2: Option<Internal<StatsSummary2D<'v>>>,
    fcinfo: pg_sys::FunctionCallInfo,
)  -> Option<Internal<StatsSummary2D<'s>>> {
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
                    Some(StatsSummary2D::from_internal(s1).into())
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

#[pg_extern(schema = "timescale_analytics_experimental")]
fn stats2d_final<'s>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<timescale_analytics_experimental::StatsSummary2D<'s>> {
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

// mostly for testing/debugging, in case we want one without the inverse functions defined.
extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.stats_agg_no_inv( value DOUBLE PRECISION )
(
    sfunc = timescale_analytics_experimental.stats1d_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats1d_final,
    combinefunc = timescale_analytics_experimental.stats1d_combine,
    serialfunc = timescale_analytics_experimental.stats1d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats1d_trans_deserialize,
    parallel = safe
);
"#);

// same things for the 2d case
extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.stats_agg( y DOUBLE PRECISION, x DOUBLE PRECISION )
(
    sfunc = timescale_analytics_experimental.stats2d_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats2d_final,
    combinefunc = timescale_analytics_experimental.stats2d_combine,
    serialfunc = timescale_analytics_experimental.stats2d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats2d_trans_deserialize,
    msfunc = timescale_analytics_experimental.stats2d_trans,
    minvfunc = timescale_analytics_experimental.stats2d_inv_trans,
    mstype = internal,
    mfinalfunc = timescale_analytics_experimental.stats2d_final,
    parallel = safe
);
"#);

// mostly for testing/debugging, in case we want one without the inverse functions defined.
extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.stats_agg_no_inv( y DOUBLE PRECISION, x DOUBLE PRECISION )
(
    sfunc = timescale_analytics_experimental.stats2d_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats2d_final,
    combinefunc = timescale_analytics_experimental.stats2d_combine,
    serialfunc = timescale_analytics_experimental.stats2d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats2d_trans_deserialize,
    parallel = safe
);
"#);

//  Currently, rollup does not have the inverse function so if you want the behavior where we don't use the inverse,
// you can use it in your window functions (useful for our own perf testing as well)

extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.rollup(ss timescale_analytics_experimental.statssummary1d)
(
    sfunc = timescale_analytics_experimental.stats1d_summary_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats1d_final,
    combinefunc = timescale_analytics_experimental.stats1d_combine,
    serialfunc = timescale_analytics_experimental.stats1d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats1d_trans_deserialize,
    parallel = safe
);
"#);

//  For UI, we decided to have slightly differently named functions for the windowed context and not, so that it reads better, as well as using the inverse function only in the window context
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


// Same as for the 1D case, but for the 2D

extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.rollup(ss timescale_analytics_experimental.statssummary2d)
(
    sfunc = timescale_analytics_experimental.stats2d_summary_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats2d_final,
    combinefunc = timescale_analytics_experimental.stats2d_combine,
    serialfunc = timescale_analytics_experimental.stats2d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats2d_trans_deserialize,
    parallel = safe
);
"#);

//  For UI, we decided to have slightly differently named functions for the windowed context and not, so that it reads better, as well as using the inverse function only in the window context
extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.rolling(ss timescale_analytics_experimental.statssummary2d)
(
    sfunc = timescale_analytics_experimental.stats2d_summary_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.stats2d_final,
    combinefunc = timescale_analytics_experimental.stats2d_combine,
    serialfunc = timescale_analytics_experimental.stats2d_trans_serialize,
    deserialfunc = timescale_analytics_experimental.stats2d_trans_deserialize,
    msfunc = timescale_analytics_experimental.stats2d_summary_trans,
    minvfunc = timescale_analytics_experimental.stats2d_summary_inv_trans,
    mstype = internal,
    mfinalfunc = timescale_analytics_experimental.stats2d_final,
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

#[pg_extern(name="sum", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats1d_sum(
    summary: timescale_analytics_experimental::StatsSummary1D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().sum()
}

#[pg_extern(name="stddev", schema = "timescale_analytics_experimental", immutable)]
fn stats1d_stddev(
    summary: Option<timescale_analytics_experimental::StatsSummary1D>,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => summary?.to_internal().stddev_pop(),
        "sample" | "samp" => summary?.to_internal().stddev_samp(),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="variance", schema = "timescale_analytics_experimental", immutable)]
fn stats1d_variance(
    summary: Option<timescale_analytics_experimental::StatsSummary1D>,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => summary?.to_internal().var_pop(),
        "sample" | "samp" => summary?.to_internal().var_samp(),
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

#[pg_extern(name="average_x", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_average_x(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().avg()?.x)
}

#[pg_extern(name="average_y", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_average_y(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().avg()?.y)
}

#[pg_extern(name="sum_x", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_sum_x(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().sum()?.x)
}

#[pg_extern(name="sum_y", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_sum_y(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().sum()?.y)
}

#[pg_extern(name="stddev_x", schema = "timescale_analytics_experimental", immutable)]
fn stats2d_stddev_x(
    summary: Option<timescale_analytics_experimental::StatsSummary2D>,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().stddev_pop()?.x),
        "sample" | "samp" => Some(summary?.to_internal().stddev_samp()?.x),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="stddev_y", schema = "timescale_analytics_experimental", immutable)]
fn stats2d_stddev_y(
    summary: Option<timescale_analytics_experimental::StatsSummary2D>,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().stddev_pop()?.y),
        "sample" | "samp" => Some(summary?.to_internal().stddev_samp()?.y),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="variance_x", schema = "timescale_analytics_experimental", immutable)]
fn stats2d_variance_x(
    summary: Option<timescale_analytics_experimental::StatsSummary2D>,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().var_pop()?.x),
        "sample" | "samp" => Some(summary?.to_internal().var_samp()?.x),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="variance_y", schema = "timescale_analytics_experimental", immutable)]
fn stats2d_variance_y(
    summary: Option<timescale_analytics_experimental::StatsSummary2D>,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().var_pop()?.x),
        "sample" | "samp" => Some(summary?.to_internal().var_samp()?.x),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="num_vals", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_num_vals(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> i64 {
    summary.to_internal().count()
}

#[pg_extern(name="slope", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_slope(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().slope()
}

#[pg_extern(name="corr", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_corr(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().corr()
}

#[pg_extern(name="intercept", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_intercept(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().intercept()
}

#[pg_extern(name="x_intercept", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_x_intercept(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().x_intercept()
}

#[pg_extern(name="determination_coeff", schema = "timescale_analytics_experimental", strict, immutable)]
fn stats2d_determination_coeff(
    summary: timescale_analytics_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().determination_coeff()
}

#[pg_extern(name="covariance", schema = "timescale_analytics_experimental", immutable)]
fn stats2d_covar(
    summary: Option<timescale_analytics_experimental::StatsSummary2D>,
    method: default!(String, "population"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => summary?.to_internal().covar_pop(),
        "sample" | "samp" => summary?.to_internal().covar_samp(),
        _ => panic!("unknown analysis method"),
    }
}


// TODO: Add testing - probably want to do some fuzz testing against the Postgres implementations of the same. Possibly translate the Postgres tests as well?
// #[cfg(any(test, feature = "pg_test"))]
// mod tests {

//     use approx::assert_relative_eq;
//     use pgx::*;
//     use super::*;

//     macro_rules! select_one {
//         ($client:expr, $stmt:expr, $type:ty) => {
//             $client
//                 .select($stmt, None, None)
//                 .first()
//                 .get_one::<$type>()
//                 .unwrap()
//         };
//     }

//     //do proper numerical comparisons on the values where that matters, use exact where it should be exact.
//     #[track_caller]
//     fn stats1d_assert_close_enough(p1:&StatsSummary1D, p2:&StatsSummary1D) {
//         assert_eq!(p1.n, p2.n, "n");
//         assert_relative_eq!(p1.sx, p2.sx);
//         assert_relative_eq!(p1.sxx, p2.sxx);
//     }
//     #[track_caller]
//     fn stats2d_assert_close_enough(p1:&StatsSummary2D, p2:&StatsSummary2D) {
//         assert_eq!(p1.n, p2.n, "n");
//         assert_relative_eq!(p1.sx, p2.sx);
//         assert_relative_eq!(p1.sxx, p2.sxx);
//         assert_relative_eq!(p1.sy, p2.sy);
//         assert_relative_eq!(p1.syy, p2.syy);
//         assert_relative_eq!(p1.sxy, p2.sxy);
//     }



//     // #[pg_test]
//     // fn test_combine_aggregate(){
//     //     Spi::execute(|client| {

//     //     });
//     // }
// }