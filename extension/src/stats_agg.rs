use std::{
    slice,
};

use pgx::*;

use flat_serialize::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    json_inout_funcs,
    build,
    palloc::Internal,
    pg_type,
};

use stats_agg::XYPair;
pub use stats_agg::stats1d::StatsSummary1D as InternalStatsSummary1D;
pub use stats_agg::stats2d::StatsSummary2D as InternalStatsSummary2D;



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


// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
mod toolkit_experimental {
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
    pub fn from_internal(st: InternalStatsSummary1D) -> Self {
        build!(
            StatsSummary1D {
                n: st.n,
                sx: st.sx,
                sxx: st.sxx,
            }
        )
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
        build!(
            StatsSummary2D {
                n: st.n,
                sx: st.sx,
                sxx: st.sxx,
                sy: st.sy,
                syy: st.syy,
                sxy: st.sxy,
            }
        )
    }
}



#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe, strict)]
pub fn stats1d_trans_serialize<'s>(
    state: Internal<StatsSummary1D<'s>>,
) -> bytea {
    let ser: &StatsSummary1DData = &*state;
    crate::do_serialize!(ser)
}

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe, strict)]
pub fn stats1d_trans_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<StatsSummary1D<'static>> {
    let de: StatsSummary1D = crate::do_deserialize!(bytes, StatsSummary1DData);
    de.into()
}

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe, strict)]
pub fn stats2d_trans_serialize<'s>(
    state: Internal<StatsSummary2D<'s>>,
) -> bytea {
    let ser: &StatsSummary2DData = &*state;
    crate::do_serialize!(ser)
}

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe, strict)]
pub fn stats2d_trans_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<StatsSummary2D<'static>> {
    let de: StatsSummary2D = crate::do_deserialize!(bytes, StatsSummary2DData);
    de.into()
}

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
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
#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
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


#[pg_extern(schema = "toolkit_experimental",immutable)]
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

#[pg_extern(schema = "toolkit_experimental",immutable)]
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


#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
pub fn stats1d_summary_trans<'s, 'v>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    value: Option<toolkit_experimental::StatsSummary1D<'v>>,
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



#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
pub fn stats2d_summary_trans<'s, 'v>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    value: Option<toolkit_experimental::StatsSummary2D<'v>>,
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

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
pub fn stats1d_summary_inv_trans<'s, 'v>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    value: Option<toolkit_experimental::StatsSummary1D<'v>>,
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

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
pub fn stats2d_summary_inv_trans<'s, 'v>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    value: Option<toolkit_experimental::StatsSummary2D<'v>>,
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

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
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

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
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

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
fn stats1d_final<'s>(
    state: Option<Internal<StatsSummary1D<'s>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::StatsSummary1D<'s>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match state {
                None => None,
                Some(state) => Some(state.in_current_context()),
            }
        })
    }
}

#[pg_extern(schema = "toolkit_experimental",immutable, parallel_safe)]
fn stats2d_final<'s>(
    state: Option<Internal<StatsSummary2D<'s>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::StatsSummary2D<'s>> {
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
CREATE AGGREGATE toolkit_experimental.stats_agg( value DOUBLE PRECISION )
(
    sfunc = toolkit_experimental.stats1d_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats1d_final,
    combinefunc = toolkit_experimental.stats1d_combine,
    serialfunc = toolkit_experimental.stats1d_trans_serialize,
    deserialfunc = toolkit_experimental.stats1d_trans_deserialize,
    msfunc = toolkit_experimental.stats1d_trans,
    minvfunc = toolkit_experimental.stats1d_inv_trans,
    mstype = internal,
    mfinalfunc = toolkit_experimental.stats1d_final,
    parallel = safe
);
"#);

// mostly for testing/debugging, in case we want one without the inverse functions defined.
extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.stats_agg_no_inv( value DOUBLE PRECISION )
(
    sfunc = toolkit_experimental.stats1d_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats1d_final,
    combinefunc = toolkit_experimental.stats1d_combine,
    serialfunc = toolkit_experimental.stats1d_trans_serialize,
    deserialfunc = toolkit_experimental.stats1d_trans_deserialize,
    parallel = safe
);
"#);

// same things for the 2d case
extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.stats_agg( y DOUBLE PRECISION, x DOUBLE PRECISION )
(
    sfunc = toolkit_experimental.stats2d_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats2d_final,
    combinefunc = toolkit_experimental.stats2d_combine,
    serialfunc = toolkit_experimental.stats2d_trans_serialize,
    deserialfunc = toolkit_experimental.stats2d_trans_deserialize,
    msfunc = toolkit_experimental.stats2d_trans,
    minvfunc = toolkit_experimental.stats2d_inv_trans,
    mstype = internal,
    mfinalfunc = toolkit_experimental.stats2d_final,
    parallel = safe
);
"#);

// mostly for testing/debugging, in case we want one without the inverse functions defined.
extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.stats_agg_no_inv( y DOUBLE PRECISION, x DOUBLE PRECISION )
(
    sfunc = toolkit_experimental.stats2d_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats2d_final,
    combinefunc = toolkit_experimental.stats2d_combine,
    serialfunc = toolkit_experimental.stats2d_trans_serialize,
    deserialfunc = toolkit_experimental.stats2d_trans_deserialize,
    parallel = safe
);
"#);

//  Currently, rollup does not have the inverse function so if you want the behavior where we don't use the inverse,
// you can use it in your window functions (useful for our own perf testing as well)

extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.rollup(ss toolkit_experimental.statssummary1d)
(
    sfunc = toolkit_experimental.stats1d_summary_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats1d_final,
    combinefunc = toolkit_experimental.stats1d_combine,
    serialfunc = toolkit_experimental.stats1d_trans_serialize,
    deserialfunc = toolkit_experimental.stats1d_trans_deserialize,
    parallel = safe
);
"#);

//  For UI, we decided to have slightly differently named functions for the windowed context and not, so that it reads better, as well as using the inverse function only in the window context
extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.rolling(ss toolkit_experimental.statssummary1d)
(
    sfunc = toolkit_experimental.stats1d_summary_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats1d_final,
    combinefunc = toolkit_experimental.stats1d_combine,
    serialfunc = toolkit_experimental.stats1d_trans_serialize,
    deserialfunc = toolkit_experimental.stats1d_trans_deserialize,
    msfunc = toolkit_experimental.stats1d_summary_trans,
    minvfunc = toolkit_experimental.stats1d_summary_inv_trans,
    mstype = internal,
    mfinalfunc = toolkit_experimental.stats1d_final,
    parallel = safe
);
"#);


// Same as for the 1D case, but for the 2D

extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.rollup(ss toolkit_experimental.statssummary2d)
(
    sfunc = toolkit_experimental.stats2d_summary_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats2d_final,
    combinefunc = toolkit_experimental.stats2d_combine,
    serialfunc = toolkit_experimental.stats2d_trans_serialize,
    deserialfunc = toolkit_experimental.stats2d_trans_deserialize,
    parallel = safe
);
"#);

//  For UI, we decided to have slightly differently named functions for the windowed context and not, so that it reads better, as well as using the inverse function only in the window context
extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.rolling(ss toolkit_experimental.statssummary2d)
(
    sfunc = toolkit_experimental.stats2d_summary_trans,
    stype = internal,
    finalfunc = toolkit_experimental.stats2d_final,
    combinefunc = toolkit_experimental.stats2d_combine,
    serialfunc = toolkit_experimental.stats2d_trans_serialize,
    deserialfunc = toolkit_experimental.stats2d_trans_deserialize,
    msfunc = toolkit_experimental.stats2d_summary_trans,
    minvfunc = toolkit_experimental.stats2d_summary_inv_trans,
    mstype = internal,
    mfinalfunc = toolkit_experimental.stats2d_final,
    parallel = safe
);
"#);



#[pg_extern(name="average", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats1d_average(
    summary: toolkit_experimental::StatsSummary1D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().avg()
}

#[pg_extern(name="sum", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats1d_sum(
    summary: toolkit_experimental::StatsSummary1D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().sum()
}

#[pg_extern(name="stddev", schema = "toolkit_experimental", immutable, parallel_safe)]
fn stats1d_stddev(
    summary: Option<toolkit_experimental::StatsSummary1D>,
    method: default!(String, "sample"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => summary?.to_internal().stddev_pop(),
        "sample" | "samp" => summary?.to_internal().stddev_samp(),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="variance", schema = "toolkit_experimental", immutable, parallel_safe)]
fn stats1d_variance(
    summary: Option<toolkit_experimental::StatsSummary1D>,
    method: default!(String, "sample"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => summary?.to_internal().var_pop(),
        "sample" | "samp" => summary?.to_internal().var_samp(),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="num_vals", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats1d_num_vals(
    summary: toolkit_experimental::StatsSummary1D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> i64 {
    summary.to_internal().count()
}

#[pg_extern(name="average_x", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_average_x(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().avg()?.x)
}

#[pg_extern(name="average_y", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_average_y(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().avg()?.y)
}

#[pg_extern(name="sum_x", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_sum_x(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().sum()?.x)
}

#[pg_extern(name="sum_y", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_sum_y(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    Some(summary.to_internal().sum()?.y)
}

#[pg_extern(name="stddev_x", schema = "toolkit_experimental", immutable, parallel_safe)]
fn stats2d_stddev_x(
    summary: Option<toolkit_experimental::StatsSummary2D>,
    method: default!(String, "sample"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().stddev_pop()?.x),
        "sample" | "samp" => Some(summary?.to_internal().stddev_samp()?.x),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="stddev_y", schema = "toolkit_experimental", immutable, parallel_safe)]
fn stats2d_stddev_y(
    summary: Option<toolkit_experimental::StatsSummary2D>,
    method: default!(String, "sample"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().stddev_pop()?.y),
        "sample" | "samp" => Some(summary?.to_internal().stddev_samp()?.y),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="variance_x", schema = "toolkit_experimental", immutable, parallel_safe)]
fn stats2d_variance_x(
    summary: Option<toolkit_experimental::StatsSummary2D>,
    method: default!(String, "sample"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().var_pop()?.x),
        "sample" | "samp" => Some(summary?.to_internal().var_samp()?.x),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="variance_y", schema = "toolkit_experimental", immutable, parallel_safe)]
fn stats2d_variance_y(
    summary: Option<toolkit_experimental::StatsSummary2D>,
    method: default!(String, "sample"),
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(summary?.to_internal().var_pop()?.y),
        "sample" | "samp" => Some(summary?.to_internal().var_samp()?.y),
        _ => panic!("unknown analysis method"),
    }
}

#[pg_extern(name="num_vals", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_num_vals(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> i64 {
    summary.to_internal().count()
}

#[pg_extern(name="slope", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_slope(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().slope()
}

#[pg_extern(name="corr", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_corr(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().corr()
}

#[pg_extern(name="intercept", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_intercept(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().intercept()
}

#[pg_extern(name="x_intercept", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_x_intercept(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().x_intercept()
}

#[pg_extern(name="determination_coeff", schema = "toolkit_experimental", strict, immutable, parallel_safe)]
fn stats2d_determination_coeff(
    summary: toolkit_experimental::StatsSummary2D,
    _fcinfo: pg_sys::FunctionCallInfo,
)-> Option<f64> {
    summary.to_internal().determination_coeff()
}

#[pg_extern(name="covariance", schema = "toolkit_experimental", immutable, parallel_safe)]
fn stats2d_covar(
    summary: Option<toolkit_experimental::StatsSummary2D>,
    method: default!(String, "sample"),
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

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;
    use approx::relative_eq;
    use rand::rngs::SmallRng;
    use rand::seq::SliceRandom;
    use rand::{self, Rng, SeedableRng};

    const RUNS: usize = 10;          // Number of runs to generate
    const VALS: usize = 10000;       // Number of values to use for each run
    const SEED: Option<u64> = None;  // RNG seed, generated from entropy if None
    const PRINT_VALS: bool = false;  // Print out test values on error, this can be spammy if VALS is high

    #[pg_test]
    fn stats_agg_fuzz() {
        let mut state = TestState::new(RUNS, VALS, SEED);
        for _ in 0..state.runs {
            state.populate_values();
            test_aggs(&mut state);
            state.passed += 1;
        }
    }

    struct TestState {
        runs: usize,
        values: usize,
        passed: usize,
        x_values: Vec<f64>,
        y_values: Vec<f64>,
        seed: u64,
        gen: SmallRng,
    }

    impl TestState {
        pub fn new(runs: usize, values: usize, seed: Option<u64>) -> TestState {
            let seed = match seed {
                Some(s) => s,
                None => SmallRng::from_entropy().gen()
            };

            TestState {
                runs,
                values,
                passed: 0,
                x_values: Vec::new(),
                y_values: Vec::new(),
                seed,
                gen: SmallRng::seed_from_u64(seed),
            }
        }

        pub fn populate_values(&mut self) {
            // Discard old values
            self.x_values = Vec::with_capacity(self.values);
            self.y_values = Vec::with_capacity(self.values);

            // We'll cluster the exponential components of the random values around a particular value
            let exp_base = self.gen.gen_range((f64::MIN_EXP / 10) as f64..(f64::MAX_EXP / 10) as f64);

            for _ in 0..self.values {
                let exp = self.gen.gen_range((exp_base - 2.)..=(exp_base + 2.));
                let mantissa = self.gen.gen_range((1.)..2.);
                let sign = [-1., 1.].choose(&mut self.gen).unwrap();
                self.x_values.push(sign * mantissa * exp.exp2());

                let exp = self.gen.gen_range((exp_base - 2.)..=(exp_base + 2.));
                let mantissa = self.gen.gen_range((1.)..2.);
                let sign = [-1., 1.].choose(&mut self.gen).unwrap();
                self.y_values.push(sign * mantissa * exp.exp2());
            }
        }

        pub fn failed_msg(&self, dump_vals: bool) -> String {
            format!("Failed after {} successful iterations, run using {} values generated from seed {}{}", self.passed, self.x_values.len(), self.seed,
                if dump_vals {
                    format!("\nX-values:\n{:?}\n\nY-values:\n{:?}", self.x_values, self.y_values)
                } else {
                    "".to_string()
                }
            )
        }
    }

    fn check_agg_equivalence(state: &TestState, client: &SpiClient, pg_cmd: &String, tk_cmd: &String, allowed_diff: f64) {
        let pg_result = client.select(&pg_cmd, None, None)
            .first()
            .get_one::<f64>()
            .unwrap();

        let tk_result = client.select(&tk_cmd, None, None)
            .first()
            .get_one::<f64>()
            .unwrap();

        let result = if allowed_diff == 0.0 {
            pg_result == tk_result
        } else {
            relative_eq!(pg_result, tk_result, max_relative = allowed_diff)
        };

        if !result {
            let abs_diff = f64::abs(pg_result - tk_result);
            let abs_max = f64::abs(pg_result).max(f64::abs(tk_result));
            panic!(
                "Output didn't match between postgres command: {}\n\
                and stats_agg command: {} \n\
                \tpostgres result: {}\n\
                \tstatsagg result: {}\n\
                \trelative difference:         {}\n\
                \tallowed relative difference: {}\n\
                {}", pg_cmd, tk_cmd, pg_result, tk_result, abs_diff / abs_max, allowed_diff, state.failed_msg(PRINT_VALS));
        }
    }

    fn pg1d_aggx(agg: &str) -> String {
        format!("SELECT {}(test_x) FROM test_table", agg)
    }

    fn pg1d_aggy(agg: &str) -> String {
        format!("SELECT {}(test_y) FROM test_table", agg)
    }

    fn pg2d_agg(agg: &str) -> String {
        format!("SELECT {}(test_y, test_x) FROM test_table", agg)
    }

    fn tk1d_agg(agg: &str) -> String {
        format!("SELECT toolkit_experimental.{}(toolkit_experimental.stats_agg(test_x)) FROM test_table", agg)
    }

    fn tk1d_agg_arg(agg: &str, arg: &str) -> String {
        format!("SELECT toolkit_experimental.{}(toolkit_experimental.stats_agg(test_x), '{}') FROM test_table", agg, arg)
    }

    fn tk2d_agg(agg: &str) -> String {
        format!("SELECT toolkit_experimental.{}(toolkit_experimental.stats_agg(test_y, test_x)) FROM test_table", agg)
    }

    fn tk2d_agg_arg(agg: &str, arg: &str) -> String {
        format!("SELECT toolkit_experimental.{}(toolkit_experimental.stats_agg(test_y, test_x), '{}') FROM test_table", agg, arg)
    }

    fn test_aggs(state: &mut TestState) {
        Spi::execute(|client| {
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select(
                "CREATE TABLE test_table (test_x DOUBLE PRECISION, test_y DOUBLE PRECISION)",
                None,
                None
            );

            client.select(&format!("INSERT INTO test_table VALUES {}",
                state.x_values.iter().zip(state.y_values.iter()).map(
                    |(x, y)| "(".to_string() + &x.to_string() + "," + &y.to_string()+ ")" + ","
                ).collect::<String>().trim_end_matches(",")), None, None);

            // Definitions for allowed errors for different aggregates
            const NONE: f64 = 0.;                 // Exact match
            const EPS1: f64 = f64::EPSILON;       // Generally enough to handle float rounding
            const EPS2: f64 = 2. * f64::EPSILON;  // stddev is sqrt(variance), so a bit tighter bound
            const EPS3: f64 = 3. * f64::EPSILON;  // Sum of squares in variance agg accumulates a bit more error

            check_agg_equivalence(&state, &client, &pg1d_aggx("avg"), &tk1d_agg("average"), NONE);
            check_agg_equivalence(&state, &client, &pg1d_aggx("sum"), &tk1d_agg("sum"), NONE);
            check_agg_equivalence(&state, &client, &pg1d_aggx("count"), &tk1d_agg("num_vals"), NONE);
            check_agg_equivalence(&state, &client, &pg1d_aggx("stddev"), &tk1d_agg("stddev"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggx("stddev_pop"), &tk1d_agg_arg("stddev", "population"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggx("stddev_samp"), &tk1d_agg_arg("stddev", "sample"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggx("variance"), &tk1d_agg("variance"), EPS3);
            check_agg_equivalence(&state, &client, &pg1d_aggx("var_pop"), &tk1d_agg_arg("variance", "population"), EPS3);
            check_agg_equivalence(&state, &client, &pg1d_aggx("var_samp"), &tk1d_agg_arg("variance", "sample"), EPS3);

            check_agg_equivalence(&state, &client, &pg2d_agg("regr_avgx"), &tk2d_agg("average_x"), NONE);
            check_agg_equivalence(&state, &client, &pg2d_agg("regr_avgy"), &tk2d_agg("average_y"), NONE);
            check_agg_equivalence(&state, &client, &pg1d_aggx("sum"), &tk2d_agg("sum_x"), NONE);
            check_agg_equivalence(&state, &client, &pg1d_aggy("sum"), &tk2d_agg("sum_y"), NONE);
            check_agg_equivalence(&state, &client, &pg1d_aggx("stddev"), &tk2d_agg("stddev_x"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggy("stddev"), &tk2d_agg("stddev_y"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggx("stddev_pop"), &tk2d_agg_arg("stddev_x", "population"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggy("stddev_pop"), &tk2d_agg_arg("stddev_y", "population"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggx("stddev_samp"), &tk2d_agg_arg("stddev_x", "sample"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggy("stddev_samp"), &tk2d_agg_arg("stddev_y", "sample"), EPS2);
            check_agg_equivalence(&state, &client, &pg1d_aggx("variance"), &tk2d_agg("variance_x"), EPS3);
            check_agg_equivalence(&state, &client, &pg1d_aggy("variance"), &tk2d_agg("variance_y"), EPS3);
            check_agg_equivalence(&state, &client, &pg1d_aggx("var_pop"), &tk2d_agg_arg("variance_x", "population"), EPS3);
            check_agg_equivalence(&state, &client, &pg1d_aggy("var_pop"), &tk2d_agg_arg("variance_y", "population"), EPS3);
            check_agg_equivalence(&state, &client, &pg1d_aggx("var_samp"), &tk2d_agg_arg("variance_x", "sample"), EPS3);
            check_agg_equivalence(&state, &client, &pg1d_aggy("var_samp"), &tk2d_agg_arg("variance_y", "sample"), EPS3);
            check_agg_equivalence(&state, &client, &pg2d_agg("regr_count"), &tk2d_agg("num_vals"), NONE);

            check_agg_equivalence(&state, &client, &pg2d_agg("regr_slope"), &tk2d_agg("slope"), EPS1);
            check_agg_equivalence(&state, &client, &pg2d_agg("corr"), &tk2d_agg("corr"), EPS1);
            check_agg_equivalence(&state, &client, &pg2d_agg("regr_intercept"), &tk2d_agg("intercept"), EPS1);
            // check_agg_equivalence(&state, &client, &pg2d_agg(""), &tk2d_agg("x_intercept"), 0.0000001); !!! No postgres equivalent for x_intercept
            check_agg_equivalence(&state, &client, &pg2d_agg("regr_r2"), &tk2d_agg("determination_coeff"), EPS1);
            check_agg_equivalence(&state, &client, &pg2d_agg("covar_pop"), &tk2d_agg_arg("covariance", "population"), EPS1);
            check_agg_equivalence(&state, &client, &pg2d_agg("covar_samp"), &tk2d_agg_arg("covariance", "sample"), EPS1);

            client.select("DROP TABLE test_table",
                None,
                None
            );
        });
    }
}
