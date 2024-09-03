use pgrx::*;
use twofloat::TwoFloat;

use crate::{
    accessors::{
        AccessorAverage, AccessorAverageX, AccessorAverageY, AccessorCorr, AccessorCovar,
        AccessorDeterminationCoeff, AccessorIntercept, AccessorKurtosis, AccessorKurtosisX,
        AccessorKurtosisY, AccessorNumVals, AccessorSkewness, AccessorSkewnessX, AccessorSkewnessY,
        AccessorSlope, AccessorStdDev, AccessorStdDevX, AccessorStdDevY, AccessorSum, AccessorSumX,
        AccessorSumY, AccessorVariance, AccessorVarianceX, AccessorVarianceY, AccessorXIntercept,
    },
    aggregate_utils::in_aggregate_context,
    build,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

pub use stats_agg::stats1d::StatsSummary1D as InternalStatsSummary1D;
pub use stats_agg::stats2d::StatsSummary2D as InternalStatsSummary2D;
use stats_agg::XYPair;

use self::Method::*;

use crate::raw::bytea;

type StatsSummary1DTF = InternalStatsSummary1D<TwoFloat>;
type StatsSummary2DTF = InternalStatsSummary2D<TwoFloat>;

pg_type! {
    #[derive(Debug, PartialEq)]
    struct StatsSummary1D {
        n: u64,
        sx: f64,
        sx2: f64,
        sx3: f64,
        sx4: f64,
    }
}

pg_type! {
    #[derive(Debug, PartialEq)]
    struct StatsSummary2D {
        n: u64,
        sx: f64,
        sx2: f64,
        sx3: f64,
        sx4: f64,
        sy: f64,
        sy2: f64,
        sy3: f64,
        sy4: f64,
        sxy: f64,
    }
}

ron_inout_funcs!(StatsSummary1D);
ron_inout_funcs!(StatsSummary2D);

impl<'input> StatsSummary1D<'input> {
    fn to_internal(&self) -> InternalStatsSummary1D<f64> {
        InternalStatsSummary1D {
            n: self.n,
            sx: self.sx,
            sx2: self.sx2,
            sx3: self.sx3,
            sx4: self.sx4,
        }
    }
    pub fn from_internal(st: InternalStatsSummary1D<f64>) -> Self {
        build!(StatsSummary1D {
            n: st.n,
            sx: st.sx,
            sx2: st.sx2,
            sx3: st.sx3,
            sx4: st.sx4,
        })
    }
}

impl<'input> StatsSummary2D<'input> {
    fn to_internal(&self) -> InternalStatsSummary2D<f64> {
        InternalStatsSummary2D {
            n: self.n,
            sx: self.sx,
            sx2: self.sx2,
            sx3: self.sx3,
            sx4: self.sx4,
            sy: self.sy,
            sy2: self.sy2,
            sy3: self.sy3,
            sy4: self.sy4,
            sxy: self.sxy,
        }
    }
    fn from_internal(st: InternalStatsSummary2D<f64>) -> Self {
        build!(StatsSummary2D {
            n: st.n,
            sx: st.sx,
            sx2: st.sx2,
            sx3: st.sx3,
            sx4: st.sx4,
            sy: st.sy,
            sy2: st.sy2,
            sy3: st.sy3,
            sy4: st.sy4,
            sxy: st.sxy,
        })
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn stats1d_trans_serialize(state: Internal) -> bytea {
    let ser: &StatsSummary1DData = unsafe { state.get().unwrap() };
    crate::do_serialize!(ser)
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn stats1d_trans_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    stats1d_trans_deserialize_inner(bytes).internal()
}
pub fn stats1d_trans_deserialize_inner(bytes: bytea) -> Inner<StatsSummary1D<'static>> {
    let de: StatsSummary1D = crate::do_deserialize!(bytes, StatsSummary1DData);
    de.into()
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn stats2d_trans_serialize(state: Internal) -> bytea {
    let ser: &StatsSummary2DData = unsafe { state.get().unwrap() };
    crate::do_serialize!(ser)
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn stats2d_trans_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    stats2d_trans_deserialize_inner(bytes).internal()
}
pub fn stats2d_trans_deserialize_inner(bytes: bytea) -> Inner<StatsSummary2D<'static>> {
    let de: StatsSummary2D = crate::do_deserialize!(bytes, StatsSummary2DData);
    de.into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats1d_trans<'s>(
    state: Internal,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats1d_trans_inner(unsafe { state.to_inner() }, val, fcinfo).internal()
}
#[pg_extern(immutable, parallel_safe)]
pub fn stats1d_tf_trans<'s>(
    state: Internal,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats1d_tf_trans_inner(unsafe { state.to_inner() }, val, fcinfo).internal()
}
pub fn stats1d_trans_inner(
    state: Option<Inner<StatsSummary1D>>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary1D>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, val) {
                (None, None) => {
                    Some(StatsSummary1D::from_internal(InternalStatsSummary1D::new()).into())
                } // return an empty one from the trans function because otherwise it breaks in the window context
                (Some(state), None) => Some(state),
                (None, Some(val)) => {
                    let mut s = InternalStatsSummary1D::new();
                    s.accum(val).unwrap();
                    Some(StatsSummary1D::from_internal(s).into())
                }
                (Some(mut state), Some(val)) => {
                    let mut s: InternalStatsSummary1D<f64> = state.to_internal();
                    s.accum(val).unwrap();
                    *state = StatsSummary1D::from_internal(s);
                    Some(state)
                }
            }
        })
    }
}
pub fn stats1d_tf_trans_inner(
    state: Option<Inner<StatsSummary1DTF>>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary1DTF>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, val) {
                (None, None) => Some(InternalStatsSummary1D::new().into()), // return an empty one from the trans function because otherwise it breaks in the window context
                (Some(state), None) => Some(state),
                (None, Some(val)) => {
                    let val = TwoFloat::from(val);
                    let mut s = InternalStatsSummary1D::new();
                    s.accum(val).unwrap();
                    Some(s.into())
                }
                (Some(mut state), Some(val)) => {
                    let val = TwoFloat::from(val);
                    state.accum(val).unwrap();
                    Some(state)
                }
            }
        })
    }
}

// Note that in general, for all stats2d cases, if either the y or x value is missing, we disregard the entire point as the n is shared between them
// if the user wants us to treat nulls as a particular value (ie zero), they can use COALESCE to do so
#[pg_extern(immutable, parallel_safe)]
pub fn stats2d_trans(
    state: Internal,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats2d_trans_inner(unsafe { state.to_inner() }, y, x, fcinfo).internal()
}
pub fn stats2d_trans_inner(
    state: Option<Inner<StatsSummary2D>>,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary2D>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let val: Option<XYPair<f64>> = match (y, x) {
                (None, _) => None,
                (_, None) => None,
                (Some(y), Some(x)) => Some(XYPair { y, x }),
            };
            match (state, val) {
                (None, None) => {
                    // return an empty one from the trans function because otherwise it breaks in the window context
                    Some(StatsSummary2D::from_internal(InternalStatsSummary2D::new()).into())
                }
                (Some(state), None) => Some(state),
                (None, Some(val)) => {
                    let mut s = InternalStatsSummary2D::new();
                    s.accum(val).unwrap();
                    Some(StatsSummary2D::from_internal(s).into())
                }
                (Some(mut state), Some(val)) => {
                    let mut s: InternalStatsSummary2D<f64> = state.to_internal();
                    s.accum(val).unwrap();
                    *state = StatsSummary2D::from_internal(s);
                    Some(state)
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats2d_tf_trans(
    state: Internal,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats2d_tf_trans_inner(unsafe { state.to_inner() }, y, x, fcinfo).internal()
}
pub fn stats2d_tf_trans_inner(
    state: Option<Inner<StatsSummary2DTF>>,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary2DTF>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let val: Option<XYPair<TwoFloat>> = match (y, x) {
                (None, _) => None,
                (_, None) => None,
                (Some(y), Some(x)) => Some(XYPair {
                    y: y.into(),
                    x: x.into(),
                }),
            };
            match (state, val) {
                (None, None) => {
                    // return an empty one from the trans function because otherwise it breaks in the window context
                    Some(StatsSummary2DTF::new().into())
                }
                (Some(state), None) => Some(state),
                (None, Some(val)) => {
                    let mut s = InternalStatsSummary2D::new();
                    s.accum(val).unwrap();
                    Some(s.into())
                }
                (Some(mut state), Some(val)) => {
                    let mut s: StatsSummary2DTF = *state;
                    s.accum(val).unwrap();
                    *state = s;
                    Some(state)
                }
            }
        })
    }
}

#[pg_extern(immutable)]
pub fn stats1d_inv_trans(
    state: Internal,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats1d_inv_trans_inner(unsafe { state.to_inner() }, val, fcinfo).internal()
}
pub fn stats1d_inv_trans_inner(
    state: Option<Inner<StatsSummary1D>>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary1D>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, val) {
            (None, _) => panic!("Inverse function should never be called with NULL state"),
            (Some(state), None) => Some(state),
            (Some(state), Some(val)) => {
                let s: InternalStatsSummary1D<f64> = state.to_internal();
                let s = s.remove(val);
                s.map(|s| StatsSummary1D::from_internal(s).into())
            }
        })
    }
}

#[pg_extern(immutable)]
pub fn stats1d_tf_inv_trans(
    state: Internal,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats1d_tf_inv_trans_inner(unsafe { state.to_inner() }, val, fcinfo).internal()
}
pub fn stats1d_tf_inv_trans_inner(
    state: Option<Inner<StatsSummary1DTF>>,
    val: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary1DTF>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, val) {
            (None, _) => panic!("Inverse function should never be called with NULL state"),
            (Some(state), None) => Some(state),
            (Some(state), Some(val)) => {
                let val = TwoFloat::new_add(val, 0.0);
                let state = state.remove(val);
                state.map(|s| s.into())
            }
        })
    }
}

#[pg_extern(immutable)]
pub fn stats2d_inv_trans(
    state: Internal,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats2d_inv_trans_inner(unsafe { state.to_inner() }, y, x, fcinfo).internal()
}
pub fn stats2d_inv_trans_inner(
    state: Option<Inner<StatsSummary2D>>,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary2D>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let val: Option<XYPair<f64>> = match (y, x) {
                (None, _) => None,
                (_, None) => None,
                (Some(y), Some(x)) => Some(XYPair { y, x }),
            };
            match (state, val) {
                (None, _) => panic!("Inverse function should never be called with NULL state"),
                (Some(state), None) => Some(state),
                (Some(state), Some(val)) => {
                    let s: InternalStatsSummary2D<f64> = state.to_internal();
                    let s = s.remove(val);
                    s.map(|s| StatsSummary2D::from_internal(s).into())
                }
            }
        })
    }
}

#[pg_extern(immutable)]
pub fn stats2d_tf_inv_trans(
    state: Internal,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats2d_tf_inv_trans_inner(unsafe { state.to_inner() }, y, x, fcinfo).internal()
}
pub fn stats2d_tf_inv_trans_inner(
    state: Option<Inner<StatsSummary2DTF>>,
    y: Option<f64>,
    x: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary2DTF>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let val: Option<XYPair<TwoFloat>> = match (y, x) {
                (None, _) => None,
                (_, None) => None,
                (Some(y), Some(x)) => Some(XYPair {
                    y: y.into(),
                    x: x.into(),
                }),
            };
            match (state, val) {
                (None, _) => panic!("Inverse function should never be called with NULL state"),
                (Some(state), None) => Some(state),
                (Some(state), Some(val)) => {
                    let s: InternalStatsSummary2D<TwoFloat> = *state;
                    let s = s.remove(val);
                    s.map(|s| s.into())
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats1d_summary_trans<'a>(
    state: Internal,
    value: Option<StatsSummary1D<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats1d_summary_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn stats1d_summary_trans_inner<'s>(
    state: Option<Inner<StatsSummary1D<'s>>>,
    value: Option<StatsSummary1D<'s>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (state, None) => state,
            (None, Some(value)) => Some(value.in_current_context().into()),
            (Some(state), Some(value)) => {
                let s = state.to_internal();
                let v = value.to_internal();
                let s = s.combine(v).unwrap();
                let s = StatsSummary1D::from_internal(s);
                Some(s.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats2d_summary_trans<'a>(
    state: Internal,
    value: Option<StatsSummary2D<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats2d_summary_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn stats2d_summary_trans_inner<'s>(
    state: Option<Inner<StatsSummary2D<'s>>>,
    value: Option<StatsSummary2D<'s>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary2D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (state, None) => state,
            (None, Some(value)) => Some(value.in_current_context().into()),
            (Some(state), Some(value)) => {
                let s = state.to_internal();
                let v = value.to_internal();
                let s = s.combine(v).unwrap();
                let s = StatsSummary2D::from_internal(s);
                Some(s.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats1d_summary_inv_trans<'a>(
    state: Internal,
    value: Option<StatsSummary1D<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats1d_summary_inv_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn stats1d_summary_inv_trans_inner<'s>(
    state: Option<Inner<StatsSummary1D<'s>>>,
    value: Option<StatsSummary1D>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, &value) {
            (None, _) => panic!("Inverse function should never be called with NULL state"),
            (Some(state), None) => Some(state),
            (Some(state), Some(value)) => {
                let s = state.to_internal();
                let v = value.to_internal();
                let s = s.remove_combined(v);
                s.map(|s| StatsSummary1D::from_internal(s).into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats2d_summary_inv_trans<'a>(
    state: Internal,
    value: Option<StatsSummary2D<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    stats2d_summary_inv_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn stats2d_summary_inv_trans_inner<'s>(
    state: Option<Inner<StatsSummary2D<'s>>>,
    value: Option<StatsSummary2D>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary2D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, &value) {
            (None, _) => panic!("Inverse function should never be called with NULL state"),
            (Some(state), None) => Some(state),
            (Some(state), Some(value)) => {
                let s = state.to_internal();
                let v = value.to_internal();
                let s = s.remove_combined(v);
                s.map(|s| StatsSummary2D::from_internal(s).into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats1d_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { stats1d_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}
pub fn stats1d_combine_inner<'s, 'v>(
    state1: Option<Inner<StatsSummary1D<'s>>>,
    state2: Option<Inner<StatsSummary1D<'v>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary1D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(state2)) => {
                let s = state2.in_current_context();
                Some(s.into())
            }
            (Some(state1), None) => {
                let s = state1.in_current_context();
                Some(s.into())
            }
            (Some(state1), Some(state2)) => {
                let s1 = state1.to_internal();
                let s2 = state2.to_internal();
                let s1 = s1.combine(s2).unwrap();
                Some(StatsSummary1D::from_internal(s1).into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn stats2d_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { stats2d_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}
pub fn stats2d_combine_inner<'s, 'v>(
    state1: Option<Inner<StatsSummary2D<'s>>>,
    state2: Option<Inner<StatsSummary2D<'v>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<StatsSummary2D<'s>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(state2)) => {
                let s = state2.in_current_context();
                Some(s.into())
            }
            (Some(state1), None) => {
                let s = state1.in_current_context();
                Some(s.into())
            }
            (Some(state1), Some(state2)) => {
                let s1 = state1.to_internal();
                let s2 = state2.to_internal();
                let s1 = s1.combine(s2).unwrap();
                Some(StatsSummary2D::from_internal(s1).into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn stats1d_final<'s>(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StatsSummary1D<'s>> {
    unsafe {
        in_aggregate_context(fcinfo, || match state.get() {
            None => None,
            Some(state) => {
                let state: &StatsSummary1D = state;
                Some(state.in_current_context())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn stats1d_tf_final<'s>(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
    // return a normal stats summary here
) -> Option<StatsSummary1D<'s>> {
    unsafe {
        in_aggregate_context(fcinfo, || match state.get() {
            None => None,
            Some(state) => {
                let state: &StatsSummary1DTF = state;
                let state: InternalStatsSummary1D<TwoFloat> = *state;
                let state: InternalStatsSummary1D<f64> = state.into();
                let state: StatsSummary1D = StatsSummary1D::from_internal(state);
                Some(state.in_current_context())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn stats2d_final<'s>(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StatsSummary2D<'s>> {
    unsafe {
        in_aggregate_context(fcinfo, || match state.get() {
            None => None,
            Some(state) => {
                let state: &StatsSummary2D = state;
                Some(state.in_current_context())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn stats2d_tf_final<'s>(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StatsSummary2D<'s>> {
    unsafe {
        in_aggregate_context(fcinfo, || match state.get() {
            None => None,
            Some(state) => {
                let state: StatsSummary2DTF = *state;
                let state: InternalStatsSummary2D<f64> = state.into();
                let state: StatsSummary2D = StatsSummary2D::from_internal(state);
                Some(state.in_current_context())
            }
        })
    }
}

// no serial/unserial/combine function for TwoFloats since moving aggregate mode and partial aggregate mode are mutually exclusiveca f
extension_sql!(
    "\n\
    CREATE AGGREGATE stats_agg( value DOUBLE PRECISION )\n\
    (\n\
        sfunc = stats1d_trans,\n\
        stype = internal,\n\
        finalfunc = stats1d_final,\n\
        combinefunc = stats1d_combine,\n\
        serialfunc = stats1d_trans_serialize,\n\
        deserialfunc = stats1d_trans_deserialize,\n\
        msfunc = stats1d_tf_trans,\n\
        minvfunc = stats1d_tf_inv_trans,\n\
        mstype = internal,\n\
        mfinalfunc = stats1d_tf_final,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_agg_1d",
    requires = [
        stats1d_trans,
        stats1d_final,
        stats1d_combine,
        stats1d_trans_serialize,
        stats1d_trans_deserialize,
        stats1d_trans,
        stats1d_inv_trans,
        stats1d_final
    ],
);

extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.stats_agg_tf( value DOUBLE PRECISION )\n\
    (\n\
        sfunc = stats1d_tf_trans,\n\
        stype = internal,\n\
        finalfunc = stats1d_tf_final,\n\
        msfunc = stats1d_tf_trans,\n\
        minvfunc = stats1d_tf_inv_trans,\n\
        mstype = internal,\n\
        mfinalfunc = stats1d_tf_final,\n\
        parallel = safe\n\
    );",
    name = "stats_agg_tf_1d",
    requires = [
        stats1d_tf_trans,
        stats1d_tf_final,
        stats1d_tf_trans,
        stats1d_tf_inv_trans,
        stats1d_tf_final
    ],
);

// mostly for testing/debugging, in case we want one without the inverse functions defined.
extension_sql!(
    "\n\
    CREATE AGGREGATE stats_agg_no_inv( value DOUBLE PRECISION )\n\
    (\n\
        sfunc = stats1d_trans,\n\
        stype = internal,\n\
        finalfunc = stats1d_final,\n\
        combinefunc = stats1d_combine,\n\
        serialfunc = stats1d_trans_serialize,\n\
        deserialfunc = stats1d_trans_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_agg_no_inv",
    requires = [stats1d_trans, stats1d_final, stats1d_combine],
);

// same things for the 2d case
extension_sql!(
    "\n\
    CREATE AGGREGATE stats_agg( y DOUBLE PRECISION, x DOUBLE PRECISION )\n\
    (\n\
        sfunc = stats2d_trans,\n\
        stype = internal,\n\
        finalfunc = stats2d_final,\n\
        combinefunc = stats2d_combine,\n\
        serialfunc = stats2d_trans_serialize,\n\
        deserialfunc = stats2d_trans_deserialize,\n\
        msfunc = stats2d_tf_trans,\n\
        minvfunc = stats2d_tf_inv_trans,\n\
        mstype = internal,\n\
        mfinalfunc = stats2d_tf_final,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_agg_2d",
    requires = [
        stats2d_trans,
        stats2d_final,
        stats2d_combine,
        stats2d_trans_serialize,
        stats2d_trans_deserialize,
        stats2d_trans,
        stats2d_inv_trans,
        stats2d_final
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.stats_agg_tf( y DOUBLE PRECISION, x DOUBLE PRECISION )\n\
    (\n\
        sfunc = stats2d_tf_trans,\n\
        stype = internal,\n\
        finalfunc = stats2d_tf_final,\n\
        msfunc = stats2d_tf_trans,\n\
        minvfunc = stats2d_tf_inv_trans,\n\
        mstype = internal,\n\
        mfinalfunc = stats2d_tf_final,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_agg_2d_tf",
    requires = [stats2d_tf_trans, stats2d_tf_inv_trans, stats2d_tf_final],
);

// mostly for testing/debugging, in case we want one without the inverse functions defined.
extension_sql!(
    "\n\
    CREATE AGGREGATE stats_agg_no_inv( y DOUBLE PRECISION, x DOUBLE PRECISION )\n\
    (\n\
        sfunc = stats2d_trans,\n\
        stype = internal,\n\
        finalfunc = stats2d_final,\n\
        combinefunc = stats2d_combine,\n\
        serialfunc = stats2d_trans_serialize,\n\
        deserialfunc = stats2d_trans_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_agg2_no_inv",
    requires = [stats2d_trans, stats2d_final, stats2d_combine],
);

//  Currently, rollup does not have the inverse function so if you want the behavior where we don't use the inverse,
// you can use it in your window functions (useful for our own perf testing as well)

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(ss statssummary1d)\n\
    (\n\
        sfunc = stats1d_summary_trans,\n\
        stype = internal,\n\
        finalfunc = stats1d_final,\n\
        combinefunc = stats1d_combine,\n\
        serialfunc = stats1d_trans_serialize,\n\
        deserialfunc = stats1d_trans_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_1d_rollup",
    requires = [
        stats1d_summary_trans,
        stats1d_final,
        stats1d_combine,
        stats1d_trans_serialize,
        stats1d_trans_deserialize
    ],
);

//  For UI, we decided to have slightly differently named functions for the windowed context and not, so that it reads better, as well as using the inverse function only in the window context
extension_sql!(
    "\n\
    CREATE AGGREGATE rolling(ss statssummary1d)\n\
    (\n\
        sfunc = stats1d_summary_trans,\n\
        stype = internal,\n\
        finalfunc = stats1d_final,\n\
        combinefunc = stats1d_combine,\n\
        serialfunc = stats1d_trans_serialize,\n\
        deserialfunc = stats1d_trans_deserialize,\n\
        msfunc = stats1d_summary_trans,\n\
        minvfunc = stats1d_summary_inv_trans,\n\
        mstype = internal,\n\
        mfinalfunc = stats1d_final,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_1d_rolling",
    requires = [
        stats1d_summary_trans,
        stats1d_final,
        stats1d_combine,
        stats1d_trans_serialize,
        stats1d_trans_deserialize,
        stats1d_summary_inv_trans
    ],
);

// Same as for the 1D case, but for the 2D

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(ss statssummary2d)\n\
    (\n\
        sfunc = stats2d_summary_trans,\n\
        stype = internal,\n\
        finalfunc = stats2d_final,\n\
        combinefunc = stats2d_combine,\n\
        serialfunc = stats2d_trans_serialize,\n\
        deserialfunc = stats2d_trans_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_2d_rollup",
    requires = [
        stats2d_summary_trans,
        stats2d_final,
        stats2d_combine,
        stats2d_trans_serialize,
        stats2d_trans_deserialize
    ],
);

//  For UI, we decided to have slightly differently named functions for the windowed context and not, so that it reads better, as well as using the inverse function only in the window context
extension_sql!(
    "\n\
    CREATE AGGREGATE rolling(ss statssummary2d)\n\
    (\n\
        sfunc = stats2d_summary_trans,\n\
        stype = internal,\n\
        finalfunc = stats2d_final,\n\
        combinefunc = stats2d_combine,\n\
        serialfunc = stats2d_trans_serialize,\n\
        deserialfunc = stats2d_trans_deserialize,\n\
        msfunc = stats2d_summary_trans,\n\
        minvfunc = stats2d_summary_inv_trans,\n\
        mstype = internal,\n\
        mfinalfunc = stats2d_final,\n\
        parallel = safe\n\
    );\n\
",
    name = "stats_2d_rolling",
    requires = [
        stats2d_summary_trans,
        stats2d_final,
        stats2d_combine,
        stats2d_trans_serialize,
        stats2d_trans_deserialize,
        stats2d_summary_inv_trans
    ],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats1d_average<'a>(
    sketch: StatsSummary1D<'a>,
    _accessor: AccessorAverage<'a>,
) -> Option<f64> {
    stats1d_average(sketch)
}

#[pg_extern(name = "average", strict, immutable, parallel_safe)]
pub(crate) fn stats1d_average<'a>(summary: StatsSummary1D<'a>) -> Option<f64> {
    summary.to_internal().avg()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats1d_sum<'a>(
    sketch: StatsSummary1D<'a>,
    _accessor: AccessorSum<'a>,
) -> Option<f64> {
    stats1d_sum(sketch)
}

#[pg_extern(name = "sum", strict, immutable, parallel_safe)]
pub(crate) fn stats1d_sum<'a>(summary: StatsSummary1D<'a>) -> Option<f64> {
    summary.to_internal().sum()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats1d_stddev<'a>(
    sketch: Option<StatsSummary1D<'a>>,
    accessor: AccessorStdDev<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats1d_stddev(sketch, &method)
}

#[pg_extern(name = "stddev", immutable, parallel_safe)]
fn stats1d_stddev<'a>(
    summary: Option<StatsSummary1D<'a>>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => summary?.to_internal().stddev_pop(),
        Sample => summary?.to_internal().stddev_samp(),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats1d_variance<'a>(
    sketch: Option<StatsSummary1D<'a>>,
    accessor: AccessorVariance<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats1d_variance(sketch, &method)
}

#[pg_extern(name = "variance", immutable, parallel_safe)]
fn stats1d_variance<'a>(
    summary: Option<StatsSummary1D<'a>>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => summary?.to_internal().var_pop(),
        Sample => summary?.to_internal().var_samp(),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats1d_skewness<'a>(
    sketch: StatsSummary1D<'a>,
    accessor: AccessorSkewness<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats1d_skewness(sketch, &method)
}

#[pg_extern(name = "skewness", immutable, parallel_safe)]
fn stats1d_skewness<'a>(
    summary: StatsSummary1D<'a>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => summary.to_internal().skewness_pop(),
        Sample => summary.to_internal().skewness_samp(),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats1d_kurtosis<'a>(
    sketch: StatsSummary1D<'a>,
    accessor: AccessorKurtosis<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats1d_kurtosis(sketch, &method)
}

#[pg_extern(name = "kurtosis", immutable, parallel_safe)]
fn stats1d_kurtosis<'a>(
    summary: StatsSummary1D<'a>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => summary.to_internal().kurtosis_pop(),
        Sample => summary.to_internal().kurtosis_samp(),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats1d_num_vals<'a>(
    sketch: StatsSummary1D<'a>,
    _accessor: AccessorNumVals<'a>,
) -> i64 {
    stats1d_num_vals(sketch)
}

#[pg_extern(name = "num_vals", strict, immutable, parallel_safe)]
fn stats1d_num_vals<'a>(summary: StatsSummary1D<'a>) -> i64 {
    summary.to_internal().count()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_average_x<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorAverageX<'a>,
) -> Option<f64> {
    stats2d_average_x(sketch)
}

#[pg_extern(name = "average_x", strict, immutable, parallel_safe)]
fn stats2d_average_x<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    Some(summary.to_internal().avg()?.x)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_average_y<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorAverageY<'a>,
) -> Option<f64> {
    stats2d_average_y(sketch)
}

#[pg_extern(name = "average_y", strict, immutable, parallel_safe)]
fn stats2d_average_y<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    Some(summary.to_internal().avg()?.y)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_sum_x<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorSumX<'a>,
) -> Option<f64> {
    stats2d_sum_x(sketch)
}

#[pg_extern(name = "sum_x", strict, immutable, parallel_safe)]
fn stats2d_sum_x<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    Some(summary.to_internal().sum()?.x)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_sum_y<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorSumY<'a>,
) -> Option<f64> {
    stats2d_sum_y(sketch)
}

#[pg_extern(name = "sum_y", strict, immutable, parallel_safe)]
fn stats2d_sum_y<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    Some(summary.to_internal().sum()?.y)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_stdddev_x<'a>(
    sketch: Option<StatsSummary2D<'a>>,
    accessor: AccessorStdDevX<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_stddev_x(sketch, &method)
}

#[pg_extern(name = "stddev_x", immutable, parallel_safe)]
fn stats2d_stddev_x<'a>(
    summary: Option<StatsSummary2D<'a>>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary?.to_internal().stddev_pop()?.x),
        Sample => Some(summary?.to_internal().stddev_samp()?.x),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_stdddev_y<'a>(
    sketch: Option<StatsSummary2D<'a>>,
    accessor: AccessorStdDevY<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_stddev_y(sketch, &method)
}

#[pg_extern(name = "stddev_y", immutable, parallel_safe)]
fn stats2d_stddev_y<'a>(
    summary: Option<StatsSummary2D<'a>>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary?.to_internal().stddev_pop()?.y),
        Sample => Some(summary?.to_internal().stddev_samp()?.y),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_variance_x<'a>(
    sketch: Option<StatsSummary2D<'a>>,
    accessor: AccessorVarianceX<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_variance_x(sketch, &method)
}

#[pg_extern(name = "variance_x", immutable, parallel_safe)]
fn stats2d_variance_x<'a>(
    summary: Option<StatsSummary2D<'a>>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary?.to_internal().var_pop()?.x),
        Sample => Some(summary?.to_internal().var_samp()?.x),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_variance_y<'a>(
    sketch: Option<StatsSummary2D<'a>>,
    accessor: AccessorVarianceY<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_variance_y(sketch, &method)
}

#[pg_extern(name = "variance_y", immutable, parallel_safe)]
fn stats2d_variance_y<'a>(
    summary: Option<StatsSummary2D<'a>>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary?.to_internal().var_pop()?.y),
        Sample => Some(summary?.to_internal().var_samp()?.y),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_skewness_x<'a>(
    sketch: StatsSummary2D<'a>,
    accessor: AccessorSkewnessX<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_skewness_x(sketch, &method)
}

#[pg_extern(name = "skewness_x", strict, immutable, parallel_safe)]
fn stats2d_skewness_x<'a>(
    summary: StatsSummary2D<'a>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary.to_internal().skewness_pop()?.x),
        Sample => Some(summary.to_internal().skewness_samp()?.x),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_skewness_y<'a>(
    sketch: StatsSummary2D<'a>,
    accessor: AccessorSkewnessY<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_skewness_y(sketch, &method)
}

#[pg_extern(name = "skewness_y", strict, immutable, parallel_safe)]
fn stats2d_skewness_y<'a>(
    summary: StatsSummary2D<'a>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary.to_internal().skewness_pop()?.y),
        Sample => Some(summary.to_internal().skewness_samp()?.y),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_kurtosis_x<'a>(
    sketch: StatsSummary2D<'a>,
    accessor: AccessorKurtosisX<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_kurtosis_x(sketch, &method)
}

#[pg_extern(name = "kurtosis_x", strict, immutable, parallel_safe)]
fn stats2d_kurtosis_x<'a>(
    summary: StatsSummary2D<'a>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary.to_internal().kurtosis_pop()?.x),
        Sample => Some(summary.to_internal().kurtosis_samp()?.x),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_kurtosis_y<'a>(
    sketch: StatsSummary2D<'a>,
    accessor: AccessorKurtosisY<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_kurtosis_y(sketch, &method)
}

#[pg_extern(name = "kurtosis_y", strict, immutable, parallel_safe)]
fn stats2d_kurtosis_y<'a>(
    summary: StatsSummary2D<'a>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => Some(summary.to_internal().kurtosis_pop()?.y),
        Sample => Some(summary.to_internal().kurtosis_samp()?.y),
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_num_vals<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorNumVals<'a>,
) -> i64 {
    stats2d_num_vals(sketch)
}

#[pg_extern(name = "num_vals", strict, immutable, parallel_safe)]
fn stats2d_num_vals<'a>(summary: StatsSummary2D<'a>) -> i64 {
    summary.to_internal().count()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_slope<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorSlope<'a>,
) -> Option<f64> {
    stats2d_slope(sketch)
}

#[pg_extern(name = "slope", strict, immutable, parallel_safe)]
fn stats2d_slope<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    summary.to_internal().slope()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_corr<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorCorr<'a>,
) -> Option<f64> {
    stats2d_corr(sketch)
}

#[pg_extern(name = "corr", strict, immutable, parallel_safe)]
fn stats2d_corr<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    summary.to_internal().corr()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_intercept<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorIntercept<'a>,
) -> Option<f64> {
    stats2d_intercept(sketch)
}

#[pg_extern(name = "intercept", strict, immutable, parallel_safe)]
fn stats2d_intercept<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    summary.to_internal().intercept()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_x_intercept<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorXIntercept<'a>,
) -> Option<f64> {
    stats2d_x_intercept(sketch)
}

#[pg_extern(name = "x_intercept", strict, immutable, parallel_safe)]
fn stats2d_x_intercept<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    summary.to_internal().x_intercept()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_determination_coeff<'a>(
    sketch: StatsSummary2D<'a>,
    _accessor: AccessorDeterminationCoeff<'a>,
) -> Option<f64> {
    stats2d_determination_coeff(sketch)
}

#[pg_extern(name = "determination_coeff", strict, immutable, parallel_safe)]
fn stats2d_determination_coeff<'a>(summary: StatsSummary2D<'a>) -> Option<f64> {
    summary.to_internal().determination_coeff()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_stats2d_covar<'a>(
    sketch: Option<StatsSummary2D<'a>>,
    accessor: AccessorCovar<'a>,
) -> Option<f64> {
    let method = String::from_utf8_lossy(accessor.bytes.as_slice());
    stats2d_covar(sketch, &method)
}

#[pg_extern(name = "covariance", immutable, parallel_safe)]
fn stats2d_covar<'a>(
    summary: Option<StatsSummary2D<'a>>,
    method: default!(&str, "'sample'"),
) -> Option<f64> {
    match method_kind(method) {
        Population => summary?.to_internal().covar_pop(),
        Sample => summary?.to_internal().covar_samp(),
    }
}

#[derive(Clone, Copy)]
pub enum Method {
    Population,
    Sample,
}

#[track_caller]
pub fn method_kind(method: &str) -> Method {
    match as_method(method) {
        Some(method) => method,
        None => {
            pgrx::error!("unknown analysis method. Valid methods are 'population' and 'sample'")
        }
    }
}

pub fn as_method(method: &str) -> Option<Method> {
    match method.trim().to_lowercase().as_str() {
        "population" | "pop" => Some(Population),
        "sample" | "samp" => Some(Sample),
        _ => None,
    }
}

// TODO: Add testing - probably want to do some fuzz testing against the Postgres implementations of the same. Possibly translate the Postgres tests as well?
// #[cfg(any(test, feature = "pg_test"))]
// mod tests {

//     use approx::assert_relative_eq;
//     use pgrx::*;
//     use super::*;

//     macro_rules! select_one {
//         ($client:expr, $stmt:expr, $type:ty) => {
//             $client
//                 .update($stmt, None, None)
//                 .first()
//                 .get_one::<$type>()
//                 .unwrap()
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
//     //     Spi::connect(|mut client| {

//     //     });
//     // }
// }

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use approx::relative_eq;

    use pgrx_macros::pg_test;
    use rand::rngs::SmallRng;
    use rand::seq::SliceRandom;
    use rand::{self, Rng, SeedableRng};

    const RUNS: usize = 10; // Number of runs to generate
    const VALS: usize = 10000; // Number of values to use for each run
    const SEED: Option<u64> = None; // RNG seed, generated from entropy if None
    const PRINT_VALS: bool = false; // Print out test values on error, this can be spammy if VALS is high

    #[pg_test]
    fn test_stats_agg_text_io() {
        Spi::connect(|mut client| {
            client
                .update(
                    "CREATE TABLE test_table (test_x DOUBLE PRECISION, test_y DOUBLE PRECISION)",
                    None,
                    None,
                )
                .unwrap();

            let test = client
                .update(
                    "SELECT stats_agg(test_y, test_x)::TEXT FROM test_table",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert!(test.is_none());

            client
                .update("INSERT INTO test_table VALUES (10, 10);", None, None)
                .unwrap();

            let test = client
                .update(
                    "SELECT stats_agg(test_y, test_x)::TEXT FROM test_table",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                test,
                "(version:1,n:1,sx:10,sx2:0,sx3:0,sx4:0,sy:10,sy2:0,sy3:0,sy4:0,sxy:0)"
            );

            client
                .update("INSERT INTO test_table VALUES (20, 20);", None, None)
                .unwrap();
            let test = client
                .update(
                    "SELECT stats_agg(test_y, test_x)::TEXT FROM test_table",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            let expected =
                "(version:1,n:2,sx:30,sx2:50,sx3:0,sx4:1250,sy:30,sy2:50,sy3:0,sy4:1250,sxy:50)";
            assert_eq!(test, expected);

            // Test a few functions to see that the text serialized object behave the same as the constructed one
            assert_eq!(
                client
                    .update(
                        "SELECT skewness_x(stats_agg(test_y, test_x)) FROM test_table",
                        None,
                        None
                    )
                    .unwrap()
                    .first()
                    .get_one::<f64>(),
                client
                    .update(
                        &format!("SELECT skewness_x('{}'::StatsSummary2D)", expected),
                        None,
                        None
                    )
                    .unwrap()
                    .first()
                    .get_one::<f64>()
            );
            assert_eq!(
                client
                    .update(
                        "SELECT kurtosis_y(stats_agg(test_y, test_x)) FROM test_table",
                        None,
                        None
                    )
                    .unwrap()
                    .first()
                    .get_one::<f64>(),
                client
                    .update(
                        &format!("SELECT kurtosis_y('{}'::StatsSummary2D)", expected),
                        None,
                        None
                    )
                    .unwrap()
                    .first()
                    .get_one::<f64>()
            );
            assert_eq!(
                client
                    .update(
                        "SELECT covariance(stats_agg(test_y, test_x)) FROM test_table",
                        None,
                        None
                    )
                    .unwrap()
                    .first()
                    .get_one::<f64>(),
                client
                    .update(
                        &format!("SELECT covariance('{}'::StatsSummary2D)", expected),
                        None,
                        None
                    )
                    .unwrap()
                    .first()
                    .get_one::<f64>()
            );

            // Test text round trip
            assert_eq!(
                client
                    .update(
                        &format!("SELECT '{}'::StatsSummary2D::TEXT", expected),
                        None,
                        None
                    )
                    .unwrap()
                    .first()
                    .get_one::<String>()
                    .unwrap()
                    .unwrap(),
                expected
            );

            client
                .update("INSERT INTO test_table VALUES ('NaN', 30);", None, None)
                .unwrap();
            let test = client
                .update(
                    "SELECT stats_agg(test_y, test_x)::TEXT FROM test_table",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(test, "(version:1,n:3,sx:NaN,sx2:NaN,sx3:NaN,sx4:NaN,sy:60,sy2:200,sy3:0,sy4:20000,sxy:NaN)");

            client
                .update("INSERT INTO test_table VALUES (40, 'Inf');", None, None)
                .unwrap();
            let test = client
                .update(
                    "SELECT stats_agg(test_y, test_x)::TEXT FROM test_table",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(test, "(version:1,n:4,sx:NaN,sx2:NaN,sx3:NaN,sx4:NaN,sy:inf,sy2:NaN,sy3:NaN,sy4:NaN,sxy:NaN)");
        });
    }

    #[pg_test]
    fn test_stats_agg_byte_io() {
        unsafe {
            use std::ptr;
            let state = stats1d_trans_inner(None, Some(14.0), ptr::null_mut());
            let state = stats1d_trans_inner(state, Some(18.0), ptr::null_mut());
            let state = stats1d_trans_inner(state, Some(22.7), ptr::null_mut());
            let state = stats1d_trans_inner(state, Some(39.42), ptr::null_mut());
            let state = stats1d_trans_inner(state, Some(-43.0), ptr::null_mut());

            let control = state.unwrap();
            let buffer = stats1d_trans_serialize(Inner::from(control.clone()).internal().unwrap());
            let slice_for_test =
                pgrx::varlena::varlena_to_byte_slice(buffer.0.cast_mut_ptr::<pg_sys::varlena>());
            println!(
                "debug serializer output after return:\n{:?}",
                slice_for_test
            );

            // let expected = pgrx::varlena::rust_byte_slice_to_bytea(buffer);
            // let new_state =
            //     stats1d_trans_deserialize_inner(bytea(pg_sys::Datum::from(expected.as_ptr())));
            let new_state = stats1d_trans_deserialize_inner(buffer);

            assert_eq!(
                &*new_state, &*control,
                "unexpected difference in bytes output, got:\n{:?}\nexpected:\n{:?}",
                &*new_state, &*control
            );
        }
    }

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
                None => SmallRng::from_entropy().gen(),
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
            let exp_base = self
                .gen
                .gen_range((f64::MIN_EXP / 10) as f64..(f64::MAX_EXP / 10) as f64);

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

    #[allow(clippy::float_cmp)]
    fn check_agg_equivalence(
        state: &TestState,
        client: &mut pgrx::spi::SpiClient,
        pg_cmd: &str,
        tk_cmd: &str,
        allowed_diff: f64,
        do_moving_agg: bool,
    ) {
        warning!("pg_cmd={} ; tk_cmd={}", pg_cmd, tk_cmd);
        let pg_row = client.update(pg_cmd, None, None).unwrap().first();
        let (pg_result, pg_moving_agg_result) = if do_moving_agg {
            pg_row.get_two::<f64, f64>().unwrap()
        } else {
            (pg_row.get_one::<f64>().unwrap(), None)
        };
        let pg_result = pg_result.unwrap();

        let (tk_result, arrow_result, tk_moving_agg_result) = client
            .update(tk_cmd, None, None)
            .unwrap()
            .first()
            .get_three::<f64, f64, f64>()
            .unwrap();
        let (tk_result, arrow_result) = (tk_result.unwrap(), arrow_result.unwrap());
        assert_eq!(tk_result, arrow_result, "Arrow didn't match in {}", tk_cmd);

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
                {}",
                pg_cmd,
                tk_cmd,
                pg_result,
                tk_result,
                abs_diff / abs_max,
                allowed_diff,
                state.failed_msg(PRINT_VALS)
            );
        }

        if do_moving_agg {
            approx::assert_relative_eq!(
                pg_moving_agg_result.unwrap(),
                tk_moving_agg_result.unwrap(),
                max_relative = 1e-9,
            )
        }
    }

    fn pg1d_aggx(agg: &str) -> String {
        format!("SELECT {agg}(test_x)::float, (SELECT {agg}(test_x) OVER (ORDER BY test_x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM test_table LIMIT 1 OFFSET 3)::float FROM test_table", agg = agg)
    }

    fn pg1d_aggy(agg: &str) -> String {
        format!("SELECT {agg}(test_y), (SELECT {agg}(test_y) OVER (ORDER BY test_x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM test_table LIMIT 1 OFFSET 3) FROM test_table", agg = agg)
    }

    fn pg2d_agg(agg: &str) -> String {
        format!("SELECT {agg}(test_y, test_x)::float, (SELECT {agg}(test_y, test_x) OVER (ORDER BY test_x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM test_table LIMIT 1 OFFSET 3)::float FROM test_table", agg = agg)
    }

    fn tk1d_agg(agg: &str) -> String {
        format!(
            "SELECT \
            {agg}(stats_agg(test_x))::float, \
            (stats_agg(test_x)->{agg}())::float, \
            {agg}((SELECT stats_agg(test_x) OVER (ORDER BY test_x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM test_table LIMIT 1 OFFSET 3))::float \
        FROM test_table",
            agg = agg
        )
    }

    fn tk1d_agg_arg(agg: &str, arg: &str) -> String {
        format!(
            "SELECT \
            {agg}(stats_agg(test_x), '{arg}'), \
            stats_agg(test_x)->{agg}('{arg}'), \
            {agg}((SELECT stats_agg(test_x) OVER (ORDER BY test_x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM test_table LIMIT 1 OFFSET 3), '{arg}') \
        FROM test_table",
            agg = agg,
            arg = arg
        )
    }

    fn tk2d_agg(agg: &str) -> String {
        format!(
            "SELECT \
            {agg}(stats_agg(test_y, test_x))::float, \
            (stats_agg(test_y, test_x)->{agg}())::float, \
            {agg}((SELECT stats_agg(test_y, test_x) OVER (ORDER BY test_x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM test_table LIMIT 1 OFFSET 3))::float \
        FROM test_table",
            agg = agg
        )
    }

    fn tk2d_agg_arg(agg: &str, arg: &str) -> String {
        format!(
            "SELECT \
            {agg}(stats_agg(test_y, test_x), '{arg}'), \
            stats_agg(test_y, test_x)->{agg}('{arg}'), \
            {agg}((SELECT stats_agg(test_y, test_x) OVER (ORDER BY test_x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM test_table LIMIT 1 OFFSET 3), '{arg}') \
        FROM test_table",
            agg = agg,
            arg = arg
        )
    }

    fn pg_moment_pop_query(moment: i32, column: &str) -> String {
        format!("select sum(({} - a.avg)^{}) / count({}) / (stddev_pop({})^{}) from test_table, (select avg({}) from test_table) a", column, moment, column, column, moment, column)
    }

    fn pg_moment_samp_query(moment: i32, column: &str) -> String {
        format!("select sum(({} - a.avg)^{}) / (count({}) - 1) / (stddev_samp({})^{}) from test_table, (select avg({}) from test_table) a", column, moment, column, column, moment, column)
    }

    fn test_aggs(state: &mut TestState) {
        Spi::connect(|mut client| {
            client
                .update(
                    "CREATE TABLE test_table (test_x DOUBLE PRECISION, test_y DOUBLE PRECISION)",
                    None,
                    None,
                )
                .unwrap();

            client
                .update(
                    &format!(
                        "INSERT INTO test_table VALUES {}",
                        state
                            .x_values
                            .iter()
                            .zip(state.y_values.iter())
                            .map(|(x, y)| "(".to_string()
                                + &x.to_string()
                                + ","
                                + &y.to_string()
                                + ")"
                                + ",")
                            .collect::<String>()
                            .trim_end_matches(',')
                    ),
                    None,
                    None,
                )
                .unwrap();

            // Definitions for allowed errors for different aggregates
            const NONE: f64 = 0.; // Exact match
            const EPS1: f64 = f64::EPSILON; // Generally enough to handle float rounding
            const EPS2: f64 = 2. * f64::EPSILON; // stddev is sqrt(variance), so a bit looser bound
            const EPS3: f64 = 3. * f64::EPSILON; // Sum of squares in variance agg accumulates a bit more error
            const BILLIONTH: f64 = 1e-9; // Higher order moments exponentially compound the error

            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("avg"),
                &tk1d_agg("average"),
                NONE,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("sum"),
                &tk1d_agg("sum"),
                NONE,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("count"),
                &tk1d_agg("num_vals"),
                NONE,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("stddev"),
                &tk1d_agg("stddev"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("stddev_pop"),
                &tk1d_agg_arg("stddev", "population"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("stddev_samp"),
                &tk1d_agg_arg("stddev", "sample"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("variance"),
                &tk1d_agg("variance"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("var_pop"),
                &tk1d_agg_arg("variance", "population"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("var_samp"),
                &tk1d_agg_arg("variance", "sample"),
                EPS3,
                true,
            );

            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("regr_avgx"),
                &tk2d_agg("average_x"),
                NONE,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("regr_avgy"),
                &tk2d_agg("average_y"),
                NONE,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("sum"),
                &tk2d_agg("sum_x"),
                NONE,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggy("sum"),
                &tk2d_agg("sum_y"),
                NONE,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("stddev"),
                &tk2d_agg("stddev_x"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggy("stddev"),
                &tk2d_agg("stddev_y"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("stddev_pop"),
                &tk2d_agg_arg("stddev_x", "population"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggy("stddev_pop"),
                &tk2d_agg_arg("stddev_y", "population"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("stddev_samp"),
                &tk2d_agg_arg("stddev_x", "sample"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggy("stddev_samp"),
                &tk2d_agg_arg("stddev_y", "sample"),
                EPS2,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("variance"),
                &tk2d_agg("variance_x"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggy("variance"),
                &tk2d_agg("variance_y"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("var_pop"),
                &tk2d_agg_arg("variance_x", "population"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggy("var_pop"),
                &tk2d_agg_arg("variance_y", "population"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggx("var_samp"),
                &tk2d_agg_arg("variance_x", "sample"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg1d_aggy("var_samp"),
                &tk2d_agg_arg("variance_y", "sample"),
                EPS3,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("regr_count"),
                &tk2d_agg("num_vals"),
                NONE,
                true,
            );

            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("regr_slope"),
                &tk2d_agg("slope"),
                EPS1,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("corr"),
                &tk2d_agg("corr"),
                EPS1,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("regr_intercept"),
                &tk2d_agg("intercept"),
                EPS1,
                true,
            );

            // No postgres equivalent for x_intercept, so we only test function vs. arrow operator.
            {
                let query = tk2d_agg("x_intercept");
                let (result, arrow_result) = client
                    .update(&query, None, None)
                    .unwrap()
                    .first()
                    .get_two::<f64, f64>()
                    .unwrap();
                assert_eq!(result, arrow_result, "Arrow didn't match in {}", query);
            }

            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("regr_r2"),
                &tk2d_agg("determination_coeff"),
                EPS1,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("covar_pop"),
                &tk2d_agg_arg("covariance", "population"),
                BILLIONTH,
                true,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg2d_agg("covar_samp"),
                &tk2d_agg_arg("covariance", "sample"),
                BILLIONTH,
                true,
            );

            // Skewness and kurtosis don't have aggregate functions in postgres, but we can compute them
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_pop_query(3, "test_x"),
                &tk1d_agg_arg("skewness", "population"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_pop_query(3, "test_x"),
                &tk2d_agg_arg("skewness_x", "population"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_pop_query(3, "test_y"),
                &tk2d_agg_arg("skewness_y", "population"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_pop_query(4, "test_x"),
                &tk1d_agg_arg("kurtosis", "population"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_pop_query(4, "test_x"),
                &tk2d_agg_arg("kurtosis_x", "population"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_pop_query(4, "test_y"),
                &tk2d_agg_arg("kurtosis_y", "population"),
                BILLIONTH,
                false,
            );

            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_samp_query(3, "test_x"),
                &tk1d_agg_arg("skewness", "sample"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_samp_query(3, "test_x"),
                &tk2d_agg_arg("skewness_x", "sample"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_samp_query(3, "test_y"),
                &tk2d_agg_arg("skewness_y", "sample"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_samp_query(4, "test_x"),
                &tk1d_agg_arg("kurtosis", "sample"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_samp_query(4, "test_x"),
                &tk2d_agg_arg("kurtosis_x", "sample"),
                BILLIONTH,
                false,
            );
            check_agg_equivalence(
                state,
                &mut client,
                &pg_moment_samp_query(4, "test_y"),
                &tk2d_agg_arg("kurtosis_y", "sample"),
                BILLIONTH,
                false,
            );

            client.update("DROP TABLE test_table", None, None).unwrap();
        });
    }

    #[pg_test]
    fn stats_agg_rolling() {
        Spi::connect(|mut client| {
            client
                .update(
                    "
SET timezone TO 'UTC';
CREATE TABLE prices(ts TIMESTAMPTZ, price FLOAT);
INSERT INTO prices (
    WITH dates AS
        (SELECT
            *
        FROM
            generate_series('2020-01-01 00:00'::timestamp, '2020-02-01 12:00', '10 minutes') time)
    SELECT
        dates.time,
        (select (random()+EXTRACT(seconds FROM dates.time))*100 ) price
    FROM
        dates
);
",
                    None,
                    None,
                )
                .unwrap();

            let mut vals = client.update(
                "SELECT stddev(data.stats_agg) FROM (SELECT stats_agg(price) OVER (ORDER BY ts RANGE '50 minutes' PRECEDING) FROM prices) data",
                None, None
            ).unwrap();
            assert!(vals.next().unwrap()[1]
                .value::<f64>()
                .unwrap()
                .unwrap()
                .is_nan());
            assert!(vals.next().unwrap()[1].value::<f64>().unwrap().is_some());
            assert!(vals.next().unwrap()[1].value::<f64>().unwrap().is_some());

            let mut vals = client.update(
                "SELECT slope(data.stats_agg) FROM (SELECT stats_agg((EXTRACT(minutes FROM ts)), price) OVER (ORDER BY ts RANGE '50 minutes' PRECEDING) FROM prices) data;",
                None, None
            ).unwrap();
            assert!(vals.next().unwrap()[1].value::<f64>().unwrap().is_none()); // trendline is zero initially
            assert!(vals.next().unwrap()[1].value::<f64>().unwrap().is_some());
            assert!(vals.next().unwrap()[1].value::<f64>().unwrap().is_some());
        });
    }
}
