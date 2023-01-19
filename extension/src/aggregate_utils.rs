use std::ptr::null_mut;

use pgx::pg_sys;

use crate::palloc::InternalAsValue as _;
use crate::palloc::ToInternal as _;

// TODO move to func_utils once there are enough function to warrant one
pub unsafe fn get_collation(fcinfo: pg_sys::FunctionCallInfo) -> Option<pg_sys::Oid> {
    if (*fcinfo).fncollation == 0 {
        None
    } else {
        Some((*fcinfo).fncollation)
    }
}

pub unsafe fn combine<State, F: FnOnce(Option<&State>, Option<&State>) -> Option<State>>(
    state1: pgx::Internal,
    state2: pgx::Internal,
    fcinfo: pg_sys::FunctionCallInfo,
    f: F,
) -> Option<pgx::Internal> {
    unsafe_combine(state1, state2, fcinfo, f)
}

fn unsafe_combine<State, F: FnOnce(Option<&State>, Option<&State>) -> Option<State>>(
    state1: pgx::Internal,
    state2: pgx::Internal,
    fcinfo: pg_sys::FunctionCallInfo,
    f: F,
) -> Option<pgx::Internal> {
    let state1 = unsafe { state1.to_inner() };
    let state2 = unsafe { state2.to_inner() };
    let state1 = match &state1 {
        None => None,
        Some(inner) => Some(&**inner),
    };
    let state2 = match &state2 {
        None => None,
        Some(inner) => Some(&**inner),
    };
    let f = || f(state1, state2);
    unsafe { in_aggregate_context(fcinfo, f) }
        .map(|internal| internal.into())
        .internal()
}

pub unsafe fn transition<State, F: FnOnce(Option<State>) -> Option<State>>(
    state: pgx::Internal,
    fcinfo: pg_sys::FunctionCallInfo,
    f: F,
) -> Option<pgx::Internal> {
    unsafe_transition(state, fcinfo, f)
}

fn unsafe_transition<State, F: FnOnce(Option<State>) -> Option<State>>(
    state: pgx::Internal,
    fcinfo: pg_sys::FunctionCallInfo,
    f: F,
) -> Option<pgx::Internal> {
    let mut inner = unsafe { state.to_inner() };
    let state: Option<State> = match &mut inner {
        None => None,
        Some(inner) => Option::take(&mut **inner),
    };
    let f = || {
        let result: Option<State> = f(state);
        inner = match (inner, result) {
            (None, None) => None,
            (None, result @ Some(..)) => Some(result.into()),
            (Some(mut inner), result) => {
                *inner = result;
                Some(inner)
            }
        };
        inner.internal()
    };
    unsafe { in_aggregate_context(fcinfo, f) }
}

pub unsafe fn in_aggregate_context<T, F: FnOnce() -> T>(
    fcinfo: pg_sys::FunctionCallInfo,
    f: F,
) -> T {
    // TODO Is this unsafe for any reason other than "all FFI is unsafe"?
    let mctx =
        aggregate_mctx(fcinfo).unwrap_or_else(|| pgx::error!("cannot call as non-aggregate"));
    crate::palloc::in_memory_context(mctx, f)
}

pub unsafe fn aggregate_mctx(fcinfo: pg_sys::FunctionCallInfo) -> Option<pg_sys::MemoryContext> {
    if fcinfo.is_null() {
        // TODO Is this unsafe for any reason other than "all FFI is unsafe"?
        return Some(pg_sys::CurrentMemoryContext);
    }
    let mut mctx = null_mut();
    // TODO Is this unsafe for any reason other than "all FFI is unsafe"?
    let is_aggregate = pg_sys::AggCheckCallContext(fcinfo, &mut mctx);
    if is_aggregate == 0 {
        None
    } else {
        debug_assert!(!mctx.is_null());
        Some(mctx)
    }
}
