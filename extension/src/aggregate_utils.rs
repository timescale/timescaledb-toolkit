use std::ptr::null_mut;

use pgx::pg_sys;

// TODO move to func_utils once there are enough function to warrant one
/// This will return the collation from a postgres FunctionCallInfo, if the collation is present (else None)
pub unsafe fn get_collation(fcinfo: pg_sys::FunctionCallInfo) -> Option<pg_sys::Oid> {
    if (*fcinfo).fncollation == 0 {
        None
    } else {
        Some((*fcinfo).fncollation)
    }
}

/// Given a postgres FunctionCallInfo and a closure, this will run the closure using the aggregate memory context as the current context.
pub unsafe fn in_aggregate_context<T, F: FnOnce() -> T>(
    fcinfo: pg_sys::FunctionCallInfo,
    f: F,
) -> T {
    let mctx =
        aggregate_mctx(fcinfo).unwrap_or_else(|| pgx::error!("cannot call as non-aggregate"));
    crate::palloc::in_memory_context(mctx, f)
}

/// Given the FunctionalCallInfo for a postgres function call, this will return the aggregate memory context if it exists.
/// If passed a null fcinfo, this will return the current memory context.
pub unsafe fn aggregate_mctx(fcinfo: pg_sys::FunctionCallInfo) -> Option<pg_sys::MemoryContext> {
    if fcinfo.is_null() {
        return Some(pg_sys::CurrentMemoryContext)
    }
    let mut mctx = null_mut();
    let is_aggregate = pg_sys::AggCheckCallContext(fcinfo, &mut mctx);
    if is_aggregate == 0 {
        None
    } else {
        debug_assert!(!mctx.is_null());
        Some(mctx)
    }
}
