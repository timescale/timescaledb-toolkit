
use std::ptr::null_mut;

use pgx::pg_sys;

pub fn aggregate_mctx(fcinfo: pg_sys::FunctionCallInfo)
-> Option<pg_sys::MemoryContext> {
    let mut mctx = null_mut();
    let is_aggregate = unsafe {
        pg_sys::AggCheckCallContext(fcinfo, &mut mctx)
    };
    if is_aggregate == 0 {
        return None
    } else {
        debug_assert!(!mctx.is_null());
        return Some(mctx)
    }
}
