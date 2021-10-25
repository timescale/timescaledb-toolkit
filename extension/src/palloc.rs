
use std::{
    alloc::{GlobalAlloc, Layout, System},
    ptr::NonNull,
};

use pgx::*;

pub unsafe fn in_memory_context<T, F: FnOnce() -> T>(
    mctx: pg_sys::MemoryContext,
    f: F
) -> T {
    let prev_ctx = pg_sys::CurrentMemoryContext;
    pg_sys::CurrentMemoryContext = mctx;
    let t = f();
    pg_sys::CurrentMemoryContext = prev_ctx;
    t
}


pub struct Internal<T>(pub NonNull<T>);

impl<T> FromDatum for Internal<T> {
    #[inline]
    unsafe fn from_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        _: pg_sys::Oid,
    ) -> Option<Internal<T>> {
        if is_null {
            return None
        }

        let ptr = datum as *mut T;
        // FIXME it looks like timescale occasionally passes a 0 ptr as non-null
        //       we special case 0-sized types to ensure that we still function
        //       in that case
        if std::mem::size_of::<T>() == 0 && ptr.is_null() {
            return Some(Internal(NonNull::dangling()))
        }
        let nn = NonNull::new(ptr).unwrap_or_else(||
                panic!("Internal-type Datum flagged not null but its datum is zero"));
        Some(Internal(nn))
    }
}

impl<T> IntoDatum for Internal<T> {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        Some(self.0.as_ptr() as pg_sys::Datum)
    }

    fn type_oid() -> pg_sys::Oid {
        pg_sys::INTERNALOID
    }
}

impl<T> From<T> for Internal<T> {
    fn from(t: T) -> Self {
        let ptr = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(t);
        Self(NonNull::new(ptr).unwrap())
    }
}

impl<T> std::ops::Deref for Internal<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl<T> std::ops::DerefMut for Internal<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

// By default rust will `abort()` the process when the allocator returns NULL.
// Since many systems can't reliably determine when an allocation will cause the
// process to run out of memory, and just rely on the OOM killer cleaning up
// afterwards, this is acceptable for many workloads. However, `abort()`-ing a
// Postgres will restart the database, and since we often run Postgres on
// systems which _can_ reliably return NULL on out-of-memory, we would like to
// take advantage of this to cleanly shut down a single transaction when we fail
// to allocate. Long-term the solution for this likely involves the `oom=panic`
// flag[1], but at the time of writing the flag is not yet stable.
//
// This allocator implements a partial solution for turning out-of-memory into
// transaction-rollback instead of process-abort. It is a thin shim over the
// System allocator that `panic!()`s when the System allocator returns `NULL`.
// In the event that still have enough remaining memory to serve the panic, this
// will unwind the stack all the way to transaction-rollback. In the event we
// don't even have enough memory to handle unwinding this will merely abort the
// process with a panic-in-panic instead of a memory-allocation-failure. Under
// the assumption that we're more likely to fail due to a few large allocations
// rather than a very large number of small allocations, it seems likely that we
// will have some memory remaining for unwinding, and that this will reduce the
// likelihood of aborts.
//
// [1] `oom=panic` tracking issue: https://github.com/rust-lang/rust/issues/43596
struct PanickingAllocator;

#[global_allocator]
static ALLOCATOR: PanickingAllocator = PanickingAllocator;

unsafe impl GlobalAlloc for PanickingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if p.is_null() {
            panic!("Out of memory")
        }
        return p

    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if p.is_null() {
            panic!("Out of memory")
        }
        return p
    }

    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize
    ) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if p.is_null() {
            panic!("Out of memory")
        }
        return p
    }
}
