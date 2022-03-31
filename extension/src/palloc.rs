
use std::{alloc::{GlobalAlloc, Layout, System}, ops::{Deref, DerefMut}, ptr::NonNull};

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

pub use pgx::Internal;

/// Extension trait to translate postgres-understood `pgx::Internal` type into
/// the well-typed pointer type `Option<Inner<T>>`.
///
/// # Safety
///
/// This trait should only ever be implemented for `pgx::Internal`
/// There is an lifetime constraint on the returned pointer, though this is
/// currently implicit.
pub unsafe trait InternalAsValue {
    // unsafe fn value_or<T, F: FnOnce() -> T>(&mut self) -> &mut T;
    unsafe fn to_inner<T>(self) -> Option<Inner<T>>;
}

unsafe impl InternalAsValue for Internal {
    // unsafe fn value_or<T, F: FnOnce() -> T>(&mut self, f: F) -> &mut T {
    //     if let Some(t) = self.get_mut() {
    //         t
    //     }

    //     *self = Internal::new(f());
    //     self.get_mut().unwrap()
    // }

    unsafe fn to_inner<T>(self) -> Option<Inner<T>> {
        self.unwrap().map(|p| Inner(NonNull::new(p as _).unwrap()))
    }
}

/// Extension trait to turn the typed pointers `Inner<...>` and
/// `Option<Inner<...>>` into the postgres-understood `pgx::Internal` type.
///
/// # Safety
/// The value input must live as long as postgres expects. TODO more info
pub unsafe trait ToInternal {
    fn internal(self) -> Option<Internal>;
}

pub struct Inner<T>(pub NonNull<T>);

impl<T> Deref for Inner<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl<T> DerefMut for Inner<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

unsafe impl<T> ToInternal for Option<Inner<T>> {
    fn internal(self) -> Option<Internal> {
        self.map(|p| Internal::from(Some(p.0.as_ptr() as pg_sys::Datum)))
    }
}

unsafe impl<T> ToInternal for Inner<T> {
    fn internal(self) -> Option<Internal> {
        Some(Internal::from(Some(self.0.as_ptr() as pg_sys::Datum)))
    }
}

impl<T> From<T> for Inner<T> {
    fn from(t: T) -> Self {
        unsafe {
            Internal::new(t).to_inner().unwrap()
        }
    }
}

// TODO these last two should probably be `unsafe`
unsafe impl<T> ToInternal for *mut T {
    fn internal(self) -> Option<Internal> {
        Some(Internal::from(Some(self as pg_sys::Datum)))
    }
}

unsafe impl<T> ToInternal for *const T {
    fn internal(self) -> Option<Internal> {
        Some(Internal::from(Some(self as pg_sys::Datum)))
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
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if p.is_null() {
            panic!("Out of memory")
        }
        p
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
        p
    }
}
