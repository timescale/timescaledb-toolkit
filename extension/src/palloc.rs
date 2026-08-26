use std::{
    alloc::{GlobalAlloc, Layout, System},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use pgrx::*;

pub unsafe fn in_memory_context<T, F: FnOnce() -> T>(mctx: pg_sys::MemoryContext, f: F) -> T {
    let prev_ctx = unsafe { pg_sys::CurrentMemoryContext };
    unsafe { pg_sys::CurrentMemoryContext = mctx };
    let t = f();
    unsafe { pg_sys::CurrentMemoryContext = prev_ctx };
    t
}

pub use pgrx::Internal;

/// Extension trait to translate postgres-understood `pgrx::Internal` type into
/// the well-typed pointer type `Option<Inner<T>>`.
///
/// # Safety
///
/// This trait should only ever be implemented for `pgrx::Internal`
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
        self.unwrap()
            .map(|p| Inner(NonNull::new(p.cast_mut_ptr()).unwrap()))
    }
}

/// Extension trait to turn the typed pointers `Inner<...>` and
/// `Option<Inner<...>>` into the postgres-understood `pgrx::Internal` type.
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
        self.map(|p| Internal::from(Some(pg_sys::Datum::from(p.0.as_ptr()))))
    }
}

unsafe impl<T> ToInternal for Inner<T> {
    fn internal(self) -> Option<Internal> {
        Some(Internal::from(Some(pg_sys::Datum::from(self.0.as_ptr()))))
    }
}

impl<T> From<T> for Inner<T> {
    fn from(t: T) -> Self {
        unsafe { Internal::new(t).to_inner().unwrap() }
    }
}

// TODO these last two should probably be `unsafe`
unsafe impl<T> ToInternal for *mut T {
    fn internal(self) -> Option<Internal> {
        Some(Internal::from(Some(pg_sys::Datum::from(self))))
    }
}

unsafe impl<T> ToInternal for *const T {
    fn internal(self) -> Option<Internal> {
        Some(Internal::from(Some(pg_sys::Datum::from(self))))
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
//
// Update (2026): on rustc >= 1.93, the panic!() approach aborts the whole
// backend. The second out-of-memory panic that unwinds out of the global
// allocator in one backend fails with "fatal runtime error: failed to
// initiate panic" (bisected: 1.92 unwinds correctly, 1.93 aborts). An
// unwind out of GlobalAlloc is documented undefined behavior, `-Zoom=panic`
// was removed in 1.94, and `set_alloc_error_hook` is still nightly-only, so
// no stable panic-based design exists. Instead, on the backend main thread
// (recorded in _PG_init) we now report the error the way PostgreSQL C code
// does: errstart/errfinish longjmp to the error handler and abort the
// transaction without unwinding Rust frames. The skipped frames leak their
// allocations on this path, which is acceptable. Other threads keep the
// panic fallback. PostgreSQL's ErrorContext has a preallocated reserve, so
// the report also works under genuine memory exhaustion.
struct PanickingAllocator;

#[global_allocator]
static ALLOCATOR: PanickingAllocator = PanickingAllocator;

// pgrx does not expose these in pg_sys (it routes errors through its
// panic-based ereport! macro, which is exactly what we cannot use here).
// Resolve them at runtime with dlsym: a link-time reference would make
// the `cargo pgrx test` harness executable fail to link, because unlike
// the extension cdylib it cannot have undefined symbols.
unsafe extern "C" {
    fn dlsym(
        handle: *mut ::core::ffi::c_void,
        symbol: *const ::core::ffi::c_char,
    ) -> *mut ::core::ffi::c_void;
}

#[cfg(target_os = "macos")]
const RTLD_DEFAULT: *mut ::core::ffi::c_void = -2isize as *mut ::core::ffi::c_void;
#[cfg(not(target_os = "macos"))]
const RTLD_DEFAULT: *mut ::core::ffi::c_void = std::ptr::null_mut();

type Errstart = unsafe extern "C" fn(::core::ffi::c_int, *const ::core::ffi::c_char) -> bool;
type Errcode = unsafe extern "C" fn(::core::ffi::c_int) -> ::core::ffi::c_int;
type Errmsg = unsafe extern "C" fn(*const ::core::ffi::c_char, ...) -> ::core::ffi::c_int;
type Errfinish = unsafe extern "C" fn(
    *const ::core::ffi::c_char,
    ::core::ffi::c_int,
    *const ::core::ffi::c_char,
);

/// The PostgreSQL backend's main thread, recorded in _PG_init.
static PG_MAIN_THREAD: std::sync::OnceLock<std::thread::ThreadId> = std::sync::OnceLock::new();

/// Record the current thread as the PostgreSQL main thread.
/// Call from _PG_init, which PostgreSQL runs on the backend's thread.
pub fn record_pg_main_thread() {
    let _ = PG_MAIN_THREAD.set(std::thread::current().id());
}

fn on_pg_main_thread() -> bool {
    PG_MAIN_THREAD.get() == Some(&std::thread::current().id())
}

/// Raise a PostgreSQL "Out of memory" ERROR, which longjmps to the active
/// error handler and aborts the transaction. Never unwinds Rust frames, so
/// it is safe to call from inside the global allocator.
unsafe fn pg_oom_error() -> ! {
    unsafe {
        let errstart = dlsym(RTLD_DEFAULT, c"errstart".as_ptr());
        let errcode = dlsym(RTLD_DEFAULT, c"errcode".as_ptr());
        let errmsg = dlsym(RTLD_DEFAULT, c"errmsg".as_ptr());
        let errfinish = dlsym(RTLD_DEFAULT, c"errfinish".as_ptr());
        if !errstart.is_null() && !errcode.is_null() && !errmsg.is_null() && !errfinish.is_null() {
            let errstart: Errstart = std::mem::transmute(errstart);
            let errcode: Errcode = std::mem::transmute(errcode);
            let errmsg: Errmsg = std::mem::transmute(errmsg);
            let errfinish: Errfinish = std::mem::transmute(errfinish);
            if errstart(
                pg_sys::elog::PgLogLevel::ERROR as ::core::ffi::c_int,
                std::ptr::null(),
            ) {
                errcode(
                    pg_sys::errcodes::PgSqlErrorCode::ERRCODE_OUT_OF_MEMORY as ::core::ffi::c_int,
                );
                errmsg(c"Out of memory".as_ptr());
                errfinish(c"palloc.rs".as_ptr(), 0, std::ptr::null());
            }
        }
    }
    // errfinish never returns for ERROR; only reachable outside a real
    // PostgreSQL backend or if errstart refused the report.
    std::process::abort()
}

/// Handle an allocation failure: PostgreSQL ERROR on the main thread,
/// Rust panic elsewhere.
unsafe fn oom() -> ! {
    if on_pg_main_thread() {
        unsafe { pg_oom_error() }
    }
    panic!("Out of memory")
}

unsafe impl GlobalAlloc for PanickingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe {
            let p = System.alloc(layout);
            if p.is_null() {
                oom()
            }
            p
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe {
            let p = System.alloc_zeroed(layout);
            if p.is_null() {
                oom()
            }
            p
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        unsafe {
            let p = System.realloc(ptr, layout, new_size);
            if p.is_null() {
                oom()
            }
            p
        }
    }
}
