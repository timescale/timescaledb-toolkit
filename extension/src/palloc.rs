
use std::{
    alloc::{GlobalAlloc, Layout},
    convert::TryInto,
    ptr::NonNull,
};

use pgx::*;

struct PallocAllocator;

/// There is an uncomfortable mismatch between rust's memory allocation and
/// postgres's; rust tries to clean memory by using stack-based destructors,
/// while postgres does so using arenas. The issue we encounter is that postgres
/// implements exception-handling using setjmp/longjmp, which will can jump over
/// stack frames containing rust destructors. To avoid needing to register a
/// setjmp handler at every call to a postgres function, we use postgres's
/// MemoryContexts to manage memory, even though this is not strictly speaking
/// safe. Though it is tempting to try to get more control over which
/// MemoryContext we allocate in, there doesn't seem to be way to do so that is
/// safe in the context of postgres exceptions and doesn't incur the cost of
/// setjmp
unsafe impl GlobalAlloc for PallocAllocator {
    //FIXME allow for switching the memory context allocated in
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        pg_sys::MemoryContextAlloc(
            pg_sys::CurrentMemoryContext,
            layout.size().try_into().unwrap()
        )  as *mut _
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        pg_sys::pfree(ptr as *mut _)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        pg_sys::MemoryContextAllocZero(
            pg_sys::CurrentMemoryContext,
            layout.size().try_into().unwrap()
        ) as *mut _
    }

    unsafe fn realloc(&self, ptr: *mut u8, _layout: Layout, new_size: usize) -> *mut u8 {
        pg_sys::repalloc(ptr as *mut _, new_size.try_into().unwrap()) as *mut _
    }
}

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
        let nn = NonNull::new(datum as *mut T).unwrap_or_else(||
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
        Self(Box::leak(Box::new(t)).into())
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
