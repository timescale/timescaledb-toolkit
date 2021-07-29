
use std::{
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
