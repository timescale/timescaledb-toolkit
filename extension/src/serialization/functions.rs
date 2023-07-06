use std::{
    ffi::{CStr, CString},
    mem::{align_of, size_of, MaybeUninit},
    os::raw::c_char,
    slice,
};

use flat_serialize::{impl_flat_serializable, FlatSerializable, WrapErr};

use serde::{Deserialize, Serialize};

use pg_sys::{Datum, Oid};
use pgrx::*;

/// `PgProcId` provides provides the ability to serialize and deserialize
/// regprocedures as `namespace.name(args)`
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct PgProcId(pub Oid);

impl_flat_serializable!(PgProcId);

// FIXME upstream to pgrx
// TODO use this or regprocedureout()?
extern "C" {
    pub fn format_procedure_qualified(procedure_oid: pg_sys::Oid) -> *const c_char;
}

impl Serialize for PgProcId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        unsafe {
            let qualified_name = format_procedure_qualified(self.0);
            let len = CStr::from_ptr(qualified_name).to_bytes().len();
            let qualified_name =
                pg_sys::pg_server_to_any(qualified_name, len as _, pg_sys::pg_enc_PG_UTF8 as _);
            let qualified_name = CStr::from_ptr(qualified_name);
            let qualified_name = qualified_name.to_str().unwrap();
            qualified_name.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for PgProcId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // FIXME pgrx wraps all functions in rust wrappers, which makes them
        //       uncallable with DirectFunctionCall(). Is there a way to
        //       export both?
        extern "C" {
            fn regprocedurein(fcinfo: pg_sys::FunctionCallInfo) -> Datum;
        }
        let qualified_name = <&str>::deserialize(deserializer)?;
        let qualified_name = CString::new(qualified_name).unwrap();
        let oid = unsafe {
            pg_sys::DirectFunctionCall1Coll(
                Some(regprocedurein),
                pg_sys::InvalidOid,
                pg_sys::Datum::from(qualified_name.as_ptr()),
            )
        };

        Ok(Self(unsafe { Oid::from_u32_unchecked(oid.value() as _) }))
    }
}
