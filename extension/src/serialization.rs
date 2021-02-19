
use std::{convert::TryInto, ffi::CStr, os::raw::{c_char, c_int}};
pub use self::types::{PgTypId, ShortTypeId};
pub use self::collations::PgCollationId;


mod types;
mod collations;



// FIXME upstream to pgx
pub(crate) const PG_UTF8: i32 = 6;
extern "C" {
    pub fn pg_server_to_any(s: *const c_char, len: c_int, encoding: c_int) -> *const c_char;
    pub fn pg_any_to_server(s: *const c_char, len: c_int, encoding: c_int) -> *const c_char;
    pub fn GetDatabaseEncoding() -> c_int;
}

pub enum EncodedStr<'s> {
    Utf8(&'s str),
    Other(&'s CStr)
}

pub fn str_to_db_encoding(s: &str) -> EncodedStr {
    if unsafe { GetDatabaseEncoding() == PG_UTF8 } {
        return EncodedStr::Utf8(s)
    }

    let bytes = s.as_bytes();
    let encoded = unsafe {
        pg_any_to_server(bytes.as_ptr() as *const c_char, bytes.len().try_into().unwrap(), PG_UTF8)
    };
    if encoded as usize == bytes.as_ptr() as usize {
        return EncodedStr::Utf8(s)
    }

    let cstr = unsafe { CStr::from_ptr(encoded) };
    return EncodedStr::Other(cstr)
}

pub fn str_from_db_encoding(s: &CStr) -> &str {
    if unsafe { GetDatabaseEncoding() == PG_UTF8 } {
        return s.to_str().unwrap()
    }

    let str_len = s.to_bytes().len().try_into().unwrap();
    let encoded = unsafe {
        pg_server_to_any(s.as_ptr(), str_len, PG_UTF8)
    };
    if encoded as usize == s.as_ptr() as usize {
        //TODO redundant check?
        return s.to_str().unwrap()
    }
    return unsafe { CStr::from_ptr(encoded).to_str().unwrap() }
}

// NOTE this module assumes that the rust allocator is the postgres allocator,
//      and thus leaks memory assuming that MemoryContext deletion will clean
//      it up
pub(crate) mod serde_reference_adaptor {
    use serde::{Deserialize, Deserializer};

    pub(crate) fn deserialize<'de, D, T>(deserializer: D) -> Result<&'static T, D::Error>
    where D: Deserializer<'de>, T: Deserialize<'de> {
        let boxed = T::deserialize(deserializer)?.into();
        Ok(Box::leak(boxed))
    }

    pub(crate) fn deserialize_slice<'de, D, T>(deserializer: D) -> Result<&'static [T], D::Error>
    where D: Deserializer<'de>, T: Deserialize<'de> {
        let boxed = <Box<[T]>>::deserialize(deserializer)?.into();
        Ok(Box::leak(boxed))
    }

    pub(crate) fn default_padding() -> &'static [u8; 3] {
        &[0; 3]
    }

    pub(crate) fn default_header() -> &'static u32 {
        &0
    }
}
