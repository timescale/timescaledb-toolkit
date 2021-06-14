use std::{
    ffi::{CStr, CString},
    os::raw::{c_char, c_int},
    slice,
    mem::{size_of, align_of, MaybeUninit},
};

use flat_serialize::{impl_flat_serializable, FlatSerializable, WrapErr};

use serde::{Deserialize, Serialize};

use pg_sys::{Datum, Oid};
use pgx::*;

// TODO short collation serializer?

/// `PgCollationId` provides provides the ability to serialize and deserialize
/// collation Oids as `(namespace, name)` pairs.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct PgCollationId(pub Oid);

impl_flat_serializable!(PgCollationId);

impl PgCollationId {
    pub fn is_invalid(&self) -> bool {
        self.0 == pg_sys::InvalidOid
    }

    pub fn to_option_oid(&self) -> Option<Oid> {
        if self.is_invalid() {
            None
        } else {
            Some(self.0)
        }
    }
}

// FIXME upstream to pgx
const PG_UTF8: i32 = 6;
#[allow(non_upper_case_globals)]
const Anum_pg_collation_oid: u32 = 1;
// https://github.com/postgres/postgres/blob/e955bd4b6c2bcdbd253837f6cf4c7520b98e69d4/src/include/catalog/pg_collation.dat
const DEFAULT_COLLATION_OID: u32 = 100;
extern "C" {
    fn pg_server_to_any(s: *const c_char, len: c_int, encoding: c_int) -> *const c_char;
    fn pg_any_to_server(s: *const c_char, len: c_int, encoding: c_int) -> *const c_char;
    fn GetDatabaseEncoding() -> c_int;
}
#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
#[repr(C)]
struct FormData_pg_collation {
    oid: pg_sys::Oid,
    collname: pg_sys::NameData,
    collnamespace: pg_sys::Oid,
    collowner: pg_sys::Oid,
    collprovider: c_char,
    collisdeterministic: bool,
    collencoding: i32,
    collcollate: pg_sys::NameData,
    collctype: pg_sys::NameData,
}

#[allow(non_camel_case_types)]
type Form_pg_collation = *mut FormData_pg_collation;

#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
#[repr(C)]
struct FormData_pg_database {
    oid: Oid,
    datname: pg_sys::NameData,
    datdba: Oid,
    encoding: i32,
    datcollate: pg_sys::NameData,
    // TODO more fields I'm ignoring
}

#[allow(non_camel_case_types)]
type Form_pg_database = *mut FormData_pg_database;



impl Serialize for PgCollationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        unsafe {
            let mut layout: Option<(&str, &str)> = None;
            if self.is_invalid() {
                return layout.serialize(serializer)
            }

            let tuple =
                pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier_COLLOID as _, self.0 as _);
            if tuple.is_null() {
                pgx::error!("no collation info for oid {}", self.0);
            }

            let collation_tuple: Form_pg_collation = get_struct(tuple);

            let namespace = pg_sys::get_namespace_name((*collation_tuple).collnamespace);
            if namespace.is_null() {
                pgx::error!("invalid schema oid {}", (*collation_tuple).collnamespace);
            }

            let namespace_len = CStr::from_ptr(namespace).to_bytes().len();
            let namespace = pg_server_to_any(namespace, namespace_len as _, PG_UTF8);
            let namespace = CStr::from_ptr(namespace);
            let namespace = namespace.to_str().unwrap();

            // the 'default' collation isn't really a collation, and we need to
            // look in pg_database to discover what real collation is
            let mut db_tuple = None;
            let collation_name =
                if self.0 == DEFAULT_COLLATION_OID {
                    let tuple = pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier_DATABASEOID as _, pg_sys::MyDatabaseId as _);
                    if tuple.is_null() {
                        pgx::error!("no database info");
                    }
                    db_tuple = Some(tuple);

                    let database_tuple: Form_pg_database = get_struct(tuple);
                    (*database_tuple).datcollate.data.as_ptr()
                } else {
                    (*collation_tuple).collname.data.as_ptr()
                };

            let collation_name_len = CStr::from_ptr(collation_name).to_bytes().len();
            let collation_name = pg_server_to_any(collation_name, collation_name_len as _, PG_UTF8);
            let collation_name = CStr::from_ptr(collation_name);
            let collation_name = collation_name.to_str().unwrap();

            let qualified_name: (&str, &str) = (namespace, collation_name);
            layout = Some(qualified_name);
            let res = layout.serialize(serializer);
            if let Some(db_tuple) = db_tuple {
                pg_sys::ReleaseSysCache(db_tuple);
            }
            pg_sys::ReleaseSysCache(tuple);
            res
        }
    }
}

impl<'de> Deserialize<'de> for PgCollationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let collation = <Option<(&str, &str)>>::deserialize(deserializer)?;
        let (namespace, name) = match collation {
            None => return Ok(Self(0)),
            Some(qualified_name) => qualified_name,
        };

        let (namespace, name) = (
            CString::new(namespace).unwrap(),
            CString::new(name).unwrap(),
        );
        let (namespace_len, name_len) = (namespace.to_bytes().len(), name.to_bytes().len());
        unsafe {
            let namespace = pg_any_to_server(namespace.as_ptr(), namespace_len as _, PG_UTF8);
            let namespace = CStr::from_ptr(namespace);

            let name = pg_any_to_server(name.as_ptr(), name_len as _, PG_UTF8);
            let name = CStr::from_ptr(name);

            let namespace_id = pg_sys::LookupExplicitNamespace(namespace.as_ptr(), true as _);
            if namespace_id == pg_sys::InvalidOid {
                return Err(D::Error::custom(format!(
                    "invalid namespace {:?}",
                    namespace
                )));
            }

            // COLLNAMEENCNSP is based on a triple `(collname, collencoding, collnamespace)`,
            // however, `(collname, collnamespace)` is enough to uniquely determine
            // a collation, though we need to check both collencoding = -1 and
            // collencoding = DatabaseEncoding
            // see:
            //   https://www.postgresql.org/docs/13/catalog-pg-collation.html
            //   https://github.com/postgres/postgres/blob/e955bd4b6c2bcdbd253837f6cf4c7520b98e69d4/src/backend/commands/collationcmds.c#L246

            let mut collation_id = pg_sys::GetSysCacheOid(
                pg_sys::SysCacheIdentifier_COLLNAMEENCNSP as _,
                Anum_pg_collation_oid as _,
                name.as_ptr() as Datum,
                GetDatabaseEncoding() as _,
                namespace_id as Datum,
                0, //unused
            );

            if collation_id == pg_sys::InvalidOid {
                collation_id = pg_sys::GetSysCacheOid(
                    pg_sys::SysCacheIdentifier_COLLNAMEENCNSP as _,
                    Anum_pg_collation_oid as _,
                    name.as_ptr() as Datum,
                    (-1isize) as usize,
                    namespace_id as Datum,
                    0, //unused
                );
            }

            if collation_id == pg_sys::InvalidOid {
                return Err(D::Error::custom(format!(
                    "invalid collation {:?}.{:?}",
                    namespace, name
                )));
            }

            return Ok(PgCollationId(collation_id));
        }
    }
}

unsafe fn get_struct<T>(tuple: pg_sys::HeapTuple) -> *mut T {
    //((char *) ((TUP)->t_data) + (TUP)->t_data->t_hoff)
    let t_data: *mut u8 = (*tuple).t_data.cast();
    let t_hoff = (*(*tuple).t_data).t_hoff;
    t_data.add(t_hoff as usize).cast()
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {

    use super::PgCollationId;
    use pgx::{
        pg_guard,
        pg_sys,
        pg_test,
    };

    // TODO is there a way we can test more of this without making it flaky?
    #[pg_test]
    fn test_pg_collation_id_serialize_default_collation_json() {
        let serialized = serde_json::to_string(&PgCollationId(100)).unwrap();
        let deserialized: PgCollationId = serde_json::from_str(&serialized).unwrap();
        assert_ne!(deserialized.0, 0);
        let serialized = serde_json::to_string(&PgCollationId(100)).unwrap();
        let deserialized2: PgCollationId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized2.0, deserialized.0);
    }

    #[pg_test]
    fn test_pg_collation_id_serialize_c_collation() {
        let serialized = bincode::serialize(&PgCollationId(950)).unwrap();
        assert_eq!(
            serialized,
            vec![1, 10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111, 103, 1, 0, 0, 0, 0, 0, 0, 0, 67]
        );
        let deserialized: PgCollationId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.0, 950);
    }

    // TODO this test may be too flaky depending on what the default collation actually is
    #[pg_test]
    fn test_pg_collation_id_serialize_c_collation_json() {
        let serialized = serde_json::to_string(&PgCollationId(950)).unwrap();
        assert_eq!(&*serialized, "[\"pg_catalog\",\"C\"]",);
        let deserialized: PgCollationId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, 950);
    }

    #[pg_test]
    fn test_pg_collation_id_serialize_posix_collation() {
        let serialized = bincode::serialize(&PgCollationId(951)).unwrap();
        assert_eq!(
            serialized,
            vec![1, 10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111, 103, 5, 0, 0, 0, 0, 0, 0, 0, 80, 79, 83, 73, 88]
        );
        let deserialized: PgCollationId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.0, 951);
    }

    // TODO this test may be too flaky depending on what the default collation actually is
    #[pg_test]
    fn test_pg_collation_id_serialize_posix_collation_json() {
        let serialized = serde_json::to_string(&PgCollationId(951)).unwrap();
        assert_eq!(&*serialized, "[\"pg_catalog\",\"POSIX\"]",);
        let deserialized: PgCollationId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, 951);
    }
}
