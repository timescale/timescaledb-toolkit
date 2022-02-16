use std::{
    ffi::{CStr, CString},
    slice,
    mem::{size_of, align_of, MaybeUninit},
};

use flat_serialize::{impl_flat_serializable, FlatSerializable, WrapErr};

use serde::{Deserialize, Serialize};

use pg_sys::{Datum, Oid};
use pgx::*;

/// Possibly a premature optimization, `ShortTypId` provides the ability to
/// serialize and deserialize type Oids as `(namespace, name)` pairs, special
/// casing a number of types with hardcoded Oids that we expect to be common so
/// that these types can be stored more compactly if desired.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct ShortTypeId(pub u32);

impl_flat_serializable!(ShortTypeId);

impl From<u32> for ShortTypeId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<ShortTypeId> for u32 {
    fn from(id: ShortTypeId) -> Self {
        id.0
    }
}

impl Serialize for ShortTypeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        ShortTypIdSerializer::from_oid(self.0).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ShortTypeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let type_id = ShortTypIdSerializer::deserialize(deserializer)?;
        Ok(Self(type_id.to_oid()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)]
enum ShortTypIdSerializer {
    BOOL,
    BYTEA,
    CHAR,
    NAME,
    INT8,
    INT2,
    INT2VECTOR,
    INT4,
    REGPROC,
    TEXT,
    JSON,
    XML,
    POINT,
    FLOAT4,
    FLOAT8,
    MACADDR8,
    VARCHAR,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    INTERVAL,
    TIMETZ,
    JSONB,
    BOOLARRAY,
    BYTEAARRAY,
    CHARARRAY,
    NAMEARRAY,
    INT8ARRAY,
    INT2ARRAY,
    INT4ARRAY,
    TEXTARRAY,
    FLOAT4ARRAY,
    FLOAT8ARRAY,
    DATEARRAY,
    TIMEARRAY,
    TIMESTAMPARRAY,
    TIMESTAMPTZARRAY,
    INTERVALARRAY,
    TIMETZARRAY,
    NUMERICARRAY,
    JSONBARRAY,
    #[serde(rename = "Type")]
    Other(PgTypId),
}

impl ShortTypIdSerializer {
    pub fn from_oid(oid: Oid) -> Self {
        use ShortTypIdSerializer::*;
        match oid {
            pg_sys::BOOLOID => BOOL,
            pg_sys::BYTEAOID => BYTEA,
            pg_sys::CHAROID => CHAR,
            pg_sys::NAMEOID => NAME,
            pg_sys::INT8OID => INT8,
            pg_sys::INT2OID => INT2,
            pg_sys::INT2VECTOROID => INT2VECTOR,
            pg_sys::INT4OID => INT4,
            pg_sys::REGPROCOID => REGPROC,
            pg_sys::TEXTOID => TEXT,
            pg_sys::JSONOID => JSON,
            pg_sys::XMLOID => XML,
            pg_sys::POINTOID => POINT,
            pg_sys::FLOAT4OID => FLOAT4,
            pg_sys::FLOAT8OID => FLOAT8,
            pg_sys::MACADDR8OID => MACADDR8,
            pg_sys::VARCHAROID => VARCHAR,
            pg_sys::DATEOID => DATE,
            pg_sys::TIMEOID => TIME,
            pg_sys::TIMESTAMPOID => TIMESTAMP,
            pg_sys::TIMESTAMPTZOID => TIMESTAMPTZ,
            pg_sys::INTERVALOID => INTERVAL,
            pg_sys::TIMETZOID => TIMETZ,
            pg_sys::JSONBOID => JSONB,
            pg_sys::BOOLARRAYOID => BOOLARRAY,
            pg_sys::BYTEAARRAYOID => BYTEAARRAY,
            pg_sys::CHARARRAYOID => CHARARRAY,
            pg_sys::NAMEARRAYOID => NAMEARRAY,
            pg_sys::INT8ARRAYOID => INT8ARRAY,
            pg_sys::INT2ARRAYOID => INT2ARRAY,
            pg_sys::INT4ARRAYOID => INT4ARRAY,
            pg_sys::TEXTARRAYOID => TEXTARRAY,
            pg_sys::FLOAT4ARRAYOID => FLOAT4ARRAY,
            pg_sys::FLOAT8ARRAYOID => FLOAT8ARRAY,
            pg_sys::DATEARRAYOID => DATEARRAY,
            pg_sys::TIMEARRAYOID => TIMEARRAY,
            pg_sys::TIMESTAMPARRAYOID => TIMESTAMPARRAY,
            pg_sys::TIMESTAMPTZARRAYOID => TIMESTAMPTZARRAY,
            pg_sys::INTERVALARRAYOID => INTERVALARRAY,
            pg_sys::TIMETZARRAYOID => TIMETZARRAY,
            pg_sys::NUMERICARRAYOID => NUMERICARRAY,
            pg_sys::JSONBARRAYOID => JSONBARRAY,
            other => Other(PgTypId(other)),
        }
    }

    pub fn to_oid(&self) -> Oid {
        use ShortTypIdSerializer::*;
        match self {
            BOOL => pg_sys::BOOLOID,
            BYTEA => pg_sys::BYTEAOID,
            CHAR => pg_sys::CHAROID,
            NAME => pg_sys::NAMEOID,
            INT8 => pg_sys::INT8OID,
            INT2 => pg_sys::INT2OID,
            INT2VECTOR => pg_sys::INT2VECTOROID,
            INT4 => pg_sys::INT4OID,
            REGPROC => pg_sys::REGPROCOID,
            TEXT => pg_sys::TEXTOID,
            JSON => pg_sys::JSONOID,
            XML => pg_sys::XMLOID,
            POINT => pg_sys::POINTOID,
            FLOAT4 => pg_sys::FLOAT4OID,
            FLOAT8 => pg_sys::FLOAT8OID,
            MACADDR8 => pg_sys::MACADDR8OID,
            VARCHAR => pg_sys::VARCHAROID,
            DATE => pg_sys::DATEOID,
            TIME => pg_sys::TIMEOID,
            TIMESTAMP => pg_sys::TIMESTAMPOID,
            TIMESTAMPTZ => pg_sys::TIMESTAMPTZOID,
            INTERVAL => pg_sys::INTERVALOID,
            TIMETZ => pg_sys::TIMETZOID,
            JSONB => pg_sys::JSONBOID,
            BOOLARRAY => pg_sys::BOOLARRAYOID,
            BYTEAARRAY => pg_sys::BYTEAARRAYOID,
            CHARARRAY => pg_sys::CHARARRAYOID,
            NAMEARRAY => pg_sys::NAMEARRAYOID,
            INT8ARRAY => pg_sys::INT8ARRAYOID,
            INT2ARRAY => pg_sys::INT2ARRAYOID,
            INT4ARRAY => pg_sys::INT4ARRAYOID,
            TEXTARRAY => pg_sys::TEXTARRAYOID,
            FLOAT4ARRAY => pg_sys::FLOAT4ARRAYOID,
            FLOAT8ARRAY => pg_sys::FLOAT8ARRAYOID,
            DATEARRAY => pg_sys::DATEARRAYOID,
            TIMEARRAY => pg_sys::TIMEARRAYOID,
            TIMESTAMPARRAY => pg_sys::TIMESTAMPARRAYOID,
            TIMESTAMPTZARRAY => pg_sys::TIMESTAMPTZARRAYOID,
            INTERVALARRAY => pg_sys::INTERVALARRAYOID,
            TIMETZARRAY => pg_sys::TIMETZARRAYOID,
            NUMERICARRAY => pg_sys::NUMERICARRAYOID,
            JSONBARRAY => pg_sys::JSONBARRAYOID,
            Other(other) => other.0,
        }
    }
}

/// `PgTypId` provides provides the ability to serialize and deserialize type
/// Oids as `(namespace, name)` pairs.
#[derive(Debug)]
#[repr(transparent)]
pub struct PgTypId(pub Oid);

impl Serialize for PgTypId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        unsafe {
            let tuple =
                pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier_TYPEOID as _, self.0 as _);
            if tuple.is_null() {
                pgx::error!("no type info for oid {}", self.0);
            }

            let type_tuple: pg_sys::Form_pg_type = get_struct(tuple);

            let namespace = pg_sys::get_namespace_name((*type_tuple).typnamespace);
            if namespace.is_null() {
                pgx::error!("invalid schema oid {}", (*type_tuple).typnamespace);
            }

            let namespace_len = CStr::from_ptr(namespace).to_bytes().len();
            let namespace = pg_sys::pg_server_to_any(namespace, namespace_len as _, pg_sys::pg_enc_PG_UTF8 as _);
            let namespace = CStr::from_ptr(namespace);
            let namespace = namespace.to_str().unwrap();

            let type_name = (*type_tuple).typname.data.as_ptr();
            let type_name_len = CStr::from_ptr(type_name).to_bytes().len();
            let type_name = pg_sys::pg_server_to_any(type_name, type_name_len as _, pg_sys::pg_enc_PG_UTF8 as _);
            let type_name = CStr::from_ptr(type_name);
            let type_name = type_name.to_str().unwrap();

            let qualified_name: (&str, &str) = (namespace, type_name);
            let res = qualified_name.serialize(serializer);
            pg_sys::ReleaseSysCache(tuple);
            res
        }
    }
}

impl<'de> Deserialize<'de> for PgTypId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let (namespace, name) = <(&str, &str)>::deserialize(deserializer)?;
        let (namespace, name) = (
            CString::new(namespace).unwrap(),
            CString::new(name).unwrap(),
        );
        let (namespace_len, name_len) = (namespace.to_bytes().len(), name.to_bytes().len());
        unsafe {
            let namespace = pg_sys::pg_any_to_server(namespace.as_ptr(), namespace_len as _, pg_sys::pg_enc_PG_UTF8 as _);
            let namespace = CStr::from_ptr(namespace);

            let name = pg_sys::pg_any_to_server(name.as_ptr(), name_len as _, pg_sys::pg_enc_PG_UTF8 as _);
            let name = CStr::from_ptr(name);

            let namespace_id = pg_sys::LookupExplicitNamespace(namespace.as_ptr(), true);
            if namespace_id == pg_sys::InvalidOid {
                return Err(D::Error::custom(format!(
                    "invalid namespace {:?}",
                    namespace
                )));
            }

            let type_id = pg_sys::GetSysCacheOid(
                pg_sys::SysCacheIdentifier_TYPENAMENSP as _,
                pg_sys::Anum_pg_type_oid as _,
                name.as_ptr() as Datum,
                namespace_id as Datum,
                0, //unused
                0, //unused
            );
            if type_id == pg_sys::InvalidOid {
                return Err(D::Error::custom(format!(
                    "invalid type {:?}.{:?}",
                    namespace, name
                )));
            }

            Ok(PgTypId(type_id))
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
#[pg_schema]
mod tests {

    use super::{PgTypId, ShortTypeId};
    use pgx::{
        pg_guard,
        pg_sys::{self, BOOLOID, CHAROID, CIRCLEOID},
        pg_test,
    };

    #[pg_test]
    fn test_pg_type_id_serialize_char_type() {
        let serialized = bincode::serialize(&PgTypId(CHAROID)).unwrap();
        assert_eq!(
            serialized,
            vec![
                10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111, 103, 4, 0, 0, 0,
                0, 0, 0, 0, 99, 104, 97, 114
            ]
        );
        let deserialized: PgTypId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.0, CHAROID);
    }

    #[pg_test]
    fn test_pg_type_id_serialize_char_type_ron() {
        let serialized = ron::to_string(&PgTypId(CHAROID)).unwrap();
        assert_eq!(&*serialized, "(\"pg_catalog\",\"char\")",);
        let deserialized: PgTypId = ron::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, CHAROID);
    }

    #[pg_test]
    fn test_pg_type_id_serialize_bool_type() {
        let serialized = bincode::serialize(&PgTypId(BOOLOID)).unwrap();
        assert_eq!(
            serialized,
            vec![
                10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111, 103, 4, 0, 0, 0,
                0, 0, 0, 0, 98, 111, 111, 108
            ]
        );
        let deserialized: PgTypId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.0, BOOLOID);
    }
    #[pg_test]
    fn test_pg_type_id_serialize_bool_type_ron() {
        let serialized = ron::to_string(&PgTypId(BOOLOID)).unwrap();
        assert_eq!(&*serialized, "(\"pg_catalog\",\"bool\")",);
        let deserialized: PgTypId = ron::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, BOOLOID);
    }

    #[pg_test]
    fn test_short_type_id_serialize_char_type() {
        let serialized = bincode::serialize(&ShortTypeId(CHAROID)).unwrap();
        assert_eq!(serialized, vec![2, 0, 0, 0],);
        let deserialized: ShortTypeId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.0, CHAROID);
    }

    #[pg_test]
    fn test_short_type_id_serialize_char_type_ron() {
        let serialized = ron::to_string(&ShortTypeId(CHAROID)).unwrap();
        assert_eq!(&*serialized, "CHAR",);
        let deserialized: ShortTypeId = ron::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, CHAROID);
    }

    #[pg_test]
    fn test_short_type_id_serialize_bool_type() {
        let serialized = bincode::serialize(&ShortTypeId(BOOLOID)).unwrap();
        assert_eq!(serialized, vec![0, 0, 0, 0],);
        let deserialized: ShortTypeId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.0, BOOLOID);
    }

    #[pg_test]
    fn test_short_type_id_serialize_bool_type_ron() {
        let serialized = ron::to_string(&ShortTypeId(BOOLOID)).unwrap();
        assert_eq!(&*serialized, "BOOL",);
        let deserialized: ShortTypeId = ron::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, BOOLOID);
    }

    #[pg_test]
    fn test_short_type_id_serialize_circle_type() {
        let serialized = bincode::serialize(&ShortTypeId(CIRCLEOID)).unwrap();
        assert_eq!(serialized, vec![42, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111, 103, 6, 0, 0, 0, 0, 0, 0, 0, 99, 105, 114, 99, 108, 101],);
        let deserialized: ShortTypeId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.0, CIRCLEOID);
    }

    #[pg_test]
    fn test_short_type_id_serialize_circle_type_ron() {
        let serialized = ron::to_string(&ShortTypeId(CIRCLEOID)).unwrap();
        assert_eq!(&*serialized, "Type((\"pg_catalog\",\"circle\"))");
        let deserialized: ShortTypeId = ron::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, CIRCLEOID);
    }
}
