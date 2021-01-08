
use std::{hash::{BuildHasher, Hasher}, mem::size_of, slice};

use serde::{Serialize, Deserialize};

use pgx::*;
use pg_sys::{Datum, Oid};

use flat_serialize::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    debug_inout_funcs,
    flatten,
    palloc::Internal,
    pg_type
};

use hyperloglog::{HyperLogLog as HLL, HyperLogLogger};

#[derive(Clone, Serialize, Deserialize)]
pub struct HyperLogLogTrans {
    logger: HyperLogLogger<Datum, DatumHashBuilder>,
}

#[allow(non_camel_case_types)]
type int = i32;
type AnyElement = Datum;

#[pg_extern]
pub fn hyperloglog_trans(
    state: Option<Internal<HyperLogLogTrans>>,
    size: int,
    value: Option<AnyElement>,
    fc: pg_sys::FunctionCallInfo,
) -> Option<Internal<HyperLogLogTrans>> {
    unsafe {
        in_aggregate_context(fc, || {
            //TODO is this the right way to handle NULL?
            let value = match value {
                None => return state,
                Some(value) => value,
            };
            let mut state = match state {
                None => {
                    let typ = pgx::get_getarg_type(fc, 2);
                    let hasher = DatumHashBuilder::from_type_id(typ);
                    let trans = HyperLogLogTrans {
                        logger: HyperLogLogger::with_hash(size as usize, hasher),
                    };
                    trans.into()
                },
                Some(state) => state,
            };
            state.logger.add(&value);
            Some(state)
        })
    }
}

#[pg_extern]
pub fn hyperloglog_combine(
    state1: Option<Internal<HyperLogLogTrans>>,
    state2: Option<Internal<HyperLogLogTrans>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<HyperLogLogTrans>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => {
                    let logger = HLL::merge(
                        &state1.logger.as_hyperloglog(),
                        &state2.logger.as_hyperloglog(),
                    );
                    Some(HyperLogLogTrans{
                        logger,
                    }.into())
                }
            }
        })
    }
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

#[pg_extern]
pub fn hyperloglog_serialize(
    state: Internal<HyperLogLogTrans>,
) -> bytea {
    crate::do_serialize!(state)
}

#[pg_extern]
pub fn hyperloglog_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<HyperLogLogTrans> {
    crate::do_deserialize!(bytes, HyperLogLogTrans)
}


pg_type!{
    #[derive(Debug)]
    struct HyperLogLog {
        // Oids are stored in postgres arrays, so it should be safe to store them
        // in our types as long as we do send/recv and in/out correctly
        // see https://github.com/postgres/postgres/blob/b8d0cda53377515ac61357ec4a60e85ca873f486/src/include/utils/array.h#L90
        element_type: Oid, //FIXME use Oid that I/O and send/recv as typename
        b: u32,
        registers: [u8; (1 as usize) << self.b],
    }
}

debug_inout_funcs!(HyperLogLog);

#[pg_extern]
fn hyperloglog_final(
    state: Option<Internal<HyperLogLogTrans>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<HyperLogLog<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };

            let element_type = state.logger.buildhasher().type_id;
            let log = state.logger.as_hyperloglog();

            // we need to flatten the vector to a single buffer that contains
            // both the size, the data, and the varlen header
            flatten!(
                HyperLogLog {
                    element_type: &element_type,
                    b: &(log.b as u32),
                    registers: log.registers,
                }
            ).into()
        })
    }
}

#[pg_extern]
pub fn hyperloglog_count<'input>(
    hyperloglog: HyperLogLog<'input>,
) -> int {
    // count does not depend on the type parameters
    HLL::<()> {
        registers: hyperloglog.registers,
        b: *hyperloglog.b as _,
        buildhasher: Default::default(),
        phantom: Default::default(),

    }.count() as int
}




// TODO move to it's own mod if we reuse it
struct DatumHashBuilder {
    info: pg_sys::FunctionCallInfo,
    type_id: pg_sys::Oid,
}

impl DatumHashBuilder {

    unsafe fn from_type_id(type_id: pg_sys::Oid) -> Self {
        let entry = pg_sys::lookup_type_cache(type_id, pg_sys::TYPECACHE_HASH_PROC_FINFO as _);
        Self::from_type_cache_entry(entry)
    }

    unsafe fn from_type_cache_entry(tentry: *const pg_sys::TypeCacheEntry) -> Self {

        if (*tentry).hash_proc_finfo.fn_addr.is_none() {
            todo!()
        }

        // only need space for 1 arg, but we're allocating for two to avoid
        // issues in the even we're calculating alignment wrong
        let size = size_of::<pg_sys::FunctionCallInfoBaseData>()
            + size_of::<pg_sys::NullableDatum>() * 2;
        let mut info = pg_sys::palloc0(size) as pg_sys::FunctionCallInfo;
        // InitFunctionCallInfoData
        (*info).flinfo = &(*tentry).hash_proc_finfo as *const _ as *mut _;
        (*info).context = std::ptr::null_mut();
        (*info).resultinfo = std::ptr::null_mut();
        (*info).fncollation = (*tentry).typcollation;
        (*info).isnull = false;
        (*info).nargs = 1;

        Self { info, type_id: (*tentry).type_id }
    }
}

impl Clone for DatumHashBuilder {
    fn clone(&self) -> Self {
        Self { info: self.info, type_id: self.type_id, }
    }
}

impl BuildHasher for DatumHashBuilder {
    type Hasher = DatumHashBuilder;

    fn build_hasher(&self) -> Self::Hasher {
        Self { info: self.info, type_id: self.type_id, }
    }
}

impl Hasher for DatumHashBuilder {
    fn finish(&self) -> u64 {
        //FIXME ehhh, this is wildly unsafe, should at least have a separate hash
        //      buffer for each, probably should have separate args
        let value = unsafe {
            let value = (*(*self.info).flinfo).fn_addr.unwrap()(self.info);
            (*self.info).args.as_mut_slice(1)[0] = pg_sys::NullableDatum {
                value: 0,
                isnull: true,
            };
            (*self.info).isnull = false;
            //TODO is it an issue that this only returns a 32bit hash?
            value as u32
        };
        // run through a round of FNV to mix the bits into the full 64bit range
        let mut hash = 0xcbf29ce484222325;
        for byte in value.to_ne_bytes().iter() {
            hash = hash ^ (*byte as u64);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() != size_of::<usize>() {
            panic!("invalid datum hash")
        }

        let mut b = [0; size_of::<usize>()];
        for i in 0..size_of::<usize>() {
            b[i] = bytes[i]
        }
        self.write_usize(usize::from_ne_bytes(b))
    }

    fn write_usize(&mut self, i: usize) {
        unsafe {
            (*self.info).args.as_mut_slice(1)[0] = pg_sys::NullableDatum {
                value: i,
                isnull: false,
            };
            (*self.info).isnull = false;
        }
    }
}

impl PartialEq for DatumHashBuilder {
    fn eq(&self, other: &Self) -> bool {
        self.type_id.eq(&other.type_id)
    }
}

impl Eq for DatumHashBuilder {}

impl Serialize for DatumHashBuilder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        use std::ffi::CStr;
        unsafe {
            let tuple = pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier_TYPEOID as _, self.type_id as _);
            if tuple.is_null() {
                todo!()
            }

            let type_tuple: pg_sys::Form_pg_type = get_struct(tuple);
            // TODO also send namespace
            let bytes = CStr::from_ptr((*type_tuple).typname.data.as_ptr())
                .to_bytes_with_nul();
            serializer.serialize_bytes(bytes)
        }
    }
}

impl<'de> Deserialize<'de> for DatumHashBuilder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        use std::fmt;
        use serde::de::{self, Visitor};

        struct StrTypeVisitor;

        impl<'de> Visitor<'de> for StrTypeVisitor {
            type Value = DatumHashBuilder;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("the string representation of a type")
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: de::Error,  {
                    todo!()
            }

        }

        deserializer.deserialize_bytes(StrTypeVisitor)
    }
}

unsafe fn get_struct<T>(tuple: pg_sys::HeapTuple) -> *mut T {
    //((char *) ((TUP)->t_data) + (TUP)->t_data->t_hoff)
    (*tuple).t_data.add((*(*tuple).t_data).t_hoff as usize).cast()
}


#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hll_aggregate() {
        Spi::execute(|client| {
            let text = client
                .select("SELECT hyperloglog(5, v::float)::TEXT FROM generate_series(1, 100) v", None, None)
                .first()
                .get_one::<String>();
            assert_eq!(text.unwrap(), "HyperLogLogData { header: 192, version: 1, padding: [0, 0, 0], element_type: 701, b: 5, registers: [5, 5, 4, 4, 4, 3, 3, 2, 3, 2, 6, 4, 2, 3, 3, 5, 5, 0, 2, 2, 1, 4, 9, 1, 2, 6, 1, 1, 0, 5, 4, 3] }");

            let count = client
                .select("SELECT hyperloglog_count(hyperloglog(5, v::float)) FROM generate_series(1, 100) v", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(count, Some(104));
        });
    }
}
