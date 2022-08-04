use std::{
    fmt,
    hash::{BuildHasher, Hasher},
    mem::size_of,
    slice,
};

use serde::{
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};

use pg_sys::{Datum, Oid};
use pgx::*;

use crate::serialization::{PgCollationId, ShortTypeId};

pub(crate) unsafe fn deep_copy_datum(datum: Datum, typoid: Oid) -> Datum {
    let tentry = pg_sys::lookup_type_cache(typoid, 0_i32);
    if (*tentry).typbyval {
        datum
    } else if (*tentry).typlen > 0 {
        // only varlena's can be toasted, manually copy anything with len >0
        let size = (*tentry).typlen as usize;
        let copy = pg_sys::palloc0(size);
        std::ptr::copy(datum as *const u8, copy as *mut u8, size);
        copy as Datum
    } else {
        pg_sys::pg_detoast_datum_copy(datum as _) as _
    }
}

// TODO: is there a better place for this?
// Note that this requires an reference time to deal with variable length intervals (days or months)
pub fn interval_to_ms(ref_time: &crate::raw::TimestampTz, interval: &crate::raw::Interval) -> i64 {
    extern "C" {
        fn timestamptz_pl_interval(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
    }
    let bound = unsafe {
        pg_sys::DirectFunctionCall2Coll(
            Some(timestamptz_pl_interval),
            pg_sys::InvalidOid,
            ref_time.0,
            interval.0,
        )
    };
    bound as i64 - ref_time.0 as i64
}

pub struct TextSerializableDatumWriter {
    flinfo: pg_sys::FmgrInfo,
}

impl TextSerializableDatumWriter {
    pub fn from_oid(typoid: Oid) -> Self {
        let mut type_output = 0;
        let mut typ_is_varlena = false;
        let mut flinfo = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };

        unsafe {
            pg_sys::getTypeOutputInfo(typoid, &mut type_output, &mut typ_is_varlena);
            pg_sys::fmgr_info(type_output, &mut flinfo);
        }

        TextSerializableDatumWriter { flinfo }
    }

    pub fn make_serializable(&mut self, datum: Datum) -> TextSerializeableDatum {
        TextSerializeableDatum(datum, &mut self.flinfo)
    }
}

pub struct DatumFromSerializedTextReader {
    flinfo: pg_sys::FmgrInfo,
    typ_io_param: u32,
}

impl DatumFromSerializedTextReader {
    pub fn from_oid(typoid: Oid) -> Self {
        let mut type_input = 0;
        let mut typ_io_param = 0;
        let mut flinfo = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        unsafe {
            pg_sys::getTypeInputInfo(typoid, &mut type_input, &mut typ_io_param);
            pg_sys::fmgr_info(type_input, &mut flinfo);
        }

        DatumFromSerializedTextReader {
            flinfo,
            typ_io_param,
        }
    }

    pub fn read_datum(&mut self, datum_str: &str) -> Datum {
        let cstr = pgx::cstr_core::CString::new(datum_str).unwrap(); // TODO: error handling
        let cstr_ptr = cstr.as_ptr() as *mut std::os::raw::c_char;
        unsafe { pg_sys::InputFunctionCall(&mut self.flinfo, cstr_ptr, self.typ_io_param, -1) }
    }
}

pub struct TextSerializeableDatum(Datum, *mut pg_sys::FmgrInfo);

impl Serialize for TextSerializeableDatum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let chars = unsafe { pg_sys::OutputFunctionCall(self.1, self.0) };
        let cstr = unsafe { pgx::cstr_core::CStr::from_ptr(chars) };
        serializer.serialize_str(cstr.to_str().unwrap())
    }
}

pub(crate) struct DatumHashBuilder {
    pub info: pg_sys::FunctionCallInfo,
    pub type_id: pg_sys::Oid,
    pub collation: pg_sys::Oid,
}

impl DatumHashBuilder {
    pub(crate) unsafe fn from_type_id(type_id: pg_sys::Oid, collation: Option<Oid>) -> Self {
        let entry =
            pg_sys::lookup_type_cache(type_id, pg_sys::TYPECACHE_HASH_EXTENDED_PROC_FINFO as _);
        Self::from_type_cache_entry(entry, collation)
    }

    pub(crate) unsafe fn from_type_cache_entry(
        tentry: *const pg_sys::TypeCacheEntry,
        collation: Option<Oid>,
    ) -> Self {
        let flinfo = if (*tentry).hash_extended_proc_finfo.fn_addr.is_some() {
            &(*tentry).hash_extended_proc_finfo
        } else {
            pgx::error!("no hash function");
        };

        // 1 argument for the key, 1 argument for the seed
        let size =
            size_of::<pg_sys::FunctionCallInfoBaseData>() + size_of::<pg_sys::NullableDatum>() * 2;
        let mut info = pg_sys::palloc0(size) as pg_sys::FunctionCallInfo;

        (*info).flinfo = flinfo as *const pg_sys::FmgrInfo as *mut pg_sys::FmgrInfo;
        (*info).context = std::ptr::null_mut();
        (*info).resultinfo = std::ptr::null_mut();
        (*info).fncollation = (*tentry).typcollation;
        (*info).isnull = false;
        (*info).nargs = 1;

        let collation = match collation {
            Some(collation) => collation,
            None => (*tentry).typcollation,
        };

        Self {
            info,
            type_id: (*tentry).type_id,
            collation,
        }
    }
}

impl Clone for DatumHashBuilder {
    fn clone(&self) -> Self {
        Self {
            info: self.info,
            type_id: self.type_id,
            collation: self.collation,
        }
    }
}

impl BuildHasher for DatumHashBuilder {
    type Hasher = DatumHashBuilder;

    fn build_hasher(&self) -> Self::Hasher {
        Self {
            info: self.info,
            type_id: self.type_id,
            collation: self.collation,
        }
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
            //FIXME 32bit vs 64 bit get value from datum on 32b arch
            value
        };
        value as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() != size_of::<usize>() {
            panic!("invalid datum hash")
        }

        let mut b = [0; size_of::<usize>()];
        b[..size_of::<usize>()].clone_from_slice(&bytes[..size_of::<usize>()]);
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
        S: serde::Serializer,
    {
        let collation = if self.collation == 0 {
            None
        } else {
            Some(PgCollationId(self.collation))
        };
        (ShortTypeId(self.type_id), collation).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DatumHashBuilder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (type_id, collation) =
            <(ShortTypeId, Option<PgCollationId>)>::deserialize(deserializer)?;
        //FIXME no collation?
        let deserialized = unsafe { Self::from_type_id(type_id.0, collation.map(|c| c.0)) };
        Ok(deserialized)
    }
}

#[inline]
fn div_round_up(numerator: usize, divisor: usize) -> usize {
    (numerator + divisor - 1) / divisor
}

#[inline]
fn round_to_multiple(value: usize, multiple: usize) -> usize {
    div_round_up(value, multiple) * multiple
}

#[inline]
fn padded_va_len(ptr: *const pg_sys::varlena) -> usize {
    unsafe { round_to_multiple(varsize_any(ptr), 8) }
}

flat_serialize_macro::flat_serialize! {
    #[derive(Debug)]
    struct DatumStore<'input> {
        type_oid: crate::serialization::ShortTypeId,
        data_len: u32,
        // XXX this must be aligned to 8-bytes to ensure the stored data is correctly aligned
        data: [u8; self.data_len],
    }
}

impl<'a> Serialize for DatumStore<'a> {
    // TODO currently this always serializes inner data as text. When we start
    // working on more-efficient network serialization, or we start using this
    // in a transition state, we should use the binary format if we don't need
    // human-readable output.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut writer = TextSerializableDatumWriter::from_oid(self.type_oid.0);
        let count = self.iter().count();
        let mut seq = serializer.serialize_seq(Some(count + 1))?;
        seq.serialize_element(&self.type_oid.0)?;
        for element in self.iter() {
            seq.serialize_element(&writer.make_serializable(element))?;
        }
        seq.end()
    }
}

impl<'a, 'de> Deserialize<'de> for DatumStore<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DatumStoreVisitor<'a>(std::marker::PhantomData<core::cell::Cell<&'a ()>>);

        impl<'de, 'a> Visitor<'de> for DatumStoreVisitor<'a> {
            type Value = DatumStore<'a>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence encoding a DatumStore object")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let oid = seq.next_element::<Oid>().unwrap().unwrap(); // TODO: error handling

                // TODO seperate human-readable and binary forms
                let mut reader = DatumFromSerializedTextReader::from_oid(oid);

                let mut data = vec![];
                while let Some(next) = seq.next_element::<&str>()? {
                    data.push(reader.read_datum(next));
                }

                Ok((oid, data).into())
            }
        }

        deserializer.deserialize_seq(DatumStoreVisitor(std::marker::PhantomData))
    }
}

impl From<(Oid, Vec<Datum>)> for DatumStore<'_> {
    fn from(input: (Oid, Vec<Datum>)) -> Self {
        let (oid, datums) = input;
        let (tlen, typbyval) = unsafe {
            let tentry = pg_sys::lookup_type_cache(oid, 0_i32);
            ((*tentry).typlen, (*tentry).typbyval)
        };
        assert!(tlen.is_positive() || tlen == -1 || tlen == -2);

        if typbyval {
            // Datum by value

            // pad entries out to 8 byte aligned values...this may be a source of inefficiency
            let data_len = round_to_multiple(tlen as usize, 8) as u32 * datums.len() as u32;

            let mut data: Vec<u8> = vec![];
            for datum in datums {
                data.extend_from_slice(&datum.to_ne_bytes());
            }

            DatumStore {
                type_oid: oid.into(),
                data_len,
                data: data.into(),
            }
        } else if tlen == -1 {
            // Varlena

            let mut ptrs = Vec::new();
            let mut total_data_bytes = 0;

            for datum in datums {
                unsafe {
                    let ptr = pg_sys::pg_detoast_datum_packed(datum as *mut pg_sys::varlena);
                    let va_len = varsize_any(ptr);

                    ptrs.push(ptr);
                    total_data_bytes += round_to_multiple(va_len, 8); // Round up to 8 byte boundary
                }
            }

            let mut buffer = vec![0u8; total_data_bytes];

            let mut target_byte = 0;
            for ptr in ptrs {
                unsafe {
                    let va_len = varsize_any(ptr);
                    std::ptr::copy(
                        ptr as *const u8,
                        std::ptr::addr_of_mut!(buffer[target_byte]),
                        va_len,
                    );
                    target_byte += round_to_multiple(va_len, 8);
                }
            }

            DatumStore {
                type_oid: oid.into(),
                data_len: total_data_bytes as u32,
                data: buffer.into(),
            }
        } else if tlen == -2 {
            // Null terminated string, should not be possible in this context
            panic!("Unexpected null-terminated string type encountered.");
        } else {
            // Fixed size reference

            // Round size to multiple of 8 bytes
            let len = round_to_multiple(tlen as usize, 8);
            let total_length = len * datums.len();

            let mut buffer = vec![0u8; total_length];
            for (i, datum) in datums.iter().enumerate() {
                unsafe {
                    std::ptr::copy(
                        *datum as *const u8,
                        std::ptr::addr_of_mut!(buffer[i * len]),
                        tlen as usize,
                    )
                };
            }

            DatumStore {
                type_oid: oid.into(),
                data_len: total_length as u32,
                data: buffer.into(),
            }
        }
    }
}

pub enum DatumStoreIterator<'a, 'b> {
    Value {
        iter: slice::Iter<'a, Datum>,
    },
    Varlena {
        store: &'b DatumStore<'a>,
        next_offset: u32,
    },
    FixedSize {
        store: &'b DatumStore<'a>,
        next_index: u32,
        datum_size: u32,
    },
}

impl<'a, 'b> Iterator for DatumStoreIterator<'a, 'b> {
    type Item = Datum;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DatumStoreIterator::Value { iter } => iter.next().copied(),
            DatumStoreIterator::Varlena { store, next_offset } => {
                if *next_offset >= store.data_len {
                    None
                } else {
                    unsafe {
                        let va = store.data.slice().as_ptr().offset(*next_offset as _)
                            as *const pg_sys::varlena;
                        *next_offset += padded_va_len(va) as u32;
                        Some(va as pg_sys::Datum)
                    }
                }
            }
            DatumStoreIterator::FixedSize {
                store,
                next_index,
                datum_size,
            } => {
                let idx = *next_index * *datum_size;
                if idx >= store.data_len {
                    None
                } else {
                    *next_index += 1;
                    Some(unsafe { store.data.slice().as_ptr().offset(idx as _) } as pg_sys::Datum)
                }
            }
        }
    }
}

impl<'a> DatumStore<'a> {
    pub fn iter<'b>(&'b self) -> DatumStoreIterator<'a, 'b> {
        unsafe {
            let tentry = pg_sys::lookup_type_cache(self.type_oid.into(), 0_i32);
            if (*tentry).typbyval {
                // Datum by value
                DatumStoreIterator::Value {
                    // SAFETY `data` is guaranteed to be 8-byte aligned, so it should be safe to use as a slice
                    iter: std::slice::from_raw_parts(
                        self.data.as_slice().as_ptr() as *const Datum,
                        self.data_len as usize / 8,
                    )
                    .iter(),
                }
            } else if (*tentry).typlen == -1 {
                // Varlena
                DatumStoreIterator::Varlena {
                    store: self,
                    next_offset: 0,
                }
            } else if (*tentry).typlen == -2 {
                // Null terminated string
                unreachable!()
            } else {
                // Fixed size reference
                assert!((*tentry).typlen.is_positive());
                DatumStoreIterator::FixedSize {
                    store: self,
                    next_index: 0,
                    datum_size: round_to_multiple((*tentry).typlen as usize, 8) as u32,
                }
            }
        }
    }
}

// This is essentially the same as the DatumStoreIterator except that it takes ownership of the DatumStore,
// there should be some way to efficiently merge these implementations
pub enum DatumStoreIntoIterator<'a> {
    Value {
        store: DatumStore<'a>,
        next_idx: u32,
    },
    Varlena {
        store: DatumStore<'a>,
        next_offset: u32,
    },
    FixedSize {
        store: DatumStore<'a>,
        next_index: u32,
        datum_size: u32,
    },
}

// iterate over the set of values in the datum store
// will return pointers into the datum store if it's a by-ref type
impl<'a> Iterator for DatumStoreIntoIterator<'a> {
    type Item = Datum;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DatumStoreIntoIterator::Value { store, next_idx } => {
                let idx = *next_idx as usize;
                let bound = store.data_len as usize / 8;
                if idx >= bound {
                    None
                } else {
                    // SAFETY `data` is guaranteed to be 8-byte aligned, so it is safe to use as a usize slice
                    let dat = unsafe {std::slice::from_raw_parts(
                        store.data.as_slice().as_ptr() as *const Datum,
                        bound,
                    )[idx]};
                    *next_idx += 1;
                    Some(dat)
                }
            },
            DatumStoreIntoIterator::Varlena { store, next_offset } => {
                if *next_offset >= store.data_len {
                    None
                } else {
                    unsafe {
                        let va = store.data.slice().as_ptr().offset(*next_offset as _)
                            as *const pg_sys::varlena;
                        *next_offset += padded_va_len(va) as u32;
                        Some(va as pg_sys::Datum)
                    }
                }
            }
            DatumStoreIntoIterator::FixedSize {
                store,
                next_index,
                datum_size,
            } => {
                let idx = *next_index * *datum_size;
                if idx >= store.data_len {
                    None
                } else {
                    *next_index += 1;
                    Some(unsafe { store.data.slice().as_ptr().offset(idx as _) } as pg_sys::Datum)
                }
            }
        }
    }
}

impl<'a> IntoIterator for DatumStore<'a> {
    type Item = Datum;
    type IntoIter = DatumStoreIntoIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let tentry = pg_sys::lookup_type_cache(self.type_oid.into(), 0_i32);
            if (*tentry).typbyval {
                // Datum by value
                DatumStoreIntoIterator::Value {
                    store: self,
                    next_idx: 0,
                }
            } else if (*tentry).typlen == -1 {
                // Varlena
                DatumStoreIntoIterator::Varlena {
                    store: self,
                    next_offset: 0,
                }
            } else if (*tentry).typlen == -2 {
                // Null terminated string
                unreachable!()
            } else {
                // Fixed size reference
                assert!((*tentry).typlen.is_positive());
                DatumStoreIntoIterator::FixedSize {
                    store: self,
                    next_index: 0,
                    datum_size: round_to_multiple((*tentry).typlen as usize, 8) as u32,
                }
            }
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::{build, palloc::Inner, pg_type, ron_inout_funcs};
    use aggregate_builder::*;
    use pgx_macros::pg_test;

    #[pg_schema]
    pub mod toolkit_experimental {
        use super::*;
        pg_type! {
            #[derive(Debug)]
            struct DatumStoreTester<'input> {
                datums: DatumStore<'input>,
            }
        }
        ron_inout_funcs!(DatumStoreTester);

        #[aggregate]
        impl toolkit_experimental::datum_test_agg {
            type State = (Oid, Vec<Datum>);

            fn transition(
                state: Option<State>,
                #[sql_type("AnyElement")] value: AnyElement,
            ) -> Option<State> {
                match state {
                    Some((oid, mut vector)) => {
                        unsafe { vector.push(deep_copy_datum(value.datum(), oid)) };
                        Some((oid, vector))
                    }
                    None => Some((
                        value.oid(),
                        vec![unsafe { deep_copy_datum(value.datum(), value.oid()) }],
                    )),
                }
            }

            fn finally(state: Option<&mut State>) -> Option<DatumStoreTester<'static>> {
                state.map(|state| {
                    build! {
                        DatumStoreTester {
                            datums: DatumStore::from(std::mem::take(state)),
                        }
                    }
                })
            }
        }
    }

    #[pg_test]
    fn test_value_datum_store() {
        Spi::execute(|client| {
            let test = client.select("SELECT toolkit_experimental.datum_test_agg(r.data)::TEXT FROM (SELECT generate_series(10, 100, 10) as data) r", None, None)
                .first()
                .get_one::<String>().unwrap();
            let expected = "(version:1,datums:[23,\"10\",\"20\",\"30\",\"40\",\"50\",\"60\",\"70\",\"80\",\"90\",\"100\"])";
            assert_eq!(test, expected);
        });
    }

    #[pg_test]
    fn test_varlena_datum_store() {
        Spi::execute(|client| {
            let test = client.select("SELECT toolkit_experimental.datum_test_agg(r.data)::TEXT FROM (SELECT generate_series(10, 100, 10)::TEXT as data) r", None, None)
                .first()
                .get_one::<String>().unwrap();
            let expected = "(version:1,datums:[25,\"10\",\"20\",\"30\",\"40\",\"50\",\"60\",\"70\",\"80\",\"90\",\"100\"])";
            assert_eq!(test, expected);
        });
    }

    #[pg_test]
    fn test_byref_datum_store() {
        Spi::execute(|client| {
            let test = client.select("SELECT toolkit_experimental.datum_test_agg(r.data)::TEXT FROM (SELECT (generate_series(10, 100, 10)::TEXT || ' seconds')::INTERVAL as data) r", None, None)
                .first()
                .get_one::<String>().unwrap();
            let expected = "(version:1,datums:[1186,\"00:00:10\",\"00:00:20\",\"00:00:30\",\"00:00:40\",\"00:00:50\",\"00:01:00\",\"00:01:10\",\"00:01:20\",\"00:01:30\",\"00:01:40\"])";
            assert_eq!(test, expected);
        });
    }
}
