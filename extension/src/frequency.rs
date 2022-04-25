//! Based on the paper: https://cs.ucsb.edu/sites/default/files/documents/2005-23.pdf

use std::fmt;

use pgx::*;

use pg_sys::{Datum, Oid};

use serde::{
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};

use flat_serialize::*;

use crate::{
    aggregate_utils::{get_collation, in_aggregate_context},
    build,
    datum_utils::{
        deep_copy_datum, DatumFromSerializedTextReader, DatumHashBuilder, DatumStore,
        TextSerializableDatumWriter,
    },
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_any_element::{PgAnyElement, PgAnyElementHashMap},
    pg_type,
    raw::{bytea, text},
    ron_inout_funcs,
};

use spfunc::zeta::zeta;
use statrs::function::harmonic::gen_harmonic;

use crate::frequency::toolkit_experimental::{
    SpaceSavingAggregate, SpaceSavingBigIntAggregate, SpaceSavingTextAggregate,
};

// Helper functions for zeta distribution

// Default s-value
const DEFAULT_ZETA_SKEW: f64 = 1.1;

// probability of the nth element of a zeta distribution
fn zeta_eq_n(skew: f64, n: u64) -> f64 {
    1.0 / zeta(skew) * (n as f64).powf(-1. * skew)
}
// cumulative distribution <= n in a zeta distribution
fn zeta_le_n(skew: f64, n: u64) -> f64 {
    gen_harmonic(n, skew) / zeta(skew)
}

struct SpaceSavingEntry {
    value: Datum,
    count: u64,
    overcount: u64,
}

impl SpaceSavingEntry {
    fn clone(&self, typoid: Oid) -> SpaceSavingEntry {
        SpaceSavingEntry {
            value: unsafe { deep_copy_datum(self.value, typoid) },
            count: self.count,
            overcount: self.overcount,
        }
    }
}

pub struct SpaceSavingTransState {
    entries: Vec<SpaceSavingEntry>,
    indices: PgAnyElementHashMap<usize>,
    total_vals: u64,
    freq_param: f64, // This is the minimum frequency for a freq_agg or the skew for a topn_agg
    topn: u32,       // 0 for freq_agg, creation parameter for topn_agg
    max_size: u32,   // Maximum size for indices
}

impl Clone for SpaceSavingTransState {
    fn clone(&self) -> Self {
        let mut new_state = Self {
            entries: vec![],
            indices: PgAnyElementHashMap::with_hasher(self.indices.hasher().clone()),
            total_vals: self.total_vals,
            freq_param: self.freq_param,
            max_size: self.max_size,
            topn: self.topn,
        };

        let typoid = self.type_oid();
        for entry in &self.entries {
            new_state.entries.push(SpaceSavingEntry {
                value: unsafe { deep_copy_datum(entry.value, typoid) },
                count: entry.count,
                overcount: entry.overcount,
            })
        }
        new_state.update_all_map_indices();
        new_state
    }
}

// SpaceSavingTransState is a little tricky to serialize due to needing the typ oid to serialize the Datums.
// This sort of requirement doesn't play nicely with the serde framework, so as a workaround we simply
// serialize the object as one big sequence.  The serialized sequence should look like this:
//   total_vals as u64
//   min_freq as f64
//   max_idx as u32
//   topn as u32
//   indices.hasher as DatumHashBuilder
//   entries as repeated (str, u64, u64) tuples
impl Serialize for SpaceSavingTransState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.entries.len() + 5))?;
        seq.serialize_element(&self.total_vals)?;
        seq.serialize_element(&self.freq_param)?;
        seq.serialize_element(&self.max_size)?;
        seq.serialize_element(&self.topn)?;
        seq.serialize_element(&self.indices.hasher())?;

        // TODO JOSH use a writer that switches based on whether we want binary or not
        let mut writer = TextSerializableDatumWriter::from_oid(self.type_oid());

        for entry in &self.entries {
            seq.serialize_element(&(
                writer.make_serializable(entry.value),
                entry.count,
                entry.overcount,
            ))?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for SpaceSavingTransState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FrequencyTransStateVisitor();

        impl<'de> Visitor<'de> for FrequencyTransStateVisitor {
            type Value = SpaceSavingTransState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence encoding a FrequencyTransState object")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let total_vals = seq.next_element::<u64>()?.unwrap();
                let min_freq = seq.next_element::<f64>()?.unwrap();
                let max_size = seq.next_element::<u32>()?.unwrap();
                let topn = seq.next_element::<u32>()?.unwrap();
                let hasher = seq.next_element::<DatumHashBuilder>()?.unwrap();

                let mut state = SpaceSavingTransState {
                    entries: vec![],
                    indices: PgAnyElementHashMap::with_hasher(hasher),
                    total_vals,
                    freq_param: min_freq,
                    max_size,
                    topn,
                };

                let typid = state.type_oid();
                let mut reader = DatumFromSerializedTextReader::from_oid(typid);

                while let Some((datum_str, count, overcount)) =
                    seq.next_element::<(&str, u64, u64)>()?
                {
                    let datum = reader.read_datum(datum_str);

                    state.entries.push(SpaceSavingEntry {
                        value: unsafe { deep_copy_datum(datum, typid) },
                        count,
                        overcount,
                    });
                }
                state.update_all_map_indices();
                Ok(state)
            }
        }

        deserializer.deserialize_seq(FrequencyTransStateVisitor())
    }
}

impl SpaceSavingTransState {
    fn max_size_for_freq(min_freq: f64) -> u32 {
        (1. / min_freq) as u32 + 1
    }

    fn freq_agg_from_type_id(min_freq: f64, typ: pg_sys::Oid, collation: Option<Oid>) -> Self {
        SpaceSavingTransState {
            entries: vec![],
            indices: PgAnyElementHashMap::new(typ, collation),
            total_vals: 0,
            freq_param: min_freq,
            max_size: SpaceSavingTransState::max_size_for_freq(min_freq),
            topn: 0,
        }
    }

    fn topn_agg_from_type_id(
        skew: f64,
        nval: u32,
        typ: pg_sys::Oid,
        collation: Option<Oid>,
    ) -> Self {
        if nval == 0 {
            pgx::error!("topn aggregate requires an n value > 0")
        }
        if skew <= 1.0 {
            pgx::error!("topn aggregate requires a skew factor > 1.0")
        }

        let prob_eq_n = zeta_eq_n(skew, nval as u64);
        let prob_lt_n = zeta_le_n(skew, nval as u64 - 1);

        SpaceSavingTransState {
            entries: vec![],
            indices: PgAnyElementHashMap::new(typ, collation),
            total_vals: 0,
            freq_param: skew,
            max_size: nval - 1
                + SpaceSavingTransState::max_size_for_freq(prob_eq_n / (1.0 - prob_lt_n)),
            topn: nval,
        }
    }

    fn type_oid(&self) -> Oid {
        self.indices.typoid()
    }

    fn add(&mut self, element: PgAnyElement) {
        self.total_vals += 1;
        if let Some(idx) = self.indices.get(&element) {
            let idx = *idx;
            self.entries[idx].count += 1;
            self.move_left(idx);
        } else if self.entries.len() < self.max_size as usize {
            let new_idx = self.entries.len();
            self.entries.push(SpaceSavingEntry {
                value: element.deep_copy_datum(),
                count: 1,
                overcount: 0,
            });

            // Important to create the indices entry using the datum in the local context
            self.indices.insert(
                (self.entries[new_idx].value, self.type_oid()).into(),
                new_idx,
            );
        } else {
            let new_value = element.deep_copy_datum();

            // TODO: might be more efficient to replace the lowest indexed tail value (count matching last) and not call move_up
            let typoid = self.type_oid();
            let entry = self.entries.last_mut().unwrap();
            self.indices.remove(&(entry.value, typoid).into());
            entry.value = new_value; // JOSH FIXME should we pfree() old value if by-ref?
            entry.overcount = entry.count;
            entry.count += 1;
            self.indices
                .insert((new_value, typoid).into(), self.entries.len() - 1);
            self.move_left(self.entries.len() - 1);
        }
    }

    // swap element i with an earlier element in the 'entries' vector to maintain decreasing order
    fn move_left(&mut self, i: usize) {
        let count = self.entries[i].count;
        let mut target = i;
        while target > 0 && self.entries[target - 1].count < count {
            target -= 1;
        }
        if target != i {
            self.entries.swap(i, target);

            self.update_map_index(i);
            self.update_map_index(target);
        }
    }

    // Adds the 'indices' lookup entry for the value at 'entries' index i
    fn update_map_index(&mut self, i: usize) {
        let element_for_i = (self.entries[i].value, self.type_oid()).into();
        if let Some(entry) = self.indices.get_mut(&element_for_i) {
            *entry = i;
        } else {
            self.indices.insert(element_for_i, i);
        }
    }

    fn update_all_map_indices(&mut self) {
        for i in 0..self.entries.len() {
            self.update_map_index(i);
        }
    }

    fn combine(one: &SpaceSavingTransState, two: &SpaceSavingTransState) -> SpaceSavingTransState {
        // This takes an entry from a TransState, updates it with any state from the other TransState, and adds the result into the map
        fn new_entry(
            entry: &SpaceSavingEntry,
            other: &SpaceSavingTransState,
            map: &mut PgAnyElementHashMap<SpaceSavingEntry>,
        ) {
            let typoid = other.type_oid();

            let mut new_ent = entry.clone(typoid);
            let new_dat = (new_ent.value, typoid).into();
            match other.indices.get(&new_dat) {
                Some(&idx) => {
                    new_ent.count += other.entries[idx].count;
                    new_ent.overcount += other.entries[idx].overcount;
                }
                None => {
                    // If the entry value isn't present in the other state, we have to assume that it was recently bumped (unless the other state is not fully populated).
                    let min = if other.indices.len() < other.max_size as usize {
                        0
                    } else {
                        other.entries.last().unwrap().count
                    };
                    new_ent.count += min;
                    new_ent.overcount += min;
                }
            }
            map.insert(new_dat, new_ent);
        }

        let hasher = one.indices.hasher().clone();
        let mut temp = PgAnyElementHashMap::with_hasher(hasher);

        // First go through the first state, and add all entries (updated with other other state) to our temporary hashmap
        for entry in &one.entries {
            new_entry(entry, two, &mut temp);
        }

        // Next add in anything in the second state that isn't already in the map.
        // TODO JOSH does filter make this easier to read
        for entry in &two.entries {
            if !temp.contains_key(&(entry.value, one.type_oid()).into()) {
                new_entry(entry, one, &mut temp);
            }
        }

        // TODO: get this into_iter working without making temp.0 public
        let mut entries: Vec<SpaceSavingEntry> = temp.0.into_iter().map(|(_, v)| v).collect();
        entries.sort_by(|a, b| b.count.partial_cmp(&a.count).unwrap()); // swap a and b for descending

        entries.truncate(one.max_size as usize);

        let mut result = SpaceSavingTransState {
            entries,
            indices: PgAnyElementHashMap::with_hasher(one.indices.hasher().clone()),
            total_vals: one.total_vals + two.total_vals,
            freq_param: one.freq_param,
            max_size: one.max_size,
            topn: one.topn,
        };

        result.update_all_map_indices();
        result
    }
}

#[pg_schema]
pub mod toolkit_experimental {
    pub(crate) use super::*;

    pg_type! {
        #[derive(Debug)]
        struct SpaceSavingAggregate<'input> {
            type_oid: u32,
            num_values: u32,
            values_seen: u64,
            freq_param: f64,
            topn: u64, // bump this up to u64 to keep alignment
            counts: [u64; self.num_values], // JOSH TODO look at AoS instead of SoA at some point
            overcounts: [u64; self.num_values],
            datums: DatumStore<'input>,
        }
    }

    impl<'input> From<&SpaceSavingTransState> for SpaceSavingAggregate<'input> {
        fn from(trans: &SpaceSavingTransState) -> Self {
            let mut values = Vec::new();
            let mut counts = Vec::new();
            let mut overcounts = Vec::new();

            for entry in &trans.entries {
                values.push(entry.value);
                counts.push(entry.count);
                overcounts.push(entry.overcount);
            }

            build! {
                SpaceSavingAggregate {
                    type_oid: trans.type_oid() as _,
                    num_values: trans.entries.len() as _,
                    values_seen: trans.total_vals,
                    freq_param: trans.freq_param,
                    topn: trans.topn as u64,
                    counts: counts.into(),
                    overcounts: overcounts.into(),
                    datums: DatumStore::from((trans.type_oid(), values)),
                }
            }
        }
    }

    ron_inout_funcs!(SpaceSavingAggregate);

    pg_type! {
        #[derive(Debug)]
        struct SpaceSavingBigIntAggregate<'input> {
            num_values: u32,
            topn: u32,
            values_seen: u64,
            freq_param: f64,
            counts: [u64; self.num_values], // JOSH TODO look at AoS instead of SoA at some point
            overcounts: [u64; self.num_values],
            datums: [i64; self.num_values],
        }
    }

    impl<'input> From<&SpaceSavingTransState> for SpaceSavingBigIntAggregate<'input> {
        fn from(trans: &SpaceSavingTransState) -> Self {
            assert_eq!(trans.type_oid(), pg_sys::INT8OID);

            let mut values = Vec::new();
            let mut counts = Vec::new();
            let mut overcounts = Vec::new();

            for entry in &trans.entries {
                values.push(entry.value as i64);
                counts.push(entry.count);
                overcounts.push(entry.overcount);
            }

            build! {
                SpaceSavingBigIntAggregate {
                    num_values: trans.entries.len() as _,
                    values_seen: trans.total_vals,
                    freq_param: trans.freq_param,
                    topn: trans.topn,
                    counts: counts.into(),
                    overcounts: overcounts.into(),
                    datums: values.into(),
                }
            }
        }
    }

    ron_inout_funcs!(SpaceSavingBigIntAggregate);

    pg_type! {
        #[derive(Debug)]
        struct SpaceSavingTextAggregate<'input> {
            num_values: u32,
            topn: u32,
            values_seen: u64,
            freq_param: f64,
            counts: [u64; self.num_values], // JOSH TODO look at AoS instead of SoA at some point
            overcounts: [u64; self.num_values],
            datums: DatumStore<'input>,
        }
    }

    impl<'input> From<&SpaceSavingTransState> for SpaceSavingTextAggregate<'input> {
        fn from(trans: &SpaceSavingTransState) -> Self {
            assert_eq!(trans.type_oid(), pg_sys::TEXTOID);

            let mut values = Vec::new();
            let mut counts = Vec::new();
            let mut overcounts = Vec::new();

            for entry in &trans.entries {
                values.push(entry.value);
                counts.push(entry.count);
                overcounts.push(entry.overcount);
            }

            build! {
                SpaceSavingTextAggregate {
                    num_values: trans.entries.len() as _,
                    values_seen: trans.total_vals,
                    freq_param: trans.freq_param,
                    topn: trans.topn,
                    counts: counts.into(),
                    overcounts: overcounts.into(),
                    datums: DatumStore::from((trans.type_oid(), values)),
                }
            }
        }
    }

    ron_inout_funcs!(SpaceSavingTextAggregate);
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn topn_agg_trans(
    state: Internal,
    n: i32,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    topn_agg_with_skew_trans(state, n, DEFAULT_ZETA_SKEW, value, fcinfo)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn topn_agg_bigint_trans(
    state: Internal,
    n: i32,
    value: Option<i64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    topn_agg_with_skew_bigint_trans(state, n, DEFAULT_ZETA_SKEW, value, fcinfo)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn topn_agg_text_trans(
    state: Internal,
    n: i32,
    value: Option<crate::raw::text>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    topn_agg_with_skew_text_trans(state, n, DEFAULT_ZETA_SKEW, value, fcinfo)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn topn_agg_with_skew_trans(
    state: Internal,
    n: i32,
    skew: f64,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    space_saving_trans(
        unsafe { state.to_inner() },
        value,
        fcinfo,
        |typ, collation| {
            SpaceSavingTransState::topn_agg_from_type_id(skew, n as u32, typ, collation)
        },
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn topn_agg_with_skew_bigint_trans(
    state: Internal,
    n: i32,
    skew: f64,
    value: Option<i64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let value = match value {
        None => None,
        Some(val) => unsafe {
            AnyElement::from_datum(val as pg_sys::Datum, false, pg_sys::INT8OID)
        },
    };

    space_saving_trans(
        unsafe { state.to_inner() },
        value,
        fcinfo,
        |typ, collation| {
            SpaceSavingTransState::topn_agg_from_type_id(skew, n as u32, typ, collation)
        },
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn topn_agg_with_skew_text_trans(
    state: Internal,
    n: i32,
    skew: f64,
    value: Option<crate::raw::text>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let txt = value.map(|v| unsafe { pg_sys::pg_detoast_datum_copy(v.0 as *mut pg_sys::varlena) });
    let value = match txt {
        None => None,
        Some(val) => unsafe {
            AnyElement::from_datum(val as pg_sys::Datum, false, pg_sys::TEXTOID)
        },
    };

    space_saving_trans(
        unsafe { state.to_inner() },
        value,
        fcinfo,
        |typ, collation| {
            SpaceSavingTransState::topn_agg_from_type_id(skew, n as u32, typ, collation)
        },
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn freq_agg_trans(
    state: Internal,
    freq: f64,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    if freq <= 0. || freq >= 1.0 {
        pgx::error!("frequency aggregate requires a frequency in the range (0.0, 1.0)")
    }

    space_saving_trans(
        unsafe { state.to_inner() },
        value,
        fcinfo,
        |typ, collation| SpaceSavingTransState::freq_agg_from_type_id(freq, typ, collation),
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn freq_agg_bigint_trans(
    state: Internal,
    freq: f64,
    value: Option<i64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let value = match value {
        None => None,
        Some(val) => unsafe {
            AnyElement::from_datum(val as pg_sys::Datum, false, pg_sys::INT8OID)
        },
    };
    freq_agg_trans(state, freq, value, fcinfo)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn freq_agg_text_trans(
    state: Internal,
    freq: f64,
    value: Option<crate::raw::text>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let txt = value.map(|v| unsafe { pg_sys::pg_detoast_datum_copy(v.0 as *mut pg_sys::varlena) });
    let value = match txt {
        None => None,
        Some(val) => unsafe {
            AnyElement::from_datum(val as pg_sys::Datum, false, pg_sys::TEXTOID)
        },
    };
    freq_agg_trans(state, freq, value, fcinfo)
}

pub fn space_saving_trans<F>(
    state: Option<Inner<SpaceSavingTransState>>,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
    make_trans_state: F,
) -> Option<Inner<SpaceSavingTransState>>
where
    F: FnOnce(pg_sys::Oid, Option<pg_sys::Oid>) -> SpaceSavingTransState,
{
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                Some(value) => value,
            };
            let mut state = match state {
                None => {
                    let typ = value.oid();
                    let collation = if fcinfo.is_null() {
                        Some(100) // TODO: default OID, there should be a constant for this
                    } else {
                        get_collation(fcinfo)
                    };
                    make_trans_state(typ, collation).into()
                }
                Some(state) => state,
            };

            state.add(value.into());
            Some(state)
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn space_saving_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { space_saving_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}
pub fn space_saving_combine_inner(
    a: Option<Inner<SpaceSavingTransState>>,
    b: Option<Inner<SpaceSavingTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<SpaceSavingTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (a, b) {
            (Some(a), Some(b)) => Some(SpaceSavingTransState::combine(&*a, &*b).into()),
            (Some(a), None) => Some(a.clone().into()),
            (None, Some(b)) => Some(b.clone().into()),
            (None, None) => None,
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn space_saving_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::SpaceSavingAggregate<'static>> {
    let state: Option<&SpaceSavingTransState> = unsafe { state.get() };
    state.map(SpaceSavingAggregate::from)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn space_saving_bigint_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::SpaceSavingBigIntAggregate<'static>> {
    let state: Option<&SpaceSavingTransState> = unsafe { state.get() };
    state.map(SpaceSavingBigIntAggregate::from)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn space_saving_text_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::SpaceSavingTextAggregate<'static>> {
    let state: Option<&SpaceSavingTransState> = unsafe { state.get() };
    state.map(SpaceSavingTextAggregate::from)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn space_saving_serialize(state: Internal) -> bytea {
    let state: Inner<SpaceSavingTransState> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn space_saving_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: SpaceSavingTransState = crate::do_deserialize!(bytes, SpaceSavingTransState);
    Inner::from(i).internal()
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.raw_freq_agg(\n\
        frequency double precision, value AnyElement\n\
    ) (\n\
        sfunc = toolkit_experimental.freq_agg_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "freq_agg",
    requires = [
        freq_agg_trans,
        space_saving_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.freq_agg(\n\
        frequency double precision, value INT8\n\
    ) (\n\
        sfunc = toolkit_experimental.freq_agg_bigint_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_bigint_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "freq_bigint_agg",
    requires = [
        freq_agg_bigint_trans,
        space_saving_bigint_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.freq_agg(\n\
        frequency double precision, value TEXT\n\
    ) (\n\
        sfunc = toolkit_experimental.freq_agg_text_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_text_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "freq_text_agg",
    requires = [
        freq_agg_text_trans,
        space_saving_text_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.raw_topn_agg(\n\
        count integer, value AnyElement\n\
    ) (\n\
        sfunc = toolkit_experimental.topn_agg_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "topn_agg",
    requires = [
        topn_agg_trans,
        space_saving_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.topn_agg(\n\
        count integer, value INT8\n\
    ) (\n\
        sfunc = toolkit_experimental.topn_agg_bigint_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_bigint_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "topn_bigint_agg",
    requires = [
        topn_agg_bigint_trans,
        space_saving_bigint_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.topn_agg(\n\
        count integer, value TEXT\n\
    ) (\n\
        sfunc = toolkit_experimental.topn_agg_text_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_text_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "topn_text_agg",
    requires = [
        topn_agg_text_trans,
        space_saving_text_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.raw_topn_agg(\n\
        count integer, skew double precision, value AnyElement\n\
    ) (\n\
        sfunc = toolkit_experimental.topn_agg_with_skew_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "topn_agg_with_skew",
    requires = [
        topn_agg_with_skew_trans,
        space_saving_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.topn_agg(\n\
        count integer, skew double precision, value int8\n\
    ) (\n\
        sfunc = toolkit_experimental.topn_agg_with_skew_bigint_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_bigint_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "topn_agg_with_skew_bigint",
    requires = [
        topn_agg_with_skew_bigint_trans,
        space_saving_bigint_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.topn_agg(\n\
        count integer, skew double precision, value text\n\
    ) (\n\
        sfunc = toolkit_experimental.topn_agg_with_skew_text_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.space_saving_text_final,\n\
        combinefunc = toolkit_experimental.space_saving_combine,\n\
        serialfunc = toolkit_experimental.space_saving_serialize,\n\
        deserialfunc = toolkit_experimental.space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "topn_agg_with_skew_text",
    requires = [
        topn_agg_with_skew_text_trans,
        space_saving_text_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

#[pg_extern(
    immutable,
    parallel_safe,
    name = "into_values",
    schema = "toolkit_experimental"
)]
pub fn freq_iter(
    agg: SpaceSavingAggregate<'_>,
    ty: AnyElement,
) -> impl std::iter::Iterator<
    Item = (
        name!(value, AnyElement),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> + '_ {
    unsafe {
        if ty.oid() != agg.type_oid {
            pgx::error!("mischatched types")
        }
        let counts = agg.counts.slice().iter().zip(agg.overcounts.slice().iter());
        agg.datums.clone().into_iter().zip(counts).map_while(
            move |(value, (&count, &overcount))| {
                let total = agg.values_seen as f64;
                let value = AnyElement::from_datum(value, false, agg.type_oid).unwrap();
                let min_freq = (count - overcount) as f64 / total;
                let max_freq = count as f64 / total;
                Some((value, min_freq, max_freq))
            },
        )
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "into_values",
    schema = "toolkit_experimental"
)]
pub fn freq_bigint_iter(
    agg: SpaceSavingBigIntAggregate<'_>,
) -> impl std::iter::Iterator<
    Item = (
        name!(value, i64),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> + '_ {
    let counts = agg.counts.slice().iter().zip(agg.overcounts.slice().iter());
    agg.datums
        .clone()
        .into_iter()
        .zip(counts)
        .map_while(move |(value, (&count, &overcount))| {
            let total = agg.values_seen as f64;
            let min_freq = (count - overcount) as f64 / total;
            let max_freq = count as f64 / total;
            Some((value, min_freq, max_freq))
        })
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "into_values",
    schema = "toolkit_experimental"
)]
pub fn freq_text_iter(
    agg: SpaceSavingTextAggregate<'_>,
) -> impl std::iter::Iterator<
    Item = (
        name!(value, String),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> + '_ {
    let counts = agg.counts.slice().iter().zip(agg.overcounts.slice().iter());
    agg.datums
        .clone()
        .into_iter()
        .zip(counts)
        .map_while(move |(value, (&count, &overcount))| {
            let total = agg.values_seen as f64;
            let data = unsafe { varlena_to_string(value as *const pg_sys::varlena) };
            let min_freq = (count - overcount) as f64 / total;
            let max_freq = count as f64 / total;
            Some((data, min_freq, max_freq))
        })
}

fn validate_topn_for_topn_agg(
    n: i32,
    topn: u32,
    skew: f64,
    total_vals: u64,
    counts: impl Iterator<Item = u64>,
) {
    if topn == 0 {
        // Not a topn aggregate
        return;
    }

    // TODO: should we allow this if we have enough data?
    if n > topn as i32 {
        pgx::error!(
            "requested N ({}) exceeds creation parameter of topn aggregate ({})",
            n,
            topn
        )
    }

    // For topn_aggregates distributions we check that the top 'n' values satisfy the cumulative distribution
    // for our zeta curve.
    let needed_count = (zeta_le_n(skew, n as u64) * total_vals as f64).ceil() as u64;
    if counts.take(n as usize).sum::<u64>() < needed_count {
        pgx::error!("data is not skewed enough to find top {} parameters with a skew of {}, try reducing the skew factor", n , skew)
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn(
    agg: SpaceSavingAggregate<'_>,
    n: i32,
    ty: AnyElement,
) -> impl std::iter::Iterator<Item = AnyElement> + '_ {
    if ty.oid() != agg.type_oid {
        pgx::error!("mischatched types")
    }

    validate_topn_for_topn_agg(
        n,
        agg.topn as u32,
        agg.freq_param,
        agg.values_seen,
        agg.counts.iter(),
    );
    let min_freq = if agg.topn == 0 { agg.freq_param } else { 0. };

    let type_oid: u32 = agg.type_oid;
    TopNIterator::new(
        agg.datums.clone().into_iter(),
        agg.counts.clone().into_vec(),
        agg.values_seen as f64,
        n,
        min_freq,
    )
    // TODO Shouldn't failure to convert to AnyElement cause error, not early stop?
    .map_while(move |value| unsafe { AnyElement::from_datum(value, false, type_oid) })
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "topn",
    schema = "toolkit_experimental"
)]
pub fn default_topn(
    agg: SpaceSavingAggregate<'_>,
    ty: AnyElement,
) -> impl std::iter::Iterator<Item = AnyElement> + '_ {
    if agg.topn == 0 {
        pgx::error!("frequency aggregates require a N parameter to topn")
    }
    let n = agg.topn as i32;
    topn(agg, n, ty)
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "topn",
    schema = "toolkit_experimental"
)]
pub fn topn_bigint(
    agg: SpaceSavingBigIntAggregate<'_>,
    n: i32,
) -> impl std::iter::Iterator<Item = i64> + '_ {
    validate_topn_for_topn_agg(
        n,
        agg.topn,
        agg.freq_param,
        agg.values_seen,
        agg.counts.iter(),
    );
    let min_freq = if agg.topn == 0 { agg.freq_param } else { 0. };

    TopNIterator::new(
        agg.datums.clone().into_iter(),
        agg.counts.clone().into_vec(),
        agg.values_seen as f64,
        n,
        min_freq,
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "topn",
    schema = "toolkit_experimental"
)]
pub fn default_topn_bigint(
    agg: SpaceSavingBigIntAggregate<'_>,
) -> impl std::iter::Iterator<Item = i64> + '_ {
    if agg.topn == 0 {
        pgx::error!("frequency aggregates require a N parameter to topn")
    }
    let n = agg.topn as i32;
    topn_bigint(agg, n)
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "topn",
    schema = "toolkit_experimental"
)]
pub fn topn_text(
    agg: SpaceSavingTextAggregate<'_>,
    n: i32,
) -> impl std::iter::Iterator<Item = String> + '_ {
    validate_topn_for_topn_agg(
        n,
        agg.topn,
        agg.freq_param,
        agg.values_seen,
        agg.counts.iter(),
    );
    let min_freq = if agg.topn == 0 { agg.freq_param } else { 0. };

    TopNIterator::new(
        agg.datums.clone().into_iter(),
        agg.counts.clone().into_vec(),
        agg.values_seen as f64,
        n,
        min_freq,
    )
    .map(|value| unsafe { varlena_to_string(value as *const pg_sys::varlena) })
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "topn",
    schema = "toolkit_experimental"
)]
pub fn default_topn_text(
    agg: SpaceSavingTextAggregate<'_>,
) -> impl std::iter::Iterator<Item = String> + '_ {
    if agg.topn == 0 {
        pgx::error!("frequency aggregates require a N parameter to topn")
    }
    let n = agg.topn as i32;
    topn_text(agg, n)
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn max_frequency(agg: SpaceSavingAggregate<'_>, value: AnyElement) -> f64 {
    let value: PgAnyElement = value.into();
    match agg
        .datums
        .iter()
        .position(|datum| value == (datum, agg.type_oid).into())
    {
        Some(idx) => agg.counts.slice()[idx] as f64 / agg.values_seen as f64,
        None => 0.,
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn min_frequency(agg: SpaceSavingAggregate<'_>, value: AnyElement) -> f64 {
    let value: PgAnyElement = value.into();
    match agg
        .datums
        .iter()
        .position(|datum| value == (datum, agg.type_oid).into())
    {
        Some(idx) => {
            (agg.counts.slice()[idx] - agg.overcounts.slice()[idx]) as f64 / agg.values_seen as f64
        }
        None => 0.,
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "max_frequency",
    schema = "toolkit_experimental"
)]
pub fn max_bigint_frequency(agg: SpaceSavingBigIntAggregate<'_>, value: i64) -> f64 {
    match agg.datums.iter().position(|datum| value == datum) {
        Some(idx) => agg.counts.slice()[idx] as f64 / agg.values_seen as f64,
        None => 0.,
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "min_frequency",
    schema = "toolkit_experimental"
)]
pub fn min_bigint_frequency(agg: SpaceSavingBigIntAggregate<'_>, value: i64) -> f64 {
    match agg.datums.iter().position(|datum| value == datum) {
        Some(idx) => {
            (agg.counts.slice()[idx] - agg.overcounts.slice()[idx]) as f64 / agg.values_seen as f64
        }
        None => 0.,
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "max_frequency",
    schema = "toolkit_experimental"
)]
pub fn max_text_frequency(agg: SpaceSavingTextAggregate<'_>, value: text) -> f64 {
    let value: PgAnyElement = (value.0, pg_sys::TEXTOID).into();
    match agg
        .datums
        .iter()
        .position(|datum| value == (datum, pg_sys::TEXTOID).into())
    {
        Some(idx) => agg.counts.slice()[idx] as f64 / agg.values_seen as f64,
        None => 0.,
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "min_frequency",
    schema = "toolkit_experimental"
)]
pub fn min_text_frequency(agg: SpaceSavingTextAggregate<'_>, value: text) -> f64 {
    let value: PgAnyElement = (value.0, pg_sys::TEXTOID).into();
    match agg
        .datums
        .iter()
        .position(|datum| value == (datum, pg_sys::TEXTOID).into())
    {
        Some(idx) => {
            (agg.counts.slice()[idx] - agg.overcounts.slice()[idx]) as f64 / agg.values_seen as f64
        }
        None => 0.,
    }
}

struct TopNIterator<Input, InputIterator: std::iter::Iterator<Item = Input>> {
    datums_iter: InputIterator,
    counts_iter: std::vec::IntoIter<u64>,
    values_seen: f64,
    max_n: u32,
    min_freq: f64,
    i: u32,
}

impl<Input, InputIterator: std::iter::Iterator<Item = Input>> TopNIterator<Input, InputIterator> {
    fn new(
        datums_iter: InputIterator,
        counts: Vec<u64>,
        values_seen: f64,
        max_n: i32,
        min_freq: f64,
    ) -> Self {
        Self {
            datums_iter,
            counts_iter: counts.into_iter(),
            values_seen,
            max_n: max_n as u32,
            min_freq,
            i: 0,
        }
    }
}

impl<Input, InputIterator: std::iter::Iterator<Item = Input>> Iterator
    for TopNIterator<Input, InputIterator>
{
    type Item = Input;
    fn next(&mut self) -> Option<Self::Item> {
        match (self.datums_iter.next(), self.counts_iter.next()) {
            (Some(value), Some(count)) => {
                self.i += 1;
                if self.i > self.max_n || count as f64 / self.values_seen < self.min_freq {
                    None
                } else {
                    Some(value)
                }
            }
            _ => None,
        }
    }
}

unsafe fn varlena_to_string(vl: *const pg_sys::varlena) -> String {
    let bytes: &[u8] = varlena_to_byte_slice(vl);
    let s = std::str::from_utf8(bytes).expect("Error creating string from text data");
    s.into()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;
    use rand::distributions::{Distribution, Uniform};
    use rand::RngCore;
    use rand_distr::Zeta;

    #[pg_test]
    fn test_freq_aggregate() {
        Spi::execute(|client| {
            // using the search path trick for this test to make it easier to stabilize later on
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select("SET TIMEZONE to UTC", None, None);
            client.select(
                "CREATE TABLE test (data INTEGER, time TIMESTAMPTZ)",
                None,
                None,
            );

            for i in (0..100).rev() {
                client.select(&format!("INSERT INTO test SELECT i, '2020-1-1'::TIMESTAMPTZ + ('{} days, ' || i::TEXT || ' seconds')::INTERVAL FROM generate_series({}, 99, 1) i", 100 - i, i), None, None);
            }

            let test = client.select("SELECT freq_agg(0.015, s.data)::TEXT FROM (SELECT data FROM test ORDER BY time) s", None, None)
                .first()
                .get_one::<String>().unwrap();
            let expected = "(version:1,num_values:67,topn:0,values_seen:5050,freq_param:0.015,counts:[100,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67],overcounts:[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66],datums:[99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66])";
            assert_eq!(test, expected);
        });
    }

    #[pg_test]
    fn test_topn_aggregate() {
        Spi::execute(|client| {
            // using the search path trick for this test to make it easier to stabilize later on
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select("SET TIMEZONE to UTC", None, None);
            client.select(
                "CREATE TABLE test (data INTEGER, time TIMESTAMPTZ)",
                None,
                None,
            );

            for i in (0..200).rev() {
                client.select(&format!("INSERT INTO test SELECT i, '2020-1-1'::TIMESTAMPTZ + ('{} days, ' || i::TEXT || ' seconds')::INTERVAL FROM generate_series({}, 199, 1) i", 200 - i, i), None, None);
            }

            let test = client.select("SELECT topn_agg(10, s.data)::TEXT FROM (SELECT data FROM test ORDER BY time) s", None, None)
                .first()
                .get_one::<String>().unwrap();
            let expected = "(version:1,num_values:110,topn:10,values_seen:20100,freq_param:1.1,counts:[200,199,198,197,196,195,194,193,192,191,190,189,188,187,186,185,184,183,182,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181,181],overcounts:[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180,180],datums:[199,198,197,196,195,194,193,192,191,190,189,188,187,186,185,184,183,182,181,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180])";
            assert_eq!(test, expected);
        });
    }

    #[pg_test]
    fn explicit_aggregate_test() {
        let freq = 0.0625;
        let fcinfo = std::ptr::null_mut(); // dummy value, will use default collation
        let mut state = None.into();

        for i in 11..=20 {
            for j in i..=20 {
                let value =
                    unsafe { AnyElement::from_datum(j as pg_sys::Datum, false, pg_sys::INT4OID) };
                state = super::freq_agg_trans(state, freq, value, fcinfo).unwrap();
            }
        }

        let first = super::space_saving_serialize(state);

        let bytes = unsafe {
            std::slice::from_raw_parts(
                vardata_any(first.0 as *const pg_sys::varlena) as *const u8,
                varsize_any_exhdr(first.0 as *const pg_sys::varlena),
            )
        };
        let expected = [
            1, 1, // versions
            15, 0, 0, 0, 0, 0, 0, 0, // size hint for sequence
            55, 0, 0, 0, 0, 0, 0, 0, // elements seen
            0, 0, 0, 0, 0, 0, 176, 63, // frequency (f64 encoding of 0.0625)
            17, 0, 0, 0, // elements tracked
            0, 0, 0, 0, // topn
            7, 0, 0, 0, 1, 1, 10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111,
            103, 11, 0, 0, 0, 0, 0, 0, 0, 101, 110, 95, 85, 83, 46, 85, 84, 70, 45,
            56, // INT4 hasher
            2, 0, 0, 0, 0, 0, 0, 0, 50, 48, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 20, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 57, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 19, count 9, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 56, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 18, count 8, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 55, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 17, count 7, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 54, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 16, count 6, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 53, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 15, count 5, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 52, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 14, count 4, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 51, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 13, count 3, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 50, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 12, count 2, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 49, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 11, count 1, overcount 0
        ];
        // encoding of hasher can vary on platform and across postgres version (even in length), ignore it and check the other fields
        let prefix_len = 8 * 4 + 2;
        let suffix_len = (8 + 2 + 16) * 10;
        assert_eq!(bytes[..prefix_len], expected[..prefix_len]);
        assert_eq!(
            bytes[bytes.len() - suffix_len..],
            expected[expected.len() - suffix_len..]
        );

        state = None.into();

        for i in (1..=10).rev() {
            // reverse here introduces less error in the aggregate
            for j in i..=20 {
                let value =
                    unsafe { AnyElement::from_datum(j as pg_sys::Datum, false, pg_sys::INT4OID) };
                state = super::freq_agg_trans(state, freq, value, fcinfo).unwrap();
            }
        }

        let second = super::space_saving_serialize(state);

        let bytes = unsafe {
            std::slice::from_raw_parts(
                vardata_any(second.0 as *const pg_sys::varlena) as *const u8,
                varsize_any_exhdr(second.0 as *const pg_sys::varlena),
            )
        };
        let expected = [
            1, 1, // versions
            22, 0, 0, 0, 0, 0, 0, 0, // size hint for sequence
            155, 0, 0, 0, 0, 0, 0, 0, // elements seen
            0, 0, 0, 0, 0, 0, 176, 63, // frequency (f64 encoding of 0.0625)
            17, 0, 0, 0, // elements tracked
            0, 0, 0, 0, // topn
            7, 0, 0, 0, 1, 1, 10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111,
            103, 11, 0, 0, 0, 0, 0, 0, 0, 101, 110, 95, 85, 83, 46, 85, 84, 70, 45,
            56, // INT4 hasher
            2, 0, 0, 0, 0, 0, 0, 0, 49, 48, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 10, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 49, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 11, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 50, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 12, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 51, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 13, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 52, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 14, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 53, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 15, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 54, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 16, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 55, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 17, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 56, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 18, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 57, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 19, count 10, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 50, 48, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 20, count 10, overcount 0
            1, 0, 0, 0, 0, 0, 0, 0, 57, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 9, count 9, overcount 0
            1, 0, 0, 0, 0, 0, 0, 0, 56, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 8, count 8, overcount 0
            1, 0, 0, 0, 0, 0, 0, 0, 52, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 4, count 7, overcount 6
            1, 0, 0, 0, 0, 0, 0, 0, 53, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 5, count 7, overcount 6
            1, 0, 0, 0, 0, 0, 0, 0, 54, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 6, count 7, overcount 6
            1, 0, 0, 0, 0, 0, 0, 0, 55, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 7, count 7, overcount 6
        ];
        // encoding of hasher can vary on platform and across postgres version (even in length), ignore it and check the other fields
        let suffix_len = (8 + 2 + 16) * 11 + (8 + 1 + 16) * 6;
        assert_eq!(bytes[..prefix_len], expected[..prefix_len]);
        assert_eq!(
            bytes[bytes.len() - suffix_len..],
            expected[expected.len() - suffix_len..]
        );

        let combined = super::space_saving_serialize(
            super::space_saving_combine(
                super::space_saving_deserialize(first, None.into()).unwrap(),
                super::space_saving_deserialize(second, None.into()).unwrap(),
                fcinfo,
            )
            .unwrap(),
        );

        let bytes = unsafe {
            std::slice::from_raw_parts(
                vardata_any(combined.0 as *const pg_sys::varlena) as *const u8,
                varsize_any_exhdr(combined.0 as *const pg_sys::varlena),
            )
        };
        let expected = [
            1, 1, // versions
            22, 0, 0, 0, 0, 0, 0, 0, // size hint for sequence
            210, 0, 0, 0, 0, 0, 0, 0, // elements seen
            0, 0, 0, 0, 0, 0, 176, 63, // frequency (f64 encoding of 0.0625)
            17, 0, 0, 0, // elements tracked
            0, 0, 0, 0, // topn
            7, 0, 0, 0, 1, 1, 10, 0, 0, 0, 0, 0, 0, 0, 112, 103, 95, 99, 97, 116, 97, 108, 111,
            103, 11, 0, 0, 0, 0, 0, 0, 0, 101, 110, 95, 85, 83, 46, 85, 84, 70, 45,
            56, // INT4 hasher
            2, 0, 0, 0, 0, 0, 0, 0, 50, 48, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 20, count 20, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 57, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 19, count 19, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 56, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 18, count 18, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 55, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 17, count 17, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 54, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 16, count 16, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 53, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 15, count 15, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 52, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 14, count 14, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 51, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 13, count 13, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 50, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 12, count 12, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 49, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 11, count 11, overcount 0
            2, 0, 0, 0, 0, 0, 0, 0, 49, 48, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 10, count 10, overcount 0
            1, 0, 0, 0, 0, 0, 0, 0, 57, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 9, count 9, overcount 0
            1, 0, 0, 0, 0, 0, 0, 0, 56, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // string 8, count 8, overcount 0
            1, 0, 0, 0, 0, 0, 0, 0, 52, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 4, count 7, overcount 6
            1, 0, 0, 0, 0, 0, 0, 0, 54, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 6, count 7, overcount 6
            1, 0, 0, 0, 0, 0, 0, 0, 53, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 5, count 7, overcount 6
            1, 0, 0, 0, 0, 0, 0, 0, 55, 7, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, // string 7, count 7, overcount 6
        ];
        // encoding of hasher can vary on platform and across postgres version (even in length), ignore it and check the other fields
        let suffix_len = (8 + 2 + 16) * 11 + (8 + 1 + 16) * 6;
        assert_eq!(bytes[..prefix_len], expected[..prefix_len]);
        assert_eq!(
            bytes[bytes.len() - suffix_len..],
            expected[expected.len() - suffix_len..]
        );
    }

    // Setup environment and create table 'test' with some aggregates in table 'aggs'
    fn setup_with_test_table(client: &SpiClient) {
        // using the search path trick for this test to make it easier to stabilize later on
        let sp = client
            .select(
                "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                None,
                None,
            )
            .first()
            .get_one::<String>()
            .unwrap();
        client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

        client.select("SET TIMEZONE to UTC", None, None);
        client.select(
            "CREATE TABLE test (data INTEGER, time TIMESTAMPTZ)",
            None,
            None,
        );

        for i in (0..20).rev() {
            client.select(&format!("INSERT INTO test SELECT i, '2020-1-1'::TIMESTAMPTZ + ('{} days, ' || i::TEXT || ' seconds')::INTERVAL FROM generate_series({}, 19, 1) i", 10 - i, i), None, None);
        }

        client.select(
            "CREATE TABLE aggs (name TEXT, agg SPACESAVINGBIGINTAGGREGATE)",
            None,
            None,
        );
        client.select("INSERT INTO aggs SELECT 'topn_default', topn_agg(5, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None);
        client.select("INSERT INTO aggs SELECT 'topn_1.5', topn_agg(5, 1.5, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None);
        client.select("INSERT INTO aggs SELECT 'topn_2', topn_agg(5, 2, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None);
        client.select("INSERT INTO aggs SELECT 'freq_8', freq_agg(0.08, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None);
        client.select("INSERT INTO aggs SELECT 'freq_5', freq_agg(0.05, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None);
        client.select("INSERT INTO aggs SELECT 'freq_2', freq_agg(0.02, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None);
    }

    // API tests
    #[pg_test]
    fn test_topn() {
        Spi::execute(|client| {
            setup_with_test_table(&client);

            // simple tests
            let rows = client
                .select(
                    "SELECT topn(agg) FROM aggs WHERE name = 'topn_default'",
                    None,
                    None,
                )
                .count();
            assert_eq!(rows, 5);
            let rows = client
                .select(
                    "SELECT topn(agg, 5) FROM aggs WHERE name = 'freq_5'",
                    None,
                    None,
                )
                .count();
            assert_eq!(rows, 5);

            // can limit below topn_agg value
            let rows = client
                .select(
                    "SELECT topn(agg, 3) FROM aggs WHERE name = 'topn_default'",
                    None,
                    None,
                )
                .count();
            assert_eq!(rows, 3);

            // only 4 rows with freq >= 0.08
            let rows = client
                .select(
                    "SELECT topn(agg, 5) FROM aggs WHERE name = 'freq_8'",
                    None,
                    None,
                )
                .count();
            assert_eq!(rows, 4);
        });
    }

    // TODO:  Tests that expect failures will not currently run correctly in CI.  Uncomment the following tests once this is fixed.
    // #[pg_test(error = "data is not skewed enough to find top 5 parameters with a skew of 1.5, try reducing the skew factor")]
    // fn topn_on_underskewed_topn_agg() {
    //     Spi::execute(|client| {
    //         setup_with_test_table(&client);
    //         client.select("SELECT topn(agg, 0::int) FROM aggs WHERE name = 'topn_1.5'", None, None).count();
    //     });
    // }

    // #[pg_test(error = "requested N (8) exceeds creation parameter of topn aggregate (5)")]
    // fn topn_high_n_on_topn_agg() {
    //     Spi::execute(|client| {
    //         setup_with_test_table(&client);
    //         client.select("SELECT topn(agg, 8, 0::int) FROM aggs WHERE name = 'topn_default'", None, None).count();
    //     });
    // }

    // #[pg_test(error = "frequency aggregates require a N parameter to topn")]
    // fn topn_requires_n_for_freq_agg() {
    //     Spi::execute(|client| {
    //         setup_with_test_table(&client);
    //         client.select("SELECT topn(agg, 0::int) FROM aggs WHERE name = 'freq_2'", None, None).count();
    //     });
    // }

    #[pg_test]
    fn test_into_values() {
        Spi::execute(|client| {
            setup_with_test_table(&client);

            let rows = client
                .select(
                    "SELECT into_values(agg) FROM aggs WHERE name = 'freq_8'",
                    None,
                    None,
                )
                .count();
            assert_eq!(rows, 13);
            let rows = client
                .select(
                    "SELECT into_values(agg) FROM aggs WHERE name = 'freq_5'",
                    None,
                    None,
                )
                .count();
            assert_eq!(rows, 20);
            let rows = client
                .select(
                    "SELECT into_values(agg) FROM aggs WHERE name = 'freq_2'",
                    None,
                    None,
                )
                .count();
            assert_eq!(rows, 20);
        });
    }

    #[pg_test]
    fn test_frequency_getters() {
        Spi::execute(|client| {
            setup_with_test_table(&client);

            // simple tests
            let (min, max) = client.select("SELECT min_frequency(agg, 3), max_frequency(agg, 3) FROM aggs WHERE name = 'freq_2'", None, None)
                .first()
                .get_two::<f64,f64>();
            assert_eq!(min.unwrap(), 0.01904761904761905);
            assert_eq!(max.unwrap(), 0.01904761904761905);

            let (min, max) = client.select("SELECT min_frequency(agg, 11), max_frequency(agg, 11) FROM aggs WHERE name = 'topn_default'", None, None)
                .first()
                .get_two::<f64,f64>();
            assert_eq!(min.unwrap(), 0.05714285714285714);
            assert_eq!(max.unwrap(), 0.05714285714285714);

            // missing value
            let (min, max) = client.select("SELECT min_frequency(agg, 3), max_frequency(agg, 3) FROM aggs WHERE name = 'freq_8'", None, None)
                .first()
                .get_two::<f64,f64>();
            assert_eq!(min.unwrap(), 0.);
            assert_eq!(max.unwrap(), 0.);

            let (min, max) = client.select("SELECT min_frequency(agg, 20), max_frequency(agg, 20) FROM aggs WHERE name = 'topn_2'", None, None)
                .first()
                .get_two::<f64,f64>();
            assert_eq!(min.unwrap(), 0.);
            assert_eq!(max.unwrap(), 0.);

            // noisy value
            let (min, max) = client.select("SELECT min_frequency(agg, 8), max_frequency(agg, 8) FROM aggs WHERE name = 'topn_1.5'", None, None)
                .first()
                .get_two::<f64,f64>();
            assert_eq!(min.unwrap(), 0.004761904761904762);
            assert_eq!(max.unwrap(), 0.05238095238095238);
        });
    }

    #[pg_test]
    fn test_freq_agg_invariant() {
        // The frequency agg invariant is that any element with frequency >= f will appear in the freq_agg(f)

        // This test will randomly generate 200 values in the uniform range [0, 99] and check to see any value
        // that shows up at least 3 times appears in a frequency aggregate created with freq = 0.015
        let rand100 = Uniform::new_inclusive(0, 99);
        let mut rng = rand::thread_rng();

        let mut counts = [0; 100];

        let mut state = None.into();
        let freq = 0.015;
        let fcinfo = std::ptr::null_mut(); // dummy value, will use default collation

        for _ in 0..200 {
            let v = rand100.sample(&mut rng);
            let value =
                unsafe { AnyElement::from_datum(v as pg_sys::Datum, false, pg_sys::INT4OID) };
            state = super::freq_agg_trans(state, freq, value, fcinfo).unwrap();
            counts[v] += 1;
        }

        let state = space_saving_final(state, fcinfo).unwrap();
        let vals: std::collections::HashSet<usize> = state.datums.iter().collect();

        for (val, &count) in counts.iter().enumerate() {
            if count >= 3 {
                assert!(vals.contains(&val));
            }
        }
    }

    #[pg_test]
    fn test_topn_agg_invariant() {
        // The ton agg invariant is that we'll be able to track the top n values for any data
        // with a distribution at least as skewed as a zeta distribution

        // To test this we will generate a topn aggregate with a random skew (1.01 - 2.0) and
        // n (5-10).  We then generate a random sample with skew 5% greater than our aggregate
        // (this should be enough to keep the sample above the target even with bad luck), and
        // verify that we correctly identify the top n values.
        let mut rng = rand::thread_rng();

        let n = rng.next_u64() % 6 + 5;
        let skew = (rng.next_u64() % 100) as f64 / 100. + 1.01;

        let zeta = Zeta::new(skew * 1.05).unwrap();

        let mut counts = [0; 100];

        let mut state = None.into();
        let fcinfo = std::ptr::null_mut(); // dummy value, will use default collation

        for _ in 0..100000 {
            let v = zeta.sample(&mut rng).floor() as usize;
            if v == usize::MAX {
                continue; // These tail values can start to add up at low skew values
            }
            let value =
                unsafe { AnyElement::from_datum(v as pg_sys::Datum, false, pg_sys::INT4OID) };
            state = super::topn_agg_with_skew_trans(state, n as i32, skew, value, fcinfo).unwrap();
            if v < 100 {
                // anything greater than 100 will not be in the top values
                counts[v] += 1;
            }
        }

        let state = space_saving_final(state, fcinfo).unwrap();
        let value = unsafe { AnyElement::from_datum(0, false, pg_sys::INT4OID) };
        let t: Vec<AnyElement> = default_topn(state, value.unwrap()).collect();
        let agg_topn: Vec<usize> = t.iter().map(|x| x.datum()).collect();

        let mut temp: Vec<(usize, &usize)> = counts.iter().enumerate().collect();
        temp.sort_by(|(_, cnt1), (_, cnt2)| cnt2.cmp(cnt1)); // descending order by count
        let top_vals: Vec<usize> = temp.into_iter().map(|(val, _)| val).collect();

        for i in 0..n as usize {
            assert_eq!(agg_topn[i], top_vals[i]);
        }
    }
}
