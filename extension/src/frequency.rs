//! Based on the paper: https://cs.ucsb.edu/sites/default/files/documents/2005-23.pdf

use std::fmt;

use pgrx::{
    iter::{SetOfIterator, TableIterator},
    *,
};

use pg_sys::{Datum, Oid};

use serde::{
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};

use crate::{
    accessors::{
        AccessorIntoValues, AccessorMaxFrequencyInt, AccessorMinFrequencyInt, AccessorTopNCount,
        AccessorTopn,
    },
    aggregate_utils::{get_collation_or_default, in_aggregate_context},
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
    freq_param: f64, // This is the minimum frequency for a freq_agg or the skew for a mcv_agg
    topn: u32,       // 0 for freq_agg, creation parameter for mcv_agg
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

    fn mcv_agg_from_type_id(
        skew: f64,
        nval: u32,
        typ: pg_sys::Oid,
        collation: Option<Oid>,
    ) -> Self {
        if nval == 0 {
            pgrx::error!("mcv aggregate requires an n value > 0")
        }
        if skew <= 1.0 {
            pgrx::error!("mcv aggregate requires a skew factor > 1.0")
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

    fn ingest_aggregate_data(
        &mut self,
        val_count: u64,
        values: &DatumStore,
        counts: &[u64],
        overcounts: &[u64],
    ) {
        assert_eq!(self.total_vals, 0); // This should only be called on an empty aggregate
        self.total_vals = val_count;

        for (idx, datum) in values.iter().enumerate() {
            self.entries.push(SpaceSavingEntry {
                value: unsafe { deep_copy_datum(datum, self.indices.typoid()) },
                count: counts[idx],
                overcount: overcounts[idx],
            });
            self.indices
                .insert((self.entries[idx].value, self.type_oid()).into(), idx);
        }
    }

    fn ingest_aggregate_ints(
        &mut self,
        val_count: u64,
        values: &[i64],
        counts: &[u64],
        overcounts: &[u64],
    ) {
        assert_eq!(self.total_vals, 0); // This should only be called on an empty aggregate
        assert_eq!(self.type_oid(), pg_sys::INT8OID);
        self.total_vals = val_count;

        for (idx, val) in values.iter().enumerate() {
            self.entries.push(SpaceSavingEntry {
                value: Datum::from(*val),
                count: counts[idx],
                overcount: overcounts[idx],
            });
            self.indices
                .insert((self.entries[idx].value, self.type_oid()).into(), idx);
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
        let mut entries: Vec<SpaceSavingEntry> = temp.0.into_values().collect();
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
                type_oid: trans.type_oid().into(),
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

impl<'input> From<(&SpaceSavingAggregate<'input>, &pg_sys::FunctionCallInfo)>
    for SpaceSavingTransState
{
    fn from(data_in: (&SpaceSavingAggregate<'input>, &pg_sys::FunctionCallInfo)) -> Self {
        let (agg, fcinfo) = data_in;
        let collation = get_collation_or_default(*fcinfo);
        let mut trans = if agg.topn == 0 {
            SpaceSavingTransState::freq_agg_from_type_id(
                agg.freq_param,
                pg_sys::Oid::from(agg.type_oid),
                collation,
            )
        } else {
            SpaceSavingTransState::mcv_agg_from_type_id(
                agg.freq_param,
                agg.topn as u32,
                pg_sys::Oid::from(agg.type_oid),
                collation,
            )
        };
        trans.ingest_aggregate_data(
            agg.values_seen,
            &agg.datums,
            agg.counts.as_slice(),
            agg.overcounts.as_slice(),
        );
        trans
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
            values.push(entry.value.value() as i64);
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

impl<'input>
    From<(
        &SpaceSavingBigIntAggregate<'input>,
        &pg_sys::FunctionCallInfo,
    )> for SpaceSavingTransState
{
    fn from(
        data_in: (
            &SpaceSavingBigIntAggregate<'input>,
            &pg_sys::FunctionCallInfo,
        ),
    ) -> Self {
        let (agg, fcinfo) = data_in;
        let collation = get_collation_or_default(*fcinfo);
        let mut trans = if agg.topn == 0 {
            SpaceSavingTransState::freq_agg_from_type_id(agg.freq_param, pg_sys::INT8OID, collation)
        } else {
            SpaceSavingTransState::mcv_agg_from_type_id(
                agg.freq_param,
                agg.topn,
                pg_sys::INT8OID,
                collation,
            )
        };
        trans.ingest_aggregate_ints(
            agg.values_seen,
            agg.datums.as_slice(),
            agg.counts.as_slice(),
            agg.overcounts.as_slice(),
        );
        trans
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

impl<'input> From<(&SpaceSavingTextAggregate<'input>, &pg_sys::FunctionCallInfo)>
    for SpaceSavingTransState
{
    fn from(data_in: (&SpaceSavingTextAggregate<'input>, &pg_sys::FunctionCallInfo)) -> Self {
        let (agg, fcinfo) = data_in;
        let collation = get_collation_or_default(*fcinfo);
        let mut trans = if agg.topn == 0 {
            SpaceSavingTransState::freq_agg_from_type_id(agg.freq_param, pg_sys::TEXTOID, collation)
        } else {
            SpaceSavingTransState::mcv_agg_from_type_id(
                agg.freq_param,
                agg.topn,
                pg_sys::TEXTOID,
                collation,
            )
        };
        trans.ingest_aggregate_data(
            agg.values_seen,
            &agg.datums,
            agg.counts.as_slice(),
            agg.overcounts.as_slice(),
        );
        trans
    }
}

ron_inout_funcs!(SpaceSavingTextAggregate);

#[pg_extern(immutable, parallel_safe)]
pub fn mcv_agg_trans(
    state: Internal,
    n: i32,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    mcv_agg_with_skew_trans(state, n, DEFAULT_ZETA_SKEW, value, fcinfo)
}

#[pg_extern(immutable, parallel_safe)]
pub fn mcv_agg_bigint_trans(
    state: Internal,
    n: i32,
    value: Option<i64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    mcv_agg_with_skew_bigint_trans(state, n, DEFAULT_ZETA_SKEW, value, fcinfo)
}

#[pg_extern(immutable, parallel_safe)]
pub fn mcv_agg_text_trans(
    state: Internal,
    n: i32,
    value: Option<crate::raw::text>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    mcv_agg_with_skew_text_trans(state, n, DEFAULT_ZETA_SKEW, value, fcinfo)
}

#[pg_extern(immutable, parallel_safe)]
pub fn mcv_agg_with_skew_trans(
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
            SpaceSavingTransState::mcv_agg_from_type_id(skew, n as u32, typ, collation)
        },
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn mcv_agg_with_skew_bigint_trans(
    state: Internal,
    n: i32,
    skew: f64,
    value: Option<i64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let value = match value {
        None => None,
        Some(val) => unsafe {
            AnyElement::from_polymorphic_datum(pg_sys::Datum::from(val), false, pg_sys::INT8OID)
        },
    };

    space_saving_trans(
        unsafe { state.to_inner() },
        value,
        fcinfo,
        |typ, collation| {
            SpaceSavingTransState::mcv_agg_from_type_id(skew, n as u32, typ, collation)
        },
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn mcv_agg_with_skew_text_trans(
    state: Internal,
    n: i32,
    skew: f64,
    value: Option<crate::raw::text>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let txt = value.map(|v| unsafe { pg_sys::pg_detoast_datum_copy(v.0.cast_mut_ptr()) });
    let value = match txt {
        None => None,
        Some(val) => unsafe {
            AnyElement::from_polymorphic_datum(pg_sys::Datum::from(val), false, pg_sys::TEXTOID)
        },
    };

    space_saving_trans(
        unsafe { state.to_inner() },
        value,
        fcinfo,
        |typ, collation| {
            SpaceSavingTransState::mcv_agg_from_type_id(skew, n as u32, typ, collation)
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
        pgrx::error!("frequency aggregate requires a frequency in the range (0.0, 1.0)")
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
            AnyElement::from_polymorphic_datum(pg_sys::Datum::from(val), false, pg_sys::INT8OID)
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
    let txt = value.map(|v| unsafe { pg_sys::pg_detoast_datum_copy(v.0.cast_mut_ptr()) });
    let value = match txt {
        None => None,
        Some(val) => unsafe {
            AnyElement::from_polymorphic_datum(pg_sys::Datum::from(val), false, pg_sys::TEXTOID)
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
                    let collation = get_collation_or_default(fcinfo);
                    make_trans_state(typ, collation).into()
                }
                Some(state) => state,
            };

            state.add(value.into());
            Some(state)
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn rollup_agg_trans<'input>(
    state: Internal,
    value: Option<SpaceSavingAggregate<'input>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let value = match value {
        None => return Some(state),
        Some(v) => v,
    };
    rollup_agg_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}

pub fn rollup_agg_trans_inner(
    state: Option<Inner<SpaceSavingTransState>>,
    value: SpaceSavingAggregate,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<SpaceSavingTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let trans = (&value, &fcinfo).into();
            if let Some(state) = state {
                Some(SpaceSavingTransState::combine(&state, &trans).into())
            } else {
                Some(trans.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn rollup_agg_bigint_trans<'input>(
    state: Internal,
    value: Option<SpaceSavingBigIntAggregate<'input>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let value = match value {
        None => return Some(state),
        Some(v) => v,
    };
    rollup_agg_bigint_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}

pub fn rollup_agg_bigint_trans_inner(
    state: Option<Inner<SpaceSavingTransState>>,
    value: SpaceSavingBigIntAggregate,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<SpaceSavingTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let trans = (&value, &fcinfo).into();
            if let Some(state) = state {
                Some(SpaceSavingTransState::combine(&state, &trans).into())
            } else {
                Some(trans.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn rollup_agg_text_trans<'input>(
    state: Internal,
    value: Option<SpaceSavingTextAggregate<'input>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let value = match value {
        None => return Some(state),
        Some(v) => v,
    };
    rollup_agg_text_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}

pub fn rollup_agg_text_trans_inner(
    state: Option<Inner<SpaceSavingTransState>>,
    value: SpaceSavingTextAggregate,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<SpaceSavingTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let trans = (&value, &fcinfo).into();
            if let Some(state) = state {
                Some(SpaceSavingTransState::combine(&state, &trans).into())
            } else {
                Some(trans.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
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
            (Some(a), Some(b)) => Some(SpaceSavingTransState::combine(&a, &b).into()),
            (Some(a), None) => Some(a.clone().into()),
            (None, Some(b)) => Some(b.clone().into()),
            (None, None) => None,
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn space_saving_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<SpaceSavingAggregate<'static>> {
    let state: Option<&SpaceSavingTransState> = unsafe { state.get() };
    state.map(SpaceSavingAggregate::from)
}

#[pg_extern(immutable, parallel_safe)]
fn space_saving_bigint_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<SpaceSavingBigIntAggregate<'static>> {
    let state: Option<&SpaceSavingTransState> = unsafe { state.get() };
    state.map(SpaceSavingBigIntAggregate::from)
}

#[pg_extern(immutable, parallel_safe)]
fn space_saving_text_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<SpaceSavingTextAggregate<'static>> {
    let state: Option<&SpaceSavingTransState> = unsafe { state.get() };
    state.map(SpaceSavingTextAggregate::from)
}

#[pg_extern(immutable, parallel_safe)]
fn space_saving_serialize(state: Internal) -> bytea {
    let state: Inner<SpaceSavingTransState> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
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
        finalfunc = space_saving_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
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
        finalfunc = space_saving_bigint_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
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
        finalfunc = space_saving_text_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
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
    CREATE AGGREGATE raw_mcv_agg(\n\
        count integer, value AnyElement\n\
    ) (\n\
        sfunc = mcv_agg_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "mcv_agg",
    requires = [
        mcv_agg_trans,
        space_saving_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE mcv_agg(\n\
        count integer, value INT8\n\
    ) (\n\
        sfunc = mcv_agg_bigint_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_bigint_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "mcv_bigint_agg",
    requires = [
        mcv_agg_bigint_trans,
        space_saving_bigint_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE mcv_agg(\n\
        count integer, value TEXT\n\
    ) (\n\
        sfunc = mcv_agg_text_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_text_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "mcv_text_agg",
    requires = [
        mcv_agg_text_trans,
        space_saving_text_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE raw_mcv_agg(\n\
        count integer, skew double precision, value AnyElement\n\
    ) (\n\
        sfunc = mcv_agg_with_skew_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "mcv_agg_with_skew",
    requires = [
        mcv_agg_with_skew_trans,
        space_saving_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE mcv_agg(\n\
        count integer, skew double precision, value int8\n\
    ) (\n\
        sfunc = mcv_agg_with_skew_bigint_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_bigint_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "mcv_agg_with_skew_bigint",
    requires = [
        mcv_agg_with_skew_bigint_trans,
        space_saving_bigint_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE mcv_agg(\n\
        count integer, skew double precision, value text\n\
    ) (\n\
        sfunc = mcv_agg_with_skew_text_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_text_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "mcv_agg_with_skew_text",
    requires = [
        mcv_agg_with_skew_text_trans,
        space_saving_text_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        agg SpaceSavingAggregate\n\
    ) (\n\
        sfunc = rollup_agg_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "freq_agg_rollup",
    requires = [
        rollup_agg_trans,
        space_saving_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        agg SpaceSavingBigIntAggregate\n\
    ) (\n\
        sfunc = rollup_agg_bigint_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_bigint_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "freq_agg_bigint_rollup",
    requires = [
        rollup_agg_bigint_trans,
        space_saving_bigint_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        agg SpaceSavingTextAggregate\n\
    ) (\n\
        sfunc = rollup_agg_text_trans,\n\
        stype = internal,\n\
        finalfunc = space_saving_text_final,\n\
        combinefunc = space_saving_combine,\n\
        serialfunc = space_saving_serialize,\n\
        deserialfunc = space_saving_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "freq_agg_text_rollup",
    requires = [
        rollup_agg_text_trans,
        space_saving_text_final,
        space_saving_combine,
        space_saving_serialize,
        space_saving_deserialize
    ],
);

#[pg_extern(immutable, parallel_safe, name = "into_values")]
pub fn freq_iter<'a>(
    agg: SpaceSavingAggregate<'a>,
    ty: AnyElement,
) -> TableIterator<
    'a,
    (
        name!(value, AnyElement),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> {
    unsafe {
        if ty.oid().as_u32() != agg.type_oid {
            pgrx::error!("mischatched types")
        }
        let counts = agg.counts.slice().iter().zip(agg.overcounts.slice().iter());
        TableIterator::new(agg.datums.clone().into_iter().zip(counts).map_while(
            move |(value, (&count, &overcount))| {
                let total = agg.values_seen as f64;
                let value = AnyElement::from_polymorphic_datum(
                    value,
                    false,
                    pg_sys::Oid::from(agg.type_oid),
                )
                .unwrap();
                let min_freq = (count - overcount) as f64 / total;
                let max_freq = count as f64 / total;
                Some((value, min_freq, max_freq))
            },
        ))
    }
}

#[pg_extern(immutable, parallel_safe, name = "into_values")]
pub fn freq_bigint_iter<'a>(
    agg: SpaceSavingBigIntAggregate<'a>,
) -> TableIterator<
    'a,
    (
        name!(value, i64),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> {
    let counts = agg.counts.slice().iter().zip(agg.overcounts.slice().iter());
    TableIterator::new(agg.datums.clone().into_iter().zip(counts).map_while(
        move |(value, (&count, &overcount))| {
            let total = agg.values_seen as f64;
            let min_freq = (count - overcount) as f64 / total;
            let max_freq = count as f64 / total;
            Some((value, min_freq, max_freq))
        },
    ))
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_freq_bigint_iter<'a>(
    agg: SpaceSavingBigIntAggregate<'a>,
    _accessor: AccessorIntoValues<'static>,
) -> TableIterator<
    'a,
    (
        name!(value, i64),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> {
    freq_bigint_iter(agg)
}

#[pg_extern(immutable, parallel_safe, name = "into_values")]
pub fn freq_text_iter<'a>(
    agg: SpaceSavingTextAggregate<'a>,
) -> TableIterator<
    'a,
    (
        name!(value, String),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> {
    let counts = agg.counts.slice().iter().zip(agg.overcounts.slice().iter());
    TableIterator::new(agg.datums.clone().into_iter().zip(counts).map_while(
        move |(value, (&count, &overcount))| {
            let total = agg.values_seen as f64;
            let data = unsafe { varlena_to_string(value.cast_mut_ptr()) };
            let min_freq = (count - overcount) as f64 / total;
            let max_freq = count as f64 / total;
            Some((data, min_freq, max_freq))
        },
    ))
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_freq_text_iter<'a>(
    agg: SpaceSavingTextAggregate<'a>,
    _accessor: AccessorIntoValues<'static>,
) -> TableIterator<
    'a,
    (
        name!(value, String),
        name!(min_freq, f64),
        name!(max_freq, f64),
    ),
> {
    freq_text_iter(agg)
}

fn validate_topn_for_mcv_agg(
    n: i32,
    topn: u32,
    skew: f64,
    total_vals: u64,
    counts: impl Iterator<Item = u64>,
) {
    if topn == 0 {
        // Not a mcv aggregate
        return;
    }

    // TODO: should we allow this if we have enough data?
    if n > topn as i32 {
        pgrx::error!(
            "requested N ({}) exceeds creation parameter of mcv aggregate ({})",
            n,
            topn
        )
    }

    // For mcv_aggregates distributions we check that the top 'n' values satisfy the cumulative distribution
    // for our zeta curve.
    let needed_count = (zeta_le_n(skew, n as u64) * total_vals as f64).ceil() as u64;
    if counts.take(n as usize).sum::<u64>() < needed_count {
        pgrx::error!("data is not skewed enough to find top {} parameters with a skew of {}, try reducing the skew factor", n , skew)
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn topn(
    agg: SpaceSavingAggregate<'_>,
    n: i32,
    ty: Option<AnyElement>,
) -> SetOfIterator<AnyElement> {
    // If called with a NULL, assume type matches
    if ty.is_some() && ty.unwrap().oid().as_u32() != agg.type_oid {
        pgrx::error!("mischatched types")
    }

    validate_topn_for_mcv_agg(
        n,
        agg.topn as u32,
        agg.freq_param,
        agg.values_seen,
        agg.counts.iter(),
    );
    let min_freq = if agg.topn == 0 { agg.freq_param } else { 0. };

    let type_oid: u32 = agg.type_oid;
    SetOfIterator::new(
        TopNIterator::new(
            agg.datums.clone().into_iter(),
            agg.counts.clone().into_vec(),
            agg.values_seen as f64,
            n,
            min_freq,
        )
        // TODO Shouldn't failure to convert to AnyElement cause error, not early stop?
        .map_while(move |value| unsafe {
            AnyElement::from_polymorphic_datum(value, false, pg_sys::Oid::from(type_oid))
        }),
    )
}

#[pg_extern(immutable, parallel_safe, name = "topn")]
pub fn default_topn(
    agg: SpaceSavingAggregate<'_>,
    ty: Option<AnyElement>,
) -> SetOfIterator<AnyElement> {
    if agg.topn == 0 {
        pgrx::error!("frequency aggregates require a N parameter to topn")
    }
    let n = agg.topn as i32;
    topn(agg, n, ty)
}

#[pg_extern(immutable, parallel_safe, name = "topn")]
pub fn topn_bigint(agg: SpaceSavingBigIntAggregate<'_>, n: i32) -> SetOfIterator<i64> {
    validate_topn_for_mcv_agg(
        n,
        agg.topn,
        agg.freq_param,
        agg.values_seen,
        agg.counts.iter(),
    );
    let min_freq = if agg.topn == 0 { agg.freq_param } else { 0. };

    SetOfIterator::new(TopNIterator::new(
        agg.datums.clone().into_iter(),
        agg.counts.clone().into_vec(),
        agg.values_seen as f64,
        n,
        min_freq,
    ))
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_topn_bigint<'a>(
    agg: SpaceSavingBigIntAggregate<'a>,
    accessor: AccessorTopNCount<'static>,
) -> SetOfIterator<'a, i64> {
    topn_bigint(agg, accessor.count as i32)
}

#[pg_extern(immutable, parallel_safe, name = "topn")]
pub fn default_topn_bigint(agg: SpaceSavingBigIntAggregate<'_>) -> SetOfIterator<i64> {
    if agg.topn == 0 {
        pgrx::error!("frequency aggregates require a N parameter to topn")
    }
    let n = agg.topn as i32;
    topn_bigint(agg, n)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_default_topn_bigint<'a>(
    agg: SpaceSavingBigIntAggregate<'a>,
    _accessor: AccessorTopn<'static>,
) -> SetOfIterator<'a, i64> {
    default_topn_bigint(agg)
}

#[pg_extern(immutable, parallel_safe, name = "topn")]
pub fn topn_text(agg: SpaceSavingTextAggregate<'_>, n: i32) -> SetOfIterator<String> {
    validate_topn_for_mcv_agg(
        n,
        agg.topn,
        agg.freq_param,
        agg.values_seen,
        agg.counts.iter(),
    );
    let min_freq = if agg.topn == 0 { agg.freq_param } else { 0. };

    SetOfIterator::new(
        TopNIterator::new(
            agg.datums.clone().into_iter(),
            agg.counts.clone().into_vec(),
            agg.values_seen as f64,
            n,
            min_freq,
        )
        .map(|value| unsafe { varlena_to_string(value.cast_mut_ptr()) }),
    )
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_topn_text<'a>(
    agg: SpaceSavingTextAggregate<'a>,
    accessor: AccessorTopNCount<'static>,
) -> SetOfIterator<'a, String> {
    topn_text(agg, accessor.count as i32)
}

#[pg_extern(immutable, parallel_safe, name = "topn")]
pub fn default_topn_text(agg: SpaceSavingTextAggregate<'_>) -> SetOfIterator<String> {
    if agg.topn == 0 {
        pgrx::error!("frequency aggregates require a N parameter to topn")
    }
    let n = agg.topn as i32;
    topn_text(agg, n)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_default_topn_text<'a>(
    agg: SpaceSavingTextAggregate<'a>,
    _accessor: AccessorTopn<'static>,
) -> SetOfIterator<'a, String> {
    default_topn_text(agg)
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_frequency(agg: SpaceSavingAggregate<'_>, value: AnyElement) -> f64 {
    let value: PgAnyElement = value.into();
    match agg
        .datums
        .iter()
        .position(|datum| value == (datum, pg_sys::Oid::from(agg.type_oid)).into())
    {
        Some(idx) => agg.counts.slice()[idx] as f64 / agg.values_seen as f64,
        None => 0.,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_frequency(agg: SpaceSavingAggregate<'_>, value: AnyElement) -> f64 {
    let value: PgAnyElement = value.into();
    match agg
        .datums
        .iter()
        .position(|datum| value == (datum, pg_sys::Oid::from(agg.type_oid)).into())
    {
        Some(idx) => {
            (agg.counts.slice()[idx] - agg.overcounts.slice()[idx]) as f64 / agg.values_seen as f64
        }
        None => 0.,
    }
}

#[pg_extern(immutable, parallel_safe, name = "max_frequency")]
pub fn max_bigint_frequency(agg: SpaceSavingBigIntAggregate<'_>, value: i64) -> f64 {
    match agg.datums.iter().position(|datum| value == datum) {
        Some(idx) => agg.counts.slice()[idx] as f64 / agg.values_seen as f64,
        None => 0.,
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_max_bigint_frequency<'a>(
    agg: SpaceSavingBigIntAggregate<'a>,
    accessor: AccessorMaxFrequencyInt<'static>,
) -> f64 {
    max_bigint_frequency(agg, accessor.value)
}

#[pg_extern(immutable, parallel_safe, name = "min_frequency")]
pub fn min_bigint_frequency(agg: SpaceSavingBigIntAggregate<'_>, value: i64) -> f64 {
    match agg.datums.iter().position(|datum| value == datum) {
        Some(idx) => {
            (agg.counts.slice()[idx] - agg.overcounts.slice()[idx]) as f64 / agg.values_seen as f64
        }
        None => 0.,
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_min_bigint_frequency<'a>(
    agg: SpaceSavingBigIntAggregate<'a>,
    accessor: AccessorMinFrequencyInt<'static>,
) -> f64 {
    min_bigint_frequency(agg, accessor.value)
}

// Still needs an arrow operator defined, but the text datum input is a bit finicky.
#[pg_extern(immutable, parallel_safe, name = "max_frequency")]
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

// Still needs an arrow operator defined, but the text datum input is a bit finicky.
#[pg_extern(immutable, parallel_safe, name = "min_frequency")]
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
    use pgrx_macros::pg_test;
    use rand::distributions::{Distribution, Uniform};
    use rand::prelude::SliceRandom;
    use rand::thread_rng;
    use rand::RngCore;
    use rand_distr::Zeta;

    #[pg_test]
    fn test_freq_aggregate() {
        Spi::connect(|mut client| {
            // using the search path trick for this test to make it easier to stabilize later on
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {}", sp), None, None)
                .unwrap();

            client.update("SET TIMEZONE to UTC", None, None).unwrap();
            client
                .update(
                    "CREATE TABLE test (data INTEGER, time TIMESTAMPTZ)",
                    None,
                    None,
                )
                .unwrap();

            for i in (0..100).rev() {
                client.update(&format!("INSERT INTO test SELECT i, '2020-1-1'::TIMESTAMPTZ + ('{} days, ' || i::TEXT || ' seconds')::INTERVAL FROM generate_series({}, 99, 1) i", 100 - i, i), None, None).unwrap();
            }

            let test = client.update("SELECT freq_agg(0.015, s.data)::TEXT FROM (SELECT data FROM test ORDER BY time) s", None, None)
                .unwrap().first()
                .get_one::<String>().unwrap().unwrap();
            let expected = "(version:1,num_values:67,topn:0,values_seen:5050,freq_param:0.015,counts:[100,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67],overcounts:[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66],datums:[99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66])";
            assert_eq!(test, expected);

            let test = client.update("SELECT raw_freq_agg(0.015, s.data)::TEXT FROM (SELECT data FROM test ORDER BY time) s", None, None)
                .unwrap().first()
                .get_one::<String>().unwrap().unwrap();
            let expected = "(version:1,type_oid:23,num_values:67,values_seen:5050,freq_param:0.015,topn:0,counts:[100,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67],overcounts:[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66],datums:[23,\"99\",\"98\",\"97\",\"96\",\"95\",\"94\",\"93\",\"92\",\"91\",\"90\",\"89\",\"88\",\"87\",\"86\",\"85\",\"84\",\"83\",\"82\",\"81\",\"80\",\"79\",\"78\",\"77\",\"76\",\"75\",\"74\",\"73\",\"72\",\"71\",\"70\",\"69\",\"68\",\"67\",\"33\",\"34\",\"35\",\"36\",\"37\",\"38\",\"39\",\"40\",\"41\",\"42\",\"43\",\"44\",\"45\",\"46\",\"47\",\"48\",\"49\",\"50\",\"51\",\"52\",\"53\",\"54\",\"55\",\"56\",\"57\",\"58\",\"59\",\"60\",\"61\",\"62\",\"63\",\"64\",\"65\",\"66\"])";
            assert_eq!(test, expected);
        });
    }

    #[pg_test]
    fn test_topn_aggregate() {
        Spi::connect(|mut client| {
            // using the search path trick for this test to make it easier to stabilize later on
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {}", sp), None, None)
                .unwrap();

            client.update("SET TIMEZONE to UTC", None, None).unwrap();
            client
                .update(
                    "CREATE TABLE test (data INTEGER, time TIMESTAMPTZ)",
                    None,
                    None,
                )
                .unwrap();

            for i in (0..200).rev() {
                client.update(&format!("INSERT INTO test SELECT i, '2020-1-1'::TIMESTAMPTZ + ('{} days, ' || i::TEXT || ' seconds')::INTERVAL FROM generate_series({}, 199, 1) i", 200 - i, i), None, None).unwrap();
            }

            let test = client
                .update(
                    "SELECT mcv_agg(10, s.data)::TEXT FROM (SELECT data FROM test ORDER BY time) s",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
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
                let value = unsafe {
                    AnyElement::from_polymorphic_datum(
                        pg_sys::Datum::from(j),
                        false,
                        pg_sys::INT4OID,
                    )
                };
                state = super::freq_agg_trans(state, freq, value, fcinfo).unwrap();
            }
        }

        let first = super::space_saving_serialize(state);

        let bytes = unsafe {
            std::slice::from_raw_parts(
                vardata_any(first.0.cast_mut_ptr()) as *const u8,
                varsize_any_exhdr(first.0.cast_mut_ptr()),
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
                let value = unsafe {
                    AnyElement::from_polymorphic_datum(
                        pg_sys::Datum::from(j),
                        false,
                        pg_sys::INT4OID,
                    )
                };
                state = super::freq_agg_trans(state, freq, value, fcinfo).unwrap();
            }
        }

        let second = super::space_saving_serialize(state);

        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                vardata_any(second.0.cast_mut_ptr()) as *const u8,
                varsize_any_exhdr(second.0.cast_mut_ptr()),
            )
        };
        let expected: [u8; 513] = [
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
                vardata_any(combined.0.cast_mut_ptr()) as *const u8,
                varsize_any_exhdr(combined.0.cast_mut_ptr()),
            )
        };
        let expected: [u8; 513] = [
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
    fn setup_with_test_table(client: &mut pgrx::spi::SpiClient) {
        // using the search path trick for this test to make it easier to stabilize later on
        let sp = client
            .update(
                "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                None,
                None,
            )
            .unwrap()
            .first()
            .get_one::<String>()
            .unwrap()
            .unwrap();
        client
            .update(&format!("SET LOCAL search_path TO {}", sp), None, None)
            .unwrap();

        client.update("SET TIMEZONE to UTC", None, None).unwrap();
        client
            .update(
                "CREATE TABLE test (data INTEGER, time TIMESTAMPTZ)",
                None,
                None,
            )
            .unwrap();

        for i in (0..20).rev() {
            client.update(&format!("INSERT INTO test SELECT i, '2020-1-1'::TIMESTAMPTZ + ('{} days, ' || i::TEXT || ' seconds')::INTERVAL FROM generate_series({}, 19, 1) i", 10 - i, i), None, None).unwrap();
        }

        client
            .update(
                "CREATE TABLE aggs (name TEXT, agg SPACESAVINGBIGINTAGGREGATE)",
                None,
                None,
            )
            .unwrap();
        client.update("INSERT INTO aggs SELECT 'mcv_default', mcv_agg(5, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None).unwrap();
        client.update("INSERT INTO aggs SELECT 'mcv_1.5', mcv_agg(5, 1.5, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None).unwrap();
        client.update("INSERT INTO aggs SELECT 'mcv_2', mcv_agg(5, 2, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None).unwrap();
        client.update("INSERT INTO aggs SELECT 'freq_8', freq_agg(0.08, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None).unwrap();
        client.update("INSERT INTO aggs SELECT 'freq_5', freq_agg(0.05, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None).unwrap();
        client.update("INSERT INTO aggs SELECT 'freq_2', freq_agg(0.02, s.data) FROM (SELECT data FROM test ORDER BY time) s", None, None).unwrap();
    }

    // API tests
    #[pg_test]
    fn test_topn() {
        Spi::connect(|mut client| {
            setup_with_test_table(&mut client);

            // simple tests
            let rows = client
                .update(
                    "SELECT topn(agg) FROM aggs WHERE name = 'mcv_default'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 5);
            let rows = client
                .update(
                    "SELECT agg -> topn() FROM aggs WHERE name = 'mcv_default'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 5);
            let rows = client
                .update(
                    "SELECT topn(agg, 5) FROM aggs WHERE name = 'freq_5'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 5);

            // can limit below topn_agg value
            let rows = client
                .update(
                    "SELECT topn(agg, 3) FROM aggs WHERE name = 'mcv_default'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 3);
            let rows = client
                .update(
                    "SELECT agg -> topn(3) FROM aggs WHERE name = 'mcv_default'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 3);

            // only 4 rows with freq >= 0.08
            let rows = client
                .update(
                    "SELECT topn(agg, 5) FROM aggs WHERE name = 'freq_8'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 4);
        });
    }

    #[pg_test(
        error = "data is not skewed enough to find top 0 parameters with a skew of 1.5, try reducing the skew factor"
    )]
    fn topn_on_underskewed_mcv_agg() {
        Spi::connect(|mut client| {
            setup_with_test_table(&mut client);
            client
                .update(
                    "SELECT topn(agg, 0::int) FROM aggs WHERE name = 'mcv_1.5'",
                    None,
                    None,
                )
                .unwrap()
                .count();
        });
    }

    #[pg_test(error = "requested N (8) exceeds creation parameter of mcv aggregate (5)")]
    fn topn_high_n_on_mcv_agg() {
        Spi::connect(|mut client| {
            setup_with_test_table(&mut client);
            client
                .update(
                    "SELECT topn(agg, 8) FROM aggs WHERE name = 'mcv_default'",
                    None,
                    None,
                )
                .unwrap()
                .count();
        });
    }

    #[pg_test(error = "frequency aggregates require a N parameter to topn")]
    fn topn_requires_n_for_freq_agg() {
        Spi::connect(|mut client| {
            setup_with_test_table(&mut client);
            assert_eq!(
                0,
                client
                    .update(
                        "SELECT topn(agg) FROM aggs WHERE name = 'freq_2'",
                        None,
                        None
                    )
                    .unwrap()
                    .count(),
            );
        });
    }

    #[pg_test]
    fn test_into_values() {
        Spi::connect(|mut client| {
            setup_with_test_table(&mut client);

            let rows = client
                .update(
                    "SELECT into_values(agg) FROM aggs WHERE name = 'freq_8'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 13);
            let rows = client
                .update(
                    "SELECT into_values(agg) FROM aggs WHERE name = 'freq_5'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 20);
            let rows = client
                .update(
                    "SELECT into_values(agg) FROM aggs WHERE name = 'freq_2'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 20);

            let rows = client
                .update(
                    "SELECT agg -> into_values() FROM aggs WHERE name = 'freq_8'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 13);
            let rows = client
                .update(
                    "SELECT agg -> into_values() FROM aggs WHERE name = 'freq_5'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 20);
            let rows = client
                .update(
                    "SELECT agg -> into_values() FROM aggs WHERE name = 'freq_2'",
                    None,
                    None,
                )
                .unwrap()
                .count();
            assert_eq!(rows, 20);
        });
    }

    #[pg_test]
    fn test_frequency_getters() {
        Spi::connect(|mut client| {
            setup_with_test_table(&mut client);

            // simple tests
            let (min, max) = client.update("SELECT min_frequency(agg, 3), max_frequency(agg, 3) FROM aggs WHERE name = 'freq_2'", None, None)
                .unwrap().first()
                .get_two::<f64,f64>().unwrap();
            assert_eq!(min.unwrap(), 0.01904761904761905);
            assert_eq!(max.unwrap(), 0.01904761904761905);

            let (min, max) = client.update("SELECT min_frequency(agg, 11), max_frequency(agg, 11) FROM aggs WHERE name = 'mcv_default'", None, None)
                .unwrap().first()
                .get_two::<f64,f64>().unwrap();
            assert_eq!(min.unwrap(), 0.05714285714285714);
            assert_eq!(max.unwrap(), 0.05714285714285714);
            let (min, max) = client.update("SELECT agg -> min_frequency(3), agg -> max_frequency(3) FROM aggs WHERE name = 'freq_2'", None, None)
                .unwrap().first()
                .get_two::<f64,f64>().unwrap();
            assert_eq!(min.unwrap(), 0.01904761904761905);
            assert_eq!(max.unwrap(), 0.01904761904761905);

            let (min, max) = client.update("SELECT agg -> min_frequency(11), agg -> max_frequency(11) FROM aggs WHERE name = 'mcv_default'", None, None)
                .unwrap().first()
                .get_two::<f64,f64>().unwrap();
            assert_eq!(min.unwrap(), 0.05714285714285714);
            assert_eq!(max.unwrap(), 0.05714285714285714);

            // missing value
            let (min, max) = client.update("SELECT min_frequency(agg, 3), max_frequency(agg, 3) FROM aggs WHERE name = 'freq_8'", None, None)
                .unwrap().first()
                .get_two::<f64,f64>().unwrap();
            assert_eq!(min.unwrap(), 0.);
            assert_eq!(max.unwrap(), 0.);

            let (min, max) = client.update("SELECT min_frequency(agg, 20), max_frequency(agg, 20) FROM aggs WHERE name = 'mcv_2'", None, None)
                .unwrap().first()
                .get_two::<f64,f64>().unwrap();
            assert_eq!(min.unwrap(), 0.);
            assert_eq!(max.unwrap(), 0.);

            // noisy value
            let (min, max) = client.update("SELECT min_frequency(agg, 8), max_frequency(agg, 8) FROM aggs WHERE name = 'mcv_1.5'", None, None)
                .unwrap().first()
                .get_two::<f64,f64>().unwrap();
            assert_eq!(min.unwrap(), 0.004761904761904762);
            assert_eq!(max.unwrap(), 0.05238095238095238);
        });
    }

    #[pg_test]
    fn test_rollups() {
        Spi::connect(|mut client| {
            client.update(
                "CREATE TABLE test (raw_data DOUBLE PRECISION, int_data INTEGER, text_data TEXT, bucket INTEGER)",
                None,
                None,
            ).unwrap();

            // Generate an array of 10000 values by taking the probability curve for a
            // zeta curve with an s of 1.1 for the top 5 values, then adding smaller
            // amounts of the next 5 most common values, and finally filling with unique values.
            let mut vals = vec![1; 945];
            vals.append(&mut vec![2; 441]);
            vals.append(&mut vec![3; 283]);
            vals.append(&mut vec![4; 206]);
            vals.append(&mut vec![5; 161]);
            for v in 6..=10 {
                vals.append(&mut vec![v, 125]);
            }
            for v in 0..(10000 - 945 - 441 - 283 - 206 - 161 - (5 * 125)) {
                vals.push(11 + v);
            }
            vals.shuffle(&mut thread_rng());

            // Probably not the most efficient way of populating this table...
            for v in vals {
                let cmd = format!(
                    "INSERT INTO test SELECT {}, {}::INT, {}::TEXT, FLOOR(RANDOM() * 10)",
                    v, v, v
                );
                client.update(&cmd, None, None).unwrap();
            }

            // No matter how the values are batched into subaggregates, we should always
            // see the same top 5 values
            let mut result = client.update(
                "WITH aggs AS (SELECT bucket, raw_mcv_agg(5, raw_data) as raw_agg FROM test GROUP BY bucket)
                SELECT topn(rollup(raw_agg), NULL::DOUBLE PRECISION)::TEXT from aggs",
                None, None
            ).unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("1"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("2"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("3"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("4"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("5"));
            assert!(result.next().is_none());

            let mut result = client.update(
                "WITH aggs AS (SELECT bucket, mcv_agg(5, int_data) as int_agg FROM test GROUP BY bucket)
                SELECT topn(rollup(int_agg))::TEXT from aggs",
                None, None
            ).unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("1"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("2"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("3"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("4"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("5"));
            assert!(result.next().is_none());

            let mut result = client.update(
                "WITH aggs AS (SELECT bucket, mcv_agg(5, text_data) as text_agg FROM test GROUP BY bucket)
                SELECT topn(rollup(text_agg))::TEXT from aggs",
                None, None
            ).unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("1"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("2"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("3"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("4"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("5"));
            assert!(result.next().is_none());
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
            let value = unsafe {
                AnyElement::from_polymorphic_datum(pg_sys::Datum::from(v), false, pg_sys::INT4OID)
            };
            state = super::freq_agg_trans(state, freq, value, fcinfo).unwrap();
            counts[v] += 1;
        }

        let state = space_saving_final(state, fcinfo).unwrap();
        let vals: std::collections::HashSet<usize> =
            state.datums.iter().map(|datum| datum.value()).collect();

        for (val, &count) in counts.iter().enumerate() {
            if count >= 3 {
                assert!(vals.contains(&val));
            }
        }
    }

    #[pg_test]
    fn test_freq_agg_rollup_maintains_invariant() {
        // The frequency agg invariant is that any element with frequency >= f will appear in the freq_agg(f)

        // This test will randomly generate 200 values in the uniform range [0, 99] and check to see any value
        // that shows up at least 3 times appears in a frequency aggregate created with freq = 0.015
        let rand100 = Uniform::new_inclusive(0, 99);
        let mut rng = rand::thread_rng();

        let mut counts = [0; 100];

        let freq = 0.015;
        let fcinfo = std::ptr::null_mut(); // dummy value, will use default collation

        let mut aggs = vec![];
        for _ in 0..4 {
            let mut state = None.into();
            for _ in 0..50 {
                let v = rand100.sample(&mut rng);
                let value = unsafe {
                    AnyElement::from_polymorphic_datum(
                        pg_sys::Datum::from(v),
                        false,
                        pg_sys::INT4OID,
                    )
                };
                state = super::freq_agg_trans(state, freq, value, fcinfo).unwrap();
                counts[v] += 1;
            }
            aggs.push(space_saving_final(state, fcinfo).unwrap());
        }

        let state = {
            let mut state = None.into();
            for agg in aggs {
                state = super::rollup_agg_trans(state, Some(agg), fcinfo).unwrap();
            }
            space_saving_final(state, fcinfo).unwrap()
        };
        let vals: std::collections::HashSet<usize> =
            state.datums.iter().map(|datum| datum.value()).collect();

        for (val, &count) in counts.iter().enumerate() {
            if count >= 3 {
                assert!(vals.contains(&val));
            }
        }
    }

    #[pg_test]
    fn test_mcv_agg_invariant() {
        // The ton agg invariant is that we'll be able to track the top n values for any data
        // with a distribution at least as skewed as a zeta distribution

        // To test this we will generate a mcv aggregate with a random skew (1.01 - 2.0) and
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
            let value = unsafe {
                AnyElement::from_polymorphic_datum(pg_sys::Datum::from(v), false, pg_sys::INT4OID)
            };
            state = super::mcv_agg_with_skew_trans(state, n as i32, skew, value, fcinfo).unwrap();
            if v < 100 {
                // anything greater than 100 will not be in the top values
                counts[v] += 1;
            }
        }

        let state = space_saving_final(state, fcinfo).unwrap();
        let value =
            unsafe { AnyElement::from_polymorphic_datum(Datum::from(0), false, pg_sys::INT4OID) };
        let t: Vec<AnyElement> = default_topn(state, Some(value.unwrap())).collect();
        let agg_topn: Vec<usize> = t.iter().map(|x| x.datum().value()).collect();

        let mut temp: Vec<(usize, &usize)> = counts.iter().enumerate().collect();
        temp.sort_by(|(_, cnt1), (_, cnt2)| cnt2.cmp(cnt1)); // descending order by count
        let top_vals: Vec<usize> = temp.into_iter().map(|(val, _)| val).collect();

        for i in 0..n as usize {
            assert_eq!(agg_topn[i], top_vals[i]);
        }
    }
}
