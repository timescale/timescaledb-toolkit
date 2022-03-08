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
    aggregate_utils::get_collation,
    build,
    datum_utils::{
        deep_copy_datum, DatumFromSerializedTextReader, DatumHashBuilder, DatumStore,
        TextSerializableDatumWriter,
    },
    palloc::{Inner, Internal, InternalAsValue},
    pg_any_element::{PgAnyElement, PgAnyElementHashMap},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};

use aggregate_builder::aggregate;

use crate::frequency::toolkit_experimental::FrequencyAggregate;

struct FrequencyEntry {
    value: Datum,
    count: u64,
    overcount: u64,
}

impl FrequencyEntry {
    fn clone(&self, typoid: Oid) -> FrequencyEntry {
        FrequencyEntry {
            value: unsafe { deep_copy_datum(self.value, typoid) },
            count: self.count,
            overcount: self.overcount,
        }
    }
}

pub struct FrequencyTransState {
    entries: Vec<FrequencyEntry>,
    indicies: PgAnyElementHashMap<usize>,
    total_vals: u64,
    min_freq: f64,
    max_size: u64,  // Maximum size for indices
}

impl Clone for FrequencyTransState {
    fn clone(&self) -> Self {
        let mut new_state = Self {
            entries: vec![],
            indicies: PgAnyElementHashMap::with_hasher(self.indicies.hasher().clone()),
            total_vals: self.total_vals,
            min_freq: self.min_freq,
            max_size: self.max_size,
        };

        let typoid = self.type_oid();
        for entry in &self.entries {
            new_state.entries.push(FrequencyEntry {
                value: unsafe { deep_copy_datum(entry.value, typoid) },
                count: entry.count,
                overcount: entry.overcount,
            })
        }
        new_state.update_all_map_indicies();
        new_state
    }
}

// FrequencyTransState is a little tricky to serialize due to needing the typ oid to serialize the Datums.
// This sort of requirement doesn't play nicely with the serde framework, so as a workaround we simply
// serialize the object as one big sequence.  The serialized sequence should look like this:
//   total_vals as u64
//   min_freq as f64
//   max_idx as u64
//   indicies.hasher as DatumHashBuilder
//   entries as repeated (str, u64, u64) tuples
impl Serialize for FrequencyTransState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.entries.len() + 4))?;
        seq.serialize_element(&self.total_vals)?;
        seq.serialize_element(&self.min_freq)?;
        seq.serialize_element(&self.max_size)?;
        seq.serialize_element(&self.indicies.hasher())?;

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

impl<'de> Deserialize<'de> for FrequencyTransState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FrequencyTransStateVisitor();

        impl<'de> Visitor<'de> for FrequencyTransStateVisitor {
            type Value = FrequencyTransState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence encoding a FrequencyTransState object")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let total_vals = seq.next_element::<u64>()?.unwrap();
                let min_freq = seq.next_element::<f64>()?.unwrap();
                let max_size = seq.next_element::<u64>()?.unwrap();
                let hasher = seq.next_element::<DatumHashBuilder>()?.unwrap();

                let mut state = FrequencyTransState {
                    entries: vec![],
                    indicies: PgAnyElementHashMap::with_hasher(hasher),
                    total_vals,
                    min_freq,
                    max_size,
                };

                let typid = state.type_oid();
                let mut reader = DatumFromSerializedTextReader::from_oid(typid);

                while let Some((datum_str, count, overcount)) =
                    seq.next_element::<(&str, u64, u64)>()?
                {
                    let datum = reader.read_datum(datum_str);

                    state.entries.push(FrequencyEntry {
                        value: unsafe { deep_copy_datum(datum, typid) },
                        count,
                        overcount,
                    });
                }
                state.update_all_map_indicies();
                Ok(state)
            }
        }

        deserializer.deserialize_seq(FrequencyTransStateVisitor())
    }
}

impl FrequencyTransState {
    fn max_size_for_freq(min_freq: f64) -> u64 {
        (1. / min_freq) as u64 + 1
    }

    unsafe fn from_type_id(min_freq: f64, typ: pg_sys::Oid, collation: Option<Oid>) -> Self {
        FrequencyTransState {
            entries: vec![],
            indicies: PgAnyElementHashMap::new(typ, collation),
            total_vals: 0,
            min_freq,
            max_size: FrequencyTransState::max_size_for_freq(min_freq),
        }
    }

    fn type_oid(&self) -> Oid {
        self.indicies.typoid()
    }

    fn add(&mut self, element: PgAnyElement) {
        self.total_vals += 1;
        if let Some(idx) = self.indicies.get(&element) {
            let idx = *idx;
            self.entries[idx].count += 1;
            self.move_left(idx);
        } else {
            if self.entries.len() < self.max_size as usize {
                let new_idx = self.entries.len();
                self.entries.push(FrequencyEntry {
                    value: element.deep_copy_datum(),
                    count: 1,
                    overcount: 0,
                });

                // Important to create the indices entry using the datum in the local context
                self.indicies.insert(
                    (self.entries[new_idx].value, self.type_oid()).into(),
                    new_idx,
                );
            } else {
                let new_value = element.deep_copy_datum();

                // TODO: might be more efficient to replace the lowest indexed tail value (count matching last) and not call move_up
                let typoid = self.type_oid();
                let entry = self.entries.last_mut().unwrap();
                self.indicies.remove(&(entry.value, typoid).into());
                entry.value = new_value; // JOSH FIXME should we pfree() old value if by-ref?
                entry.overcount = entry.count;
                entry.count += 1;
                self.indicies
                    .insert((new_value, typoid).into(), self.entries.len() - 1);
                self.move_left(self.entries.len() - 1);
            }
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

    // Adds the 'indicies' lookup entry for the value at 'entries' index i
    fn update_map_index(&mut self, i: usize) {
        let element_for_i = (self.entries[i].value, self.type_oid()).into();
        if let Some(entry) = self.indicies.get_mut(&element_for_i) {
            *entry = i;
        } else {
            self.indicies.insert(element_for_i, i);
        }
    }

    fn update_all_map_indicies(&mut self) {
        for i in 0..self.entries.len() {
            self.update_map_index(i);
        }
    }

    fn combine(one: &FrequencyTransState, two: &FrequencyTransState) -> FrequencyTransState {
        // This takes an entry from a TransState, updates it with any state from the other TransState, and adds the result into the map
        fn new_entry(
            entry: &FrequencyEntry,
            other: &FrequencyTransState,
            map: &mut PgAnyElementHashMap<FrequencyEntry>,
        ) {
            let typoid = other.type_oid();

            let mut new_ent = entry.clone(typoid);
            let new_dat = (new_ent.value, typoid).into();
            match other.indicies.get(&new_dat) {
                Some(&idx) => {
                    new_ent.count += other.entries[idx].count;
                    new_ent.overcount += other.entries[idx].overcount;
                }
                None => {
                    // If the entry value isn't present in the other state, we have to assume that it was recently bumped (unless the other state is not fully populated).
                    let min = if other.indicies.len() < other.max_size as usize {
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

        let hasher = one.indicies.hasher().clone();
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
        let mut entries: Vec<FrequencyEntry> = temp.0.into_iter().map(|(_, v)| v).collect();
        entries.sort_by(|a, b| b.count.partial_cmp(&a.count).unwrap()); // swap a and b for descending

        entries.truncate(one.max_size as usize);

        let mut result = FrequencyTransState {
            entries,
            indicies: PgAnyElementHashMap::with_hasher(one.indicies.hasher().clone()),
            total_vals: one.total_vals + two.total_vals,
            min_freq: one.min_freq,
            max_size: one.max_size,
        };

        result.update_all_map_indicies();
        result
    }
}

#[pg_schema]
pub mod toolkit_experimental {
    pub(crate) use super::*;

    pg_type! {
        #[derive(Debug)]
        struct FrequencyAggregate<'input> {
            type_oid: u32,
            num_values: u32,
            values_seen: u64,
            min_freq: f64,
            counts: [u64; self.num_values], // JOSH TODO look at AoS instead of SoA at some point
            overcounts: [u64; self.num_values],
            datums: DatumStore<'input>,
        }
    }

    impl<'input> From<Internal> for FrequencyAggregate<'input> {
        fn from(trans: Internal) -> Self {
            Self::from(unsafe { trans.to_inner().unwrap() })
        }
    }

    impl<'input> From<&mut FrequencyTransState> for FrequencyAggregate<'input> {
        fn from(trans: &mut FrequencyTransState) -> Self {
            let mut values = Vec::new();
            let mut counts = Vec::new();
            let mut overcounts = Vec::new();

            for entry in &trans.entries {
                values.push(entry.value);
                counts.push(entry.count);
                overcounts.push(entry.overcount);
            }

            build! {
                FrequencyAggregate {
                    type_oid: trans.type_oid() as _,
                    num_values: trans.entries.len() as _,
                    values_seen: trans.total_vals,
                    min_freq: trans.min_freq,
                    counts: counts.into(),
                    overcounts: overcounts.into(),
                    datums: DatumStore::from((trans.type_oid(), values)),
                }
            }
        }
    }

    impl<'input> From<Inner<FrequencyTransState>> for FrequencyAggregate<'input> {
        fn from(trans: Inner<FrequencyTransState>) -> Self {
            let mut values = Vec::new();
            let mut counts = Vec::new();
            let mut overcounts = Vec::new();

            for entry in &trans.entries {
                values.push(entry.value);
                counts.push(entry.count);
                overcounts.push(entry.overcount);
            }

            build! {
                FrequencyAggregate {
                    type_oid: trans.type_oid() as _,
                    num_values: trans.entries.len() as _,
                    values_seen: trans.total_vals,
                    min_freq: trans.min_freq,
                    counts: counts.into(),
                    overcounts: overcounts.into(),
                    datums: DatumStore::from((trans.type_oid(), values)),
                }
            }
        }
    }

    ron_inout_funcs!(FrequencyAggregate);
}

#[aggregate]
impl toolkit_experimental::freq_agg {
    type State = FrequencyTransState;

    const PARALLEL_SAFE: bool = true;

    fn transition(
        state: Option<State>,
        #[sql_type("double precision")] freq: f64,
        #[sql_type("AnyElement")] value: Option<AnyElement>,
        fcinfo: pg_sys::FunctionCallInfo,
    ) -> Option<State> {
        let value = match value {
            None => return state,
            Some(value) => value,
        };
        let mut state = match state {
            None => unsafe {
                let typ = value.oid();
                let collation = if fcinfo.is_null() {
                    Some(100) // TODO: default OID, there should be a constant for this
                } else {
                    get_collation(fcinfo)
                };
                FrequencyTransState::from_type_id(freq, typ, collation)
            },
            Some(state) => state,
        };

        state.add(value.into());
        Some(state)
    }

    fn finally(
        state: Option<&mut State>,
    ) -> Option<toolkit_experimental::FrequencyAggregate<'static>> {
        state.map(FrequencyAggregate::from)
    }

    fn serialize(state: &State) -> bytea {
        crate::do_serialize!(state)
    }

    fn deserialize(bytes: crate::raw::bytea) -> State {
        crate::do_deserialize!(bytes, FrequencyTransState)
    }

    fn combine(a: Option<&State>, b: Option<&State>) -> Option<State> {
        match (a, b) {
            (Some(a), Some(b)) => Some(FrequencyTransState::combine(a, b)),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "values",
    schema = "toolkit_experimental"
)]
pub fn freq_iter(
    agg: FrequencyAggregate<'_>,
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
                if count as f64 / total < agg.min_freq {
                    None
                } else {
                    let value = AnyElement::from_datum(value, false, agg.type_oid).unwrap();
                    let min_freq = (count - overcount) as f64 / total;
                    let max_freq = count as f64 / total;
                    Some((value, min_freq, max_freq))
                }
            },
        )
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn(
    agg: FrequencyAggregate<'_>,
    n: i32,
    ty: AnyElement,
) -> impl std::iter::Iterator<Item = AnyElement> + '_ {
    unsafe {
        if ty.oid() != agg.type_oid {
            pgx::error!("mischatched types")
        }
        let iter = agg
            .datums
            .clone()
            .into_iter()
            .zip(agg.counts.slice().iter());
        iter.enumerate().map_while(move |(i, (value, &count))| {
            let total = agg.values_seen as f64;
            if i >= n as usize || count as f64 / total < agg.min_freq {
                None
            } else {
                AnyElement::from_datum(value, false, agg.type_oid)
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

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
            client.select(
                "SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'",
                None,
                None,
            );

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
            let expected = "(version:1,type_oid:23,num_values:67,values_seen:5050,min_freq:0.015,counts:[100,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67,67],overcounts:[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66],datums:[23,\"99\",\"98\",\"97\",\"96\",\"95\",\"94\",\"93\",\"92\",\"91\",\"90\",\"89\",\"88\",\"87\",\"86\",\"85\",\"84\",\"83\",\"82\",\"81\",\"80\",\"79\",\"78\",\"77\",\"76\",\"75\",\"74\",\"73\",\"72\",\"71\",\"70\",\"69\",\"68\",\"67\",\"33\",\"34\",\"35\",\"36\",\"37\",\"38\",\"39\",\"40\",\"41\",\"42\",\"43\",\"44\",\"45\",\"46\",\"47\",\"48\",\"49\",\"50\",\"51\",\"52\",\"53\",\"54\",\"55\",\"56\",\"57\",\"58\",\"59\",\"60\",\"61\",\"62\",\"63\",\"64\",\"65\",\"66\"])";
            assert_eq!(test, expected);
        });
    }

    #[pg_test]
    fn explicit_aggregate_test() {
        let freq = 0.0625;
        let fcinfo = std::ptr::null_mut(); // dummy value, will use default collation
        let mut state = None;

        for i in 11..=20 {
            for j in i..=20 {
                let value =
                    unsafe { AnyElement::from_datum(j as pg_sys::Datum, false, pg_sys::INT4OID) };
                state = super::freq_agg::transition(state, freq, value, fcinfo);
            }
        }

        let first = super::freq_agg::serialize(&state.unwrap());

        let bytes = unsafe {
            std::slice::from_raw_parts(
                vardata_any(first.0 as *const pg_sys::varlena) as *const u8,
                varsize_any_exhdr(first.0 as *const pg_sys::varlena),
            )
        };
        let expected = [
            1, 1, // versions
            14, 0, 0, 0, 0, 0, 0, 0, // size hint for sequence
            55, 0, 0, 0, 0, 0, 0, 0, // elements seen
            0, 0, 0, 0, 0, 0, 176, 63, // frequency (f64 encoding of 0.0625)
            17, 0, 0, 0, 0, 0, 0, 0,  // elements tracked
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

        state = None;

        for i in (1..=10).rev() {
            // reverse here introduces less error in the aggregate
            for j in i..=20 {
                let value =
                    unsafe { AnyElement::from_datum(j as pg_sys::Datum, false, pg_sys::INT4OID) };
                state = super::freq_agg::transition(state, freq, value, fcinfo);
            }
        }

        let second = super::freq_agg::serialize(&state.unwrap());

        let bytes = unsafe {
            std::slice::from_raw_parts(
                vardata_any(second.0 as *const pg_sys::varlena) as *const u8,
                varsize_any_exhdr(second.0 as *const pg_sys::varlena),
            )
        };
        let expected = [
            1, 1, // versions
            21, 0, 0, 0, 0, 0, 0, 0, // size hint for sequence
            155, 0, 0, 0, 0, 0, 0, 0, // elements seen
            0, 0, 0, 0, 0, 0, 176, 63, // frequency (f64 encoding of 0.0625)
            17, 0, 0, 0, 0, 0, 0, 0,  // elements tracked
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

        let combined = super::freq_agg::serialize(
            &super::freq_agg::combine(
                Some(&super::freq_agg::deserialize(first)),
                Some(&super::freq_agg::deserialize(second)),
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
            21, 0, 0, 0, 0, 0, 0, 0, // size hint for sequence
            210, 0, 0, 0, 0, 0, 0, 0, // elements seen
            0, 0, 0, 0, 0, 0, 176, 63, // frequency (f64 encoding of 0.0625)
            17, 0, 0, 0, 0, 0, 0, 0,  // elements tracked
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
}
