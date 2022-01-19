//! Based on the paper: https://cs.ucsb.edu/sites/default/files/documents/2005-23.pdf

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use pgx::*;

use pg_sys::{Datum, Oid};

use flat_serialize::*;

use crate::{
    aggregate_utils::{get_collation, in_aggregate_context},
    datum_utils::{DatumHashBuilder, DatumStore, deep_copy_datum},
    ron_inout_funcs,
    build,
    palloc::{Internal, InternalAsValue, Inner, ToInternal}, 
    pg_type,
};

use crate::frequency::toolkit_experimental::{FrequencyAggregate};

// Unable to implement PartialEq for AnyElement, so creating a local copy
struct LocalAnyElement {
    datum: Datum,
    typoid: Oid,
}

impl PartialEq for LocalAnyElement {
    // JOSH TODO should probably store the fn pointer instead of the OID (OID or another fn ptr will also be needed for serialization)
    #[allow(clippy::field_reassign_with_default)]
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            if self.typoid != other.typoid {
                false
            } else {
                let typ = self.typoid;
                let tentry =
                    pg_sys::lookup_type_cache(typ, pg_sys::TYPECACHE_EQ_OPR_FINFO as _);
            
                let flinfo = if (*tentry).eq_opr_finfo.fn_addr.is_some() {
                    &(*tentry).eq_opr_finfo
                } else {
                    pgx::error!("no equality function");
                };

                let mut info = pg_sys::FunctionCallInfoBaseData::default();
        
                info.flinfo = flinfo as *const pg_sys::FmgrInfo as *mut pg_sys::FmgrInfo;
                info.context = std::ptr::null_mut();
                info.resultinfo = std::ptr::null_mut();
                info.fncollation = (*tentry).typcollation;
                info.isnull = false;
                info.nargs = 2;

                info.args.as_mut_slice(2)[0] = pg_sys::NullableDatum {
                    value: self.datum,
                    isnull: false,
                };
                info.args.as_mut_slice(2)[1] = pg_sys::NullableDatum {
                    value: other.datum,
                    isnull: false,
                };
                (*info.flinfo).fn_addr.unwrap()(&mut info) != 0
            }
        }
    }
}

impl Eq for LocalAnyElement {}

impl Hash for LocalAnyElement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datum.hash(state);
    }
}

impl From<AnyElement> for LocalAnyElement {
    fn from(other: AnyElement) -> Self {
        LocalAnyElement {
            datum: other.datum(),
            typoid: other.oid(),
        }
    }
}

#[derive(Clone)]
struct FrequencyEntry{
    value: Datum,
    count: u64,
    overcount: u64,
}

const MIN_SIZE: usize = 10;
const MAX_NOISE_RATIO: f64 = 0.9;

pub struct FrequencyTransState {
    entries: Vec<FrequencyEntry>,
    indicies: HashMap<LocalAnyElement, usize, DatumHashBuilder>,
    total_vals: u64,
    min_freq: f64,
    typ: Oid, // JOSH TODO redundant with the DatumHashBuilder?
    complete: bool,  // true if all seen values in entries
}

impl FrequencyTransState {
    unsafe fn from_type_id(min_freq: f64, typ: pg_sys::Oid, collation: Option<Oid>) -> Self {
        FrequencyTransState {
            entries: vec![],
            indicies: HashMap::with_hasher(DatumHashBuilder::from_type_id(typ, collation)),
            total_vals: 0,
            min_freq,
            typ,
            complete: true,
        }
    }

    fn add(&mut self, element: LocalAnyElement) {
        self.total_vals += 1;
        if let Some(idx) = self.indicies.get(&element) {
            let idx = *idx;
            self.entries[idx].count += 1;
            self.move_left(idx);
        } else {
            // TODO: might be inefficient to call should_grow on every iteration
            if self.entries.len() < MIN_SIZE || self.should_grow() {
                let new_idx = self.entries.len();
                let overcount = if self.complete { 0 } else { self.entries.last().unwrap().overcount };
                unsafe {
                    self.entries.push(
                        FrequencyEntry {
                            value: deep_copy_datum(element.datum, element.typoid),
                            count: 1 + overcount,
                            overcount,
                        }
                    );
                }
                // Important to create the indices entry using the datum in the local context
                self.indicies.insert(LocalAnyElement{ datum: self.entries[new_idx].value, typoid: self.typ }, new_idx);
            } else {
                self.complete = false;
                let new_value = unsafe { deep_copy_datum(element.datum, element.typoid) };

                // TODO: might be more efficient to replace the lowest indexed tail value (count matching last) and not call move_up
                let entry = self.entries.last_mut().unwrap();
                self.indicies.remove(&LocalAnyElement { datum: entry.value, typoid: self.typ });
                entry.value = new_value; // JOSH FIXME should we pfree() old value if by-ref?
                entry.overcount = entry.count;
                entry.count += 1;
                self.indicies.insert(LocalAnyElement{ datum: new_value, typoid: self.typ }, self.entries.len() - 1);
                self.move_left(self.entries.len() - 1);
            }
        }
    }

    fn should_grow(&self) -> bool {
        let mut used_count = 0;  // This will be the sum of the counts of elements that occur more than min_freq

        let mut i = 0;
        while i < self.entries.len() && self.entries[i].count as f64 / self.total_vals as f64 > self.min_freq {
            used_count += self.entries[i].count - self.entries[i].overcount;  // Would just using count here introduce too much error?
            i += 1
        }

        if i == self.entries.len() {
            true
        } else {
            // At this point the first 'i' entries are all of the elements that occur more than 'min_freq' and account for 'used_count' of all the entries encountered so far.

            // Noise threshold is the count below which we don't track values (this will be approximately the overcount of churning buckets)
            let noise_threhold = self.min_freq * MAX_NOISE_RATIO * self.total_vals as f64;

            // We compute our target size as 'i' plus the amount of buckets the remaining entries could be divided among if there are no values occuring between 'min_freq' and 'noise_threshold'
            let remainder = self.total_vals - used_count;
            let target_size = f64::ceil(remainder as f64 / noise_threhold) as usize + i;

            self.entries.len() < target_size
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
        let element_for_i = LocalAnyElement{ datum: self.entries[i].value, typoid: self.typ };
        *self.indicies.get_mut(&element_for_i).unwrap() = i;
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn freq_trans(
    state: Internal,
    freq: f64,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    freq_trans_inner(unsafe{ state.to_inner() }, freq, value, fcinfo).internal()
}
pub fn freq_trans_inner(
    state: Option<Inner<FrequencyTransState>>,
    freq: f64,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<FrequencyTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                Some(value) => value
            };
            let mut state = match state {
                None => {
                    let typ = value.oid();
                    let collation = get_collation(fcinfo);
                    FrequencyTransState::from_type_id(freq, typ, collation).into()
                },
                Some(state) => state,
            };

            state.add(value.into());
            Some(state)
        })
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

    impl <'input> From<Internal> for FrequencyAggregate<'input> {
        fn from(trans: Internal) -> Self {
            Self::from(unsafe { trans.to_inner().unwrap() } )
        }
    }

    impl <'input> From<Inner<FrequencyTransState>> for FrequencyAggregate<'input> {
        fn from(trans: Inner<FrequencyTransState>) -> Self {
            let mut values = Vec::new();
            let mut counts = Vec::new();
            let mut overcounts = Vec::new();
            
            for entry in &trans.entries {
                values.push(entry.value);
                counts.push(entry.count);
                overcounts.push(entry.overcount);
            }

            build!{
                FrequencyAggregate {
                    type_oid: trans.typ as _,
                    num_values: trans.entries.len() as _,
                    values_seen: trans.total_vals,
                    min_freq: trans.min_freq,
                    counts: counts.into(),
                    overcounts: overcounts.into(),
                    datums: DatumStore::from((trans.typ, values)),
                }
            }
        }
    }

    ron_inout_funcs!(FrequencyAggregate);
}

// PG function to generate a user-facing TopN object from a InternalTopN.
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn freq_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::FrequencyAggregate<'static>> {
    unsafe {
        freq_final_inner(state.to_inner(), fcinfo)
    }
}
fn freq_final_inner(
    state: Option<Inner<FrequencyTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::FrequencyAggregate<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };

            Some(FrequencyAggregate::from(state))
        })
    }
}

// TODO: add combinefunc, serialfunc, deserialfunc, and make this parallel safe
extension_sql!("\n\
    CREATE AGGREGATE toolkit_experimental.freq_agg(size double precision, value AnyElement)\n\
    (\n\
        sfunc = toolkit_experimental.freq_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.freq_final\n\
    );\n\
    ",
name = "freq_agg"); // TODO requires

#[pg_extern(immutable, parallel_safe, name="values", schema = "toolkit_experimental")]
pub fn freq_iter (
    agg: FrequencyAggregate<'_>,
    ty: AnyElement
) -> impl std::iter::Iterator<Item = (name!(value,AnyElement),name!(min_freq,f64),name!(max_freq,f64))> + '_ {
    unsafe {
        if ty.oid() != agg.type_oid {
            pgx::error!("mischatched types")
        }
        let counts = agg.counts.slice().iter().zip(agg.overcounts.slice().iter());
        agg.datums.clone().into_iter().zip(counts)
            .map_while(move |(value, (&count, &overcount))| {
                let total = agg.values_seen as f64;
                if count as f64 / total < agg.min_freq {
                    None
                } else {
                    let value = AnyElement::from_datum(value, false, agg.type_oid).unwrap();
                    let min_freq = (count - overcount) as f64 / total;
                    let max_freq = count as f64 / total;
                    Some((value, min_freq, max_freq))
                }
            })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn (
    agg: FrequencyAggregate<'_>,
    n: i32,
    ty: AnyElement
) -> impl std::iter::Iterator<Item = AnyElement> + '_ {
    unsafe {
        if ty.oid() != agg.type_oid {
            pgx::error!("mischatched types")
        }
        let iter = agg.datums.clone().into_iter().zip(agg.counts.slice().iter());
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
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select("SET TIMEZONE to UTC", None, None);
            client.select("CREATE TABLE test (data INTEGER, time TIMESTAMPTZ)", None, None);

            for i in (0..100).rev() {
                client.select(&format!("INSERT INTO test SELECT i, '2020-1-1'::TIMESTAMPTZ + ('{} days, ' || i::TEXT || ' seconds')::INTERVAL FROM generate_series({}, 99, 1) i", 100 - i, i), None, None);
            }

            let test = client.select("SELECT freq_agg(0.015, s.data)::TEXT FROM (SELECT data FROM test ORDER BY time) s", None, None)
                .first()
                .get_one::<String>().unwrap();
            let expected = "(version:1,type_oid:23,num_values:78,values_seen:5050,min_freq:0.015,counts:[100,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,84,83,82,81,81,80,80,79,78,78,77,76,76,76,75,75,75,75,75,75,75,75,75,75,75,75,75,75,75,75,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74],overcounts:[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,1,1,1,3,2,4,3,3,6,3,5,7,5,5,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,74,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73,73],datums:[23,\"99\",\"98\",\"97\",\"96\",\"95\",\"94\",\"93\",\"92\",\"91\",\"90\",\"89\",\"88\",\"87\",\"86\",\"85\",\"84\",\"81\",\"82\",\"83\",\"80\",\"77\",\"78\",\"75\",\"76\",\"79\",\"73\",\"74\",\"71\",\"69\",\"70\",\"72\",\"53\",\"54\",\"55\",\"56\",\"57\",\"58\",\"59\",\"60\",\"61\",\"62\",\"63\",\"64\",\"65\",\"66\",\"67\",\"68\",\"22\",\"23\",\"24\",\"25\",\"26\",\"27\",\"28\",\"29\",\"30\",\"31\",\"32\",\"33\",\"34\",\"35\",\"36\",\"37\",\"38\",\"39\",\"40\",\"41\",\"42\",\"43\",\"44\",\"45\",\"46\",\"47\",\"48\",\"49\",\"50\",\"51\",\"21\"])";
            assert_eq!(test, expected);
        });
    }
}
