
use std::slice;

use pgx::*;

use flat_serialize::*;

use itertools::multizip;

use crate::{
    aggregate_utils::in_aggregate_context,
    json_inout_funcs,
    flatten,
    palloc::Internal, pg_type
};

use spacesaving::SpaceSaving;
type InternalTopN = SpaceSaving<i64>;

#[allow(non_camel_case_types)]
type int = u32;

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn_serialize(
    state: Internal<InternalTopN>,
) -> bytea {
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe, strict,schema = "toolkit_experimental")]
pub fn topn_deserialize(
    bytes: bytea,
    _internal: Option<Internal<InternalTopN>>,
) -> Internal<InternalTopN> {
    crate::do_deserialize!(bytes, InternalTopN)
}

// PG object for topn
pg_type! {
    #[derive(Debug)]
    struct TopN<'input> {
        num_values: u32,
        max_values: u32,
        total_inputs: u64,
        values: [i64; self.num_values],
        counts: [u64; self.num_values],
        overcounts: [u64; self.num_values],
    }
}

json_inout_funcs!(TopN);

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
pub mod toolkit_experimental {
    pub(crate) use super::*;
    varlena_type!(TopN);
}

impl<'input> TopN<'input> {
    fn to_internal_topn(&self) -> InternalTopN {
        InternalTopN::new_from_components(1. / self.max_values as f64, self.values, self.counts, self.overcounts, self.total_inputs)
    }

    fn from_internal_topn(topn: &InternalTopN) -> TopN<'static> {
        let mut values = Vec::new();
        let mut counts = Vec::new();
        let mut overcounts = Vec::new();
        
        topn.generate_component_data(&mut values, &mut counts, &mut overcounts);

        unsafe {
            flatten!(
                TopN {
                    num_values: topn.num_entries() as _,
                    max_values: topn.max_entries() as _,
                    total_inputs: topn.total_values(),
                    values: &values,
                    counts: &counts,
                    overcounts: &overcounts
                }
            )
        }
    }
}

// PG function for adding values to a topn count.
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn_trans(
    state: Option<Internal<InternalTopN>>,
    size: int,
    value: Option<int>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<InternalTopN>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                // NaNs are nonsensical in the context of a percentile, so exclude them
                Some(value) => value,
            };
            let mut state = match state {
                None => InternalTopN::new(1. / size as f64).into(),
                Some(state) => state,
            };
            state.add(value as _);
            Some(state)
        })
    }
}

// PG function for merging topns.
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn_combine(
    state1: Option<Internal<InternalTopN>>,
    state2: Option<Internal<InternalTopN>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<InternalTopN>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => Some(
                    InternalTopN::combine(&state1, &state2).into())
            }
        })
    }
}

// PG function to generate a user-facing TopN object from a InternalTopN.
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn topn_final(
    state: Option<Internal<InternalTopN>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::TopN<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };

            TopN::from_internal_topn(&state).into()
        })
    }
}

extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.topn_agg(size int, value int)
(
    sfunc = toolkit_experimental.topn_trans,
    stype = internal,
    finalfunc = toolkit_experimental.topn_final,
    combinefunc = toolkit_experimental.topn_combine,
    serialfunc = toolkit_experimental.topn_serialize,
    deserialfunc = toolkit_experimental.topn_deserialize,
    parallel = safe
);
"#);

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn_compound_trans<'b>(
    state: Option<Internal<InternalTopN>>,
    value: Option<toolkit_experimental::TopN<'b>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<InternalTopN>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (a, None) => a,
                (None, Some(a)) => Some(a.to_internal_topn().into()),
                (Some(a), Some(b)) =>
                    Some(InternalTopN::combine(&a, &b.to_internal_topn()).into()),
            }
        })
    }
}

extension_sql!(r#"
CREATE AGGREGATE toolkit_experimental.rollup(
    toolkit_experimental.topn
) (
    sfunc = toolkit_experimental.topn_compound_trans,
    stype = internal,
    finalfunc = toolkit_experimental.topn_final,
    combinefunc = toolkit_experimental.topn_combine,
    serialfunc = toolkit_experimental.topn_serialize,
    deserialfunc = toolkit_experimental.topn_deserialize,
    parallel = safe
);
"#);

//---- Available PG operations on the topn structure
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn num_vals<'input>(
    agg: toolkit_experimental::TopN<'input>,
) -> int {
    agg.total_inputs as _
}

// SAFETY
// Normally we require all lifetimes returned to postgres to be constrained to 'static; this
// ensure that we're not accidentally returning a value which may be freed before postgres
// uses it. We further require that arguments are _not_ 'static as this could result in
// squirreling away references to free'd memory. In this particular case we believe it's safe
// to relax these requirements since:
//   1. The returned value isn't really the iterator, which only lives for the pgx SRF glue
//      code, but the tuple of integers and floats returned by said iterator. This tuple does
//      have the requisite static lifetime.
//   2. The arguments to the SRF should live across multiple calls to the SRF, which is a 
//      lifetime rust is not capable of expressing, so we model it using 'static.
#[pg_extern(immutable, parallel_safe, name="topn", schema = "toolkit_experimental")]
pub fn topn_iter (
    n: i32,
    agg: toolkit_experimental::TopN<'static>,
) -> impl std::iter::Iterator<Item = (name!(value,i64),name!(min_freq,f64),name!(max_freq,f64))> + '_ {
    assert!(n <= agg.num_values as _);
    let total = agg.total_inputs as f64;
    multizip((agg.values.iter(), agg.counts.iter(), agg.overcounts.iter()))
    .take(n as _)
    .map(move |(val, count, over)| (*val, (count-over) as f64 / total, *count as f64 / total))
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn guaranteed_topn<'input>(
    n: i32,
    agg: toolkit_experimental::TopN<'input>,
) -> bool {
    if n >= agg.num_values as _ {
        return false;
    }

    let bound = agg.counts[n as usize];
    for i in 0..n as usize {
        if agg.counts[i] - agg.overcounts[i] < bound {
            return false;
        }
    }

    true
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn max_ordered_n<'input>(
    agg: toolkit_experimental::TopN<'input>,
) -> int {
    for i in 1..agg.num_values as usize {
        if agg.counts[i] > agg.counts[i-1] - agg.overcounts[i-1] {
            return (i - 1) as _;
        }
    }

    (agg.num_values - 1) as _
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_topn_aggregate() {
        Spi::execute(|client| {
            // using the search path trick for this test to make it easier to stabilize later on
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select("CREATE TABLE test (data INTEGER)", None, None);

            for i in 0..100 {
                client.select(&format!("INSERT INTO test SELECT generate_series({}, 99, 1)", i), None, None);
            }

            client.select("CREATE TABLE aggs (size INTEGER, agg TOPN)", None, None);
            client.select("INSERT INTO aggs SELECT n, (SELECT topn_agg(n, data) FROM test) FROM generate_series(25, 100, 25) n", None, None);

            for i in (25..=100).step_by(25) {
                let test =
                    client.select(&format!("SELECT num_vals(agg) FROM aggs WHERE size={}", i), None, None)
                        .first().get_one::<i32>().unwrap();
                assert_eq!(test, 5050);
            }

            let test =
                client.select("SELECT max_ordered_n(agg) FROM aggs WHERE size=100", None, None)
                    .first().get_one::<i32>().unwrap();
            assert_eq!(test, 99);
            for i in 1..100 {
                assert!(client.select(&format!("SELECT guaranteed_topn({}, agg) FROM aggs WHERE size=100", i), None, None).first().get_one::<bool>().unwrap(), "failed on i of {}", i);
            }

            // not having any real outliers makes it hard to guarantee a topn
            let test =
                client.select("SELECT max_ordered_n(agg) FROM aggs WHERE size=75", None, None)
                    .first().get_one::<i32>().unwrap();
            assert_eq!(test, 0);
            assert!(!client.select("SELECT guaranteed_topn(5, agg) FROM aggs WHERE size=75", None, None).first().get_one::<bool>().unwrap());

            // Test top result for each size
            let test = 
                client.select("SELECT value, min_freq, max_freq FROM topn(10, (SELECT agg FROM aggs WHERE size=100))", None, None)
                    .first().get_three::<i64, f64, f64>();
            assert_eq!(test, (Some(99), Some(100./5050.), Some(100./5050.)));

            let test = 
                client.select("SELECT value, min_freq, max_freq FROM topn(10, (SELECT agg FROM aggs WHERE size=75))", None, None)
                    .first().get_three::<i64, f64, f64>();
            assert_eq!(test, (Some(99), Some(76./5050.), Some(105./5050.)));

            let test = 
                client.select("SELECT value, min_freq, max_freq FROM topn(10, (SELECT agg FROM aggs WHERE size=50))", None, None)
                    .first().get_three::<i64, f64, f64>();
            assert_eq!(test, (Some(99), Some(51./5050.), Some(126./5050.)));

            let test = 
                client.select("SELECT value, min_freq, max_freq FROM topn(10, (SELECT agg FROM aggs WHERE size=25))", None, None)
                    .first().get_three::<i64, f64, f64>();
            assert_eq!(test, (Some(99), Some(26./5050.), Some(214./5050.)));


            let test =
                client.select("SELECT num_vals(rollup(agg)) FROM aggs", None, None)
                    .first().get_one::<i32>().unwrap();
            assert_eq!(test, 20200);          
            
            let test = 
                client.select("SELECT value, min_freq, max_freq FROM topn(10, (SELECT rollup(agg) FROM aggs))", None, None)
                    .first().get_three::<i64, f64, f64>();
            assert_eq!(test, (Some(99), Some(253./20200.), Some(545./20200.)));  
        });
    }
}
