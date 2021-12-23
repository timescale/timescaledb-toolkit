
use pgx::*;

use flat_serialize::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    ron_inout_funcs,
    build,
    palloc::{Internal, InternalAsValue, Inner, ToInternal}, pg_type
};

use spacesaving::SpaceSaving;
type InternalTopN = SpaceSaving<i64>;

use crate::raw::bytea;

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn_serialize(
    state: Internal,
) -> bytea {
    let state: Inner<InternalTopN> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe, strict,schema = "toolkit_experimental")]
pub fn topn_deserialize(
    bytes: bytea,
    _internal: Internal,
) -> Internal {
    let i: InternalTopN = crate::do_deserialize!(bytes, InternalTopN);
    Inner::from(i).internal()
}

use toolkit_experimental::{TopN, TopNData};

#[pg_schema]
pub mod toolkit_experimental {
    pub(crate) use super::*;
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

    ron_inout_funcs!(TopN);
}

impl<'input> TopN<'input> {
    fn internal_topn(&self) -> InternalTopN {
        InternalTopN::new_from_components(
            1.0 / self.max_values as f64,
            self.values.slice(),
            self.counts.slice(),
            self.overcounts.slice(),
            self.total_inputs
        )
    }

    fn from_internal_topn(topn: &InternalTopN) -> TopN<'static> {
        let mut values = Vec::new();
        let mut counts = Vec::new();
        let mut overcounts = Vec::new();

        topn.generate_component_data(&mut values, &mut counts, &mut overcounts);

        build!(
            TopN {
                num_values: topn.num_entries() as _,
                max_values: topn.max_entries() as _,
                total_inputs: topn.total_values(),
                values: values.into(),
                counts: counts.into(),
                overcounts: overcounts.into(),
            }
        )
    }
}

// PG function for adding values to a topn count.
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn_trans(
    state: Internal,
    size: i32,
    value: Option<i32>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    topn_trans_inner(unsafe{ state.to_inner()}, size, value, fcinfo).internal()
}

pub fn topn_trans_inner(
    state: Option<Inner<InternalTopN>>,
    size: i32,
    value: Option<i32>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<InternalTopN>> {
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
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    unsafe {
        topn_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal()
    }
}
pub fn topn_combine_inner(
    state1: Option<Inner<InternalTopN>>,
    state2: Option<Inner<InternalTopN>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<InternalTopN>> {
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
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::TopN<'static>> {
    topn_final_inner(unsafe{ state.to_inner() }, fcinfo)
}

fn topn_final_inner(
    state: Option<Inner<InternalTopN>>,
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

extension_sql!("\n\
    CREATE AGGREGATE toolkit_experimental.topn_agg(size int, value int)\n\
    (\n\
        sfunc = toolkit_experimental.topn_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.topn_final,\n\
        combinefunc = toolkit_experimental.topn_combine,\n\
        serialfunc = toolkit_experimental.topn_serialize,\n\
        deserialfunc = toolkit_experimental.topn_deserialize,\n\
        parallel = safe\n\
    );\n\
",
name="topn_agg",
requires= [topn_trans, topn_final, topn_combine, topn_serialize, topn_deserialize],
);

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn topn_compound_trans(
    state: Internal,
    value: Option<toolkit_experimental::TopN>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    topn_compound_trans_inner(unsafe{ state.to_inner() }, value, fcinfo).internal()
}
pub fn topn_compound_trans_inner(
    state: Option<Inner<InternalTopN>>,
    value: Option<toolkit_experimental::TopN>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<InternalTopN>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (a, None) => a,
                (None, Some(a)) => Some(a.internal_topn().into()),
                (Some(a), Some(b)) =>
                    Some(InternalTopN::combine(&a, &b.internal_topn()).into()),
            }
        })
    }
}

extension_sql!("\n\
    CREATE AGGREGATE toolkit_experimental.rollup(\n\
        toolkit_experimental.topn\n\
    ) (\n\
        sfunc = toolkit_experimental.topn_compound_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.topn_final,\n\
        combinefunc = toolkit_experimental.topn_combine,\n\
        serialfunc = toolkit_experimental.topn_serialize,\n\
        deserialfunc = toolkit_experimental.topn_deserialize,\n\
        parallel = safe\n\
    );\n\
",
name="topn_rollup",
requires= [topn_compound_trans, topn_final, topn_combine, topn_serialize, topn_deserialize],
);

//---- Available PG operations on the topn structure
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn num_vals(
    agg: toolkit_experimental::TopN,
) -> i32 {
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
    agg: toolkit_experimental::TopN<'_>,
) -> impl std::iter::Iterator<Item = (name!(value,i64),name!(min_freq,f64),name!(max_freq,f64))> + '_ {
    assert!(n <= agg.num_values as _);
    let total = agg.total_inputs as f64;
    // TODO replace filter_map() with map_while() once that's stable
    (0..n as usize).filter_map(move |i| {
        let val = *agg.values.slice().get(i)?;
        let count = agg.counts.slice()[i];
        let over = agg.overcounts.slice()[i];
        (val, (count-over) as f64 / total, count as f64 / total).into()
    })
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn guaranteed_topn(
    n: i32,
    agg: toolkit_experimental::TopN,
) -> bool {
    if n >= agg.num_values as _ {
        return false;
    }

    let bound = agg.counts.slice()[n as usize];
    for i in 0..n as usize {
        if agg.counts.slice()[i] - agg.overcounts.slice()[i] < bound {
            return false;
        }
    }

    true
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn max_ordered_n(
    agg: toolkit_experimental::TopN,
) -> i32 {
    for i in 1..agg.num_values as usize {
        if agg.counts.slice()[i] > agg.counts.slice()[i-1] - agg.overcounts.slice()[i-1] {
            return (i - 1) as _;
        }
    }

    (agg.num_values - 1) as _
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

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
