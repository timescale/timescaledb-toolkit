use pgx::{iter::SetOfIterator, *};

use crate::nmost::*;

use crate::{
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};

use std::cmp::Reverse;

type MaxIntTransType = NMostTransState<Reverse<i64>>;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct MaxInts <'input> {
            capacity : u32,
            elements : u32,
            values : [i64; self.elements],
        }
    }
    ron_inout_funcs!(MaxInts);

    impl<'input> From<&mut MaxIntTransType> for MaxInts<'input> {
        fn from(item: &mut MaxIntTransType) -> Self {
            let heap = std::mem::take(&mut item.heap);
            unsafe {
                flatten!(MaxInts {
                    capacity: item.capacity as u32,
                    elements: heap.len() as u32,
                    values: heap
                        .into_sorted_vec()
                        .into_iter()
                        .map(|x| x.0)
                        .collect::<Vec<i64>>()
                        .into()
                })
            }
        }
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_int_trans(
    state: Internal,
    value: i64,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_function(
        unsafe { state.to_inner::<MaxIntTransType>() },
        Reverse(value),
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_int_rollup_trans(
    state: Internal,
    value: toolkit_experimental::MaxInts<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let values: Vec<Reverse<i64>> = value.values.clone().into_iter().map(Reverse).collect();
    nmost_rollup_trans_function(
        unsafe { state.to_inner::<MaxIntTransType>() },
        &values,
        value.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_int_combine(state1: Internal, state2: Internal) -> Option<Internal> {
    nmost_trans_combine(unsafe { state1.to_inner::<MaxIntTransType>() }, unsafe {
        state2.to_inner::<MaxIntTransType>()
    })
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_int_serialize(state: Internal) -> bytea {
    let state: Inner<MaxIntTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_int_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MaxIntTransType = crate::do_deserialize!(bytes, MaxIntTransType);
    Internal::new(i).into()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_int_final(state: Internal) -> toolkit_experimental::MaxInts<'static> {
    unsafe { &mut *state.to_inner::<MaxIntTransType>().unwrap() }.into()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_array",
    immutable,
    parallel_safe
)]
pub fn max_n_int_to_array(agg: toolkit_experimental::MaxInts<'static>) -> Vec<i64> {
    agg.values.clone().into_vec()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_values",
    immutable,
    parallel_safe
)]
pub fn max_n_int_to_values(agg: toolkit_experimental::MaxInts<'static>) -> SetOfIterator<i64> {
    SetOfIterator::new(agg.values.clone().into_iter())
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.max_n(\n\
        value bigint, capacity bigint\n\
    ) (\n\
        sfunc = toolkit_experimental.max_n_int_trans,\n\
        stype = internal,\n\
        combinefunc = toolkit_experimental.max_n_int_combine,\n\
        parallel = safe,\n\
        serialfunc = toolkit_experimental.max_n_int_serialize,\n\
        deserialfunc = toolkit_experimental.max_n_int_deserialize,\n\
        finalfunc = toolkit_experimental.max_n_int_final\n\
    );\n\
",
    name = "max_n_int",
    requires = [
        max_n_int_trans,
        max_n_int_final,
        max_n_int_combine,
        max_n_int_serialize,
        max_n_int_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.rollup(\n\
        value toolkit_experimental.MaxInts\n\
    ) (\n\
        sfunc = toolkit_experimental.max_n_int_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = toolkit_experimental.max_n_int_combine,\n\
        parallel = safe,\n\
        serialfunc = toolkit_experimental.max_n_int_serialize,\n\
        deserialfunc = toolkit_experimental.max_n_int_deserialize,\n\
        finalfunc = toolkit_experimental.max_n_int_final\n\
    );\n\
",
    name = "max_n_int_rollup",
    requires = [
        max_n_int_rollup_trans,
        max_n_int_final,
        max_n_int_combine,
        max_n_int_serialize,
        max_n_int_deserialize
    ],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn max_int_correctness() {
        Spi::connect(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE data(val INT8, category INT)", None, None);

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client.select(
                    &format!("INSERT INTO data VALUES ({}, {})", i, i % 4),
                    None,
                    None,
                );
            }

            // Test into_array
            let result =
                client.select("SELECT toolkit_experimental.into_array(toolkit_experimental.max_n(val, 5)) from data",
                    None, None,
                ).unwrap().first().get_one::<Vec<i64>>().unwrap();
            assert_eq!(result.unwrap(), vec![99, 98, 97, 96, 95]);

            // Test into_values
            let mut result =
                client.select("SELECT toolkit_experimental.into_values(toolkit_experimental.max_n(val, 3))::TEXT from data",
                    None, None,
                ).unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("99"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("98"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("97"));
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.select(
                    "WITH aggs as (SELECT category, toolkit_experimental.max_n(val, 5) as agg from data GROUP BY category)
                        SELECT toolkit_experimental.into_array(toolkit_experimental.rollup(agg)) FROM aggs",
                        None, None,
                    ).unwrap().first().get_one::<Vec<i64>>().unwrap();
            assert_eq!(result.unwrap(), vec![99, 98, 97, 96, 95]);
        })
    }
}
