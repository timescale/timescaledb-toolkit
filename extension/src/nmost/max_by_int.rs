use pgx::{iter::TableIterator, *};

use crate::nmost::max_int::*;
use crate::nmost::*;

use crate::{
    build, flatten,
    palloc::{Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

use std::cmp::Reverse;

type MaxByIntTransType = NMostByTransState<Reverse<i64>>;

pg_type! {
    #[derive(Debug)]
    struct MaxByInts<'input> {
        values: MaxIntsData<'input>,  // Nesting pg_types adds 8 bytes of header
        data: DatumStore<'input>,
    }
}
ron_inout_funcs!(MaxByInts);

impl<'input> From<MaxByIntTransType> for MaxByInts<'input> {
    fn from(item: MaxByIntTransType) -> Self {
        let (capacity, val_ary, data) = item.into_sorted_parts();
        unsafe {
            flatten!(MaxByInts {
                values: build!(MaxInts {
                    capacity: capacity as u32,
                    elements: val_ary.len() as u32,
                    values: val_ary
                        .into_iter()
                        .map(|x| x.0)
                        .collect::<Vec<i64>>()
                        .into()
                })
                .0,
                data,
            })
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_by_int_trans(
    state: Internal,
    value: i64,
    data: AnyElement,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_by_trans_function(
        unsafe { state.to_inner::<MaxByIntTransType>() },
        Reverse(value),
        data,
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_by_int_rollup_trans(
    state: Internal,
    value: MaxByInts<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let values: Vec<Reverse<i64>> = value
        .values
        .values
        .clone()
        .into_iter()
        .map(Reverse)
        .collect();
    nmost_by_rollup_trans_function(
        unsafe { state.to_inner::<MaxByIntTransType>() },
        &values,
        &value.data,
        value.values.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_by_int_final(state: Internal) -> MaxByInts<'static> {
    unsafe { state.to_inner::<MaxByIntTransType>().unwrap().clone() }.into()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn max_n_by_int_to_values(
    agg: MaxByInts<'static>,
    _dummy: Option<AnyElement>,
) -> TableIterator<'static, (name!(value, i64), name!(data, AnyElement))> {
    TableIterator::new(
        agg.values
            .values
            .clone()
            .into_iter()
            .zip(agg.data.clone().into_anyelement_iter()),
    )
}

extension_sql!(
    "\n\
    CREATE AGGREGATE max_n_by(\n\
        value bigint, data AnyElement, capacity bigint\n\
    ) (\n\
        sfunc = max_n_by_int_trans,\n\
        stype = internal,\n\
        finalfunc = max_n_by_int_final\n\
    );\n\
",
    name = "max_n_by_int",
    requires = [max_n_by_int_trans, max_n_by_int_final],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        MaxByInts\n\
    ) (\n\
        sfunc = max_n_by_int_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = max_n_by_int_final\n\
    );\n\
",
    name = "max_n_by_int_rollup",
    requires = [max_n_by_int_rollup_trans, min_n_by_int_final],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn max_by_int_correctness() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();
            client
                .update("CREATE TABLE data(val INT8, category INT)", None, None)
                .unwrap();

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client
                    .update(
                        &format!("INSERT INTO data VALUES ({}, {})", i, i % 4),
                        None,
                        None,
                    )
                    .unwrap();
            }

            // Test into_values
            let mut result = client
                .update(
                    "SELECT into_values(max_n_by(val, data, 3), NULL::data)::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(99,\"(99,3)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(98,\"(98,2)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(97,\"(97,1)\")")
            );
            assert!(result.next().is_none());

            // Test rollup
            let mut result =
                client.update(
                    "WITH aggs as (SELECT category, max_n_by(val, data, 5) as agg from data GROUP BY category)
                        SELECT into_values(rollup(agg), NULL::data)::TEXT FROM aggs",
                        None, None,
                    ).unwrap();
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(99,\"(99,3)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(98,\"(98,2)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(97,\"(97,1)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(96,\"(96,0)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(95,\"(95,3)\")")
            );
            assert!(result.next().is_none());
        })
    }
}
