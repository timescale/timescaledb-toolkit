use pgrx::{iter::TableIterator, *};

use crate::nmost::min_int::*;
use crate::nmost::*;

use crate::{
    build, flatten,
    palloc::{Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

type MinByIntTransType = NMostByTransState<i64>;

pg_type! {
    #[derive(Debug)]
    struct MinByInts<'input> {
        values: MinIntsData<'input>,  // Nesting pg_types adds 8 bytes of header
        data: DatumStore<'input>,
    }
}
ron_inout_funcs!(MinByInts<'input>);

impl<'input> From<MinByIntTransType> for MinByInts<'input> {
    fn from(item: MinByIntTransType) -> Self {
        let (capacity, val_ary, data) = item.into_sorted_parts();
        unsafe {
            flatten!(MinByInts {
                values: build!(MinInts {
                    capacity: capacity as u32,
                    elements: val_ary.len() as u32,
                    values: val_ary.into()
                })
                .0,
                data,
            })
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_by_int_trans(
    state: Internal,
    value: i64,
    data: AnyElement,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_by_trans_function(
        unsafe { state.to_inner::<MinByIntTransType>() },
        value,
        data,
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_by_int_rollup_trans(
    state: Internal,
    value: MinByInts<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_by_rollup_trans_function(
        unsafe { state.to_inner::<MinByIntTransType>() },
        value.values.values.as_slice(),
        &value.data,
        value.values.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_by_int_final(state: Internal) -> MinByInts<'static> {
    unsafe { state.to_inner::<MinByIntTransType>().unwrap().clone() }.into()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn min_n_by_int_to_values(
    agg: MinByInts<'static>,
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
    CREATE AGGREGATE min_n_by(\n\
        value bigint, data AnyElement, capacity bigint\n\
    ) (\n\
        sfunc = min_n_by_int_trans,\n\
        stype = internal,\n\
        finalfunc = min_n_by_int_final\n\
    );\n\
",
    name = "min_n_by_int",
    requires = [min_n_by_int_trans, min_n_by_int_final],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        MinByInts\n\
    ) (\n\
        sfunc = min_n_by_int_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = min_n_by_int_final\n\
    );\n\
",
    name = "min_n_by_int_rollup",
    requires = [min_n_by_int_rollup_trans, min_n_by_int_final],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn min_by_int_correctness() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            client
                .update("CREATE TABLE data(val INT8, category INT)", None, &[])
                .unwrap();

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client
                    .update(
                        &format!("INSERT INTO data VALUES ({}, {})", i, i % 4),
                        None,
                        &[],
                    )
                    .unwrap();
            }

            // Test into_values
            let mut result = client
                .update(
                    "SELECT into_values(min_n_by(val, data, 3), NULL::data)::TEXT from data",
                    None,
                    &[],
                )
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(0,\"(0,0)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(1,\"(1,1)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(2,\"(2,2)\")")
            );
            assert!(result.next().is_none());

            // Test rollup
            let mut result =
                client.update(
                    "WITH aggs as (SELECT category, min_n_by(val, data, 5) as agg from data GROUP BY category)
                        SELECT into_values(rollup(agg), NULL::data)::TEXT FROM aggs",
                        None, &[],
                    ).unwrap();
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(0,\"(0,0)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(1,\"(1,1)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(2,\"(2,2)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(3,\"(3,3)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(4,\"(4,0)\")")
            );
            assert!(result.next().is_none());
        })
    }
}
