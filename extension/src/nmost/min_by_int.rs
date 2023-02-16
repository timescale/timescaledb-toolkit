use pgx::{iter::TableIterator, *};

use crate::nmost::min_int::toolkit_experimental::*;
use crate::nmost::*;

use crate::{
    build, flatten,
    palloc::{Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

type MinByIntTransType = NMostByTransState<i64>;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct MinByInts<'input> {
            values: MinIntsData<'input>,  // Nesting pg_types adds 8 bytes of header
            data: DatumStore<'input>,
        }
    }
    ron_inout_funcs!(MinByInts);

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
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
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

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_by_int_rollup_trans(
    state: Internal,
    value: toolkit_experimental::MinByInts<'static>,
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

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_by_int_final(state: Internal) -> toolkit_experimental::MinByInts<'static> {
    unsafe { state.to_inner::<MinByIntTransType>().unwrap().clone() }.into()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_values",
    immutable,
    parallel_safe
)]
pub fn min_n_by_int_to_values(
    agg: toolkit_experimental::MinByInts<'static>,
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
    CREATE AGGREGATE toolkit_experimental.min_n_by(\n\
        value bigint, data AnyElement, capacity bigint\n\
    ) (\n\
        sfunc = toolkit_experimental.min_n_by_int_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.min_n_by_int_final\n\
    );\n\
",
    name = "min_n_by_int",
    requires = [min_n_by_int_trans, min_n_by_int_final],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.rollup(\n\
        toolkit_experimental.MinByInts\n\
    ) (\n\
        sfunc = toolkit_experimental.min_n_by_int_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.min_n_by_int_final\n\
    );\n\
",
    name = "min_n_by_int_rollup",
    requires = [min_n_by_int_rollup_trans, min_n_by_int_final],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn min_by_int_correctness() {
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
            let mut result =
                client.update("SELECT toolkit_experimental.into_values(toolkit_experimental.min_n_by(val, data, 3), NULL::data)::TEXT from data",
                    None, None,
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
            assert!(result.next().is_none());

            // Test rollup
            let mut result =
                client.update(
                    "WITH aggs as (SELECT category, toolkit_experimental.min_n_by(val, data, 5) as agg from data GROUP BY category)
                        SELECT toolkit_experimental.into_values(toolkit_experimental.rollup(agg), NULL::data)::TEXT FROM aggs",
                        None, None,
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
