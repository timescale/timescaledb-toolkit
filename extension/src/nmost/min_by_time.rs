use pgx::{iter::TableIterator, *};

use crate::nmost::min_time::toolkit_experimental::*;
use crate::nmost::*;

use crate::{
    build, flatten,
    palloc::{Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

type MinByTimeTransType = NMostByTransState<pg_sys::TimestampTz>;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct MinByTimes<'input> {
            values: MinTimesData<'input>,  // Nesting pg_types adds 8 bytes of header
            data: DatumStore<'input>,
        }
    }
    ron_inout_funcs!(MinByTimes);

    impl<'input> From<MinByTimeTransType> for MinByTimes<'input> {
        fn from(item: MinByTimeTransType) -> Self {
            let (capacity, val_ary, data) = item.into_sorted_parts();
            unsafe {
                flatten!(MinByTimes {
                    values: build!(MinTimes {
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
pub fn min_n_by_time_trans(
    state: Internal,
    value: crate::raw::TimestampTz,
    data: AnyElement,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_by_trans_function(
        unsafe { state.to_inner::<MinByTimeTransType>() },
        value.into(),
        data,
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_by_time_rollup_trans(
    state: Internal,
    value: toolkit_experimental::MinByTimes<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_by_rollup_trans_function(
        unsafe { state.to_inner::<MinByTimeTransType>() },
        value.values.values.as_slice(),
        &value.data,
        value.values.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_by_time_final(state: Internal) -> toolkit_experimental::MinByTimes<'static> {
    unsafe { state.to_inner::<MinByTimeTransType>().unwrap().clone() }.into()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_values",
    immutable,
    parallel_safe
)]
pub fn min_n_by_time_to_values(
    agg: toolkit_experimental::MinByTimes<'static>,
    _dummy: Option<AnyElement>,
) -> TableIterator<
    'static,
    (
        name!(value, crate::raw::TimestampTz),
        name!(data, AnyElement),
    ),
> {
    TableIterator::new(
        agg.values
            .values
            .clone()
            .into_iter()
            .map(crate::raw::TimestampTz::from)
            .zip(agg.data.clone().into_anyelement_iter()),
    )
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.min_n_by(\n\
        value timestamptz, data AnyElement, capacity bigint\n\
    ) (\n\
        sfunc = toolkit_experimental.min_n_by_time_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.min_n_by_time_final\n\
    );\n\
",
    name = "min_n_by_time",
    requires = [min_n_by_time_trans, min_n_by_time_final],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.rollup(\n\
        toolkit_experimental.MinByTimes\n\
    ) (\n\
        sfunc = toolkit_experimental.min_n_by_time_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.min_n_by_time_final\n\
    );\n\
",
    name = "min_n_by_time_rollup",
    requires = [min_n_by_time_rollup_trans, min_n_by_time_final],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn min_by_time_correctness() {
        Spi::connect(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select(
                "CREATE TABLE data(val TIMESTAMPTZ, category INT)",
                None,
                None,
            );

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client.select(
                    &format!("INSERT INTO data VALUES ('2020-1-1 UTC'::timestamptz + {} * '1d'::interval, {})", i, i % 4),
                    None,
                    None,
                );
            }

            // Test into_values
            let mut result =
                client.select("SELECT toolkit_experimental.into_values(toolkit_experimental.min_n_by(val, data, 3), NULL::data)::TEXT from data",
                    None, None,
                );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-01 00:00:00+00\",\"(\"\"2020-01-01 00:00:00+00\"\",0)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-02 00:00:00+00\",\"(\"\"2020-01-02 00:00:00+00\"\",1)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-03 00:00:00+00\",\"(\"\"2020-01-03 00:00:00+00\"\",2)\")")
            );
            assert!(result.next().is_none());

            // Test rollup
            let mut result =
                client.select(
                    "WITH aggs as (SELECT category, toolkit_experimental.min_n_by(val, data, 5) as agg from data GROUP BY category)
                        SELECT toolkit_experimental.into_values(toolkit_experimental.rollup(agg), NULL::data)::TEXT FROM aggs",
                        None, None,
                    );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-01 00:00:00+00\",\"(\"\"2020-01-01 00:00:00+00\"\",0)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-02 00:00:00+00\",\"(\"\"2020-01-02 00:00:00+00\"\",1)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-03 00:00:00+00\",\"(\"\"2020-01-03 00:00:00+00\"\",2)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-04 00:00:00+00\",\"(\"\"2020-01-04 00:00:00+00\"\",3)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("(\"2020-01-05 00:00:00+00\",\"(\"\"2020-01-05 00:00:00+00\"\",0)\")")
            );
            assert!(result.next().is_none());
        })
    }
}
