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

type MaxTimeTransType = NMostTransState<Reverse<pg_sys::TimestampTz>>;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct MaxTimes <'input> {
            capacity : u32,
            elements : u32,
            values : [pg_sys::TimestampTz; self.elements],
        }
    }
    ron_inout_funcs!(MaxTimes);

    impl<'input> From<&mut MaxTimeTransType> for MaxTimes<'input> {
        fn from(item: &mut MaxTimeTransType) -> Self {
            let heap = std::mem::take(&mut item.heap);
            unsafe {
                flatten!(MaxTimes {
                    capacity: item.capacity as u32,
                    elements: heap.len() as u32,
                    values: heap
                        .into_sorted_vec()
                        .into_iter()
                        .map(|x| x.0)
                        .collect::<Vec<pg_sys::TimestampTz>>()
                        .into()
                })
            }
        }
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_time_trans(
    state: Internal,
    value: crate::raw::TimestampTz,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_function(
        unsafe { state.to_inner::<MaxTimeTransType>() },
        Reverse(value.into()),
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_time_rollup_trans(
    state: Internal,
    value: toolkit_experimental::MaxTimes<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let values: Vec<Reverse<pg_sys::TimestampTz>> =
        value.values.clone().into_iter().map(Reverse).collect();
    nmost_rollup_trans_function(
        unsafe { state.to_inner::<MaxTimeTransType>() },
        &values,
        value.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_time_combine(state1: Internal, state2: Internal) -> Option<Internal> {
    nmost_trans_combine(unsafe { state1.to_inner::<MaxTimeTransType>() }, unsafe {
        state2.to_inner::<MaxTimeTransType>()
    })
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_time_serialize(state: Internal) -> bytea {
    let state: Inner<MaxTimeTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_time_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MaxTimeTransType = crate::do_deserialize!(bytes, MaxTimeTransType);
    Internal::new(i).into()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn max_n_time_final(state: Internal) -> toolkit_experimental::MaxTimes<'static> {
    unsafe { &mut *state.to_inner::<MaxTimeTransType>().unwrap() }.into()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_array",
    immutable,
    parallel_safe
)]
pub fn max_n_time_to_array(
    agg: toolkit_experimental::MaxTimes<'static>,
) -> Vec<crate::raw::TimestampTz> {
    agg.values
        .clone()
        .into_iter()
        .map(crate::raw::TimestampTz::from)
        .collect()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_values",
    immutable,
    parallel_safe
)]
pub fn max_n_time_to_values(
    agg: toolkit_experimental::MaxTimes<'static>,
) -> SetOfIterator<crate::raw::TimestampTz> {
    SetOfIterator::new(
        agg.values
            .clone()
            .into_iter()
            .map(crate::raw::TimestampTz::from),
    )
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.max_n(\n\
        value timestamptz, capacity bigint\n\
    ) (\n\
        sfunc = toolkit_experimental.max_n_time_trans,\n\
        stype = internal,\n\
        combinefunc = toolkit_experimental.max_n_time_combine,\n\
        parallel = safe,\n\
        serialfunc = toolkit_experimental.max_n_time_serialize,\n\
        deserialfunc = toolkit_experimental.max_n_time_deserialize,\n\
        finalfunc = toolkit_experimental.max_n_time_final\n\
    );\n\
",
    name = "max_n_time",
    requires = [
        max_n_time_trans,
        max_n_time_final,
        max_n_time_combine,
        max_n_time_serialize,
        max_n_time_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.rollup(\n\
        value toolkit_experimental.MaxTimes\n\
    ) (\n\
        sfunc = toolkit_experimental.max_n_time_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = toolkit_experimental.max_n_time_combine,\n\
        parallel = safe,\n\
        serialfunc = toolkit_experimental.max_n_time_serialize,\n\
        deserialfunc = toolkit_experimental.max_n_time_deserialize,\n\
        finalfunc = toolkit_experimental.max_n_time_final\n\
    );\n\
",
    name = "max_n_time_rollup",
    requires = [
        max_n_time_rollup_trans,
        max_n_time_final,
        max_n_time_combine,
        max_n_time_serialize,
        max_n_time_deserialize
    ],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn max_time_correctness() {
        Spi::execute(|client| {
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

            // Test into_array
            let result =
                client.select("SELECT toolkit_experimental.into_array(toolkit_experimental.max_n(val, 5))::TEXT from data",
                    None, None,
                ).first().get_one::<&str>();
            assert_eq!(result.unwrap(), "{\"2020-04-09 00:00:00+00\",\"2020-04-08 00:00:00+00\",\"2020-04-07 00:00:00+00\",\"2020-04-06 00:00:00+00\",\"2020-04-05 00:00:00+00\"}");

            // Test into_values
            let mut result =
                client.select("SELECT toolkit_experimental.into_values(toolkit_experimental.max_n(val, 3))::TEXT from data",
                    None, None,
                );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("2020-04-09 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("2020-04-08 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value(),
                Some("2020-04-07 00:00:00+00")
            );
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.select(
                    "WITH aggs as (SELECT category, toolkit_experimental.max_n(val, 5) as agg from data GROUP BY category)
                        SELECT toolkit_experimental.into_array(toolkit_experimental.rollup(agg))::TEXT FROM aggs",
                        None, None,
                    ).first().get_one::<&str>();
            assert_eq!(result.unwrap(), "{\"2020-04-09 00:00:00+00\",\"2020-04-08 00:00:00+00\",\"2020-04-07 00:00:00+00\",\"2020-04-06 00:00:00+00\",\"2020-04-05 00:00:00+00\"}");
        })
    }
}
