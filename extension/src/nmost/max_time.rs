use pgrx::{iter::SetOfIterator, *};

use crate::nmost::*;

use crate::{
    accessors::{AccessorIntoArray, AccessorIntoValues},
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};

use std::cmp::Reverse;

type MaxTimeTransType = NMostTransState<Reverse<pg_sys::TimestampTz>>;

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

#[pg_extern(immutable, parallel_safe)]
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

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_time_rollup_trans(
    state: Internal,
    value: MaxTimes<'static>,
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

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_time_combine(state1: Internal, state2: Internal) -> Option<Internal> {
    nmost_trans_combine(unsafe { state1.to_inner::<MaxTimeTransType>() }, unsafe {
        state2.to_inner::<MaxTimeTransType>()
    })
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_time_serialize(state: Internal) -> bytea {
    let state: Inner<MaxTimeTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_time_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MaxTimeTransType = crate::do_deserialize!(bytes, MaxTimeTransType);
    Internal::new(i).into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_time_final(state: Internal) -> MaxTimes<'static> {
    unsafe { &mut *state.to_inner::<MaxTimeTransType>().unwrap() }.into()
}

#[pg_extern(name = "into_array", immutable, parallel_safe)]
pub fn max_n_time_to_array(agg: MaxTimes<'static>) -> Vec<crate::raw::TimestampTz> {
    agg.values
        .clone()
        .into_iter()
        .map(crate::raw::TimestampTz::from)
        .collect()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn max_n_time_to_values(agg: MaxTimes<'static>) -> SetOfIterator<crate::raw::TimestampTz> {
    SetOfIterator::new(
        agg.values
            .clone()
            .into_iter()
            .map(crate::raw::TimestampTz::from),
    )
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_max_time_into_values<'a>(
    agg: MaxTimes<'static>,
    _accessor: AccessorIntoValues<'a>,
) -> SetOfIterator<'a, crate::raw::TimestampTz> {
    max_n_time_to_values(agg)
}
#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_max_time_into_array<'a>(
    agg: MaxTimes<'static>,
    _accessor: AccessorIntoArray<'a>,
) -> Vec<crate::raw::TimestampTz> {
    max_n_time_to_array(agg)
}

extension_sql!(
    "\n\
    CREATE AGGREGATE max_n(\n\
        value timestamptz, capacity bigint\n\
    ) (\n\
        sfunc = max_n_time_trans,\n\
        stype = internal,\n\
        combinefunc = max_n_time_combine,\n\
        parallel = safe,\n\
        serialfunc = max_n_time_serialize,\n\
        deserialfunc = max_n_time_deserialize,\n\
        finalfunc = max_n_time_final\n\
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
    CREATE AGGREGATE rollup(\n\
        value MaxTimes\n\
    ) (\n\
        sfunc = max_n_time_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = max_n_time_combine,\n\
        parallel = safe,\n\
        serialfunc = max_n_time_serialize,\n\
        deserialfunc = max_n_time_deserialize,\n\
        finalfunc = max_n_time_final\n\
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
    use pgrx_macros::pg_test;

    #[pg_test]
    fn max_time_correctness() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();
            client
                .update(
                    "CREATE TABLE data(val TIMESTAMPTZ, category INT)",
                    None,
                    None,
                )
                .unwrap();

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client.update(
                    &format!("INSERT INTO data VALUES ('2020-1-1 UTC'::timestamptz + {} * '1d'::interval, {})", i, i % 4),
                    None,
                    None,
                ).unwrap();
            }

            // Test into_array
            let result = client
                .update(
                    "SELECT into_array(max_n(val, 5))::TEXT from data",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap();
            assert_eq!(result.unwrap(), "{\"2020-04-09 00:00:00+00\",\"2020-04-08 00:00:00+00\",\"2020-04-07 00:00:00+00\",\"2020-04-06 00:00:00+00\",\"2020-04-05 00:00:00+00\"}");
            let result = client
                .update(
                    "SELECT (max_n(val, 5)->into_array())::TEXT from data",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap();
            assert_eq!(result.unwrap(), "{\"2020-04-09 00:00:00+00\",\"2020-04-08 00:00:00+00\",\"2020-04-07 00:00:00+00\",\"2020-04-06 00:00:00+00\",\"2020-04-05 00:00:00+00\"}");

            // Test into_values
            let mut result = client
                .update(
                    "SELECT into_values(max_n(val, 3))::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-04-09 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-04-08 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-04-07 00:00:00+00")
            );
            assert!(result.next().is_none());
            let mut result = client
                .update(
                    "SELECT (max_n(val, 3)->into_values())::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-04-09 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-04-08 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-04-07 00:00:00+00")
            );
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.update(
                    "WITH aggs as (SELECT category, max_n(val, 5) as agg from data GROUP BY category)
                        SELECT into_array(rollup(agg))::TEXT FROM aggs",
                        None, None,
                    ).unwrap().first().get_one::<&str>().unwrap();
            assert_eq!(result.unwrap(), "{\"2020-04-09 00:00:00+00\",\"2020-04-08 00:00:00+00\",\"2020-04-07 00:00:00+00\",\"2020-04-06 00:00:00+00\",\"2020-04-05 00:00:00+00\"}");
        })
    }
}
