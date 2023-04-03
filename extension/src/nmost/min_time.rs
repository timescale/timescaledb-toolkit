use pgx::{iter::SetOfIterator, *};

use crate::nmost::*;

use crate::{
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};

type MinTimeTransType = NMostTransState<pg_sys::TimestampTz>;

pg_type! {
    #[derive(Debug)]
    struct MinTimes <'input> {
        capacity : u32,
        elements : u32,
        values : [pg_sys::TimestampTz; self.elements],
    }
}
ron_inout_funcs!(MinTimes);

impl<'input> From<&mut MinTimeTransType> for MinTimes<'input> {
    fn from(item: &mut MinTimeTransType) -> Self {
        let heap = std::mem::take(&mut item.heap);
        unsafe {
            flatten!(MinTimes {
                capacity: item.capacity as u32,
                elements: heap.len() as u32,
                values: heap.into_sorted_vec().into()
            })
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_time_trans(
    state: Internal,
    value: crate::raw::TimestampTz,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_function(
        unsafe { state.to_inner::<MinTimeTransType>() },
        value.into(),
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_time_rollup_trans(
    state: Internal,
    value: MinTimes<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_rollup_trans_function(
        unsafe { state.to_inner::<MinTimeTransType>() },
        value.values.as_slice(),
        value.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_time_combine(state1: Internal, state2: Internal) -> Option<Internal> {
    nmost_trans_combine(unsafe { state1.to_inner::<MinTimeTransType>() }, unsafe {
        state2.to_inner::<MinTimeTransType>()
    })
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_time_serialize(state: Internal) -> bytea {
    let state: Inner<MinTimeTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_time_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MinTimeTransType = crate::do_deserialize!(bytes, MinTimeTransType);
    Internal::new(i).into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_time_final(state: Internal) -> MinTimes<'static> {
    unsafe { &mut *state.to_inner::<MinTimeTransType>().unwrap() }.into()
}

#[pg_extern(name = "into_array", immutable, parallel_safe)]
pub fn min_n_time_to_array(agg: MinTimes<'static>) -> Vec<crate::raw::TimestampTz> {
    agg.values
        .clone()
        .into_iter()
        .map(crate::raw::TimestampTz::from)
        .collect()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn min_n_time_to_values(agg: MinTimes<'static>) -> SetOfIterator<crate::raw::TimestampTz> {
    SetOfIterator::new(
        agg.values
            .clone()
            .into_iter()
            .map(crate::raw::TimestampTz::from),
    )
}

extension_sql!(
    "\n\
    CREATE AGGREGATE min_n(\n\
        value timestamptz, capacity bigint\n\
    ) (\n\
        sfunc = min_n_time_trans,\n\
        stype = internal,\n\
        combinefunc = min_n_time_combine,\n\
        parallel = safe,\n\
        serialfunc = min_n_time_serialize,\n\
        deserialfunc = min_n_time_deserialize,\n\
        finalfunc = min_n_time_final\n\
    );\n\
",
    name = "min_n_time",
    requires = [
        min_n_time_trans,
        min_n_time_final,
        min_n_time_combine,
        min_n_time_serialize,
        min_n_time_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        value MinTimes\n\
    ) (\n\
        sfunc = min_n_time_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = min_n_time_combine,\n\
        parallel = safe,\n\
        serialfunc = min_n_time_serialize,\n\
        deserialfunc = min_n_time_deserialize,\n\
        finalfunc = min_n_time_final\n\
    );\n\
",
    name = "min_n_time_rollup",
    requires = [
        min_n_time_rollup_trans,
        min_n_time_final,
        min_n_time_combine,
        min_n_time_serialize,
        min_n_time_deserialize
    ],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn min_time_correctness() {
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
                    "SELECT into_array(min_n(val, 5))::TEXT from data",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap();
            assert_eq!(result.unwrap(), "{\"2020-01-01 00:00:00+00\",\"2020-01-02 00:00:00+00\",\"2020-01-03 00:00:00+00\",\"2020-01-04 00:00:00+00\",\"2020-01-05 00:00:00+00\"}");

            // Test into_values
            let mut result = client
                .update(
                    "SELECT into_values(min_n(val, 3))::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-01-01 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-01-02 00:00:00+00")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("2020-01-03 00:00:00+00")
            );
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.update(
                    "WITH aggs as (SELECT category, min_n(val, 5) as agg from data GROUP BY category)
                        SELECT into_array(rollup(agg))::TEXT FROM aggs",
                        None, None,
                    ).unwrap().first().get_one::<&str>().unwrap();
            assert_eq!(result.unwrap(), "{\"2020-01-01 00:00:00+00\",\"2020-01-02 00:00:00+00\",\"2020-01-03 00:00:00+00\",\"2020-01-04 00:00:00+00\",\"2020-01-05 00:00:00+00\"}");
        })
    }
}
