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

type MinIntTransType = NMostTransState<i64>;

pg_type! {
    #[derive(Debug)]
    struct MinInts <'input> {
        capacity : u32,
        elements : u32,
        values : [i64; self.elements],
    }
}
ron_inout_funcs!(MinInts);

impl<'input> From<&mut MinIntTransType> for MinInts<'input> {
    fn from(item: &mut MinIntTransType) -> Self {
        let heap = std::mem::take(&mut item.heap);
        unsafe {
            flatten!(MinInts {
                capacity: item.capacity as u32,
                elements: heap.len() as u32,
                values: heap.into_sorted_vec().into()
            })
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_int_trans(
    state: Internal,
    value: i64,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_function(
        unsafe { state.to_inner::<MinIntTransType>() },
        value,
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_int_rollup_trans(
    state: Internal,
    value: MinInts<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_rollup_trans_function(
        unsafe { state.to_inner::<MinIntTransType>() },
        value.values.as_slice(),
        value.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_int_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_combine(
        unsafe { state1.to_inner::<MinIntTransType>() },
        unsafe { state2.to_inner::<MinIntTransType>() },
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_int_serialize(state: Internal) -> bytea {
    let state: Inner<MinIntTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_int_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MinIntTransType = crate::do_deserialize!(bytes, MinIntTransType);
    Internal::new(i).into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_int_final(state: Internal) -> MinInts<'static> {
    unsafe { &mut *state.to_inner::<MinIntTransType>().unwrap() }.into()
}

#[pg_extern(name = "into_array", immutable, parallel_safe)]
pub fn min_n_int_to_array(agg: MinInts<'static>) -> Vec<i64> {
    agg.values.clone().into_vec()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn min_n_int_to_values(agg: MinInts<'static>) -> SetOfIterator<'static, i64> {
    SetOfIterator::new(agg.values.clone().into_iter())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_min_int_into_values<'a>(
    agg: MinInts<'static>,
    _accessor: AccessorIntoValues<'a>,
) -> SetOfIterator<'a, i64> {
    min_n_int_to_values(agg)
}
#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_min_int_into_array<'a>(
    agg: MinInts<'static>,
    _accessor: AccessorIntoArray<'a>,
) -> Vec<i64> {
    min_n_int_to_array(agg)
}

extension_sql!(
    "\n\
    CREATE AGGREGATE min_n(\n\
        value bigint, capacity bigint\n\
    ) (\n\
        sfunc = min_n_int_trans,\n\
        stype = internal,\n\
        combinefunc = min_n_int_combine,\n\
        parallel = safe,\n\
        serialfunc = min_n_int_serialize,\n\
        deserialfunc = min_n_int_deserialize,\n\
        finalfunc = min_n_int_final\n\
    );\n\
",
    name = "min_n_int",
    requires = [
        min_n_int_trans,
        min_n_int_final,
        min_n_int_combine,
        min_n_int_serialize,
        min_n_int_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        value MinInts\n\
    ) (\n\
        sfunc = min_n_int_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = min_n_int_combine,\n\
        parallel = safe,\n\
        serialfunc = min_n_int_serialize,\n\
        deserialfunc = min_n_int_deserialize,\n\
        finalfunc = min_n_int_final\n\
    );\n\
",
    name = "min_n_int_rollup",
    requires = [
        min_n_int_rollup_trans,
        min_n_int_final,
        min_n_int_combine,
        min_n_int_serialize,
        min_n_int_deserialize
    ],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn min_int_correctness() {
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

            // Test into_array
            let result = client
                .update("SELECT into_array(min_n(val, 5)) from data", None, None)
                .unwrap()
                .first()
                .get_one::<Vec<i64>>()
                .unwrap();
            assert_eq!(result.unwrap(), vec![0, 1, 2, 3, 4]);
            let result = client
                .update("SELECT min_n(val, 5)->into_array() from data", None, None)
                .unwrap()
                .first()
                .get_one::<Vec<i64>>()
                .unwrap();
            assert_eq!(result.unwrap(), vec![0, 1, 2, 3, 4]);

            // Test into_values
            let mut result = client
                .update(
                    "SELECT into_values(min_n(val, 3))::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("1"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("2"));
            assert!(result.next().is_none());
            let mut result = client
                .update(
                    "SELECT (min_n(val, 3)->into_values())::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("1"));
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("2"));
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.update(
                    "WITH aggs as (SELECT category, min_n(val, 5) as agg from data GROUP BY category)
                        SELECT into_array(rollup(agg)) FROM aggs",
                        None, None,
                    ).unwrap().first().get_one::<Vec<i64>>().unwrap();
            assert_eq!(result.unwrap(), vec![0, 1, 2, 3, 4]);
        })
    }
}
