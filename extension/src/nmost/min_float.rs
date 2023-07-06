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

use ordered_float::NotNan;

type MinFloatTransType = NMostTransState<NotNan<f64>>;

pg_type! {
    #[derive(Debug)]
    struct MinFloats <'input> {
        capacity : u32,
        elements : u32,
        values : [f64; self.elements],
    }
}
ron_inout_funcs!(MinFloats);

impl<'input> From<&mut MinFloatTransType> for MinFloats<'input> {
    fn from(item: &mut MinFloatTransType) -> Self {
        let heap = std::mem::take(&mut item.heap);
        unsafe {
            flatten!(MinFloats {
                capacity: item.capacity as u32,
                elements: heap.len() as u32,
                values: heap
                    .into_sorted_vec()
                    .into_iter()
                    .map(f64::from)
                    .collect::<Vec<f64>>()
                    .into()
            })
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_float_trans(
    state: Internal,
    value: f64,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_function(
        unsafe { state.to_inner::<MinFloatTransType>() },
        NotNan::new(value).unwrap(),
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_float_rollup_trans(
    state: Internal,
    value: MinFloats<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let values: Vec<NotNan<f64>> = value
        .values
        .clone()
        .into_iter()
        .map(|x| NotNan::new(x).unwrap())
        .collect();
    nmost_rollup_trans_function(
        unsafe { state.to_inner::<MinFloatTransType>() },
        &values,
        value.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_float_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_combine(
        unsafe { state1.to_inner::<MinFloatTransType>() },
        unsafe { state2.to_inner::<MinFloatTransType>() },
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_float_serialize(state: Internal) -> bytea {
    let state: Inner<MinFloatTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_float_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MinFloatTransType = crate::do_deserialize!(bytes, MinFloatTransType);
    Internal::new(i).into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_float_final(state: Internal) -> MinFloats<'static> {
    unsafe { &mut *state.to_inner::<MinFloatTransType>().unwrap() }.into()
}

#[pg_extern(name = "into_array", immutable, parallel_safe)]
pub fn min_n_float_to_array(agg: MinFloats<'static>) -> Vec<f64> {
    agg.values.clone().into_vec()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn min_n_float_to_values(agg: MinFloats<'static>) -> SetOfIterator<f64> {
    SetOfIterator::new(agg.values.clone().into_iter())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_min_float_into_values<'a>(
    agg: MinFloats<'static>,
    _accessor: AccessorIntoValues<'a>,
) -> SetOfIterator<'a, f64> {
    min_n_float_to_values(agg)
}
#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_min_float_into_array<'a>(
    agg: MinFloats<'static>,
    _accessor: AccessorIntoArray<'a>,
) -> Vec<f64> {
    min_n_float_to_array(agg)
}

extension_sql!(
    "\n\
    CREATE AGGREGATE min_n(\n\
        value double precision, capacity bigint\n\
    ) (\n\
        sfunc = min_n_float_trans,\n\
        stype = internal,\n\
        combinefunc = min_n_float_combine,\n\
        parallel = safe,\n\
        serialfunc = min_n_float_serialize,\n\
        deserialfunc = min_n_float_deserialize,\n\
        finalfunc = min_n_float_final\n\
    );\n\
",
    name = "min_n_float",
    requires = [
        min_n_float_trans,
        min_n_float_final,
        min_n_float_combine,
        min_n_float_serialize,
        min_n_float_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        value MinFloats\n\
    ) (\n\
        sfunc = min_n_float_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = min_n_float_combine,\n\
        parallel = safe,\n\
        serialfunc = min_n_float_serialize,\n\
        deserialfunc = min_n_float_deserialize,\n\
        finalfunc = min_n_float_final\n\
    );\n\
",
    name = "min_n_float_rollup",
    requires = [
        min_n_float_rollup_trans,
        min_n_float_final,
        min_n_float_combine,
        min_n_float_serialize,
        min_n_float_deserialize
    ],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn min_float_correctness() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();
            client
                .update(
                    "CREATE TABLE data(val DOUBLE PRECISION, category INT)",
                    None,
                    None,
                )
                .unwrap();

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client
                    .update(
                        &format!("INSERT INTO data VALUES ({}.0/128, {})", i, i % 4),
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
                .get_one::<Vec<f64>>()
                .unwrap();
            assert_eq!(
                result.unwrap(),
                vec![0. / 128., 1. / 128., 2. / 128., 3. / 128., 4. / 128.]
            );
            let result = client
                .update("SELECT min_n(val, 5)->into_array() from data", None, None)
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap();
            assert_eq!(
                result.unwrap(),
                vec![0. / 128., 1. / 128., 2. / 128., 3. / 128., 4. / 128.]
            );

            // Test into_values
            let mut result = client
                .update(
                    "SELECT into_values(min_n(val, 3))::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0"));
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("0.0078125")
            );
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0.015625"));
            assert!(result.next().is_none());
            let mut result = client
                .update(
                    "SELECT (min_n(val, 3)->into_values())::TEXT from data",
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0"));
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("0.0078125")
            );
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0.015625"));
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.update(
                    "WITH aggs as (SELECT category, min_n(val, 5) as agg from data GROUP BY category)
                        SELECT into_array(rollup(agg)) FROM aggs",
                        None, None,
                    ).unwrap().first().get_one::<Vec<f64>>();
            assert_eq!(
                result.unwrap().unwrap(),
                vec![0. / 128., 1. / 128., 2. / 128., 3. / 128., 4. / 128.]
            );
        })
    }
}
