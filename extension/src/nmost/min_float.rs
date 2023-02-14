use pgx::{iter::SetOfIterator, *};

use crate::nmost::*;

use crate::{
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};

use ordered_float::NotNan;

type MinFloatTransType = NMostTransState<NotNan<f64>>;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

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
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
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

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_float_rollup_trans(
    state: Internal,
    value: toolkit_experimental::MinFloats<'static>,
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

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_float_combine(state1: Internal, state2: Internal) -> Option<Internal> {
    nmost_trans_combine(unsafe { state1.to_inner::<MinFloatTransType>() }, unsafe {
        state2.to_inner::<MinFloatTransType>()
    })
    .internal()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_float_serialize(state: Internal) -> bytea {
    let state: Inner<MinFloatTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_float_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MinFloatTransType = crate::do_deserialize!(bytes, MinFloatTransType);
    Internal::new(i).into()
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn min_n_float_final(state: Internal) -> toolkit_experimental::MinFloats<'static> {
    unsafe { &mut *state.to_inner::<MinFloatTransType>().unwrap() }.into()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_array",
    immutable,
    parallel_safe
)]
pub fn min_n_float_to_array(agg: toolkit_experimental::MinFloats<'static>) -> Vec<f64> {
    agg.values.clone().into_vec()
}

#[pg_extern(
    schema = "toolkit_experimental",
    name = "into_values",
    immutable,
    parallel_safe
)]
pub fn min_n_float_to_values(agg: toolkit_experimental::MinFloats<'static>) -> SetOfIterator<f64> {
    SetOfIterator::new(agg.values.clone().into_iter())
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.min_n(\n\
        value double precision, capacity bigint\n\
    ) (\n\
        sfunc = toolkit_experimental.min_n_float_trans,\n\
        stype = internal,\n\
        combinefunc = toolkit_experimental.min_n_float_combine,\n\
        parallel = safe,\n\
        serialfunc = toolkit_experimental.min_n_float_serialize,\n\
        deserialfunc = toolkit_experimental.min_n_float_deserialize,\n\
        finalfunc = toolkit_experimental.min_n_float_final\n\
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
    CREATE AGGREGATE toolkit_experimental.rollup(\n\
        value toolkit_experimental.MinFloats\n\
    ) (\n\
        sfunc = toolkit_experimental.min_n_float_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = toolkit_experimental.min_n_float_combine,\n\
        parallel = safe,\n\
        serialfunc = toolkit_experimental.min_n_float_serialize,\n\
        deserialfunc = toolkit_experimental.min_n_float_deserialize,\n\
        finalfunc = toolkit_experimental.min_n_float_final\n\
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
    use pgx_macros::pg_test;

    #[pg_test]
    fn min_float_correctness() {
        Spi::connect(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select(
                "CREATE TABLE data(val DOUBLE PRECISION, category INT)",
                None,
                None,
            );

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client.select(
                    &format!("INSERT INTO data VALUES ({}.0/128, {})", i, i % 4),
                    None,
                    None,
                );
            }

            // Test into_array
            let result =
                client.select("SELECT toolkit_experimental.into_array(toolkit_experimental.min_n(val, 5)) from data",
                    None, None,
                ).first().get_one::<Vec<f64>>();
            assert_eq!(
                result.unwrap(),
                vec![0. / 128., 1. / 128., 2. / 128., 3. / 128., 4. / 128.]
            );

            // Test into_values
            let mut result =
                client.select("SELECT toolkit_experimental.into_values(toolkit_experimental.min_n(val, 3))::TEXT from data",
                    None, None,
                );
            assert_eq!(result.next().unwrap()[1].value(), Some("0"));
            assert_eq!(result.next().unwrap()[1].value(), Some("0.0078125"));
            assert_eq!(result.next().unwrap()[1].value(), Some("0.015625"));
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.select(
                    "WITH aggs as (SELECT category, toolkit_experimental.min_n(val, 5) as agg from data GROUP BY category)
                        SELECT toolkit_experimental.into_array(toolkit_experimental.rollup(agg)) FROM aggs",
                        None, None,
                    ).first().get_one::<Vec<f64>>();
            assert_eq!(
                result.unwrap(),
                vec![0. / 128., 1. / 128., 2. / 128., 3. / 128., 4. / 128.]
            );
        })
    }
}
