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
use std::cmp::Reverse;

type MaxFloatTransType = NMostTransState<Reverse<NotNan<f64>>>;

pg_type! {
    #[derive(Debug)]
    struct MaxFloats <'input> {
        capacity : u32,
        elements : u32,
        values : [f64; self.elements],
    }
}
ron_inout_funcs!(MaxFloats);

impl<'input> From<&mut MaxFloatTransType> for MaxFloats<'input> {
    fn from(item: &mut MaxFloatTransType) -> Self {
        let heap = std::mem::take(&mut item.heap);
        unsafe {
            flatten!(MaxFloats {
                capacity: item.capacity as u32,
                elements: heap.len() as u32,
                values: heap
                    .into_sorted_vec()
                    .into_iter()
                    .map(|x| f64::from(x.0))
                    .collect::<Vec<f64>>()
                    .into()
            })
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_float_trans(
    state: Internal,
    value: f64,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_trans_function(
        unsafe { state.to_inner::<MaxFloatTransType>() },
        Reverse(NotNan::new(value).unwrap()),
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_float_rollup_trans(
    state: Internal,
    value: MaxFloats<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let values: Vec<Reverse<NotNan<f64>>> = value
        .values
        .clone()
        .into_iter()
        .map(|x| Reverse(NotNan::new(x).unwrap()))
        .collect();
    nmost_rollup_trans_function(
        unsafe { state.to_inner::<MaxFloatTransType>() },
        &values,
        value.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_float_combine(state1: Internal, state2: Internal) -> Option<Internal> {
    nmost_trans_combine(unsafe { state1.to_inner::<MaxFloatTransType>() }, unsafe {
        state2.to_inner::<MaxFloatTransType>()
    })
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_float_serialize(state: Internal) -> bytea {
    let state: Inner<MaxFloatTransType> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_float_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: MaxFloatTransType = crate::do_deserialize!(bytes, MaxFloatTransType);
    Internal::new(i).into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn max_n_float_final(state: Internal) -> MaxFloats<'static> {
    unsafe { &mut *state.to_inner::<MaxFloatTransType>().unwrap() }.into()
}

#[pg_extern(name = "into_array", immutable, parallel_safe)]
pub fn max_n_float_to_array(agg: MaxFloats<'static>) -> Vec<f64> {
    agg.values.clone().into_vec()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn max_n_float_to_values(agg: MaxFloats<'static>) -> SetOfIterator<f64> {
    SetOfIterator::new(agg.values.clone().into_iter())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_max_float_into_values<'a>(
    agg: MaxFloats<'static>,
    _accessor: AccessorIntoValues<'a>,
) -> SetOfIterator<'a, f64> {
    max_n_float_to_values(agg)
}
#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_max_float_into_array<'a>(
    agg: MaxFloats<'static>,
    _accessor: AccessorIntoArray<'a>,
) -> Vec<f64> {
    max_n_float_to_array(agg)
}

extension_sql!(
    "\n\
    CREATE AGGREGATE max_n(\n\
        value double precision, capacity bigint\n\
    ) (\n\
        sfunc = max_n_float_trans,\n\
        stype = internal,\n\
        combinefunc = max_n_float_combine,\n\
        parallel = safe,\n\
        serialfunc = max_n_float_serialize,\n\
        deserialfunc = max_n_float_deserialize,\n\
        finalfunc = max_n_float_final\n\
    );\n\
",
    name = "max_n_float",
    requires = [
        max_n_float_trans,
        max_n_float_final,
        max_n_float_combine,
        max_n_float_serialize,
        max_n_float_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        value MaxFloats\n\
    ) (\n\
        sfunc = max_n_float_rollup_trans,\n\
        stype = internal,\n\
        combinefunc = max_n_float_combine,\n\
        parallel = safe,\n\
        serialfunc = max_n_float_serialize,\n\
        deserialfunc = max_n_float_deserialize,\n\
        finalfunc = max_n_float_final\n\
    );\n\
",
    name = "max_n_float_rollup",
    requires = [
        max_n_float_rollup_trans,
        max_n_float_final,
        max_n_float_combine,
        max_n_float_serialize,
        max_n_float_deserialize
    ],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn max_float_correctness() {
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
                .update("SELECT into_array(max_n(val, 5)) from data", None, None)
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap();
            assert_eq!(
                result.unwrap(),
                vec![99. / 128., 98. / 128., 97. / 128., 96. / 128., 95. / 128.]
            );
            let result = client
                .update("SELECT max_n(val, 5)->into_array() from data", None, None)
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap();
            assert_eq!(
                result.unwrap(),
                vec![99. / 128., 98. / 128., 97. / 128., 96. / 128., 95. / 128.]
            );

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
                Some("0.7734375")
            );
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0.765625"));
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("0.7578125")
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
                Some("0.7734375")
            );
            assert_eq!(result.next().unwrap()[1].value().unwrap(), Some("0.765625"));
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("0.7578125")
            );
            assert!(result.next().is_none());

            // Test rollup
            let result =
                client.update(
                    "WITH aggs as (SELECT category, max_n(val, 5) as agg from data GROUP BY category)
                        SELECT into_array(rollup(agg)) FROM aggs",
                        None, None,
                    ).unwrap().first().get_one::<Vec<f64>>().unwrap();
            assert_eq!(
                result.unwrap(),
                vec![99. / 128., 98. / 128., 97. / 128., 96. / 128., 95. / 128.]
            );
        })
    }
}
