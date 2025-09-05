use pgrx::{iter::TableIterator, *};

use crate::nmost::min_float::*;
use crate::nmost::*;

use crate::{
    build, flatten,
    palloc::{Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

use ordered_float::NotNan;

type MinByFloatTransType = NMostByTransState<NotNan<f64>>;

pg_type! {
    #[derive(Debug)]
    struct MinByFloats<'input> {
        values: MinFloatsData<'input>,  // Nesting pg_types adds 8 bytes of header
        data: DatumStore<'input>,
    }
}
ron_inout_funcs!(MinByFloats<'input>);

impl<'input> From<MinByFloatTransType> for MinByFloats<'input> {
    fn from(item: MinByFloatTransType) -> Self {
        let (capacity, val_ary, data) = item.into_sorted_parts();
        unsafe {
            flatten!(MinByFloats {
                values: build!(MinFloats {
                    capacity: capacity as u32,
                    elements: val_ary.len() as u32,
                    values: val_ary
                        .into_iter()
                        .map(f64::from)
                        .collect::<Vec<f64>>()
                        .into()
                })
                .0,
                data,
            })
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_by_float_trans(
    state: Internal,
    value: f64,
    data: AnyElement,
    capacity: i64,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    nmost_by_trans_function(
        unsafe { state.to_inner::<MinByFloatTransType>() },
        NotNan::new(value).unwrap(),
        data,
        capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_by_float_rollup_trans(
    state: Internal,
    value: MinByFloats<'static>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let values: Vec<NotNan<f64>> = value
        .values
        .values
        .clone()
        .into_iter()
        .map(|x| NotNan::new(x).unwrap())
        .collect();
    nmost_by_rollup_trans_function(
        unsafe { state.to_inner::<MinByFloatTransType>() },
        &values,
        &value.data,
        value.values.capacity as usize,
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn min_n_by_float_final(state: Internal) -> MinByFloats<'static> {
    unsafe { state.to_inner::<MinByFloatTransType>().unwrap().clone() }.into()
}

#[pg_extern(name = "into_values", immutable, parallel_safe)]
pub fn min_n_by_float_to_values(
    agg: MinByFloats<'static>,
    _dummy: Option<AnyElement>,
) -> TableIterator<'static, (name!(value, f64), name!(data, AnyElement))> {
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
        value double precision, data AnyElement, capacity bigint\n\
    ) (\n\
        sfunc = min_n_by_float_trans,\n\
        stype = internal,\n\
        finalfunc = min_n_by_float_final\n\
    );\n\
",
    name = "min_n_by_float",
    requires = [min_n_by_float_trans, min_n_by_float_final],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        MinByFloats\n\
    ) (\n\
        sfunc = min_n_by_float_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = min_n_by_float_final\n\
    );\n\
",
    name = "min_n_by_float_rollup",
    requires = [min_n_by_float_rollup_trans, min_n_by_float_final],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn min_by_float_correctness() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            client
                .update(
                    "CREATE TABLE data(val DOUBLE PRECISION, category INT)",
                    None,
                    &[],
                )
                .unwrap();

            for i in 0..100 {
                let i = (i * 83) % 100; // mess with the ordering just a little

                client
                    .update(
                        &format!("INSERT INTO data VALUES ({}.0/128, {})", i, i % 4),
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
                Some("(0.0078125,\"(0.0078125,1)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(0.015625,\"(0.015625,2)\")")
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
                Some("(0.0078125,\"(0.0078125,1)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(0.015625,\"(0.015625,2)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(0.0234375,\"(0.0234375,3)\")")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(0.03125,\"(0.03125,0)\")")
            );
            assert!(result.next().is_none());
        })
    }
}
