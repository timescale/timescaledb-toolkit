#![allow(clippy::identity_op)] // clippy gets confused by flat_serialize! enums

use std::{
    convert::TryInto,
};

use serde::{Deserialize, Serialize};

use pg_sys::{Datum, Oid};
use pgx::*;

use flat_serialize::*;

use crate::{
    aggregate_utils::{get_collation, in_aggregate_context},
    datum_utils::DatumHashBuilder,
    flatten, ron_inout_funcs,
    palloc::{Internal, InternalAsValue, Inner, ToInternal},
    pg_type,
    serialization::{PgCollationId, ShortTypeId},
};

use hyperloglogplusplus::{HyperLogLog as HLL, HyperLogLogStorage};

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct HyperLogLogTrans {
    logger: HLL<'static, Datum, DatumHashBuilder>,
}

use crate::raw::AnyElement;

#[pg_extern(immutable, parallel_safe)]
pub fn hyperloglog_trans(
    state: Internal,
    size: i32,
    // TODO we want to use crate::raw::AnyElement but it doesn't work for some reason...
    value: Option<AnyElement>,
    fc: pg_sys::FunctionCallInfo,
) -> Internal {
    // let state: Internal = Internal::from_datum();
    hyperloglog_trans_inner(unsafe{ state.to_inner() }, size, value, fc).internal()
}
pub fn hyperloglog_trans_inner(
    state: Option<Inner<HyperLogLogTrans>>,
    size: i32,
    value: Option<AnyElement>,
    fc: pg_sys::FunctionCallInfo,
) -> Option<Inner<HyperLogLogTrans>> {
    unsafe {
        in_aggregate_context(fc, || {
            //TODO is this the right way to handle NULL?
            let value = match value {
                None => return state,
                Some(value) => value.0,
            };
            let mut state = match state {
                None => {
                    // TODO specialize hash function for bytea types?
                    //      ints? floats? uuids? other primitive types?
                    let size: usize = size.try_into().unwrap();
                    let b = size.checked_next_power_of_two().unwrap().trailing_zeros();
                    let typ = pgx::get_getarg_type(fc, 2);
                    let collation = get_collation(fc);
                    let hasher = DatumHashBuilder::from_type_id(typ, collation);
                    let trans = HyperLogLogTrans {
                        logger: HLL::new(b as u8, hasher),
                    };
                    trans.into()
                }
                Some(state) => state,
            };
            state.logger.add(&value);
            Some(state)
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn hyperloglog_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    unsafe {
        hyperloglog_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal()
    }
}
pub fn hyperloglog_combine_inner(
    state1: Option<Inner<HyperLogLogTrans>>,
    state2: Option<Inner<HyperLogLogTrans>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<HyperLogLogTrans>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(state2)) => Some(state2.clone().into()),
            (Some(state1), None) => Some(state1.clone().into()),
            (Some(state1), Some(state2)) => {
                let mut logger = state1.logger.clone();
                logger.merge_in(&state2.logger);
                Some(HyperLogLogTrans { logger }.into())
            }
        })
    }
}

use crate::raw::bytea;

#[pg_extern(immutable, parallel_safe)]
pub fn hyperloglog_serialize(state: Internal) -> bytea {
    let state: &mut HyperLogLogTrans = unsafe { state.get_mut().unwrap() };
    state.logger.merge_all();
    crate::do_serialize!(state)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn hyperloglog_deserialize(
    bytes: bytea,
    _internal: Internal,
) -> Internal {
    hyperloglog_deserialize_inner(bytes).internal()
}
pub fn hyperloglog_deserialize_inner(
    bytes: bytea,
) -> Inner<HyperLogLogTrans> {
    let i: HyperLogLogTrans = crate::do_deserialize!(bytes, HyperLogLogTrans);
    i.into()
}

pg_type! {
    #[derive(Debug)]
    struct HyperLogLog<'input> {
        #[flat_serialize::flatten]
        log: Storage<'input>,
    }
}

flat_serialize_macro::flat_serialize! {
    #[derive(Debug, Serialize, Deserialize)]
    enum Storage<'a> {
        storage_kind: u64,
        Sparse: 1 {
            num_compressed: u64,
            // Oids are stored in postgres arrays, so it should be safe to store them
            // in our types as long as we do send/recv and in/out correctly
            // see https://github.com/postgres/postgres/blob/b8d0cda53377515ac61357ec4a60e85ca873f486/src/include/utils/array.h#L90
            element_type: ShortTypeId,
            collation: PgCollationId,
            compressed_bytes: u32,
            precision: u8,
            compressed: [u8; self.compressed_bytes],
        },
        Dense: 2 {
            // Oids are stored in postgres arrays, so it should be safe to store them
            // in our types as long as we do send/recv and in/out correctly
            // see https://github.com/postgres/postgres/blob/b8d0cda53377515ac61357ec4a60e85ca873f486/src/include/utils/array.h#L90
            element_type: ShortTypeId,
            collation: PgCollationId,
            precision: u8,
            registers: [u8; 1 + (1usize << self.precision) * 6 / 8] //TODO should we just store len?
        },
    }
}

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
mod toolkit_experimental {
    pub(crate) use crate::accessors::toolkit_experimental::*;
}

ron_inout_funcs!(HyperLogLog);

#[pg_extern(immutable, parallel_safe)]
fn hyperloglog_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<HyperLogLog<'static>> {
    hyperloglog_final_inner(unsafe{ state.to_inner() }, fcinfo)
}
fn hyperloglog_final_inner(
    state: Option<Inner<HyperLogLogTrans>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<HyperLogLog<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };

            flatten_log(&mut state.logger).into()
        })
    }
}

extension_sql!("\n\
    CREATE AGGREGATE hyperloglog(size int, value AnyElement)\n\
    (\n\
        stype = internal,\n\
        sfunc = hyperloglog_trans,\n\
        finalfunc = hyperloglog_final,\n\
        combinefunc = hyperloglog_combine,\n\
        serialfunc = hyperloglog_serialize,\n\
        deserialfunc = hyperloglog_deserialize,\n\
        parallel = safe\n\
    );\n\
",
name = "hll_agg",
requires = [hyperloglog_trans, hyperloglog_final, hyperloglog_combine, hyperloglog_serialize, hyperloglog_deserialize],
);

#[pg_extern(immutable, parallel_safe)]
pub fn hyperloglog_union(
    state: Internal,
    other: HyperLogLog,
    fc: pg_sys::FunctionCallInfo,
) -> Internal {
    hyperloglog_union_inner(unsafe{ state.to_inner() }, other, fc).internal()
}
pub fn hyperloglog_union_inner(
    state: Option<Inner<HyperLogLogTrans>>,
    other: HyperLogLog,
    fc: pg_sys::FunctionCallInfo,
) -> Option<Inner<HyperLogLogTrans>> {
    unsafe {
        in_aggregate_context(fc, || {
            let mut state = match state {
                Some(state) => state,
                None => {
                    let state = HyperLogLogTrans {
                        logger: unflatten_log(other).into_owned(),
                    };
                    return Some(state.into())
                },
            };
            let other = unflatten_log(other);
            if state.logger.buildhasher.type_id != other.buildhasher.type_id {
                error!("missmatched types")
            }
            // TODO error on mismatched collation?
            state.logger.merge_in(&other);
            Some(state)
        })
    }
}

extension_sql!("\n\
    CREATE AGGREGATE rollup(hyperloglog Hyperloglog)\n\
    (\n\
        stype = internal,\n\
        sfunc = hyperloglog_union,\n\
        finalfunc = hyperloglog_final,\n\
        combinefunc = hyperloglog_combine,\n\
        serialfunc = hyperloglog_serialize,\n\
        deserialfunc = hyperloglog_deserialize,\n\
        parallel = safe\n\
    );\n\
",
name = "hll_rollup",
requires = [hyperloglog_union, hyperloglog_final, hyperloglog_combine, hyperloglog_serialize, hyperloglog_deserialize],
);



#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_hyperloglog_count(
    sketch: HyperLogLog,
    accessor: toolkit_experimental::AccessorDistinctCount,
) -> i64 {
    let _ = accessor;
    hyperloglog_count(sketch)
}

#[pg_extern(name="distinct_count", immutable, parallel_safe)]
pub fn hyperloglog_count(
    hyperloglog: HyperLogLog
) -> i64 {
    // count does not depend on the type parameters
    let log = match &hyperloglog.log {
        Storage::Sparse { num_compressed, precision, compressed, .. } =>
            HLL::<Datum, ()>::from_sparse_parts(compressed.slice(), *num_compressed, *precision, ()),
        Storage::Dense { precision, registers, .. } =>
        HLL::<Datum, ()>::from_dense_parts(registers.slice(), *precision, ()),
    };
    log.immutable_estimate_count() as i64
}


#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_hyperloglog_error(
    sketch: HyperLogLog,
    accessor: toolkit_experimental::AccessorStdError,
) -> f64 {
    let _ = accessor;
    hyperloglog_error(sketch)
}

#[pg_extern(name="stderror", immutable, parallel_safe)]
pub fn hyperloglog_error(
    hyperloglog: HyperLogLog
) -> f64 {
    let precision = match hyperloglog.log {
        Storage::Sparse { precision, .. } => precision,
        Storage::Dense { precision, .. } => precision,
    };
    hyperloglogplusplus::error_for_precision(precision)
}

impl HyperLogLog<'_> {
    pub fn build_from(size: i32, type_id: Oid, collation: Option<Oid>, data: impl Iterator<Item=pg_sys::Datum>)
    -> HyperLogLog<'static> {
        unsafe {
            let b = TryInto::<usize>::try_into(size).unwrap().checked_next_power_of_two().unwrap().trailing_zeros();
            let hasher = DatumHashBuilder::from_type_id(type_id, collation);
            let mut logger: HLL<pg_sys::Datum, DatumHashBuilder> = HLL::new(b as u8, hasher);

            for datum in data {
                logger.add(&datum);
            }

            flatten_log(&mut logger)
        }
    }
}

fn flatten_log(hyperloglog: &mut HLL<Datum, DatumHashBuilder>)
-> HyperLogLog<'static> {
    let (element_type, collation) = {
        let hasher = &hyperloglog.buildhasher;
        (ShortTypeId(hasher.type_id), PgCollationId(hasher.collation))
    };

    // we need to flatten the vector to a single buffer that contains
    // both the size, the data, and the varlen header

    let flat = match hyperloglog.to_parts() {
        HyperLogLogStorage::Sparse(sparse) => unsafe {
            flatten!(HyperLogLog {
                log: Storage::Sparse {
                    element_type,
                    collation,
                    num_compressed: sparse.num_compressed,
                    precision: sparse.precision,
                    compressed_bytes: sparse.compressed.num_bytes() as u32,
                    compressed: sparse.compressed.bytes().into(),
                }
            })
        },
        HyperLogLogStorage::Dense(dense) => unsafe {
            // TODO check that precision and length match?
            flatten!(HyperLogLog {
                log: Storage::Dense {
                    element_type,
                    collation,
                    precision: dense.precision,
                    registers: dense.registers.bytes().into(),
                }
            })
        },
    };
    flat
}

fn unflatten_log(hyperloglog: HyperLogLog) -> HLL<Datum, DatumHashBuilder> {
    match &hyperloglog.log {
        Storage::Sparse {
            num_compressed,
            precision,
            compressed,
            element_type,
            collation,
            compressed_bytes: _
        } => HLL::<Datum, DatumHashBuilder>::from_sparse_parts(
            compressed.slice(),
            *num_compressed,
            *precision,
            unsafe { DatumHashBuilder::from_type_id(element_type.0, Some(collation.0)) }
        ),
        Storage::Dense { precision, registers, element_type, collation  } =>
            HLL::<Datum, DatumHashBuilder>::from_dense_parts(
                registers.slice(),
                *precision,
                unsafe { DatumHashBuilder::from_type_id(element_type.0, Some(collation.0)) }
            ),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_hll_aggregate() {
        Spi::execute(|client| {
            let text = client
                .select(
                    "SELECT \
                        hyperloglog(32, v::float)::TEXT \
                        FROM generate_series(1, 100) v",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();

            let expected = "(\
                version:1,\
                log:Dense(\
                    element_type:FLOAT8,\
                    collation:None,\
                    precision:5,\
                    registers:[\
                        20,64,132,12,81,1,8,64,133,4,64,136,4,82,3,12,17,\
                        65,24,32,197,16,32,132,255\
                    ]\
                )\
            )";
            assert_eq!(text.unwrap(), expected);

            let (count, arrow_count) = client
                .select("SELECT \
                    distinct_count(\
                        hyperloglog(32, v::float)\
                    ), \
                    hyperloglog(32, v::float)->toolkit_experimental.distinct_count() \
                    FROM generate_series(1, 100) v", None, None)
                .first()
                .get_two::<i32, i32>();
            assert_eq!(count, Some(132));
            assert_eq!(count, arrow_count);

            let count2 = client
                .select(
                    &format!(
                        "SELECT distinct_count('{}')",
                        expected
                    ),
                    None,
                    None,
                )
                .first()
                .get_one::<i32>();
            assert_eq!(count2, count);
        });
    }

    #[pg_test]
    fn test_hll_byte_io() {
        unsafe {
            // Unable to build the hyperloglog through hyperloglog_trans, as that requires a valid fcinfo to determine OIDs.

            // FIXME: use named constant for default correlation oid
            let hasher = DatumHashBuilder::from_type_id(pg_sys::TEXTOID, Some(100));
            let mut control = HyperLogLogTrans {
                logger: HLL::new(6, hasher),
            };
            control.logger.add(&rust_str_to_text_p("first").into_datum().unwrap());
            control.logger.add(&rust_str_to_text_p("second").into_datum().unwrap());
            control.logger.add(&rust_str_to_text_p("first").into_datum().unwrap());
            control.logger.add(&rust_str_to_text_p("second").into_datum().unwrap());
            control.logger.add(&rust_str_to_text_p("third").into_datum().unwrap());

            let buffer = hyperloglog_serialize(Inner::from(control.clone()).internal());
            let buffer = pgx::varlena::varlena_to_byte_slice(buffer.0 as *mut pg_sys::varlena);

            let mut expected = vec![1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 136, 136, 9, 7, 8, 74, 76, 47, 200, 231, 53, 25, 3, 0, 0, 0, 0, 0, 0, 0, 6, 9, 0, 0, 0, 1];
            bincode::serialize_into(&mut expected, &PgCollationId(100)).unwrap();
            assert_eq!(buffer, expected);

            let expected = pgx::varlena::rust_byte_slice_to_bytea(&expected);
            let new_state = hyperloglog_deserialize_inner(bytea(&*expected as *const pg_sys::varlena as _));

            control.logger.merge_all();  // Sparse representation buffers always merged on serialization
            assert!(*new_state == control);

            // Now generate a dense represenataion and validate that
            for i in 0..500 {
                control.logger.add(&rust_str_to_text_p(&i.to_string()).into_datum().unwrap());
            }

            let buffer = hyperloglog_serialize(Inner::from(control.clone()).internal());
            let buffer = pgx::varlena::varlena_to_byte_slice(buffer.0 as *mut pg_sys::varlena);

            let mut expected = vec![1, 1, 1, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 0, 20, 65, 2, 12, 48, 199, 20, 33, 4, 12, 49, 67, 16, 81, 66, 32, 145, 131, 24, 49, 4, 20, 33, 5, 8, 81, 66, 12, 81, 4, 8, 49, 2, 8, 65, 131, 24, 32, 133, 12, 50, 66, 12, 48, 197, 12, 81, 130, 255, 58, 6, 255, 255, 255, 255, 255, 255, 255, 3, 9, 0, 0, 0, 1];
            bincode::serialize_into(&mut expected, &PgCollationId(100)).unwrap();
            assert_eq!(buffer, expected);

            let expected = pgx::varlena::rust_byte_slice_to_bytea(&expected);
            let new_state = hyperloglog_deserialize_inner(bytea(&*expected as *const pg_sys::varlena as _));

            assert!(*new_state == control);
        }
    }

    #[pg_test]
    fn test_hll_aggregate_int() {
        Spi::execute(|client| {
            let text = client
                .select(
                    "SELECT hyperloglog(32, v::int)::TEXT
                    FROM generate_series(1, 100) v",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();

            let expected = "(\
                version:1,\
                log:Dense(\
                    element_type:INT4,\
                    collation:None,\
                    precision:5,\
                    registers:[\
                        8,49,0,12,32,129,24,32,195,16,33,2,12,1,68,4,16,\
                        196,20,64,133,8,17,67,255\
                    ]\
                )\
            )";
            assert_eq!(text.unwrap(), expected);

            let count = client
                .select("SELECT \
                distinct_count(\
                    hyperloglog(32, v::int)\
                ) FROM generate_series(1, 100) v", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(count, Some(96));

            let count2 = client
                .select(
                    &format!("SELECT distinct_count('{}')", expected),
                    None,
                    None,
                )
                .first()
                .get_one::<i32>();
            assert_eq!(count2, count);
        });
    }

    #[pg_test]
    fn test_hll_aggregate_text() {
        Spi::execute(|client| {
            use crate::serialization::PgCollationId;

            let text = client
                .select(
                    "SELECT \
                        hyperloglog(32, v::text)::TEXT \
                    FROM generate_series(1, 100) v",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();

            let default_collation = ron::to_string(&PgCollationId(100)).unwrap();
            let expected = format!("(\
                version:1,\
                log:Dense(\
                    element_type:TEXT,\
                    collation:{},\
                    precision:5,\
                    registers:[\
                        12,33,3,8,33,4,20,50,3,12,32,133,4,32,67,8,48,\
                        128,8,33,4,8,32,197,255\
                    ]\
                )\
            )", default_collation);
            assert_eq!(text.unwrap(), expected);

            let count = client
                .select("SELECT distinct_count(\
                    hyperloglog(32, v::text)\
                ) FROM generate_series(1, 100) v", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(count, Some(111));

            let count2 = client
                .select(
                    &format!("SELECT distinct_count('{}')", expected),
                    None,
                    None,
                )
                .first()
                .get_one::<i32>();
            assert_eq!(count2, count);
        });
    }

    #[pg_test]
    fn test_hll_union_text() {
        Spi::execute(|client| {
            {
                // self-union should be a nop
                let expected = client
                    .select(
                        "SELECT \
                                hyperloglog(32, v::text)::TEXT \
                            FROM generate_series(1, 100) v",
                        None,
                        None,
                    )
                    .first()
                    .get_one::<String>()
                    .unwrap();

                let text = client
                    .select(
                        "SELECT rollup(logs)::text \
                        FROM (\
                            (SELECT hyperloglog(32, v::text) logs \
                             FROM generate_series(1, 100) v\
                            ) UNION ALL \
                            (SELECT hyperloglog(32, v::text) \
                             FROM generate_series(1, 100) v)\
                        ) q",
                        None,
                        None,
                    )
                    .first()
                    .get_one::<String>();

                assert_eq!(text.unwrap(), expected);
            }

            {
                // differing unions should be a sum of the distinct counts
                let query =
                    "SELECT distinct_count(rollup(logs)) \
                    FROM (\
                        (SELECT hyperloglog(32, v::text) logs \
                         FROM generate_series(1, 100) v) \
                        UNION ALL \
                        (SELECT hyperloglog(32, v::text) \
                         FROM generate_series(50, 150) v)\
                    ) q";
                let count = client
                    .select(
                        query,
                        None,
                        None,
                    )
                    .first()
                    .get_one::<i64>();

                assert_eq!(count, Some(152));
            }

        });
    }

    //TODO test continuous aggregates
}
