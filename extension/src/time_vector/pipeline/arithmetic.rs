use pgrx::*;

use super::*;

use super::Element::Arithmetic;
use Function::*;

#[derive(
    Debug, Copy, Clone, flat_serialize_macro::FlatSerializable, serde::Serialize, serde::Deserialize,
)]
#[repr(u64)]
//XXX note that the order here _is_ significant; it can be visible in the
//    serialized form
pub enum Function {
    // binary functions
    Add = 1,
    Sub = 2,
    Mul = 3,
    Div = 4,
    Mod = 5,
    Power = 6,
    LogN = 7,
    // Unary functions
    Abs,
    Cbrt,
    Ceil,
    Floor,
    Ln,
    Log10,
    Round, // nearest
    Sign,
    Sqrt,
    Trunc,
}

pub fn apply(
    mut series: Timevector_TSTZ_F64<'_>,
    function: Function,
    rhs: f64,
) -> Timevector_TSTZ_F64<'_> {
    let function: fn(f64, f64) -> f64 = match function {
        Add => |a, b| a + b,
        Sub => |a, b| a - b,
        Mul => |a, b| a * b,
        Div => |a, b| a / b,
        // TODO is this the right mod?
        Mod => |a, b| a % b,
        Power => |a, b| a.powf(b),
        LogN => |a, b| a.log(b),
        // unary functions just ignore the second arg
        Abs => |a, _| a.abs(),
        Cbrt => |a, _| a.cbrt(),
        Ceil => |a, _| a.ceil(),
        Floor => |a, _| a.floor(),
        Ln => |a, _| a.ln(),
        Log10 => |a, _| a.log10(),
        Round => |a, _| a.round(),
        Sign => |a, _| a.signum(),
        Sqrt => |a, _| a.sqrt(),
        Trunc => |a, _| a.trunc(),
    };
    map::map_series(&mut series, |lhs| function(lhs, rhs));
    series
}

//
// binary operations
//

#[pg_extern(
    immutable,
    parallel_safe,
    name = "add",
    schema = "toolkit_experimental"
)]
pub fn pipeline_add<'e>(rhs: f64) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic { function: Add, rhs }.flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "sub",
    schema = "toolkit_experimental"
)]
pub fn pipeline_sub<'e>(rhs: f64) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic { function: Sub, rhs }.flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "mul",
    schema = "toolkit_experimental"
)]
pub fn pipeline_mul<'e>(rhs: f64) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic { function: Mul, rhs }.flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "div",
    schema = "toolkit_experimental"
)]
pub fn pipeline_div<'e>(rhs: f64) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic { function: Div, rhs }.flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "mod",
    schema = "toolkit_experimental"
)]
pub fn pipeline_mod<'e>(rhs: f64) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic { function: Mod, rhs }.flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "power",
    schema = "toolkit_experimental"
)]
pub fn pipeline_power<'e>(rhs: f64) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Power,
        rhs,
    }
    .flatten()
}

// log(double) already exists as the log base 10 so we need a new name
#[pg_extern(
    immutable,
    parallel_safe,
    name = "logn",
    schema = "toolkit_experimental"
)]
pub fn pipeline_log_n<'e>(rhs: f64) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: LogN,
        rhs,
    }
    .flatten()
}

//
// unary operations
//

#[pg_extern(
    immutable,
    parallel_safe,
    name = "abs",
    schema = "toolkit_experimental"
)]
pub fn pipeline_abs<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Abs,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "cbrt",
    schema = "toolkit_experimental"
)]
pub fn pipeline_cbrt<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Cbrt,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "ceil",
    schema = "toolkit_experimental"
)]
pub fn pipeline_ceil<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Ceil,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "floor",
    schema = "toolkit_experimental"
)]
pub fn pipeline_floor<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Floor,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(immutable, parallel_safe, name = "ln", schema = "toolkit_experimental")]
pub fn pipeline_ln<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Ln,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "log10",
    schema = "toolkit_experimental"
)]
pub fn pipeline_log10<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Log10,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "round",
    schema = "toolkit_experimental"
)]
pub fn pipeline_round<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Round,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "sign",
    schema = "toolkit_experimental"
)]
pub fn pipeline_sign<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Sign,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "sqrt",
    schema = "toolkit_experimental"
)]
pub fn pipeline_sqrt<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Sqrt,
        rhs: 0.0,
    }
    .flatten()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "trunc",
    schema = "toolkit_experimental"
)]
pub fn pipeline_trunc<'e>() -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Arithmetic {
        function: Trunc,
        rhs: 0.0,
    }
    .flatten()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn test_simple_arith_binops() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {}", sp), None, None)
                .unwrap();

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> add(1.0))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:26),\
                (ts:\"2020-01-01 00:00:00+00\",val:11),\
                (ts:\"2020-01-03 00:00:00+00\",val:21),\
                (ts:\"2020-01-02 00:00:00+00\",val:16),\
                (ts:\"2020-01-05 00:00:00+00\",val:31)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> sub(3.0))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:22),\
                (ts:\"2020-01-01 00:00:00+00\",val:7),\
                (ts:\"2020-01-03 00:00:00+00\",val:17),\
                (ts:\"2020-01-02 00:00:00+00\",val:12),\
                (ts:\"2020-01-05 00:00:00+00\",val:27)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> mul(2.0))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:50),\
                (ts:\"2020-01-01 00:00:00+00\",val:20),\
                (ts:\"2020-01-03 00:00:00+00\",val:40),\
                (ts:\"2020-01-02 00:00:00+00\",val:30),\
                (ts:\"2020-01-05 00:00:00+00\",val:60)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> div(5.0))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:5),\
                (ts:\"2020-01-01 00:00:00+00\",val:2),\
                (ts:\"2020-01-03 00:00:00+00\",val:4),\
                (ts:\"2020-01-02 00:00:00+00\",val:3),\
                (ts:\"2020-01-05 00:00:00+00\",val:6)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> mod(5.0))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:0),\
                (ts:\"2020-01-01 00:00:00+00\",val:0),\
                (ts:\"2020-01-03 00:00:00+00\",val:0),\
                (ts:\"2020-01-02 00:00:00+00\",val:0),\
                (ts:\"2020-01-05 00:00:00+00\",val:0)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> power(2.0))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:625),\
                (ts:\"2020-01-01 00:00:00+00\",val:100),\
                (ts:\"2020-01-03 00:00:00+00\",val:400),\
                (ts:\"2020-01-02 00:00:00+00\",val:225),\
                (ts:\"2020-01-05 00:00:00+00\",val:900)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> logn(10.0))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:1.3979400086720375),\
                (ts:\"2020-01-01 00:00:00+00\",val:1),\
                (ts:\"2020-01-03 00:00:00+00\",val:1.301029995663981),\
                (ts:\"2020-01-02 00:00:00+00\",val:1.1760912590556811),\
                (ts:\"2020-01-05 00:00:00+00\",val:1.4771212547196624)\
            ],null_val:[0])"
            );
        });
    }

    #[pg_test]
    fn test_simple_arith_unaryops() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {}", sp), None, None)
                .unwrap();

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.5), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, -10.1), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.2), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, -15.6), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.3)) as v(time, value)";

            let val = client
                .update(
                    &format!("SELECT (series -> abs())::TEXT FROM ({}) s", create_series),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25.5),\
                (ts:\"2020-01-01 00:00:00+00\",val:10.1),\
                (ts:\"2020-01-03 00:00:00+00\",val:20.2),\
                (ts:\"2020-01-02 00:00:00+00\",val:15.6),\
                (ts:\"2020-01-05 00:00:00+00\",val:30.3)\
            ],null_val:[0])"
            );

            // TODO re-enable once made stable
            // let val = client.update(
            //     &format!("SELECT (series -> cbrt())::TEXT FROM ({}) s", create_series),
            //     None,
            //     None
            // )
            //     .first()
            //     .get_one::<String>().unwrap();
            // assert_eq!(val.unwrap(), "[\
            //     (ts:\"2020-01-04 00:00:00+00\",val:2.943382658441668),\
            //     (ts:\"2020-01-01 00:00:00+00\",val:-2.161592332945083),\
            //     (ts:\"2020-01-03 00:00:00+00\",val:2.7234356815688767),\
            //     (ts:\"2020-01-02 00:00:00+00\",val:-2.4986659549227817),\
            //     (ts:\"2020-01-05 00:00:00+00\",val:3.117555613369834)\
            // ]");

            let val = client
                .update(
                    &format!("SELECT (series -> ceil())::TEXT FROM ({}) s", create_series),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:26),\
                (ts:\"2020-01-01 00:00:00+00\",val:-10),\
                (ts:\"2020-01-03 00:00:00+00\",val:21),\
                (ts:\"2020-01-02 00:00:00+00\",val:-15),\
                (ts:\"2020-01-05 00:00:00+00\",val:31)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> floor())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:-11),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:-16),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            // TODO why are there `null`s here?
            // Josh - likely JSON can't represent nans correctly...
            // TODO re-enable once made stable
            // let val = client.update(
            //     &format!("SELECT (series -> ln())::TEXT FROM ({}) s", create_series),
            //     None,
            //     None
            // )
            //     .first()
            //     .get_one::<String>().unwrap();
            // assert_eq!(val.unwrap(), "[\
            //     (ts:\"2020-01-04 00:00:00+00\",val:3.2386784521643803),\
            //     (ts:\"2020-01-01 00:00:00+00\",val:null),\
            //     (ts:\"2020-01-03 00:00:00+00\",val:3.005682604407159),\
            //     (ts:\"2020-01-02 00:00:00+00\",val:null),\
            //     (ts:\"2020-01-05 00:00:00+00\",val:3.4111477125153233)\
            // ]");

            // TODO re-enable once made stable
            // let val = client.update(
            //     &format!("SELECT (series -> log10())::TEXT FROM ({}) s", create_series),
            //     None,
            //     None
            // )
            //     .first()
            //     .get_one::<String>().unwrap();
            // assert_eq!(val.unwrap(), "[\
            //     (ts:\"2020-01-04 00:00:00+00\",val:1.4065401804339552),\
            //     (ts:\"2020-01-01 00:00:00+00\",val:null),\
            //     (ts:\"2020-01-03 00:00:00+00\",val:1.3053513694466237),\
            //     (ts:\"2020-01-02 00:00:00+00\",val:null),\
            //     (ts:\"2020-01-05 00:00:00+00\",val:1.481442628502305)\
            // ]");

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> round())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:26),\
                (ts:\"2020-01-01 00:00:00+00\",val:-10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:-16),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            let val = client
                .update(
                    &format!("SELECT (series -> sign())::TEXT FROM ({}) s", create_series),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:1),\
                (ts:\"2020-01-01 00:00:00+00\",val:-1),\
                (ts:\"2020-01-03 00:00:00+00\",val:1),\
                (ts:\"2020-01-02 00:00:00+00\",val:-1),\
                (ts:\"2020-01-05 00:00:00+00\",val:1)\
            ],null_val:[0])"
            );

            // TODO re-enable once made stable
            // let val = client.update(
            //     &format!("SELECT (series -> sqrt())::TEXT FROM ({}) s", create_series),
            //     None,
            //     None
            // )
            //     .first()
            //     .get_one::<String>().unwrap();
            // assert_eq!(val.unwrap(), "[\
            //     (ts:\"2020-01-04 00:00:00+00\",val:5.049752469181039),\
            //     (ts:\"2020-01-01 00:00:00+00\",val:null),\
            //     (ts:\"2020-01-03 00:00:00+00\",val:4.494441010848846),\
            //     (ts:\"2020-01-02 00:00:00+00\",val:null),\
            //     (ts:\"2020-01-05 00:00:00+00\",val:5.504543577809154)\
            // ]");

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> trunc())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:-10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:-15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );
        });
    }
}
