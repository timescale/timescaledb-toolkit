use std::{
    mem::{self, ManuallyDrop, MaybeUninit},
    ptr,
};

use pgx::*;

use super::*;

use crate::serialization::PgProcId;

// TODO is (stable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name = "map",
    schema = "toolkit_experimental"
)]
pub fn map_lambda_pipeline_element<'l, 'e>(
    lambda: toolkit_experimental::Lambda<'l>,
) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    let expression = lambda.parse();
    if expression.ty() != &lambda::Type::Double && !expression.ty_is_ts_point() {
        panic!("invalid lambda type: the lambda must return a DOUBLE PRECISION or (TimestampTZ, DOUBLE PRECISION)")
    }

    Element::MapLambda {
        lambda: lambda.into_data(),
    }
    .flatten()
}

pub fn apply_lambda_to<'a>(
    mut series: Timevector_TSTZ_F64<'a>,
    lambda: &lambda::LambdaData<'_>,
) -> Timevector_TSTZ_F64<'a> {
    let expression = lambda.parse();
    let only_val = expression.ty() == &lambda::Type::Double;
    if !only_val && !expression.ty_is_ts_point() {
        panic!("invalid lambda type: the lambda must return a DOUBLE PRECISION or (TimestampTZ, DOUBLE PRECISION)")
    }

    let mut executor = lambda::ExpressionExecutor::new(&expression);

    let invoke = |time: i64, value: f64| {
        use lambda::Value::*;
        executor.reset();
        let result = executor.exec(value, time);
        match result {
            Double(f) => (None, Some(f)),
            Time(t) => (Some(t), None),
            Tuple(cols) => match &*cols {
                [Time(t), Double(f)] => (Some(*t), Some(*f)),
                _ => unreachable!(),
            },

            _ => unreachable!(),
        }
    };

    map_lambda_over_series(&mut series, only_val, invoke);
    series
}

pub fn map_lambda_over_series(
    series: &mut Timevector_TSTZ_F64<'_>,
    only_val: bool,
    mut func: impl FnMut(i64, f64) -> (Option<i64>, Option<f64>),
) {
    for point in series.points.as_owned() {
        let (new_time, new_val) = func(point.ts, point.val);
        *point = TSPoint {
            ts: if only_val {
                point.ts
            } else {
                new_time.unwrap_or(point.ts)
            },
            val: new_val.unwrap_or(point.val),
        }
    }
}

#[pg_extern(
    stable,
    parallel_safe,
    name = "map_series",
    schema = "toolkit_experimental"
)]
pub fn map_series_pipeline_element<'e>(
    function: crate::raw::regproc,
) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    map_series_element(crate::raw::regproc::from(function.0)).flatten()
}

pub fn map_series_element<'a>(function: crate::raw::regproc) -> Element<'a> {
    let function: pg_sys::regproc = function.0.value().try_into().unwrap();
    check_user_function_type(function);
    Element::MapSeries {
        function: PgProcId(function),
    }
}

pub fn check_user_function_type(function: pg_sys::regproc) {
    let mut argtypes: *mut pg_sys::Oid = ptr::null_mut();
    let mut nargs: ::std::os::raw::c_int = 0;
    let rettype = unsafe { pg_sys::get_func_signature(function, &mut argtypes, &mut nargs) };

    if nargs != 1 {
        error!("invalid number of mapping function arguments, expected fn(timevector) RETURNS timevector")
    }

    assert!(!argtypes.is_null());
    if unsafe { *argtypes } != *crate::time_vector::TIMEVECTOR_OID {
        error!("invalid argument type, expected fn(timevector) RETURNS timevector")
    }

    if rettype != *crate::time_vector::TIMEVECTOR_OID {
        error!("invalid return type, expected fn(timevector) RETURNS timevector")
    }
}

pub fn apply_to_series(
    mut series: Timevector_TSTZ_F64<'_>,
    func: pg_sys::RegProcedure,
) -> Timevector_TSTZ_F64<'_> {
    let mut flinfo: pg_sys::FmgrInfo = unsafe { MaybeUninit::zeroed().assume_init() };
    unsafe {
        pg_sys::fmgr_info(func, &mut flinfo);
    };

    unsafe {
        // use pg_sys::FunctionCall1Coll to get the pg_guard
        let res = pg_sys::FunctionCall1Coll(
            &mut flinfo,
            pg_sys::InvalidOid,
            // SAFETY the input memory context will not end in the sub-function
            //        and the sub-function will allocate the returned timevector
            series.cached_datum_or_flatten(),
        );
        Timevector_TSTZ_F64::from_polymorphic_datum(res, false, pg_sys::InvalidOid)
            .expect("unexpected NULL in timevector mapping function")
    }
}

// TODO is (stable, parallel_safe) correct?
#[pg_extern(
    stable,
    parallel_safe,
    name = "map_data",
    schema = "toolkit_experimental"
)]
pub fn map_data_pipeline_element<'e>(
    function: crate::raw::regproc,
) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    let mut argtypes: *mut pg_sys::Oid = ptr::null_mut();
    let mut nargs: ::std::os::raw::c_int = 0;
    let rettype = unsafe {
        pg_sys::get_func_signature(
            function.0.value().try_into().unwrap(),
            &mut argtypes,
            &mut nargs,
        )
    };

    if nargs != 1 {
        error!("invalid number of mapping function arguments, expected fn(double precision) RETURNS double precision")
    }

    if unsafe { *argtypes } != pgx::PgBuiltInOids::FLOAT8OID.value() {
        error!("invalid argument type, expected fn(double precision) RETURNS double precision")
    }

    if rettype != pgx::PgBuiltInOids::FLOAT8OID.value() {
        error!("invalid return type, expected fn(double precision) RETURNS double precision")
    }

    Element::MapData {
        function: PgProcId(function.0.value().try_into().unwrap()),
    }
    .flatten()
}

pub fn apply_to(
    mut series: Timevector_TSTZ_F64<'_>,
    func: pg_sys::RegProcedure,
) -> Timevector_TSTZ_F64<'_> {
    let mut flinfo: pg_sys::FmgrInfo = unsafe { MaybeUninit::zeroed().assume_init() };

    let fn_addr: unsafe extern "C" fn(*mut pg_sys::FunctionCallInfoBaseData) -> pg_sys::Datum;
    let mut fc_info = unsafe {
        pg_sys::fmgr_info(func, &mut flinfo);
        fn_addr = flinfo.fn_addr.expect("null function in timevector map");
        union FcInfo1 {
            data: ManuallyDrop<pg_sys::FunctionCallInfoBaseData>,
            #[allow(dead_code)]
            bytes: [u8; mem::size_of::<pg_sys::FunctionCallInfoBaseData>()
                + mem::size_of::<pg_sys::NullableDatum>()],
        }
        FcInfo1 {
            data: ManuallyDrop::new(pg_sys::FunctionCallInfoBaseData {
                flinfo: &mut flinfo,
                context: std::ptr::null_mut(),
                resultinfo: std::ptr::null_mut(),
                fncollation: pg_sys::InvalidOid,
                isnull: false,
                nargs: 1,
                args: pg_sys::__IncompleteArrayField::new(),
            }),
        }
    };

    let invoke = |val: f64| unsafe {
        let fc_info = &mut *fc_info.data;
        let args = fc_info.args.as_mut_slice(1);
        args[0].value = val.into_datum().unwrap();
        args[0].isnull = false;
        let res = fn_addr(fc_info);
        f64::from_polymorphic_datum(res, fc_info.isnull, pg_sys::InvalidOid)
            .expect("unexpected NULL in timevector mapping function")
    };

    map_series(&mut series, invoke);
    series
}

pub fn map_series(series: &mut Timevector_TSTZ_F64<'_>, mut func: impl FnMut(f64) -> f64) {
    use std::panic::AssertUnwindSafe;

    let points = series.points.as_owned().iter_mut();
    // setjump guard around the loop to reduce the amount we have to
    // call it
    // NOTE need to be careful that there's not allocation within the
    //      loop body so it cannot leak
    pg_sys::PgTryBuilder::new(AssertUnwindSafe(|| {
        for point in points {
            *point = TSPoint {
                ts: point.ts,
                val: func(point.val),
            }
        }
    }))
    .execute()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_pipeline_map_lambda() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None,
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None,
            );

            let val = client
                .select(
                    "SELECT (timevector(time, value))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            let val = client.select(
                "SELECT (timevector(time, value) -> map($$ ($time + '1 day'i, $value * 2) $$))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-05 00:00:00+00\",val:50),\
                (ts:\"2020-01-02 00:00:00+00\",val:20),\
                (ts:\"2020-01-04 00:00:00+00\",val:40),\
                (ts:\"2020-01-03 00:00:00+00\",val:30),\
                (ts:\"2020-01-06 00:00:00+00\",val:60)\
            ],null_val:[0])"
            );
        });
    }

    #[pg_test]
    fn test_pipeline_map_lambda2() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None,
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None,
            );

            let val = client
                .select(
                    "SELECT (timevector(time, value))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            let expected = "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:725.7),\
                (ts:\"2020-01-01 00:00:00+00\",val:166.2),\
                (ts:\"2020-01-03 00:00:00+00\",val:489.2),\
                (ts:\"2020-01-02 00:00:00+00\",val:302.7),\
                (ts:\"2020-01-05 00:00:00+00\",val:1012.2)\
            ],null_val:[0])";
            let val = client
                .select(
                    "SELECT (timevector(time, value) \
                    -> map($$ ($time, $value^2 + $value * 2.3 + 43.2) $$))::TEXT \
                    FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), expected);

            let val = client
                .select(
                    "SELECT (timevector(time, value) \
                    -> map($$ ($value^2 + $value * 2.3 + 43.2) $$))::TEXT \
                    FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), expected);
        });
    }

    #[pg_test]
    fn test_pipeline_map_data() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None,
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None,
            );

            let val = client
                .select(
                    "SELECT (timevector(time, value))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            client.select(
                "CREATE FUNCTION x2(double precision) RETURNS DOUBLE PRECISION AS 'SELECT $1 * 2;' LANGUAGE SQL",
                None,
                None,
            );

            let val = client
                .select(
                    "SELECT (timevector(time, value) -> map_data('x2'))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
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
        });
    }

    #[pg_test]
    fn test_pipeline_map_series() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None,
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None,
            );

            let val = client
                .select(
                    "SELECT (timevector(time, value))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            client.select(
                "CREATE FUNCTION jan_3_x3(timevector_tstz_f64) RETURNS timevector_tstz_f64 AS $$\
                    SELECT timevector(time, value * 3) \
                    FROM (SELECT (unnest($1)).*) a \
                    WHERE time='2020-01-03 00:00:00+00';\
                $$ LANGUAGE SQL",
                None,
                None,
            );

            let val = client
                .select(
                    "SELECT (timevector(time, value) -> map_series('jan_3_x3'))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:1,flags:1,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-03 00:00:00+00\",val:60)\
            ],null_val:[0])"
            );
        });
    }

    #[pg_test]
    #[should_panic = "division by zero"]
    fn test_pipeline_map_series_failure() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None,
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None,
            );
            client.select(
                "CREATE FUNCTION always_fail(timevector_tstz_f64) RETURNS timevector_tstz_f64 AS
                $$
                    SELECT 0/0;
                    SELECT $1;
                $$ LANGUAGE SQL",
                None,
                None,
            );

            client
                .select(
                    "SELECT (timevector(time, value) -> map_series('always_fail'))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
        });
    }

    #[pg_test]
    #[should_panic = " returned NULL"]
    fn test_pipeline_map_series_null() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None,
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None,
            );
            client.select(
                "CREATE FUNCTION always_null(timevector_tstz_f64) RETURNS timevector_tstz_f64 AS
                $$
                    SELECT NULL::timevector_tstz_f64;
                $$ LANGUAGE SQL",
                None,
                None,
            );

            client
                .select(
                    "SELECT (timevector(time, value) -> map_series('always_null'))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
        });
    }

    #[pg_test]
    fn test_map_io() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None,
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None,
            );

            let val = client
                .select(
                    "SELECT (timevector(time, value))::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:5,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            client.select(
                "CREATE FUNCTION serier(timevector_tstz_f64) RETURNS timevector_tstz_f64 AS $$\
                    SELECT $1;\
                $$ LANGUAGE SQL",
                None,
                None,
            );

            client.select(
                "CREATE FUNCTION dater(double precision) RETURNS double precision AS $$\
                    SELECT $1 * 3;\
                $$ LANGUAGE SQL",
                None,
                None,
            );

            let (a, b) = client
                .select(
                    "SELECT map_series('serier')::TEXT, map_data('dater')::TEXT FROM series",
                    None,
                    None,
                )
                .first()
                .get_two::<String, String>();
            let one = "\
            (\
                version:1,\
                num_elements:1,\
                elements:[\
                    MapSeries(\
                        function:\"public.serier(public.timevector_tstz_f64)\"\
                    )\
                ]\
            )";
            let two = "\
            (\
                version:1,\
                num_elements:1,\
                elements:[\
                    MapData(\
                        function:\"public.dater(double precision)\"\
                    )\
                ]\
            )";
            assert_eq!((&*a.unwrap(), &*b.unwrap()), (one, two));

            // FIXME this doesn't work yet
            let (a, b) = client
                .select(
                    &*format!(
                        "SELECT \
                    '{}'::UnstableTimevectorPipeline::Text, \
                    '{}'::UnstableTimevectorPipeline::Text",
                        one, two
                    ),
                    None,
                    None,
                )
                .first()
                .get_two::<String, String>();
            assert_eq!((&*a.unwrap(), &*b.unwrap()), (one, two));
        });
    }
}
