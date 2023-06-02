use pgrx::*;

use super::*;

// TODO is (stable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name = "filter",
    schema = "toolkit_experimental"
)]
pub fn filter_lambda_pipeline_element<'l, 'e>(
    lambda: toolkit_experimental::Lambda<'l>,
) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    let expression = lambda.parse();
    if expression.ty() != &lambda::Type::Bool {
        panic!("invalid lambda type: the lambda must return a BOOLEAN")
    }

    Element::FilterLambda {
        lambda: lambda.into_data(),
    }
    .flatten()
}

pub fn apply_lambda_to<'a>(
    mut series: Timevector_TSTZ_F64<'a>,
    lambda: &lambda::LambdaData<'_>,
) -> Timevector_TSTZ_F64<'a> {
    let expression = lambda.parse();
    if expression.ty() != &lambda::Type::Bool {
        panic!("invalid lambda type: the lambda must return a BOOLEAN")
    }

    let mut executor = lambda::ExpressionExecutor::new(&expression);

    let invoke = |time: i64, value: f64| {
        use lambda::Value::*;
        executor.reset();
        let result = executor.exec(value, time);
        match result {
            Bool(b) => b,
            _ => unreachable!(),
        }
    };

    filter_lambda_over_series(&mut series, invoke);
    series
}

pub fn filter_lambda_over_series(
    series: &mut Timevector_TSTZ_F64<'_>,
    mut func: impl FnMut(i64, f64) -> bool,
) {
    series.points.as_owned().retain(|p| func(p.ts, p.val));
    series.num_points = series.points.len() as _;
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn test_pipeline_filter_lambda() {
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

            client
                .update(
                    "CREATE TABLE series(time timestamptz, value double precision)",
                    None,
                    None,
                )
                .unwrap();
            client
                .update(
                    "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                    None,
                    None,
                )
                .unwrap();

            let val = client
                .update(
                    "SELECT (timevector(time, value))::TEXT FROM series",
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
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ],null_val:[0])"
            );

            let val = client.update(
                "SELECT (timevector(time, value) -> filter($$ $time != '2020-01-05't and ($value = 10 or $value = 20) $$))::TEXT FROM series",
                None,
                None
            )
                .unwrap().first()
                .get_one::<String>().unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:2,flags:0,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20)\
            ],null_val:[0])"
            );
        });
    }
}
