use pgx::*;
use pg_sys::TimestampTz;
use rand::SeedableRng;
use rand_pcg::Pcg64Mcg;
use rand_distr::{Distribution, Uniform, Normal};

type Interval = pg_sys::Datum;

enum Dist {
    Uniform(Uniform<f64>),
    Normal(Normal<f64>),
}

const INVALID_PARAM: &'static str = "Invalid distribution parameter";

#[pg_extern(schema = "toolkit_experimental")]
pub fn gen_random_timeseries(
    start: TimestampTz,
    stop: TimestampTz,
    step: Interval,
    dist_params: Json,
    rng_seed: Option<i64>,
) -> impl std::iter::Iterator<Item = (name!(ts, TimestampTz), name!(value, f64))> {
    let mut rng = match rng_seed {
        Some(v) => Pcg64Mcg::seed_from_u64(v as u64),
        None =>  Pcg64Mcg::from_entropy(),
    };

    // create distribution according to dist_params
    let dist: Dist =
        if let Some(v) = dist_params.0.get("uniform") {
            Dist::Uniform(Uniform::new(
                v["low"].as_f64().expect(INVALID_PARAM),
                v["high"].as_f64().expect(INVALID_PARAM),
            ))
        } else if let Some(v) = dist_params.0.get("normal") {
            Dist::Normal(Normal::new(
                v["mean"].as_f64().expect(INVALID_PARAM),
                v["std_dev"].as_f64().expect(INVALID_PARAM),
            ).unwrap())
        } else {
            error!("Invalid distribution");
        };

    let mut results: Vec<(TimestampTz, f64)> = Vec::new();

    // fetch time series and generate random data for each row
    Spi::connect(|client| {
        client
            .select(
                "SELECT ts FROM generate_series($1, $2, $3) ts",
                None,
                Some(vec![
                    (PgBuiltInOids::TIMESTAMPTZOID.oid(), start.into_datum()),
                    (PgBuiltInOids::TIMESTAMPTZOID.oid(), stop.into_datum()),
                    (PgBuiltInOids::INTERVALOID.oid(), step.into_datum()),
                ]),
            )
            .for_each(|row| {
                let data = match dist {
                    Dist::Uniform(d) => d.sample(&mut rng),
                    Dist::Normal(d) => d.sample(&mut rng),
                };
                results.push((row["ts"].value().unwrap(), data));
            });
        Ok(Some(()))
    });

    results.into_iter()
}

#[pg_extern(name = "gen_random_timeseries", schema = "toolkit_experimental")]
pub fn default_gen_random_timeseries(
    start: TimestampTz,
    stop: TimestampTz,
    step: Interval,
    dist_params: Json,
) ->
impl std::iter::Iterator<Item = (name!(ts, TimestampTz), name!(value, f64))> {
    gen_random_timeseries(start, stop, step, dist_params, None)
}

extension_sql!(r#"
CREATE OR REPLACE FUNCTION toolkit_experimental.gen_random_timeseries(
    start TIMESTAMPTZ,
    stop TIMESTAMPTZ,
    step INTERVAL
) RETURNS TABLE(ts TIMESTAMPTZ, value DOUBLE PRECISION) as $$
    SELECT *
    FROM toolkit_experimental.gen_random_timeseries(start, stop, step,
        '{"uniform": {"low": 0.0, "high": 1.0}}'::json);
$$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
"#);

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_gen_random_timeseries() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);

            // basic usage
            let sql = r#"
                SELECT ts, value
                FROM toolkit_experimental.gen_random_timeseries(
                    '2021-01-01 00:00:00'::timestamptz,
                    '2021-01-01 00:00:00'::timestamptz+interval '1 day',
                    interval '1 hour',
                    '{"normal": {"mean": 10.0, "std_dev": 2.3}}',
                    123
                )"#;
            let val = client.select(sql, None, None).len();
            assert_eq!(val, 25);

            // use same random seed should generate same data
            let (_, val) = client.select(sql, None, None).first()
                .get_two::<i64, f64>();
            let val = val.unwrap();
            let (_, val2) = client.select(sql, None, None).first()
                .get_two::<i64, f64>();
            let val2 = val2.unwrap();
            assert_eq!(val, val2);

            let sql = r#"
                SELECT ts, value
                FROM toolkit_experimental.gen_random_timeseries(
                    '2021-01-01 00:00:00'::timestamptz,
                    '2021-01-01 00:00:00'::timestamptz+interval '1 day',
                    interval '1 hour',
                    '{"uniform": {"low":2.0, "high": 3.0}}'
                )"#;
            let val = client.select(sql, None, None).len();
            assert_eq!(val, 25);
            let (_, val) = client.select(sql, None, None).first()
                .get_two::<i64, f64>();
            let val = val.unwrap();
            assert!(val >= 2.0f64 && val < 3.0f64);

            // default random data should be in [0, 1)
            let sql = r#"
                SELECT ts, value
                FROM toolkit_experimental.gen_random_timeseries(
                    '2021-01-01 00:00:00'::timestamptz,
                    '2021-01-01 00:00:00'::timestamptz+interval '1 day',
                    interval '1 hour'
                )"#;
            let val = client.select(sql, None, None).len();
            assert_eq!(val, 25);
            let (_, val) = client.select(sql, None, None).first()
                .get_two::<i64, f64>();
            let val = val.unwrap();
            assert!(val >= 0.0f64 && val < 1.0f64);
        });
    }
}
