use crate::raw::TimestampTz;
use pgrx::prelude::*;

#[pg_extern(
    name = "generate_periodic_normal_series",
    schema = "toolkit_experimental"
)]
pub fn default_generate_periodic_normal_series(
    series_start: crate::raw::TimestampTz,
    rng_seed: Option<i64>,
) -> TableIterator<'static, (name!(time, TimestampTz), name!(value, f64))> {
    generate_periodic_normal_series(series_start, None, None, None, None, None, None, rng_seed)
}

#[allow(clippy::too_many_arguments)]
pub fn alternate_generate_periodic_normal_series(
    series_start: crate::raw::TimestampTz,
    periods_per_series: i64,
    points_per_period: i64,
    seconds_between_points: i64,
    base_value: f64,
    periodic_magnitude: f64,
    standard_deviation: f64,
    rng_seed: Option<i64>,
) -> TableIterator<'static, (name!(time, TimestampTz), name!(value, f64))> {
    generate_periodic_normal_series(
        series_start,
        Some(periods_per_series * points_per_period * seconds_between_points * 1000000),
        Some(seconds_between_points * 1000000),
        Some(base_value),
        Some(points_per_period * seconds_between_points * 1000000),
        Some(periodic_magnitude),
        Some(standard_deviation),
        rng_seed,
    )
}

#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "toolkit_experimental")]
pub fn generate_periodic_normal_series(
    series_start: crate::raw::TimestampTz,
    series_len: Option<i64>,      //pg_sys::Interval,
    sample_interval: Option<i64>, //pg_sys::Interval,
    base_value: Option<f64>,
    period: Option<i64>, //pg_sys::Interval,
    periodic_magnitude: Option<f64>,
    standard_deviation: Option<f64>,
    rng_seed: Option<i64>,
) -> TableIterator<'static, (name!(time, TimestampTz), name!(value, f64))> {
    // Convenience consts to make defaults more readable
    const SECOND: i64 = 1000000;
    const MIN: i64 = 60 * SECOND;
    const HOUR: i64 = 60 * MIN;
    const DAY: i64 = 24 * HOUR;

    // TODO: exposing defaults in the PG function definition would be much nicer
    let series_len = series_len.unwrap_or(28 * DAY);
    let sample_interval = sample_interval.unwrap_or(10 * MIN);
    let base_value = base_value.unwrap_or(1000.0);
    let period = period.unwrap_or(DAY);
    let periodic_magnitude = periodic_magnitude.unwrap_or(100.0);
    let standard_deviation = standard_deviation.unwrap_or(100.0);

    use rand::SeedableRng;
    use rand_chacha::ChaCha12Rng;
    use rand_distr::Distribution;

    let mut rng = match rng_seed {
        Some(v) => ChaCha12Rng::seed_from_u64(v as u64),
        None => ChaCha12Rng::from_entropy(),
    };

    let distribution = rand_distr::Normal::new(0.0, standard_deviation).unwrap();

    let series_start: i64 = series_start.into();
    TableIterator::new(
        (0..series_len)
            .step_by(sample_interval as usize)
            .map(move |accum| {
                let time = series_start + accum;
                let base = base_value
                    + f64::sin(accum as f64 / (2.0 * std::f64::consts::PI * period as f64))
                        * periodic_magnitude;
                let error = distribution.sample(&mut rng);
                (time.into(), base + error)
            }),
    )
}

// Returns days in month
extension_sql!(
    "
CREATE FUNCTION days_in_month(date timestamptz) RETURNS int
SET search_path TO pg_catalog,pg_temp
AS $$
SELECT CAST(EXTRACT('day' FROM ($1 + interval '1 month' - $1)) as INTEGER)
$$ LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;
",
    name = "days_in_month",
);

// Normalizes metric based on reference date and days
extension_sql!(
    "
CREATE FUNCTION month_normalize(metric float8, reference_date timestamptz, days float8 DEFAULT 365.25/12) RETURNS float8
SET search_path TO pg_catalog,pg_temp
AS $$
SELECT metric * days / CAST(EXTRACT('day' FROM (reference_date + interval '1 month' - reference_date)) as INTEGER)
$$ LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;
",
    name="month_normalize",
);

// Convert a timestamp to a double precision unix epoch
extension_sql!(
    "
CREATE FUNCTION to_epoch(timestamptz) RETURNS DOUBLE PRECISION LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog,pg_temp
AS $$
SELECT EXTRACT(EPOCH FROM $1);
$$;
",
    name = "to_epoch",
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn test_to_epoch() {
        Spi::connect(|mut client| {
            let test_val = client
                .update(
                    "SELECT to_epoch('2021-01-01 00:00:00+03'::timestamptz)",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((test_val - 1609448400f64).abs() < f64::EPSILON);

            let test_val = client
                .update("SELECT to_epoch('epoch'::timestamptz)", None, None)
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((test_val - 0f64).abs() < f64::EPSILON);

            let test_val = client
                .update(
                    "SELECT to_epoch('epoch'::timestamptz - interval '42 seconds')",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((test_val - -42f64).abs() < f64::EPSILON);
        });
    }

    #[pg_test]
    fn test_days_in_month() {
        Spi::connect(|mut client| {
            let test_val = client
                .update(
                    "SELECT days_in_month('2021-01-01 00:00:00+03'::timestamptz)",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(test_val, 31);
        });

        Spi::connect(|mut client| {
            let test_val = client
                .update(
                    "SELECT days_in_month('2020-02-03 00:00:00+03'::timestamptz)",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(test_val, 29);
        });
    }
    #[pg_test]
    fn test_monthly_normalize() {
        Spi::connect(|mut client| {
            let test_val = client
                .update(
                    "SELECT month_normalize(1000,'2021-01-01 00:00:00+03'::timestamptz)",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert_eq!(test_val, 981.8548387096774f64);
        });
        Spi::connect(|mut client| {
            let test_val = client
                .update(
                    "SELECT month_normalize(1000,'2021-01-01 00:00:00+03'::timestamptz,30.5)",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert_eq!(test_val, 983.8709677419355f64);
        });
        Spi::connect(|mut client| {
            let test_val = client
                .update(
                    "SELECT month_normalize(1000,'2021-01-01 00:00:00+03'::timestamptz,30)",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert_eq!(test_val, 967.741935483871f64);
        });
    }
}
