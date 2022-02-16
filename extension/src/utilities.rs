use pgx::*;
use crate::raw::TimestampTz;

#[pg_extern(name="generate_periodic_normal_series", schema = "toolkit_experimental")]
pub fn default_generate_periodic_normal_series(
    series_start: crate::raw::TimestampTz,
    rng_seed: Option<i64>,
) -> impl std::iter::Iterator<Item = (name!(time,TimestampTz),name!(value,f64))> + 'static {
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
) -> impl std::iter::Iterator<Item = (name!(time,TimestampTz),name!(value,f64))> + 'static {
    generate_periodic_normal_series(series_start,
        Some(periods_per_series * points_per_period * seconds_between_points * 1000000),
        Some(seconds_between_points * 1000000), Some(base_value),
        Some(points_per_period * seconds_between_points * 1000000),Some(periodic_magnitude),
        Some(standard_deviation), rng_seed)
}

#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "toolkit_experimental")]
pub fn generate_periodic_normal_series(
    series_start: crate::raw::TimestampTz,
    series_len: Option<i64>, //pg_sys::Interval,
    sample_interval: Option<i64>, //pg_sys::Interval,
    base_value: Option<f64>,
    period: Option<i64>, //pg_sys::Interval,
    periodic_magnitude: Option<f64>,
    standard_deviation: Option<f64>,
    rng_seed: Option<i64>,
) -> impl std::iter::Iterator<Item = (name!(time,TimestampTz),name!(value,f64))> + 'static {
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

    use rand_distr::Distribution;
    use rand::SeedableRng;
    use rand_chacha::ChaCha12Rng;

    let mut rng = match rng_seed {
        Some(v) => ChaCha12Rng::seed_from_u64(v as u64),
        None => ChaCha12Rng::from_entropy()
    };

    let distribution = rand_distr::Normal::new(0.0, standard_deviation).unwrap();

    let series_start: i64 = series_start.into();
    (0..series_len).step_by(sample_interval as usize).map(move |accum| {
        let time = series_start + accum;
        let base = base_value + f64::sin(accum as f64 / (2.0 * std::f64::consts::PI * period as f64)) * periodic_magnitude;
        let error = distribution.sample(&mut rng);
        (time.into(), base + error)
    })
}

// Convert a timestamp to a double precision unix epoch
extension_sql!("\n\
CREATE OR REPLACE FUNCTION toolkit_experimental.to_epoch(timestamptz) RETURNS DOUBLE PRECISION LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$\n\
SELECT EXTRACT(EPOCH FROM $1);\n\
$$;\n\
",
name="to_epoch",
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_to_epoch() {
        Spi::execute(|client| {
            let test_val = client
                .select("SELECT toolkit_experimental.to_epoch('2021-01-01 00:00:00+03'::timestamptz)", None, None)
                .first()
                .get_one::<f64>().unwrap();
            assert!((test_val - 1609448400f64).abs() < f64::EPSILON);

            let test_val = client
                .select("SELECT toolkit_experimental.to_epoch('epoch'::timestamptz)", None, None)
                .first()
                .get_one::<f64>().unwrap();
            assert!((test_val - 0f64).abs() < f64::EPSILON);

            let test_val = client
                .select("SELECT toolkit_experimental.to_epoch('epoch'::timestamptz - interval '42 seconds')", None, None)
                .first()
                .get_one::<f64>().unwrap();
            assert!((test_val - -42f64).abs() < f64::EPSILON);
        });
    }
}
