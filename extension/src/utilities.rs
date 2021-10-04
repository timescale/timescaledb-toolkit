use pgx::*;
use pg_sys::{TimestampTz};

#[pg_extern(name="generate_periodic_normal_series", schema = "toolkit_experimental")]
pub fn default_generate_periodic_normal_series(
    series_start: pg_sys::TimestampTz,
    rng_seed: Option<i64>,
) -> impl std::iter::Iterator<Item = (name!(time,TimestampTz),name!(value,f64))> + 'static {
    generate_periodic_normal_series(series_start, None, None, None, None, None, None, rng_seed)
}

pub fn alternate_generate_periodic_normal_series(
    series_start: pg_sys::TimestampTz,
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

#[pg_extern(schema = "toolkit_experimental")]
pub fn generate_periodic_normal_series(
    series_start: pg_sys::TimestampTz,
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
    let series_len = match series_len {
        Some(v) => v,
        None => 28 * DAY
    };
    let sample_interval = match sample_interval {
        Some(v) => v,
        None => 10 * MIN
    };
    let base_value = match base_value {
        Some(v) => v,
        None => 1000.0
    };
    let period = match period {
        Some(v) => v,
        None => 1 * DAY
    };
    let periodic_magnitude = match periodic_magnitude {
        Some(v) => v,
        None => 100.0
    };
    let standard_deviation = match standard_deviation {
        Some(v) => v,
        None => 100.0
    };

    use rand_distr::Distribution;
    use rand::SeedableRng;
    use rand_chacha::ChaCha12Rng;

    let mut rng = match rng_seed {
        Some(v) => ChaCha12Rng::seed_from_u64(v as u64),
        None => ChaCha12Rng::from_entropy()
    };

    let distribution = rand_distr::Normal::new(0.0, standard_deviation).unwrap();

    (0..series_len).step_by(sample_interval as usize).map(move |accum| {
        let time = series_start + accum;
        let base = base_value + f64::sin(accum as f64 / (2.0 * std::f64::consts::PI * period as f64)) * periodic_magnitude;
        let error = distribution.sample(&mut rng);
        (time, base + error)
    })
}

// Convert a timestamp to a double precision unix epoch
extension_sql!(r#"
CREATE OR REPLACE FUNCTION toolkit_experimental.to_epoch(timestamptz) RETURNS DOUBLE PRECISION LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
SELECT EXTRACT(EPOCH FROM $1);
$$;
"#);

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};
    use pgx::*;

    #[pg_test]
    fn test_to_epoch() {
        Spi::execute(|client| {
            let test_val = client
                .select("SELECT toolkit_experimental.to_epoch(now())", None, None)
                .first()
                .get_one::<f64>().unwrap();
            let now =  SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            assert!((now.as_secs_f64() - test_val).abs() < 0.05);
        });
    }
}
