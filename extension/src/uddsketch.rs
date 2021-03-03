
use std::{
    slice,
};

use pgx::*;
use pg_sys::Datum;

use flat_serialize::*;

use uddsketch::{
    UDDSketch as UddSketchInternal
};


use crate::{
    aggregate_utils::in_aggregate_context,
    json_inout_funcs,
    flatten,
    palloc::Internal, pg_type
};

// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
mod timescale_analytics_experimental {
    pub(crate) use super::*;
    extension_sql!(r#"
        CREATE SCHEMA IF NOT EXISTS timescale_analytics_experimental;
    "#);
}

#[allow(non_camel_case_types)]
type int = u32;

// PG function for adding values to a sketch.
// Null values are ignored.
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn uddsketch_trans(
    state: Option<Internal<UddSketchInternal>>,
    size: int,
    max_error: f64,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<UddSketchInternal>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                Some(value) => value,
            };
            let mut state = match state {
                None => UddSketchInternal::new(size as u64, max_error).into(),
                Some(state) => state,
            };
            state.add_value(value);
            Some(state)
        })
    }
}

// PG function for merging sketches.
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn uddsketch_combine(
    state1: Option<Internal<UddSketchInternal>>,
    state2: Option<Internal<UddSketchInternal>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<UddSketchInternal>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => {
                    let mut sketch = state1.clone();
                    sketch.merge_sketch(&state2);
                    Some(sketch.into())
                }
            }
        })
    }
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn uddsketch_serialize(
    state: Internal<UddSketchInternal>,
) -> bytea {
    crate::do_serialize!(state)
}

#[pg_extern(schema = "timescale_analytics_experimental", strict)]
pub fn uddsketch_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<UddSketchInternal> {
    crate::do_deserialize!(bytes, UddSketchInternal)
}

extension_sql!(r#"
CREATE TYPE timescale_analytics_experimental.UddSketch;
"#);

// PG object for the sketch.
pg_type! {
    struct UddSketch {
        alpha: f64,
        max_buckets: u32,
        num_buckets: u32,
        compactions: u64,
        count: u64,
        sum: f64,
        keys: [uddsketch::SketchHashKey; self.num_buckets],
        counts: [u64; self.num_buckets],
    }
}

json_inout_funcs!(UddSketch);

impl<'input> UddSketch<'input> {
    fn to_uddsketch(&self) -> UddSketchInternal {
        UddSketchInternal::new_from_data(*self.max_buckets as u64, *self.alpha, *self.compactions, *self.count, *self.sum, Vec::from(self.keys), Vec::from(self.counts))
    }
}

// PG function to generate a user-facing UddSketch object from a UddSketchInternal.
#[pg_extern(schema = "timescale_analytics_experimental")]
fn uddsketch_final(
    state: Option<Internal<UddSketchInternal>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<timescale_analytics_experimental::UddSketch<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };

            let mut keys = Vec::new();
            let mut counts = Vec::new();

            for entry in state.bucket_iter() {
                let (key, value) = entry;
                keys.push(key);
                counts.push(value);
            }

            // we need to flatten the vector to a single buffer that contains
            // both the size, the data, and the varlen header
            flatten!(
                UddSketch {
                    alpha: &state.max_error(),
                    max_buckets: &(state.max_allowed_buckets() as u32),
                    num_buckets: &(state.current_buckets_count() as u32),
                    compactions: &(state.times_compacted() as u64),
                    count: &state.count(),
                    sum: &state.sum(),
                    keys: &keys,
                    counts: &counts,
                }
            ).into()
        })
    }
}

extension_sql!(r#"
CREATE OR REPLACE FUNCTION timescale_analytics_experimental.UddSketch_in(cstring)
RETURNS timescale_analytics_experimental.UddSketch
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C
AS 'MODULE_PATHNAME', 'uddsketch_in_wrapper';

CREATE OR REPLACE FUNCTION timescale_analytics_experimental.UddSketch_out(
    timescale_analytics_experimental.UddSketch
) RETURNS CString IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C
AS 'MODULE_PATHNAME', 'uddsketch_out_wrapper';

CREATE TYPE timescale_analytics_experimental.UddSketch (
    INTERNALLENGTH = variable,
    INPUT = timescale_analytics_experimental.UddSketch_in,
    OUTPUT = timescale_analytics_experimental.UddSketch_out,
    STORAGE = extended
);

CREATE AGGREGATE timescale_analytics_experimental.uddsketch(
    size int, max_error DOUBLE PRECISION, value DOUBLE PRECISION
) (
    sfunc = timescale_analytics_experimental.uddsketch_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.uddsketch_final,
    combinefunc = timescale_analytics_experimental.uddsketch_combine,
    serialfunc = timescale_analytics_experimental.uddsketch_serialize,
    deserialfunc = timescale_analytics_experimental.uddsketch_deserialize
);
"#);

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn uddsketch_compound_trans(
    state: Option<Internal<UddSketchInternal>>,
    value: Option<timescale_analytics_experimental::UddSketch>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<UddSketchInternal>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                Some(value) => value.to_uddsketch(),
            };
            let mut state = match state {
                None => return Some(value.into()),
                Some(state) => state,
            };
            state.merge_sketch(&value);
            state.into()
        })
    }
}

extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.uddsketch(
    timescale_analytics_experimental.uddsketch
) (
    sfunc = timescale_analytics_experimental.uddsketch_compound_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.uddsketch_final,
    combinefunc = timescale_analytics_experimental.uddsketch_combine,
    serialfunc = timescale_analytics_experimental.uddsketch_serialize,
    deserialfunc = timescale_analytics_experimental.uddsketch_deserialize
);
"#);

//---- Available PG operations on the sketch

// Approximate the value at the given quantile (0.0-1.0)
#[pg_extern(name="quantile", schema = "timescale_analytics_experimental")]
pub fn uddsketch_quantile(
    sketch: timescale_analytics_experimental::UddSketch,
    quantile: f64,
) -> f64 {
    sketch.to_uddsketch().estimate_quantile(quantile)
}

// Approximate the quantile at the given value
#[pg_extern(name="quantile_at_value", schema = "timescale_analytics_experimental")]
pub fn uddsketch_quantile_at_value(
    sketch: timescale_analytics_experimental::UddSketch,
    value: f64,
) -> f64 {
    sketch.to_uddsketch().estimate_quantile_at_value(value)
}

// Number of elements from which the sketch was built.
#[pg_extern(name="get_count", schema = "timescale_analytics_experimental")]
pub fn uddsketch_count(
    sketch: timescale_analytics_experimental::UddSketch,
) -> f64 {
    *sketch.count as f64
}

// Average of all the values entered in the sketch.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern(name="mean", schema = "timescale_analytics_experimental")]
pub fn uddsketch_mean(
    sketch: timescale_analytics_experimental::UddSketch,
) -> f64 {
    if *sketch.count > 0 {
        *sketch.sum / *sketch.count as f64
    } else {
        0.0
    }
}

// The maximum error (relative to the true value) for any quantile estimate.
#[pg_extern(name="error", schema = "timescale_analytics_experimental")]
pub fn uddsketch_error(
    sketch: timescale_analytics_experimental::UddSketch
) -> f64 {
    *sketch.alpha
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    // Assert equality between two floats, within some fixed error range.
    fn apx_eql(value: f64, expected: f64, error: f64) {
        assert!((value - expected).abs() < error, "Float value {} differs from expected {} by more than {}", value, expected, error);
    }

    // Assert equality between two floats, within an error expressed as a fraction of the expected value.
    fn pct_eql(value: f64, expected: f64, pct_error: f64) {
        apx_eql(value, expected, pct_error * expected);
    }

    #[pg_test]
    fn test_aggregate() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test (data DOUBLE PRECISION)", None, None);
            client.select("INSERT INTO test SELECT generate_series(0.01, 100, 0.01)", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM test", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(Some(10000), sanity);

            client.select(
                "SET timescale_analytics_acknowledge_auto_drop TO 'true'",
                None,
                None,
            );

            client.select("CREATE VIEW sketch AS \
                SELECT timescale_analytics_experimental.uddsketch(100, 0.05, data) \
                FROM test", None, None);

            client.select(
                "RESET timescale_analytics_acknowledge_auto_drop",
                None,
                None,
            );

            let sanity = client
                .select("SELECT COUNT(*) FROM sketch", None, None)
                .first()
                .get_one::<i32>();
            assert!(sanity.unwrap_or(0) > 0);

            let (mean, count) = client
                .select("SELECT \
                    timescale_analytics_experimental.mean(uddsketch), \
                    timescale_analytics_experimental.get_count(uddsketch) \
                    FROM sketch", None, None)
                .first()
                .get_two::<f64, f64>();

            apx_eql(mean.unwrap(), 50.005, 0.0001);
            apx_eql(count.unwrap(), 10000.0, 0.000001);

            let error = client
                .select("SELECT \
                    timescale_analytics_experimental.error(uddsketch) \
                    FROM sketch", None, None)
                .first()
                .get_one::<f64>();

            apx_eql(error.unwrap(), 0.05, 0.0001);

            for i in 0..=100 {
                let value = i as f64;
                let quantile = value / 100.0;

                let (est_val, est_quant) = client
                    .select(
                        &format!("SELECT \
                                timescale_analytics_experimental.quantile(uddsketch, {}), \
                                timescale_analytics_experimental.quantile_at_value(uddsketch, {}) \
                            FROM sketch", quantile, value), None, None)
                    .first()
                    .get_two::<f64, f64>();

                if i == 0 {
                    pct_eql(est_val.unwrap(), 0.01, 1.0);
                    apx_eql(est_quant.unwrap(), quantile, 0.0001);
                } else {
                    pct_eql(est_val.unwrap(), value, 1.0);
                    pct_eql(est_quant.unwrap(), quantile, 1.0);
                }
            }
        });
    }

    #[pg_test]
    fn test_compound_agg() {
        Spi::execute(|client| {
            client.select("CREATE TABLE new_test (device INTEGER, value DOUBLE PRECISION)", None, None);
            client.select("INSERT INTO new_test SELECT dev, dev - v FROM generate_series(1,10) dev, generate_series(0, 1.0, 0.01) v", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM new_test", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(Some(1010), sanity);

            client.select(
                "SET timescale_analytics_acknowledge_auto_drop TO 'true'",
                None,
                None,
            );

            client.select("CREATE VIEW sketches AS \
                SELECT device, timescale_analytics_experimental.uddsketch(20, 0.01, value) \
                FROM new_test \
                GROUP BY device", None, None);

            client.select("CREATE VIEW composite AS \
                SELECT timescale_analytics_experimental.uddsketch(uddsketch) \
                FROM sketches", None, None);

            client.select("CREATE VIEW base AS \
                SELECT timescale_analytics_experimental.uddsketch(20, 0.01, value) \
                FROM new_test", None, None);
                
            let (value, error) = client
                .select("SELECT \
                    timescale_analytics_experimental.quantile(uddsketch, 0.9), \
                    timescale_analytics_experimental.error(uddsketch) \
                    FROM base", None, None)
                .first()
                .get_two::<f64, f64>();
                
            let (test_value, test_error) = client
                .select("SELECT \
                    timescale_analytics_experimental.quantile(uddsketch, 0.9), \
                    timescale_analytics_experimental.error(uddsketch) \
                    FROM composite", None, None)
                .first()
                .get_two::<f64, f64>();

            apx_eql(test_value.unwrap(), value.unwrap(), 0.0001);
            apx_eql(test_error.unwrap(), error.unwrap(), 0.000001);
            apx_eql(test_value.unwrap(), 9.0, test_error.unwrap());
        });
    }
}
