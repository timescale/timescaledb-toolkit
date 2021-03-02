
use std::{
    convert::TryInto,
    mem::replace,
    slice,
};

use serde::{Serialize, Deserialize};

use pgx::*;
use pg_sys::Datum;

use flat_serialize::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    json_inout_funcs,
    flatten,
    palloc::Internal, pg_type
};

use tdigest::{
    TDigest as InternalTDigest,
    Centroid,
};

// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
mod timescale_analytics_experimental {
    pub(crate) use super::*;
    extension_sql!(r#"
        CREATE SCHEMA IF NOT EXISTS timescale_analytics_experimental;
    "#);
}

// Intermediate state kept in postgres.  This is a tdigest object paired
// with a vector of values that still need to be inserted.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TDigestTransState {
    #[serde(skip)]
    buffer: Vec<f64>,
    digested: InternalTDigest,
}

impl TDigestTransState {
    // Add a new value, recalculate the digest if we've crossed a threshold.
    // TODO threshold is currently set to number of digest buckets, should this be adjusted
    fn push(&mut self, value: f64) {
        self.buffer.push(value);
        if self.buffer.len() >= self.digested.max_size() {
            self.digest()
        }
    }

    // Update the digest with all accumulated values.
    fn digest(&mut self) {
        if self.buffer.is_empty() {
            return
        }
        let new = replace(&mut self.buffer, vec![]);
        self.digested = self.digested.merge_unsorted(new)
    }
}

#[allow(non_camel_case_types)]
type int = u32;

// PG function for adding values to a digest.
// Null values are ignored.
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn tdigest_trans(
    state: Option<Internal<TDigestTransState>>,
    size: int,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TDigestTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                Some(value) => value,
            };
            let mut state = match state {
                None => TDigestTransState{
                    buffer: vec![],
                    digested: InternalTDigest::new_with_size(size as _),
                }.into(),
                Some(state) => state,
            };
            state.push(value);
            Some(state)
        })
    }
}

// PG function for merging digests.
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn tdigest_combine(
    state1: Option<Internal<TDigestTransState>>,
    state2: Option<Internal<TDigestTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TDigestTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => {
                    let digvec = vec![state1.digested.clone(), state2.digested.clone()];
                    if !state1.buffer.is_empty() {
                        digvec[0].merge_unsorted(state1.buffer.clone());  // merge_unsorted should take a reference
                    }
                    if !state2.buffer.is_empty() {
                        digvec[1].merge_unsorted(state2.buffer.clone());
                    }

                    Some(TDigestTransState {
                            buffer: vec![],
                            digested: InternalTDigest::merge_digests(digvec),
                        }.into()
                    )
                }
            }
        })
    }
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn tdigest_serialize(
    mut state: Internal<TDigestTransState>,
) -> bytea {
    state.digest();
    crate::do_serialize!(state)
}

#[pg_extern(schema = "timescale_analytics_experimental", strict)]
pub fn tdigest_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TDigestTransState> {
    crate::do_deserialize!(bytes, TDigestTransState)
}

extension_sql!(r#"
CREATE TYPE timescale_analytics_experimental.TDigest;
"#);

// PG object for the digest.
pg_type! {
    #[derive(Debug)]
    struct TDigest {
        buckets: u32,
        count: u32,
        sum: f64,
        min: f64,
        max: f64,
        centroids: [Centroid; self.buckets],
        max_buckets: u32,
    }
}

json_inout_funcs!(TDigest);

impl<'input> TDigest<'input> {
    fn to_tdigest(&self) -> InternalTDigest {
        InternalTDigest::new(
            Vec::from(self.centroids),
            *self.sum,
            *self.count as f64,
            *self.max,
            *self.0.min,
            *self.max_buckets as usize
        )
    }
}

// PG function to generate a user-facing TDigest object from an internal TDigestTransState.
#[pg_extern(schema = "timescale_analytics_experimental")]
fn tdigest_final(
    state: Option<Internal<TDigestTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<timescale_analytics_experimental::TDigest<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.digest();

            let max_buckets: u32 = state.digested.max_size().try_into().unwrap();
            let count = state.digested.count() as u32;

            let centroids = state.digested.raw_centroids();

            // we need to flatten the vector to a single buffer that contains
            // both the size, the data, and the varlen header
            flatten!(
                TDigest {
                    max_buckets: &max_buckets,
                    buckets: &(centroids.len() as u32),
                    count: &count,
                    sum: &state.digested.sum(),
                    min: &state.digested.min(),
                    max: &state.digested.max(),
                    centroids: &centroids,
                }
            ).into()
        })
    }
}

extension_sql!(r#"
CREATE OR REPLACE FUNCTION
    timescale_analytics_experimental.TDigest_in(cstring)
RETURNS timescale_analytics_experimental.TDigest
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C
AS 'MODULE_PATHNAME', 'tdigest_in_wrapper';

CREATE OR REPLACE FUNCTION
    timescale_analytics_experimental.TDigest_out(timescale_analytics_experimental.TDigest)
RETURNS CString
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C
AS 'MODULE_PATHNAME', 'tdigest_out_wrapper';

CREATE TYPE timescale_analytics_experimental.TDigest (
    INTERNALLENGTH = variable,
    INPUT = timescale_analytics_experimental.TDigest_in,
    OUTPUT = timescale_analytics_experimental.TDigest_out,
    STORAGE = extended
);

CREATE AGGREGATE timescale_analytics_experimental.tdigest(size int, value DOUBLE PRECISION)
(
    sfunc = timescale_analytics_experimental.tdigest_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.tdigest_final,
    combinefunc = timescale_analytics_experimental.tdigest_combine,
    serialfunc = timescale_analytics_experimental.tdigest_serialize,
    deserialfunc = timescale_analytics_experimental.tdigest_deserialize
);
"#);

//---- Available PG operations on the digest

// Approximate the value at the given quantile (0.0-1.0)
#[pg_extern(name="quantile", schema = "timescale_analytics_experimental")]
pub fn tdigest_quantile(
    digest: timescale_analytics_experimental::TDigest,
    quantile: f64,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.to_tdigest().estimate_quantile(quantile)
}

// Approximate the quantile at the given value
#[pg_extern(name="quantile_at_value", schema = "timescale_analytics_experimental")]
pub fn tdigest_quantile_at_value(
    digest: timescale_analytics_experimental::TDigest,
    value: f64,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.to_tdigest().estimate_quantile_at_value(value)
}

// Number of elements from which the digest was built.
#[pg_extern(name="get_count", schema = "timescale_analytics_experimental")]
pub fn tdigest_count(
    digest: timescale_analytics_experimental::TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.count as f64
}

// Minimum value entered in the digest.
#[pg_extern(name="get_min", schema = "timescale_analytics_experimental")]
pub fn tdigest_min(
    digest: timescale_analytics_experimental::TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.min
}

// Maximum value entered in the digest.
#[pg_extern(name="get_max", schema = "timescale_analytics_experimental")]
pub fn tdigest_max(
    digest: timescale_analytics_experimental::TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.max
}

// Average of all the values entered in the digest.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern(name="mean", schema = "timescale_analytics_experimental")]
pub fn tdigest_mean(
    digest: timescale_analytics_experimental::TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    if *digest.count > 0 {
        *digest.sum / *digest.count as f64
    } else {
        0.0
    }
}

// Sum of all the values entered in the digest.
#[pg_extern(name="sum", schema = "timescale_analytics_experimental")]
pub fn tdigest_sum(
    digest: timescale_analytics_experimental::TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.sum
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
    fn test_tdigest_aggregate() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test (data DOUBLE PRECISION)", None, None);
            client.select("INSERT INTO test SELECT generate_series(0.01, 100, 0.01)", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM test", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(10000, sanity.unwrap());

            client.select(
                "SET timescale_analytics_acknowledge_auto_drop TO 'true'",
                None,
                None,
            );

            client.select("CREATE VIEW digest AS \
                SELECT timescale_analytics_experimental.tdigest(100, data) FROM test",
                None,
                None
            );

            client.select(
                "RESET timescale_analytics_acknowledge_auto_drop",
                None,
                None,
            );

            let (min, max, count) = client
                .select("SELECT \
                    timescale_analytics_experimental.get_min(tdigest), \
                    timescale_analytics_experimental.get_max(tdigest), \
                    timescale_analytics_experimental.get_count(tdigest) \
                    FROM digest",
                    None,
                    None
                )
                .first()
                .get_three::<f64, f64, f64>();

            apx_eql(min.unwrap(), 0.01, 0.000001);
            apx_eql(max.unwrap(), 100.0, 0.000001);
            apx_eql(count.unwrap(), 10000.0, 0.000001);

            let (mean, sum) = client
                .select("SELECT \
                    timescale_analytics_experimental.mean(tdigest), \
                    timescale_analytics_experimental.sum(tdigest) \
                    FROM digest",
                    None,
                    None
                )
                .first()
                .get_two::<f64, f64>();

            apx_eql(mean.unwrap(), 50.005, 0.0001);
            apx_eql(sum.unwrap(), 500050.0, 0.0001);

            for i in 0..=100 {
                let value = i as f64;
                let quantile = value / 100.0;

                let (est_val, est_quant) = client
                    .select(
                        &format!("SELECT
                            timescale_analytics_experimental.quantile(tdigest, {}), \
                            timescale_analytics_experimental.quantile_at_value(tdigest, {}) \
                            FROM digest",
                            quantile,
                            value),
                        None,
                        None)
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
    fn test_tdigest_small_count() {
        Spi::execute(|client| {
            let estimate = client.select("SELECT \
                    timescale_analytics_experimental.quantile(\
                        timescale_analytics_experimental.tdigest(100, data), \
                        0.99) \
                    FROM generate_series(1, 100) data;",
                None,
                None)
                .first()
                .get_one();

            assert_eq!(estimate, Some(99.5));
        });
    }

    #[pg_test]
    fn test_tdigest_io() {
        Spi::execute(|client| {
            let output = client.select("SELECT \
                timescale_analytics_experimental.tdigest(100, data)::text \
                FROM generate_series(1, 100) data;",
                None,
                None)
                .first()
                .get_one::<String>();

            let expected = "{\"version\":1,\"buckets\":88,\"count\":100,\"sum\":5050.0,\"min\":1.0,\"max\":100.0,\"centroids\":[{\"mean\":1.0,\"weight\":1.0},{\"mean\":2.0,\"weight\":1.0},{\"mean\":3.0,\"weight\":1.0},{\"mean\":4.0,\"weight\":1.0},{\"mean\":5.0,\"weight\":1.0},{\"mean\":6.0,\"weight\":1.0},{\"mean\":7.0,\"weight\":1.0},{\"mean\":8.0,\"weight\":1.0},{\"mean\":9.0,\"weight\":1.0},{\"mean\":10.0,\"weight\":1.0},{\"mean\":11.0,\"weight\":1.0},{\"mean\":12.0,\"weight\":1.0},{\"mean\":13.0,\"weight\":1.0},{\"mean\":14.0,\"weight\":1.0},{\"mean\":15.0,\"weight\":1.0},{\"mean\":16.0,\"weight\":1.0},{\"mean\":17.0,\"weight\":1.0},{\"mean\":18.0,\"weight\":1.0},{\"mean\":19.0,\"weight\":1.0},{\"mean\":20.0,\"weight\":1.0},{\"mean\":21.0,\"weight\":1.0},{\"mean\":22.0,\"weight\":1.0},{\"mean\":23.0,\"weight\":1.0},{\"mean\":24.0,\"weight\":1.0},{\"mean\":25.0,\"weight\":1.0},{\"mean\":26.0,\"weight\":1.0},{\"mean\":27.0,\"weight\":1.0},{\"mean\":28.0,\"weight\":1.0},{\"mean\":29.0,\"weight\":1.0},{\"mean\":30.0,\"weight\":1.0},{\"mean\":31.0,\"weight\":1.0},{\"mean\":32.0,\"weight\":1.0},{\"mean\":33.0,\"weight\":1.0},{\"mean\":34.0,\"weight\":1.0},{\"mean\":35.0,\"weight\":1.0},{\"mean\":36.0,\"weight\":1.0},{\"mean\":37.0,\"weight\":1.0},{\"mean\":38.0,\"weight\":1.0},{\"mean\":39.0,\"weight\":1.0},{\"mean\":40.0,\"weight\":1.0},{\"mean\":41.0,\"weight\":1.0},{\"mean\":42.0,\"weight\":1.0},{\"mean\":43.0,\"weight\":1.0},{\"mean\":44.0,\"weight\":1.0},{\"mean\":45.0,\"weight\":1.0},{\"mean\":46.0,\"weight\":1.0},{\"mean\":47.0,\"weight\":1.0},{\"mean\":48.0,\"weight\":1.0},{\"mean\":49.0,\"weight\":1.0},{\"mean\":50.0,\"weight\":1.0},{\"mean\":51.0,\"weight\":1.0},{\"mean\":52.5,\"weight\":2.0},{\"mean\":54.5,\"weight\":2.0},{\"mean\":56.5,\"weight\":2.0},{\"mean\":58.5,\"weight\":2.0},{\"mean\":60.5,\"weight\":2.0},{\"mean\":62.5,\"weight\":2.0},{\"mean\":64.0,\"weight\":1.0},{\"mean\":65.5,\"weight\":2.0},{\"mean\":67.5,\"weight\":2.0},{\"mean\":69.0,\"weight\":1.0},{\"mean\":70.5,\"weight\":2.0},{\"mean\":72.0,\"weight\":1.0},{\"mean\":73.5,\"weight\":2.0},{\"mean\":75.0,\"weight\":1.0},{\"mean\":76.0,\"weight\":1.0},{\"mean\":77.5,\"weight\":2.0},{\"mean\":79.0,\"weight\":1.0},{\"mean\":80.0,\"weight\":1.0},{\"mean\":81.5,\"weight\":2.0},{\"mean\":83.0,\"weight\":1.0},{\"mean\":84.0,\"weight\":1.0},{\"mean\":85.0,\"weight\":1.0},{\"mean\":86.0,\"weight\":1.0},{\"mean\":87.0,\"weight\":1.0},{\"mean\":88.0,\"weight\":1.0},{\"mean\":89.0,\"weight\":1.0},{\"mean\":90.0,\"weight\":1.0},{\"mean\":91.0,\"weight\":1.0},{\"mean\":92.0,\"weight\":1.0},{\"mean\":93.0,\"weight\":1.0},{\"mean\":94.0,\"weight\":1.0},{\"mean\":95.0,\"weight\":1.0},{\"mean\":96.0,\"weight\":1.0},{\"mean\":97.0,\"weight\":1.0},{\"mean\":98.0,\"weight\":1.0},{\"mean\":99.0,\"weight\":1.0},{\"mean\":100.0,\"weight\":1.0}],\"max_buckets\":100}";

            assert_eq!(output, Some(expected.into()));

            let estimate = client.select(
                &format!(
                    "SELECT timescale_analytics_experimental.quantile('{}'::timescale_analytics_experimental.tdigest, 0.90)",
                    expected
                ), None, None)
                .first()
                .get_one();
            assert_eq!(estimate, Some(90.5));
        });
    }
}
