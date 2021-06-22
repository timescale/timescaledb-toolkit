
use std::{convert::TryInto, mem::replace, ops::Deref, slice};

use serde::{Serialize, Deserialize};

use pgx::*;

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
#[pg_extern]
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
                // NaNs are nonsensical in the context of a percentile, so exclude them
                Some(value) => if value.is_nan() {return state} else {value},
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
#[pg_extern]
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
                    assert_eq!(state1.digested.max_size(), state2.digested.max_size());
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

#[pg_extern]
pub fn tdigest_serialize(
    mut state: Internal<TDigestTransState>,
) -> bytea {
    state.digest();
    crate::do_serialize!(state)
}

#[pg_extern(strict)]
pub fn tdigest_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TDigestTransState> {
    crate::do_deserialize!(bytes, TDigestTransState)
}

// PG object for the digest.
pg_type! {
    #[derive(Debug)]
    struct TDigest {
        buckets: u32,
        max_buckets: u32,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        centroids: [Centroid; self.buckets],
    }
}

json_inout_funcs!(TDigest);
varlena_type!(TDigest);

impl<'input> TDigest<'input> {
    fn to_internal_tdigest(&self) -> InternalTDigest {
        InternalTDigest::new(
            Vec::from(self.centroids),
            *self.sum,
            *self.count,
            *self.max,
            *self.0.min,
            *self.max_buckets as usize
        )
    }

    fn from_internal_tdigest(digest: &InternalTDigest) -> TDigest<'input> {
        let max_buckets: u32 = digest.max_size().try_into().unwrap();

        let centroids = digest.raw_centroids();

        // we need to flatten the vector to a single buffer that contains
        // both the size, the data, and the varlen header
        unsafe {
            flatten!(
                TDigest {
                    max_buckets: &max_buckets,
                    buckets: &(centroids.len() as u32),
                    count: &digest.count(),
                    sum: &digest.sum(),
                    min: &digest.min(),
                    max: &digest.max(),
                    centroids: &centroids,
                }
            )
        }
    }
}

// PG function to generate a user-facing TDigest object from an internal TDigestTransState.
#[pg_extern]
fn tdigest_final(
    state: Option<Internal<TDigestTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TDigest<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.digest();

            TDigest::from_internal_tdigest(&state.digested).into()
        })
    }
}


extension_sql!(r#"
CREATE AGGREGATE tdigest(size int, value DOUBLE PRECISION)
(
    sfunc = tdigest_trans,
    stype = internal,
    finalfunc = tdigest_final,
    combinefunc = tdigest_combine,
    serialfunc = tdigest_serialize,
    deserialfunc = tdigest_deserialize,
    parallel = safe
);
"#);

#[pg_extern]
pub fn tdigest_compound_trans(
    state: Option<Internal<InternalTDigest>>,
    value: Option<TDigest<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<InternalTDigest>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (a, None) => a,
                (None, Some(a)) => Some(a.to_internal_tdigest().into()),
                (Some(a), Some(b)) => {
                    assert_eq!(a.max_size(), *b.max_buckets as usize);
                    Some(InternalTDigest::merge_digests(
                            vec![a.deref().clone(), b.to_internal_tdigest()]  // TODO: TDigest merge with self
                        ).into())
                }
            }
        })
    }
}

#[pg_extern]
pub fn tdigest_compound_combine(
    state1: Option<Internal<InternalTDigest>>,
    state2: Option<Internal<InternalTDigest>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<InternalTDigest>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => {
                    assert_eq!(state1.max_size(), state2.max_size());
                    Some(InternalTDigest::merge_digests(
                            vec![state1.deref().clone(), state2.deref().clone()]  // TODO: TDigest merge with self
                        ).into())
                }
            }
        })
    }
}

#[pg_extern]
fn tdigest_compound_final(
    state: Option<Internal<InternalTDigest>>,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TDigest<'static>> {
    match state {
        None => None,
        Some(state) => Some(TDigest::from_internal_tdigest(&state.deref())),
    }
}

#[pg_extern]
fn tdigest_compound_serialize(
    state: Internal<InternalTDigest>,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> bytea {
    crate::do_serialize!(state)
}

#[pg_extern]
pub fn tdigest_compound_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<InternalTDigest> {
    crate::do_deserialize!(bytes, InternalTDigest)
}

extension_sql!(r#"
CREATE AGGREGATE rollup(
    tdigest
) (
    sfunc = tdigest_compound_trans,
    stype = internal,
    finalfunc = tdigest_compound_final,
    combinefunc = tdigest_compound_combine,
    serialfunc = tdigest_compound_serialize,
    deserialfunc = tdigest_compound_deserialize,
    parallel = safe
);
"#);

//---- Available PG operations on the digest

// Approximate the value at the given quantile (0.0-1.0)
#[pg_extern(immutable, parallel_safe, name="approx_percentile")]
pub fn tdigest_quantile(
    quantile: f64,
    digest: TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.to_internal_tdigest().estimate_quantile(quantile)
}

// Approximate the quantile at the given value
#[pg_extern(immutable, parallel_safe, name="approx_percentile_rank")]
pub fn tdigest_quantile_at_value(
    value: f64,
    digest: TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.to_internal_tdigest().estimate_quantile_at_value(value)
}

// Number of elements from which the digest was built.
#[pg_extern(immutable, parallel_safe, name="num_vals")]
pub fn tdigest_count(
    digest: TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.count as f64
}

// Minimum value entered in the digest.
#[pg_extern(immutable, parallel_safe, name="min_val")]
pub fn tdigest_min(
    digest: TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.min
}

// Maximum value entered in the digest.
#[pg_extern(immutable, parallel_safe, name="max_val")]
pub fn tdigest_max(
    digest: TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.max
}

// Average of all the values entered in the digest.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern(immutable, parallel_safe, name="mean")]
pub fn tdigest_mean(
    digest: TDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    if *digest.count > 0 {
        *digest.sum / *digest.count as f64
    } else {
        0.0
    }
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

            client.select("CREATE VIEW digest AS \
                SELECT tdigest(100, data) FROM test",
                None,
                None
            );

            let (min, max, count) = client
                .select("SELECT \
                    min_val(tdigest), \
                    max_val(tdigest), \
                    num_vals(tdigest) \
                    FROM digest",
                    None,
                    None
                )
                .first()
                .get_three::<f64, f64, f64>();

            apx_eql(min.unwrap(), 0.01, 0.000001);
            apx_eql(max.unwrap(), 100.0, 0.000001);
            apx_eql(count.unwrap(), 10000.0, 0.000001);

            let mean = client
                .select("SELECT \
                    mean(tdigest) \
                    FROM digest",
                    None,
                    None
                )
                .first()
                .get_one::<f64>();

            apx_eql(mean.unwrap(), 50.005, 0.0001);

            for i in 0..=100 {
                let value = i as f64;
                let quantile = value / 100.0;

                let (est_val, est_quant) = client
                    .select(
                        &format!("SELECT
                            approx_percentile({}, tdigest), \
                            approx_percentile_rank({}, tdigest) \
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
                    approx_percentile(\
                        0.99, \
                        tdigest(100, data)) \
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
                tdigest(100, data)::text \
                FROM generate_series(1, 100) data;",
                None,
                None)
                .first()
                .get_one::<String>();

            let expected = "{\"version\":1,\"buckets\":88,\"max_buckets\":100,\"count\":100,\"sum\":5050.0,\"min\":1.0,\"max\":100.0,\"centroids\":[{\"mean\":1.0,\"weight\":1},{\"mean\":2.0,\"weight\":1},{\"mean\":3.0,\"weight\":1},{\"mean\":4.0,\"weight\":1},{\"mean\":5.0,\"weight\":1},{\"mean\":6.0,\"weight\":1},{\"mean\":7.0,\"weight\":1},{\"mean\":8.0,\"weight\":1},{\"mean\":9.0,\"weight\":1},{\"mean\":10.0,\"weight\":1},{\"mean\":11.0,\"weight\":1},{\"mean\":12.0,\"weight\":1},{\"mean\":13.0,\"weight\":1},{\"mean\":14.0,\"weight\":1},{\"mean\":15.0,\"weight\":1},{\"mean\":16.0,\"weight\":1},{\"mean\":17.0,\"weight\":1},{\"mean\":18.0,\"weight\":1},{\"mean\":19.0,\"weight\":1},{\"mean\":20.0,\"weight\":1},{\"mean\":21.0,\"weight\":1},{\"mean\":22.0,\"weight\":1},{\"mean\":23.0,\"weight\":1},{\"mean\":24.0,\"weight\":1},{\"mean\":25.0,\"weight\":1},{\"mean\":26.0,\"weight\":1},{\"mean\":27.0,\"weight\":1},{\"mean\":28.0,\"weight\":1},{\"mean\":29.0,\"weight\":1},{\"mean\":30.0,\"weight\":1},{\"mean\":31.0,\"weight\":1},{\"mean\":32.0,\"weight\":1},{\"mean\":33.0,\"weight\":1},{\"mean\":34.0,\"weight\":1},{\"mean\":35.0,\"weight\":1},{\"mean\":36.0,\"weight\":1},{\"mean\":37.0,\"weight\":1},{\"mean\":38.0,\"weight\":1},{\"mean\":39.0,\"weight\":1},{\"mean\":40.0,\"weight\":1},{\"mean\":41.0,\"weight\":1},{\"mean\":42.0,\"weight\":1},{\"mean\":43.0,\"weight\":1},{\"mean\":44.0,\"weight\":1},{\"mean\":45.0,\"weight\":1},{\"mean\":46.0,\"weight\":1},{\"mean\":47.0,\"weight\":1},{\"mean\":48.0,\"weight\":1},{\"mean\":49.0,\"weight\":1},{\"mean\":50.0,\"weight\":1},{\"mean\":51.0,\"weight\":1},{\"mean\":52.5,\"weight\":2},{\"mean\":54.5,\"weight\":2},{\"mean\":56.5,\"weight\":2},{\"mean\":58.5,\"weight\":2},{\"mean\":60.5,\"weight\":2},{\"mean\":62.5,\"weight\":2},{\"mean\":64.0,\"weight\":1},{\"mean\":65.5,\"weight\":2},{\"mean\":67.5,\"weight\":2},{\"mean\":69.0,\"weight\":1},{\"mean\":70.5,\"weight\":2},{\"mean\":72.0,\"weight\":1},{\"mean\":73.5,\"weight\":2},{\"mean\":75.0,\"weight\":1},{\"mean\":76.0,\"weight\":1},{\"mean\":77.5,\"weight\":2},{\"mean\":79.0,\"weight\":1},{\"mean\":80.0,\"weight\":1},{\"mean\":81.5,\"weight\":2},{\"mean\":83.0,\"weight\":1},{\"mean\":84.0,\"weight\":1},{\"mean\":85.0,\"weight\":1},{\"mean\":86.0,\"weight\":1},{\"mean\":87.0,\"weight\":1},{\"mean\":88.0,\"weight\":1},{\"mean\":89.0,\"weight\":1},{\"mean\":90.0,\"weight\":1},{\"mean\":91.0,\"weight\":1},{\"mean\":92.0,\"weight\":1},{\"mean\":93.0,\"weight\":1},{\"mean\":94.0,\"weight\":1},{\"mean\":95.0,\"weight\":1},{\"mean\":96.0,\"weight\":1},{\"mean\":97.0,\"weight\":1},{\"mean\":98.0,\"weight\":1},{\"mean\":99.0,\"weight\":1},{\"mean\":100.0,\"weight\":1}]}";

            assert_eq!(output, Some(expected.into()));

            let estimate = client.select(
                &format!(
                    "SELECT approx_percentile(0.90, '{}'::tdigest)",
                    expected
                ), None, None)
                .first()
                .get_one();
            assert_eq!(estimate, Some(90.5));
        });
    }

    #[pg_test]
    fn test_tdigest_compound_agg() {
        Spi::execute(|client| {
            client.select("CREATE TABLE new_test (device INTEGER, value DOUBLE PRECISION)", None, None);
            client.select("INSERT INTO new_test SELECT dev, dev - v FROM generate_series(1,10) dev, generate_series(0, 1.0, 0.01) v", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM new_test", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(Some(1010), sanity);

            client.select("CREATE VIEW digests AS \
                SELECT device, tdigest(20, value) \
                FROM new_test \
                GROUP BY device", None, None);

            client.select("CREATE VIEW composite AS \
                SELECT tdigest(tdigest) \
                FROM digests", None, None);

            client.select("CREATE VIEW base AS \
                SELECT tdigest(20, value) \
                FROM new_test", None, None);

            let value= client
                .select("SELECT \
                    approx_percentile(0.9, tdigest) \
                    FROM base", None, None)
                .first()
                .get_one::<f64>();

            let test_value = client
                .select("SELECT \
                approx_percentile(0.9, tdigest) \
                    FROM composite", None, None)
                .first()
                .get_one::<f64>();

            apx_eql(test_value.unwrap(), value.unwrap(), 0.1);
            apx_eql(test_value.unwrap(), 9.0, 0.1);
        });
    }
}
