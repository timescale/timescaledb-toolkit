
use std::{
    convert::TryInto,
    cmp::min,
    mem::replace,
    slice,
};

use serde::{Serialize, Deserialize};

use pgx::*;
use pg_sys::Datum;

use flat_serialize::*;

use crate::{
    aggregate_utils::{aggregate_mctx, in_aggregate_context},
    debug_inout_funcs,
    flatten,
    palloc::{Internal, in_memory_context}, pg_type
};

use tdigest::{
    TDigest,
    Centroid,
};

// Intermediate state kept in postgres.  This is a tdigest object paired
// with a vector of values that still need to be inserted.
#[derive(Serialize, Deserialize, Clone)]
pub struct TDigestTransState {
    #[serde(skip_serializing)]
    buffer: Vec<f64>,
    digested: TDigest,
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
    let mctx = aggregate_mctx(fcinfo);
    let mctx = match mctx {
        None => pgx::error!("cannot call as non-aggregate"),
        Some(mctx) => mctx,
    };
    unsafe {
        in_memory_context(mctx, || {
            let value = match value {
                None => return state,
                Some(value) => value,
            };
            let mut state = match state {
                None => TDigestTransState{
                    buffer: vec![],
                    digested: TDigest::new_with_size(size as _),
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
    let mctx = aggregate_mctx(fcinfo);
    let mctx = match mctx {
        None => pgx::error!("cannot call as non-aggregate"),
        Some(mctx) => mctx,
    };
    unsafe {
        in_memory_context(mctx, || {
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
                            digested: TDigest::merge_digests(digvec),
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

#[pg_extern]
pub fn tdigest_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TDigestTransState> {
    crate::do_deserialize!(bytes, TDigestTransState)
}

// PG object for the digest.
pg_type! {
    #[derive(Debug)]
    struct TimescaleTDigest {
        buckets: u32,
        count: u32,
        sum: f64,
        min: f64,
        max: f64,
        means: [f64; std::cmp::min(self.buckets, self.count)],
        weights: [u32; std::cmp::min(self.buckets, self.count)],
    }
}

debug_inout_funcs!(TimescaleTDigest);

impl<'input> TimescaleTDigest<'input> {
    fn to_tdigest(&self) -> TDigest {
        let size = min(*self.buckets, *self.count) as usize;
        let mut cents: Vec<Centroid> = Vec::new();

        for i in 0..size {
            cents.push(Centroid::new(self.means[i], self.weights[i] as f64));
        }

        TDigest::new(cents, *self.sum, *self.count as f64, *self.max, *self.0.min, *self.buckets as usize)
    }
}

// PG function to generate a user-facing TimescaleTDigest object from an internal TDigestTransState.
#[pg_extern]
fn tdigest_final(
    state: Option<Internal<TDigestTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TimescaleTDigest<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.digest();

            let buckets : u32 = state.digested.max_size().try_into().unwrap();
            let count = state.digested.count() as u32;
            let vec_size = min(buckets as usize, count as usize);
            let mut means = vec!(0.0; vec_size);
            let mut weights = vec!(0; vec_size);

            for (i, cent) in state.digested.raw_centroids().iter().enumerate() {
                means[i] = cent.mean();
                weights[i] = cent.weight() as u32;
            }

            // we need to flatten the vector to a single buffer that contains
            // both the size, the data, and the varlen header
            flatten!(
                TimescaleTDigest {
                    buckets: &buckets,
                    count: &count,
                    sum: &state.digested.sum(),
                    min: &state.digested.min(),
                    max: &state.digested.max(),
                    means: &means,
                    weights: &weights,
                }
            ).into()
        })
    }
}

//---- Available PG operations on the digest

// Approximate the value at the given quantile (0.0-1.0)
#[pg_extern]
pub fn tdigest_quantile(
    digest: TimescaleTDigest,
    quantile: f64,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.to_tdigest().estimate_quantile(quantile)
}

// Approximate the quantile at the given value
#[pg_extern]
pub fn tdigest_quantile_at_value(
    digest: TimescaleTDigest,
    value: f64,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.to_tdigest().estimate_quantile_at_value(value)
}

// Number of elements from which the digest was built.
#[pg_extern]
pub fn tdigest_count(
    digest: TimescaleTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.count as f64
}

// Minimum value entered in the digest.
#[pg_extern]
pub fn tdigest_min(
    digest: TimescaleTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.min
}

// Maximum value entered in the digest.
#[pg_extern]
pub fn tdigest_max(
    digest: TimescaleTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    *digest.max
}

// Average of all the values entered in the digest.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern]
pub fn tdigest_mean(
    digest: TimescaleTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    if *digest.count > 0 {
        *digest.sum / *digest.count as f64
    } else {
        0.0
    }
}

// Sum of all the values entered in the digest.
#[pg_extern]
pub fn tdigest_sum(
    digest: TimescaleTDigest,
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
    fn test_aggregate() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test (data DOUBLE PRECISION)", None, None);
            client.select("INSERT INTO test SELECT generate_series(0.01, 100, 0.01)", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM test", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(10000, sanity.unwrap());

            client.select("CREATE VIEW digest AS SELECT t_digest(100, data) FROM test", None, None);
            let (min, max, count) = client
                .select("SELECT tdigest_min(t_digest), tdigest_max(t_digest), tdigest_count(t_digest) FROM digest", None, None)
                .first()
                .get_three::<f64, f64, f64>();

            apx_eql(min.unwrap(), 0.01, 0.000001);
            apx_eql(max.unwrap(), 100.0, 0.000001);
            apx_eql(count.unwrap(), 10000.0, 0.000001);

            let (mean, sum) = client
                .select("SELECT tdigest_mean(t_digest), tdigest_sum(t_digest) FROM digest", None, None)
                .first()
                .get_two::<f64, f64>();

            apx_eql(mean.unwrap(), 50.005, 0.0001);
            apx_eql(sum.unwrap(), 500050.0, 0.0001);

            for i in 0..=100 {
                let value = i as f64;
                let quantile = value / 100.0;

                let (est_val, est_quant) = client
                    .select(&format!("SELECT tdigest_quantile(t_digest, {}), tdigest_quantile_at_value(t_digest, {}) FROM digest", quantile, value), None, None)
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
}
