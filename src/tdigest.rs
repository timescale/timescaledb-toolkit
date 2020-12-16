
use std::{
    cmp::min,
    mem::replace,
    slice,
};

use serde::{Serialize, Deserialize};

use pgx::*;

use crate::palloc::{Internal, in_memory_context};
use crate::aggregate_utils::aggregate_mctx;

use tdigest::{
    TDigest,
    Centroid,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct TDigestTransState {
    #[serde(skip_serializing)]
    buffer: Vec<f64>,
    digested: TDigest,
}

impl TDigestTransState {
    fn push(&mut self, value: f64) {
        self.buffer.push(value);
        if self.buffer.len() >= self.digested.max_size() {
            self.digest()
        }
    }

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

#[pg_extern]
pub fn tdigest_final(
    state: Option<Internal<TDigestTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<tsTDigest> {
    let mctx = aggregate_mctx(fcinfo);
    let mctx = match mctx {
        None => pgx::error!("cannot call as non-aggregate"),
        Some(mctx) => mctx,
    };
    unsafe {
        in_memory_context(mctx, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.digest();

            Some(write_pg_digest(&state.digested))
        })
    }
}

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
    let size = bincode::serialized_size(&*state)
        .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
    let mut bytes = Vec::with_capacity(size as usize + 4);
    let mut varsize = [0; 4];
    unsafe {
        pgx::set_varsize(&mut varsize as *mut _ as *mut _, size as _);
    }
    bytes.extend_from_slice(&varsize);
    bincode::serialize_into(&mut bytes, &*state)
        .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
    bytes.as_mut_ptr() as pg_sys::Datum
}

#[pg_extern]
pub fn tdigest_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<TDigestTransState> {
    let tdigest: TDigestTransState = unsafe {
        let detoasted = pg_sys::pg_detoast_datum(bytes as *mut _);
        let len = pgx::varsize_any_exhdr(detoasted);
        let data = pgx::vardata_any(detoasted);
        let bytes = slice::from_raw_parts(data as *mut u8, len);
        bincode::deserialize(bytes).unwrap_or_else(|e|
            pgx::error!("deserialization error {}", e))
    };
    tdigest.into()
}

// TODO: implement a more efficient representation
#[derive(PostgresType, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub struct tsTDigest {
    buckets: usize,
    count: f64,
    sum: f64,
    min: f64,
    max: f64,
    means: Vec<f64>,
    weights: Vec<f64>,
}

fn write_pg_digest(dig: &TDigest) -> tsTDigest {
    let mut result = tsTDigest {
        buckets: dig.max_size(),
        count: dig.count(),
        sum: dig.sum(),
        min: dig.min(),
        max: dig.max(),
        means: vec!(0.0; dig.max_size()),
        weights: vec!(0.0; dig.max_size()),
    };

    for (i, cent) in dig.raw_centroids().iter().enumerate() {
        result.means[i] = cent.mean();
        result.weights[i] = cent.weight();
    }

    result
}

fn read_pg_digest(state: &tsTDigest) -> TDigest {
    let size = min(state.buckets, state.count as usize);
    let mut cents: Vec<Centroid> = Vec::new();

    for i in 0..size {
        cents.push(Centroid::new(state.means[i], state.weights[i]));
    }

    TDigest::new(cents, state.sum, state.count, state.max, state.min, state.buckets)
}

#[pg_extern]
pub fn tdigest_quantile(
    digest: tsTDigest,
    quantile: f64,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    let dig = read_pg_digest(&digest);
    dig.estimate_quantile(quantile)
}

#[pg_extern]
pub fn tdigest_quantile_at_value(
    digest: tsTDigest,
    value: f64,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    let dig = read_pg_digest(&digest);
    dig.estimate_quantile_at_value(value)
}

#[pg_extern]
pub fn tdigest_count(
    digest: tsTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.count
}

#[pg_extern]
pub fn tdigest_min(
    digest: tsTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.min
}

#[pg_extern]
pub fn tdigest_max(
    digest: tsTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.max
}

#[pg_extern]
pub fn tdigest_mean(
    digest: tsTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    if digest.count > 0.0 {
        digest.sum / digest.count
    } else {
        0.0
    }
}

#[pg_extern]
pub fn tdigest_sum(
    digest: tsTDigest,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    digest.sum
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    fn apx_eql(value: f64, expected: f64, error: f64) {
        assert!((value - expected).abs() < error, "Float value {} differs from expected {} by more than {}", value, expected, error);
    }
    
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
