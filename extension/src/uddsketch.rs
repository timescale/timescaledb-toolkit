
use std::slice;

use pgx::*;

use flat_serialize::*;

use encodings::{delta, prefix_varint};

use uddsketch::{SketchHashKey, UDDSketch as UddSketchInternal};


use crate::{
    aggregate_utils::in_aggregate_context,
    json_inout_funcs,
    flatten,
    palloc::Internal, pg_type
};


#[allow(non_camel_case_types)]
type int = u32;

// PG function for adding values to a sketch.
// Null values are ignored.
#[pg_extern()]
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

// transition function for the simpler percentile_agg aggregate, which doesn't
// take parameters for the size and error, but uses a default
#[pg_extern()]
pub fn percentile_agg_trans(
    state: Option<Internal<UddSketchInternal>>,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<UddSketchInternal>> {
    let default_size = 200;
    let default_max_error = 0.001;
    uddsketch_trans(state, default_size, default_max_error, value, fcinfo)
}

// PG function for merging sketches.
#[pg_extern()]
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

#[pg_extern()]
pub fn uddsketch_serialize(
    state: Internal<UddSketchInternal>,
) -> bytea {
    let serializable = &SerializedUddSketch::from(&*state);
    crate::do_serialize!(serializable)
}

#[pg_extern(strict)]
pub fn uddsketch_deserialize(
    bytes: bytea,
    _internal: Option<Internal<()>>,
) -> Internal<UddSketchInternal> {
    let sketch: UddSketchInternal = crate::do_deserialize!(bytes, SerializedUddSketch);
    sketch.into()
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedUddSketch {
    alpha: f64,
    max_buckets: u32,
    num_buckets: u32,
    compactions: u32,
    count: u64,
    sum: f64,
    buckets: CompressedBuckets,
}

impl From<&UddSketchInternal> for SerializedUddSketch {
    fn from(sketch: &UddSketchInternal) -> Self {
        let buckets = compress_buckets(&*sketch);
        SerializedUddSketch {
            alpha: sketch.max_error(),
            max_buckets: sketch.max_allowed_buckets() as u32,
            num_buckets: sketch.current_buckets_count() as u32,
            compactions: sketch.times_compacted(),
            count: sketch.count(),
            sum: sketch.sum(),
            buckets,
        }
    }
}

impl Into<UddSketchInternal> for SerializedUddSketch {
    fn into(self) -> UddSketchInternal {
        UddSketchInternal::new_from_data(self.max_buckets as u64, self.alpha, self.compactions as u64, self.count, self.sum, self.keys(), self.counts())
    }
}

impl SerializedUddSketch {
    fn keys(&self) -> impl Iterator<Item=SketchHashKey> + '_ {
        decompress_keys(&*self.buckets.negative_indexes, self.buckets.zero_bucket_count != 0, &*self.buckets.positive_indexes)
    }

    fn counts(&self) -> impl Iterator<Item=u64> + '_ {
        decompress_counts(&*self.buckets.negative_counts, self.buckets.zero_bucket_count, &*self.buckets.positive_counts)
    }
}

// PG object for the sketch.
pg_type! {
    #[derive(Debug)]
    struct UddSketch {
        alpha: f64,
        max_buckets: u32,
        num_buckets: u32,
        compactions: u64,
        count: u64,
        sum: f64,
        zero_bucket_count: u64,
        neg_indexes_bytes: u32,
        neg_buckets_bytes: u32,
        pos_indexes_bytes: u32,
        pos_buckets_bytes: u32,
        negative_indexes: [u8; self.neg_indexes_bytes],
        negative_counts: [u8; self.neg_buckets_bytes],
        positive_indexes: [u8; self.pos_indexes_bytes],
        positive_counts: [u8; self.pos_buckets_bytes],
    }
}

varlena_type!(UddSketch);
json_inout_funcs!(UddSketch);

impl<'input> UddSketch<'input> {
    fn keys(&self) -> impl Iterator<Item=SketchHashKey> + '_ {
        decompress_keys(self.negative_indexes, *self.zero_bucket_count != 0, self.positive_indexes)
    }

    fn counts(&self) -> impl Iterator<Item=u64> + '_ {
        decompress_counts(self.negative_counts, *self.zero_bucket_count, self.positive_counts)
    }

    fn to_uddsketch(&self) -> UddSketchInternal {
        UddSketchInternal::new_from_data(*self.max_buckets as u64, *self.alpha, *self.compactions, *self.count, *self.sum, self.keys(), self.counts())
    }
}

// PG function to generate a user-facing UddSketch object from a UddSketchInternal.
#[pg_extern()]
fn uddsketch_final(
    state: Option<Internal<UddSketchInternal>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<UddSketch<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };

            let CompressedBuckets {
                negative_indexes,
                negative_counts,
                zero_bucket_count,
                positive_indexes,
                positive_counts,
            } = compress_buckets(&*state);


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
                    zero_bucket_count: &zero_bucket_count,
                    neg_indexes_bytes: &(negative_indexes.len() as u32),
                    neg_buckets_bytes: &(negative_counts.len() as u32),
                    pos_indexes_bytes: &(positive_indexes.len() as u32),
                    pos_buckets_bytes: &(positive_counts.len() as u32),
                    negative_indexes: &negative_indexes,
                    negative_counts: &negative_counts,
                    positive_indexes: &positive_indexes,
                    positive_counts: &positive_counts,
                }
            ).into()
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CompressedBuckets {
    negative_indexes: Vec<u8>,
    negative_counts: Vec<u8>,
    zero_bucket_count: u64,
    positive_indexes: Vec<u8>,
    positive_counts: Vec<u8>,
}

fn compress_buckets(sketch: &UddSketchInternal) -> CompressedBuckets {
    let mut negative_indexes = prefix_varint::I64Compressor::with(delta::i64_encoder());
    let mut negative_counts = prefix_varint::U64Compressor::with(delta::u64_encoder());
    let mut zero_bucket_count = 0;
    let mut positive_indexes = prefix_varint::I64Compressor::with(delta::i64_encoder());
    let mut positive_counts = prefix_varint::U64Compressor::with(delta::u64_encoder());
    for (k, b) in sketch.bucket_iter() {
        match k {
            SketchHashKey::Negative(i) => {
                negative_indexes.push(i);
                negative_counts.push(b);
            },
            SketchHashKey::Zero => {
                zero_bucket_count = b
            },
            SketchHashKey::Positive(i) => {
                positive_indexes.push(i);
                positive_counts.push(b);
            },
            SketchHashKey::Invalid => unreachable!(),
        }

    }
    let negative_indexes = negative_indexes.finish();
    let negative_counts = negative_counts.finish();
    let positive_indexes = positive_indexes.finish();
    let positive_counts = positive_counts.finish();
    CompressedBuckets {
        negative_indexes,
        negative_counts,
        zero_bucket_count,
        positive_indexes,
        positive_counts,
    }
}


fn decompress_keys<'i>(
    negative_indexes: &'i [u8],
    zero_bucket: bool,
    positive_indexes: &'i [u8]
) -> impl Iterator<Item=SketchHashKey> + 'i {
    let negatives = prefix_varint::i64_decompressor(negative_indexes)
        .map(delta::i64_decoder())
        .map(SketchHashKey::Negative);

    let zero = zero_bucket.then(|| uddsketch::SketchHashKey::Zero);

    let positives = prefix_varint::i64_decompressor(positive_indexes)
        .map(delta::i64_decoder())
        .map(SketchHashKey::Positive);

    negatives.chain(zero).chain(positives)
}

fn decompress_counts<'b>(
    negative_buckets: &'b [u8],
    zero_bucket: u64,
    positive_buckets: &'b [u8],
) -> impl Iterator<Item=u64> + 'b {
    let negatives = prefix_varint::u64_decompressor(negative_buckets).map(delta::u64_decoder());
    let zero = (zero_bucket != 0).then(|| zero_bucket);
    let positives = prefix_varint::u64_decompressor(positive_buckets).map(delta::u64_decoder());

    negatives.chain(zero).chain(positives)
}

extension_sql!(r#"
CREATE AGGREGATE uddsketch(
    size int, max_error DOUBLE PRECISION, value DOUBLE PRECISION
) (
    sfunc = uddsketch_trans,
    stype = internal,
    finalfunc = uddsketch_final,
    combinefunc = uddsketch_combine,
    serialfunc = uddsketch_serialize,
    deserialfunc = uddsketch_deserialize,
    parallel = safe
);
"#);

extension_sql!(r#"
CREATE AGGREGATE percentile_agg(value DOUBLE PRECISION)
(
    sfunc = percentile_agg_trans,
    stype = internal,
    finalfunc = uddsketch_final,
    combinefunc = uddsketch_combine,
    serialfunc = uddsketch_serialize,
    deserialfunc = uddsketch_deserialize,
    parallel = safe
);
"#);

#[pg_extern()]
pub fn uddsketch_compound_trans(
    state: Option<Internal<UddSketchInternal>>,
    value: Option<UddSketch>,
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
CREATE AGGREGATE uddsketch(
    sketch uddsketch
) (
    sfunc = uddsketch_compound_trans,
    stype = internal,
    finalfunc = uddsketch_final,
    combinefunc = uddsketch_combine,
    serialfunc = uddsketch_serialize,
    deserialfunc = uddsketch_deserialize,
    parallel = safe
);
"#);
extension_sql!(r#"
CREATE AGGREGATE percentile_agg(
    sketch uddsketch
) (
    sfunc = uddsketch_compound_trans,
    stype = internal,
    finalfunc = uddsketch_final,
    combinefunc = uddsketch_combine,
    serialfunc = uddsketch_serialize,
    deserialfunc = uddsketch_deserialize,
    parallel = safe
);
"#);

//---- Available PG operations on the sketch

// Approximate the value at the given approx_percentile (0.0-1.0)
#[pg_extern(name="approx_percentile")]
pub fn uddsketch_approx_percentile(
    percentile: f64,
    sketch: UddSketch,
) -> f64 {
    uddsketch::estimate_quantile(
        percentile,
        *sketch.alpha,
        uddsketch::gamma(*sketch.alpha),
        *sketch.count,
        sketch.keys().zip(sketch.counts()),
    )
}

// Approximate the approx_percentile at the given value
#[pg_extern(name="approx_percentile_rank")]
pub fn uddsketch_approx_percentile_rank(
    value: f64,
    sketch: UddSketch,
) -> f64 {
    uddsketch::estimate_quantile_at_value(
        value,
        uddsketch::gamma(*sketch.alpha),
        *sketch.count,
        sketch.keys().zip(sketch.counts()),
    )
}

// Number of elements from which the sketch was built.
#[pg_extern(name="num_vals")]
pub fn uddsketch_num_vals(
    sketch: UddSketch,
) -> f64 {
    *sketch.count as f64
}

// Average of all the values entered in the sketch.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern(name="mean")]
pub fn uddsketch_mean(
    sketch: UddSketch,
) -> f64 {
    if *sketch.count > 0 {
        *sketch.sum / *sketch.count as f64
    } else {
        0.0
    }
}

// The maximum error (relative to the true value) for any approx_percentile estimate.
#[pg_extern(name="error")]
pub fn uddsketch_error(
    sketch: UddSketch
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

            client.select("CREATE VIEW sketch AS \
                SELECT uddsketch(100, 0.05, data) \
                FROM test", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM sketch", None, None)
                .first()
                .get_one::<i32>();
            assert!(sanity.unwrap_or(0) > 0);

            let (mean, count) = client
                .select("SELECT \
                    mean(uddsketch), \
                    num_vals(uddsketch) \
                    FROM sketch", None, None)
                .first()
                .get_two::<f64, f64>();

            apx_eql(mean.unwrap(), 50.005, 0.0001);
            apx_eql(count.unwrap(), 10000.0, 0.000001);

            let error = client
                .select("SELECT \
                    error(uddsketch) \
                    FROM sketch", None, None)
                .first()
                .get_one::<f64>();

            apx_eql(error.unwrap(), 0.05, 0.0001);

            for i in 0..=100 {
                let value = i as f64;
                let approx_percentile = value / 100.0;

                let (est_val, est_quant) = client
                    .select(
                        &format!("SELECT \
                                approx_percentile({}, uddsketch), \
                                approx_percentile_rank( {}, uddsketch) \
                            FROM sketch", approx_percentile, value), None, None)
                    .first()
                    .get_two::<f64, f64>();

                if i == 0 {
                    pct_eql(est_val.unwrap(), 0.01, 1.0);
                    apx_eql(est_quant.unwrap(), approx_percentile, 0.0001);
                } else {
                    pct_eql(est_val.unwrap(), value, 1.0);
                    pct_eql(est_quant.unwrap(), approx_percentile, 1.0);
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


            client.select("CREATE VIEW sketches AS \
                SELECT device, uddsketch(20, 0.01, value) \
                FROM new_test \
                GROUP BY device", None, None);

            client.select("CREATE VIEW composite AS \
                SELECT uddsketch(uddsketch) \
                FROM sketches", None, None);

            client.select("CREATE VIEW base AS \
                SELECT uddsketch(20, 0.01, value) \
                FROM new_test", None, None);

            let (value, error) = client
                .select("SELECT \
                    approx_percentile(0.9, uddsketch), \
                    error(uddsketch) \
                    FROM base", None, None)
                .first()
                .get_two::<f64, f64>();

            let (test_value, test_error) = client
                .select("SELECT \
                    approx_percentile(0.9, uddsketch), \
                    error(uddsketch) \
                    FROM composite", None, None)
                .first()
                .get_two::<f64, f64>();

            apx_eql(test_value.unwrap(), value.unwrap(), 0.0001);
            apx_eql(test_error.unwrap(), error.unwrap(), 0.000001);
            pct_eql(test_value.unwrap(), 9.0, test_error.unwrap());
        });
    }

    #[pg_test]
    fn test_percentile_agg() {
        Spi::execute(|client| {
            client.select("CREATE TABLE pa_test (device INTEGER, value DOUBLE PRECISION)", None, None);
            client.select("INSERT INTO pa_test SELECT dev, dev - v FROM generate_series(1,10) dev, generate_series(0, 1.0, 0.01) v", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM pa_test", None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(Some(1010), sanity);

            // use the default values for percentile_agg
            client.select("CREATE VIEW uddsketch_test AS \
                SELECT uddsketch(200, 0.001, value) as approx \
                FROM pa_test ", None, None);

            client.select("CREATE VIEW percentile_agg AS \
                SELECT percentile_agg(value) as approx \
                FROM pa_test", None, None);


            let (value, error) = client
                .select("SELECT \
                    approx_percentile(0.9, approx), \
                    error(approx) \
                    FROM uddsketch_test", None, None)
                .first()
                .get_two::<f64, f64>();

            let (test_value, test_error) = client
                .select("SELECT \
                    approx_percentile(0.9, approx), \
                    error(approx) \
                    FROM percentile_agg", None, None)
                .first()
                .get_two::<f64, f64>();

            apx_eql(test_value.unwrap(), value.unwrap(), 0.0001);
            apx_eql(test_error.unwrap(), error.unwrap(), 0.000001);
            pct_eql(test_value.unwrap(), 9.0, test_error.unwrap());
        });
    }
}
