
use pgx::*;

use flat_serialize::*;

use encodings::{delta, prefix_varint};

use uddsketch::{SketchHashKey, UDDSketch as UddSketchInternal};

use crate::{
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Internal, InternalAsValue, Inner, ToInternal}, pg_type,
    accessors::toolkit_experimental,
};

// PG function for adding values to a sketch.
// Null values are ignored.
#[pg_extern(immutable, parallel_safe)]
pub fn uddsketch_trans(
    state: Internal,
    size: i32,
    max_error: f64,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    uddsketch_trans_inner(unsafe{ state.to_inner() }, size, max_error, value, fcinfo).internal()
}

pub fn uddsketch_trans_inner(
    state: Option<Inner<UddSketchInternal>>,
    size: i32,
    max_error: f64,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<UddSketchInternal>> {
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

const PERCENTILE_AGG_DEFAULT_SIZE: u32 = 200;
const PERCENTILE_AGG_DEFAULT_ERROR: f64 = 0.001;

// transition function for the simpler percentile_agg aggregate, which doesn't
// take parameters for the size and error, but uses a default
#[pg_extern(immutable, parallel_safe)]
pub fn percentile_agg_trans(
    state: Internal,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    percentile_agg_trans_inner(unsafe{ state.to_inner() }, value, fcinfo).internal()
}

pub fn percentile_agg_trans_inner(
    state: Option<Inner<UddSketchInternal>>,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<UddSketchInternal>> {
    let default_size = PERCENTILE_AGG_DEFAULT_SIZE;
    let default_max_error = PERCENTILE_AGG_DEFAULT_ERROR;
    uddsketch_trans_inner(state, default_size as _, default_max_error, value, fcinfo)
}

// PG function for merging sketches.
#[pg_extern(immutable, parallel_safe)]
pub fn uddsketch_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    unsafe {
        uddsketch_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal()
    }
}
pub fn uddsketch_combine_inner(
    state1: Option<Inner<UddSketchInternal>>,
    state2: Option<Inner<UddSketchInternal>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<UddSketchInternal>> {
    todo!();
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

use crate::raw::bytea;

#[pg_extern(immutable, parallel_safe)]
pub fn uddsketch_serialize(
    state: Internal,
) -> bytea {
    let serializable = &SerializedUddSketch::from(unsafe { state.get().unwrap() });
    crate::do_serialize!(serializable)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn uddsketch_deserialize(
    bytes: bytea,
    _internal: Internal,
) -> Internal {
    uddsketch_deserialize_inner(bytes).internal()
}
pub fn uddsketch_deserialize_inner(
    bytes: bytea,
) -> Inner<UddSketchInternal> {
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
        let buckets = compress_buckets(sketch.bucket_iter());
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

impl From<SerializedUddSketch> for UddSketchInternal {
    fn from(sketch: SerializedUddSketch) -> Self {
        UddSketchInternal::new_from_data(sketch.max_buckets as u64, sketch.alpha, sketch.compactions as u64, sketch.count, sketch.sum, sketch.keys(), sketch.counts())
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
    struct UddSketch<'input> {
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

#[derive(serde::Serialize, serde::Deserialize)]
struct ReadableUddSketch {
    version: u8,
    alpha: f64,
    max_buckets: u32,
    num_buckets: u32,
    compactions: u64,
    count: u64,
    sum: f64,
    buckets: Vec<(SketchHashKey, u64)>
}

impl From<&UddSketch<'_>> for ReadableUddSketch {
    fn from(sketch: &UddSketch<'_>) -> Self {
        ReadableUddSketch {
            version: sketch.version,
            alpha: sketch.alpha,
            max_buckets: sketch.max_buckets,
            num_buckets: sketch.num_buckets,
            compactions: sketch.compactions,
            count: sketch.count,
            sum: sketch.sum,
            buckets: sketch.keys().zip(sketch.counts()).collect(),
        }
    }
}

impl<'a, 'b> From<&'a ReadableUddSketch> for UddSketch<'b> {
    fn from(sketch: &'a ReadableUddSketch) -> Self {
        assert_eq!(sketch.version, 1);

        let CompressedBuckets {
            negative_indexes,
            negative_counts,
            zero_bucket_count,
            positive_indexes,
            positive_counts,
        } = compress_buckets(sketch.buckets.iter().cloned());

        unsafe {
            flatten! {
                UddSketch {
                    alpha: sketch.alpha,
                    max_buckets: sketch.max_buckets,
                    num_buckets: sketch.num_buckets,
                    compactions: sketch.compactions,
                    count: sketch.count,
                    sum: sketch.sum,
                    zero_bucket_count,
                    neg_indexes_bytes: (negative_indexes.len() as u32),
                    neg_buckets_bytes: (negative_counts.len() as u32),
                    pos_indexes_bytes: (positive_indexes.len() as u32),
                    pos_buckets_bytes: (positive_counts.len() as u32),
                    negative_indexes: (&*negative_indexes).into(),
                    negative_counts: (&*negative_counts).into(),
                    positive_indexes: (&*positive_indexes).into(),
                    positive_counts: (&*positive_counts).into(),
                }
            }
        }
    }
}

impl<'input> InOutFuncs for UddSketch<'input> {
    fn output(&self, buffer: &mut StringInfo) {
        use crate::serialization::{EncodedStr::*, str_to_db_encoding};

        let stringified = ron::to_string(&ReadableUddSketch::from(self)).unwrap();
        match str_to_db_encoding(&stringified) {
            Utf8(s) => buffer.push_str(s),
            Other(s) => buffer.push_bytes(s.to_bytes()),
        }
    }

    fn input(input: &std::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        use crate::serialization::str_from_db_encoding;

        let utf8_str = str_from_db_encoding(input);
        let val: ReadableUddSketch = ron::from_str(utf8_str).unwrap();
        UddSketch::from(&val)
    }
}

impl<'input> UddSketch<'input> {
    fn keys(&self) -> impl Iterator<Item=SketchHashKey> + '_ {
        // FIXME does this really need a slice?
        decompress_keys(self.negative_indexes.as_slice(), self.zero_bucket_count != 0, self.positive_indexes.as_slice())
    }

    fn counts(&self) -> impl Iterator<Item=u64> + '_ {
        // FIXME does this really need a slice?
        decompress_counts(self.negative_counts.as_slice(), self.zero_bucket_count, self.positive_counts.as_slice())
    }

    fn to_uddsketch(&self) -> UddSketchInternal {
        UddSketchInternal::new_from_data(self.max_buckets as u64, self.alpha, self.compactions, self.count, self.sum, self.keys(), self.counts())
    }

    fn from_internal(state: &UddSketchInternal) -> Self {
        let CompressedBuckets {
            negative_indexes,
            negative_counts,
            zero_bucket_count,
            positive_indexes,
            positive_counts,
        } = compress_buckets(state.bucket_iter());


        // we need to flatten the vector to a single buffer that contains
        // both the size, the data, and the varlen header
        unsafe {
            flatten!(
                UddSketch {
                    alpha: state.max_error(),
                    max_buckets: state.max_allowed_buckets() as u32,
                    num_buckets: state.current_buckets_count() as u32,
                    compactions: state.times_compacted() as u64,
                    count: state.count(),
                    sum: state.sum(),
                    zero_bucket_count,
                    neg_indexes_bytes: negative_indexes.len() as u32,
                    neg_buckets_bytes: negative_counts.len() as u32,
                    pos_indexes_bytes: positive_indexes.len() as u32,
                    pos_buckets_bytes: positive_counts.len() as u32,
                    negative_indexes: negative_indexes.into(),
                    negative_counts: negative_counts.into(),
                    positive_indexes: positive_indexes.into(),
                    positive_counts: positive_counts.into(),
                }
            )
        }
    }
}

impl<'input> FromIterator<f64> for UddSketch<'input> {
    fn from_iter<T: IntoIterator<Item = f64>>(iter: T) -> Self {
        let mut sketch = UddSketchInternal::new(PERCENTILE_AGG_DEFAULT_SIZE.into(), PERCENTILE_AGG_DEFAULT_ERROR);
        for value in iter {
            sketch.add_value(value);
        }
        Self::from_internal(&sketch)
    }
}

// PG function to generate a user-facing UddSketch object from a UddSketchInternal.
#[pg_extern(immutable, parallel_safe)]
fn uddsketch_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<UddSketch<'static>> {
    unsafe {
        uddsketch_final_inner(state.to_inner(), fcinfo)
    }
}
fn uddsketch_final_inner(
    state: Option<Inner<UddSketchInternal>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<UddSketch<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };

            UddSketch::from_internal(&state).into()
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

fn compress_buckets(buckets: impl Iterator<Item=(SketchHashKey, u64)>) -> CompressedBuckets {
    let mut negative_indexes = prefix_varint::I64Compressor::with(delta::i64_encoder());
    let mut negative_counts = prefix_varint::U64Compressor::with(delta::u64_encoder());
    let mut zero_bucket_count = 0;
    let mut positive_indexes = prefix_varint::I64Compressor::with(delta::i64_encoder());
    let mut positive_counts = prefix_varint::U64Compressor::with(delta::u64_encoder());
    for (k, b) in buckets {
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

extension_sql!("\n\
    CREATE AGGREGATE uddsketch(\n\
        size int, max_error DOUBLE PRECISION, value DOUBLE PRECISION\n\
    ) (\n\
        sfunc = uddsketch_trans,\n\
        stype = internal,\n\
        finalfunc = uddsketch_final,\n\
        combinefunc = uddsketch_combine,\n\
        serialfunc = uddsketch_serialize,\n\
        deserialfunc = uddsketch_deserialize,\n\
        parallel = safe\n\
    );\n\
",
name = "udd_agg",
requires = [uddsketch_trans, uddsketch_final, uddsketch_combine, uddsketch_serialize, uddsketch_deserialize],
);

extension_sql!("\n\
    CREATE AGGREGATE percentile_agg(value DOUBLE PRECISION)\n\
    (\n\
        sfunc = percentile_agg_trans,\n\
        stype = internal,\n\
        finalfunc = uddsketch_final,\n\
        combinefunc = uddsketch_combine,\n\
        serialfunc = uddsketch_serialize,\n\
        deserialfunc = uddsketch_deserialize,\n\
        parallel = safe\n\
    );\n\
",
name = "percentile_agg",
requires = [percentile_agg_trans, uddsketch_final, uddsketch_combine, uddsketch_serialize, uddsketch_deserialize],
);

#[pg_extern(immutable, parallel_safe)]
pub fn uddsketch_compound_trans(
    state: Internal,
    value: Option<UddSketch>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Internal {
    unsafe {
        uddsketch_compound_trans_inner(state.to_inner(), value, fcinfo).internal()
    }
}
pub fn uddsketch_compound_trans_inner(
    state: Option<Inner<UddSketchInternal>>,
    value: Option<UddSketch>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<UddSketchInternal>> {
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

extension_sql!("\n\
    CREATE AGGREGATE rollup(\n\
        sketch uddsketch\n\
    ) (\n\
        sfunc = uddsketch_compound_trans,\n\
        stype = internal,\n\
        finalfunc = uddsketch_final,\n\
        combinefunc = uddsketch_combine,\n\
        serialfunc = uddsketch_serialize,\n\
        deserialfunc = uddsketch_deserialize,\n\
        parallel = safe\n\
    );\n\
",
name = "udd_rollup",
requires = [uddsketch_compound_trans, uddsketch_final, uddsketch_combine, uddsketch_serialize, uddsketch_deserialize],
);

//---- Available PG operations on the sketch

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_uddsketch_approx_percentile(
    sketch: UddSketch,
    accessor: toolkit_experimental::AccessorApproxPercentile,
) -> f64 {
    uddsketch_approx_percentile(accessor.percentile, sketch)
}

// Approximate the value at the given approx_percentile (0.0-1.0)
#[pg_extern(immutable, parallel_safe, name="approx_percentile")]
pub fn uddsketch_approx_percentile(
    percentile: f64,
    sketch: UddSketch,
) -> f64 {
    uddsketch::estimate_quantile(
        percentile,
        sketch.alpha,
        uddsketch::gamma(sketch.alpha),
        sketch.count,
        sketch.keys().zip(sketch.counts()),
    )
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_uddsketch_approx_rank(
    sketch: UddSketch,
    accessor: toolkit_experimental::AccessorApproxRank,
) -> f64 {
    uddsketch_approx_percentile_rank(accessor.value, sketch)
}

// Approximate the approx_percentile at the given value
#[pg_extern(immutable, parallel_safe, name="approx_percentile_rank")]
pub fn uddsketch_approx_percentile_rank(
    value: f64,
    sketch: UddSketch,
) -> f64 {
    uddsketch::estimate_quantile_at_value(
        value,
        uddsketch::gamma(sketch.alpha),
        sketch.count,
        sketch.keys().zip(sketch.counts()),
    )
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_uddsketch_num_vals(
    sketch: UddSketch,
    accessor: toolkit_experimental::AccessorNumVals,
) -> f64 {
    let _ = accessor;
    uddsketch_num_vals(sketch)
}

// Number of elements from which the sketch was built.
#[pg_extern(immutable, parallel_safe, name="num_vals")]
pub fn uddsketch_num_vals(
    sketch: UddSketch,
) -> f64 {
    sketch.count as f64
}


#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_uddsketch_mean(
    sketch: UddSketch,
    accessor: toolkit_experimental::AccessorMean,
) -> f64 {
    let _ = accessor;
    uddsketch_mean(sketch)
}

// Average of all the values entered in the sketch.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern(immutable, parallel_safe, name="mean")]
pub fn uddsketch_mean(
    sketch: UddSketch,
) -> f64 {
    if sketch.count > 0 {
        sketch.sum / sketch.count as f64
    } else {
        0.0
    }
}


#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_uddsketch_error(
    sketch: UddSketch,
    accessor: toolkit_experimental::AccessorError,
) -> f64 {
    let _ = accessor;
    uddsketch_error(sketch)
}

// The maximum error (relative to the true value) for any approx_percentile estimate.
#[pg_extern(immutable, parallel_safe, name="error")]
pub fn uddsketch_error(
    sketch: UddSketch
) -> f64 {
    sketch.alpha
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use super::*;
    use pgx_macros::pg_test;

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

            let (mean2, count2) = client
                .select("SELECT \
                    uddsketch -> toolkit_experimental.mean(), \
                    uddsketch -> toolkit_experimental.num_vals() \
                    FROM sketch", None, None)
                .first()
                .get_two::<f64, f64>();
            assert_eq!(mean, mean2);
            assert_eq!(count, count2);

            let (error, error2) = client
                .select("SELECT \
                    error(uddsketch), \
                    uddsketch -> toolkit_experimental.error() \
                    FROM sketch", None, None)
                .first()
                .get_two::<f64, f64>();

            apx_eql(error.unwrap(), 0.05, 0.0001);
            assert_eq!(error, error2);

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

                let (est_val2, est_quant2) = client
                    .select(
                        &format!("SELECT \
                                uddsketch->toolkit_experimental.approx_percentile({}), \
                                uddsketch->toolkit_experimental.approx_percentile_rank({}) \
                            FROM sketch", approx_percentile, value), None, None)
                    .first()
                    .get_two::<f64, f64>();
                assert_eq!(est_val, est_val2);
                assert_eq!(est_quant, est_quant2);
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
                SELECT rollup(uddsketch) as uddsketch \
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

    #[pg_test]
    fn uddsketch_io_test() {
        Spi::execute(|client| {
            client.select("CREATE TABLE io_test (value DOUBLE PRECISION)", None, None);
            client.select("INSERT INTO io_test VALUES (-1000), (-100), (-10), (-1), (-0.1), (-0.01), (-0.001), (0), (0.001), (0.01), (0.1), (1), (10), (100), (1000)", None, None);

            let sketch = client.select("SELECT uddsketch(10, 0.01, value)::text FROM io_test", None, None).first().get_one::<String>();

            let expected = "(\
                version:1,\
                alpha:0.9881209712069546,\
                max_buckets:10,\
                num_buckets:9,\
                compactions:8,\
                count:15,\
                sum:0,\
                buckets:[\
                    (Negative(2),1),\
                    (Negative(1),2),\
                    (Negative(0),3),\
                    (Negative(-1),1),\
                    (Zero,1),\
                    (Positive(-1),1),\
                    (Positive(0),3),\
                    (Positive(1),2),\
                    (Positive(2),1)\
                    ]\
                )";

            assert_eq!(sketch, Some(expected.into()));

            client.select("CREATE VIEW sketch AS SELECT uddsketch(10, 0.01, value) FROM io_test", None, None).first().get_one::<String>();

            for cmd in ["mean(" , "num_vals(", "error(", "approx_percentile(0.1,", "approx_percentile(0.25,", "approx_percentile(0.5,", "approx_percentile(0.6,", "approx_percentile(0.8,"] {
                let sql1 = format!("SELECT {}uddsketch) FROM sketch", cmd);
                let sql2 = format!("SELECT {}'{}'::uddsketch) FROM sketch", cmd, expected);

                let expected = client.select(&sql1, None, None).first().get_one::<f64>().unwrap();
                let test = client.select(&sql2, None, None).first().get_one::<f64>().unwrap();

                assert!((expected - test).abs() < f64::EPSILON);
            }
        });
    }

    #[pg_test]
    fn uddsketch_byte_io_test() {
        unsafe {
            use std::ptr;
            let state = uddsketch_trans_inner(None, 100, 0.005, Some(14.0), ptr::null_mut());
            let state = uddsketch_trans_inner(state, 100, 0.005, Some(18.0), ptr::null_mut());
            let state = uddsketch_trans_inner(state, 100, 0.005, Some(22.7), ptr::null_mut());
            let state = uddsketch_trans_inner(state, 100, 0.005, Some(39.42), ptr::null_mut());
            let state = uddsketch_trans_inner(state, 100, 0.005, Some(-43.0), ptr::null_mut());

            let control = state.unwrap();
            let buffer = uddsketch_serialize(Inner::from(control.clone()).internal());
            let buffer = pgx::varlena::varlena_to_byte_slice(buffer.0 as *mut pg_sys::varlena);

            let expected = [1, 1, 123, 20, 174, 71, 225, 122, 116, 63, 100, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 144, 194, 245, 40, 92, 143, 73, 64, 2, 0, 0, 0, 0, 0, 0, 0, 202, 11, 1, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 66, 8, 105, 93, 221, 4, 0, 0, 0, 0, 0, 0, 0, 5, 1, 1, 1];
            assert_eq!(buffer, expected);

            let expected = pgx::varlena::rust_byte_slice_to_bytea(&expected);
            let new_state = uddsketch_deserialize_inner(bytea(&*expected as *const pg_sys::varlena as _));
            assert_eq!(&*new_state, &*control);
        }
    }
}
