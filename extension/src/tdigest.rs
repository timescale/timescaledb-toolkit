use std::{convert::TryInto, ops::Deref};

use pgrx::*;

use crate::{
    accessors::{
        AccessorApproxPercentile, AccessorApproxPercentileRank, AccessorMaxVal, AccessorMean,
        AccessorMinVal, AccessorNumVals,
    },
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
};

use tdigest::{Centroid, TDigest as InternalTDigest};

// PG function for adding values to a digest.
// Null values are ignored.
#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_trans(
    state: Internal,
    size: i32,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    tdigest_trans_inner(unsafe { state.to_inner() }, size, value, fcinfo).internal()
}
pub fn tdigest_trans_inner(
    state: Option<Inner<tdigest::Builder>>,
    size: i32,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<tdigest::Builder>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                // NaNs are nonsensical in the context of a percentile, so exclude them
                Some(value) => {
                    if value.is_nan() {
                        return state;
                    } else {
                        value
                    }
                }
            };
            let mut state = match state {
                None => tdigest::Builder::with_size(size.try_into().unwrap()).into(),
                Some(state) => state,
            };
            state.push(value);
            Some(state)
        })
    }
}

// PG function for merging digests.
#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { tdigest_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}

pub fn tdigest_combine_inner(
    state1: Option<Inner<tdigest::Builder>>,
    state2: Option<Inner<tdigest::Builder>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<tdigest::Builder>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(state2)) => Some(state2.clone().into()),
            (Some(state1), None) => Some(state1.clone().into()),
            (Some(state1), Some(state2)) => {
                let mut merged = state1.clone();
                merged.merge(state2.clone());
                Some(merged.into())
            }
        })
    }
}

use crate::raw::bytea;

#[pg_extern(immutable, parallel_safe, strict)]
pub fn tdigest_serialize(state: Internal) -> bytea {
    let state: &mut tdigest::Builder = unsafe { state.get_mut().unwrap() };
    // TODO this macro is really broken
    let hack = state.build();
    let hackref = &hack;
    crate::do_serialize!(hackref)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn tdigest_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    tdigest_deserialize_inner(bytes).internal()
}
pub fn tdigest_deserialize_inner(bytes: bytea) -> Inner<tdigest::Builder> {
    crate::do_deserialize!(bytes, tdigest::Builder)
}

// PG object for the digest.
pg_type! {
    #[derive(Debug)]
    struct TDigest<'input> {
        // We compute this.  It's a (harmless) bug that we serialize it.
        #[serde(skip_deserializing)]
        buckets: u32,
        max_buckets: u32,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        centroids: [Centroid; self.buckets],
    }
}

impl<'input> InOutFuncs for TDigest<'input> {
    fn output(&self, buffer: &mut StringInfo) {
        use crate::serialization::{str_to_db_encoding, EncodedStr::*};

        let stringified = ron::to_string(&**self).unwrap();
        match str_to_db_encoding(&stringified) {
            Utf8(s) => buffer.push_str(s),
            Other(s) => buffer.push_bytes(s.to_bytes()),
        }
    }

    fn input(input: &std::ffi::CStr) -> TDigest<'input>
    where
        Self: Sized,
    {
        use crate::serialization::str_from_db_encoding;

        let input = str_from_db_encoding(input);
        let mut val: TDigestData = ron::from_str(input).unwrap();
        val.buckets = val
            .centroids
            .len()
            .try_into()
            .expect("centroids len fits into u32");
        unsafe { Self(val, crate::type_builder::CachedDatum::None).flatten() }
    }
}

impl<'input> TDigest<'input> {
    fn to_internal_tdigest(&self) -> InternalTDigest {
        InternalTDigest::new(
            self.centroids.iter().collect(),
            self.sum,
            self.count,
            self.max,
            self.0.min,
            self.max_buckets as usize,
        )
    }

    fn from_internal_tdigest(digest: &InternalTDigest) -> TDigest<'static> {
        let max_buckets: u32 = digest.max_size().try_into().unwrap();

        let centroids = digest.raw_centroids();

        // we need to flatten the vector to a single buffer that contains
        // both the size, the data, and the varlen header
        unsafe {
            flatten!(TDigest {
                max_buckets,
                buckets: centroids.len() as u32,
                count: digest.count(),
                sum: digest.sum(),
                min: digest.min(),
                max: digest.max(),
                centroids: centroids.into(),
            })
        }
    }
}

// PG function to generate a user-facing TDigest object from an internal tdigest::Builder.
#[pg_extern(immutable, parallel_safe)]
fn tdigest_final(state: Internal, fcinfo: pg_sys::FunctionCallInfo) -> Option<TDigest<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state: &mut tdigest::Builder = match state.get_mut() {
                None => return None,
                Some(state) => state,
            };
            TDigest::from_internal_tdigest(&state.build()).into()
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE tdigest(size integer, value DOUBLE PRECISION)\n\
    (\n\
        sfunc = tdigest_trans,\n\
        stype = internal,\n\
        finalfunc = tdigest_final,\n\
        combinefunc = tdigest_combine,\n\
        serialfunc = tdigest_serialize,\n\
        deserialfunc = tdigest_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "tdigest_agg",
    requires = [
        tdigest_trans,
        tdigest_final,
        tdigest_combine,
        tdigest_serialize,
        tdigest_deserialize
    ],
);

#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_compound_trans(
    state: Internal,
    value: Option<TDigest<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    tdigest_compound_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn tdigest_compound_trans_inner(
    state: Option<Inner<InternalTDigest>>,
    value: Option<TDigest<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<InternalTDigest>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (a, None) => a,
                (None, Some(a)) => Some(a.to_internal_tdigest().into()),
                (Some(a), Some(b)) => {
                    assert_eq!(a.max_size(), b.max_buckets as usize);
                    Some(
                        InternalTDigest::merge_digests(
                            vec![a.deref().clone(), b.to_internal_tdigest()], // TODO: TDigest merge with self
                        )
                        .into(),
                    )
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_compound_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe {
        tdigest_compound_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal()
    }
}
pub fn tdigest_compound_combine_inner(
    state1: Option<Inner<InternalTDigest>>,
    state2: Option<Inner<InternalTDigest>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<InternalTDigest>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => {
                    assert_eq!(state1.max_size(), state2.max_size());
                    Some(
                        InternalTDigest::merge_digests(
                            vec![state1.deref().clone(), state2.deref().clone()], // TODO: TDigest merge with self
                        )
                        .into(),
                    )
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn tdigest_compound_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TDigest<'static>> {
    let state: Option<&InternalTDigest> = unsafe { state.get() };
    state.map(TDigest::from_internal_tdigest)
}

#[pg_extern(immutable, parallel_safe)]
fn tdigest_compound_serialize(state: Internal, _fcinfo: pg_sys::FunctionCallInfo) -> bytea {
    let state: Inner<InternalTDigest> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_compound_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: InternalTDigest = crate::do_deserialize!(bytes, InternalTDigest);
    Inner::from(i).internal()
}

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        tdigest\n\
    ) (\n\
        sfunc = tdigest_compound_trans,\n\
        stype = internal,\n\
        finalfunc = tdigest_compound_final,\n\
        combinefunc = tdigest_compound_combine,\n\
        serialfunc = tdigest_compound_serialize,\n\
        deserialfunc = tdigest_compound_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "tdigest_rollup",
    requires = [
        tdigest_compound_trans,
        tdigest_compound_final,
        tdigest_compound_combine,
        tdigest_compound_serialize,
        tdigest_compound_deserialize
    ],
);

//---- Available PG operations on the digest

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_approx_percentile<'a>(
    sketch: TDigest<'a>,
    accessor: AccessorApproxPercentile<'a>,
) -> f64 {
    tdigest_quantile(accessor.percentile, sketch)
}

// Approximate the value at the given quantile (0.0-1.0)
#[pg_extern(immutable, parallel_safe, name = "approx_percentile")]
pub fn tdigest_quantile<'a>(quantile: f64, digest: TDigest<'a>) -> f64 {
    digest.to_internal_tdigest().estimate_quantile(quantile)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_approx_rank<'a>(
    sketch: TDigest<'a>,
    accessor: AccessorApproxPercentileRank<'a>,
) -> f64 {
    tdigest_quantile_at_value(accessor.value, sketch)
}

// Approximate the quantile at the given value
#[pg_extern(immutable, parallel_safe, name = "approx_percentile_rank")]
pub fn tdigest_quantile_at_value<'a>(value: f64, digest: TDigest<'a>) -> f64 {
    digest
        .to_internal_tdigest()
        .estimate_quantile_at_value(value)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_num_vals<'a>(sketch: TDigest<'a>, _accessor: AccessorNumVals<'a>) -> f64 {
    tdigest_count(sketch)
}

// Number of elements from which the digest was built.
#[pg_extern(immutable, parallel_safe, name = "num_vals")]
pub fn tdigest_count<'a>(digest: TDigest<'a>) -> f64 {
    digest.count as f64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_min<'a>(sketch: TDigest<'a>, _accessor: AccessorMinVal<'a>) -> f64 {
    tdigest_min(sketch)
}

// Minimum value entered in the digest.
#[pg_extern(immutable, parallel_safe, name = "min_val")]
pub fn tdigest_min<'a>(digest: TDigest<'a>) -> f64 {
    digest.min
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_max<'a>(sketch: TDigest<'a>, _accessor: AccessorMaxVal<'a>) -> f64 {
    tdigest_max(sketch)
}

// Maximum value entered in the digest.
#[pg_extern(immutable, parallel_safe, name = "max_val")]
pub fn tdigest_max<'a>(digest: TDigest<'a>) -> f64 {
    digest.max
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_mean<'a>(sketch: TDigest<'a>, _accessor: AccessorMean<'a>) -> f64 {
    tdigest_mean(sketch)
}

// Average of all the values entered in the digest.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern(immutable, parallel_safe, name = "mean")]
pub fn tdigest_mean<'a>(digest: TDigest<'a>) -> f64 {
    if digest.count > 0 {
        digest.sum / digest.count as f64
    } else {
        0.0
    }
}

/// Total sum of all the values entered in the digest.
#[pg_extern(immutable, parallel_safe, name = "total")]
pub fn tdigest_sum(digest: TDigest<'_>) -> f64 {
    digest.sum
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use pgrx_macros::pg_test;

    // Assert equality between two floats, within some fixed error range.
    fn apx_eql(value: f64, expected: f64, error: f64) {
        assert!(
            (value - expected).abs() < error,
            "Float value {} differs from expected {} by more than {}",
            value,
            expected,
            error
        );
    }

    // Assert equality between two floats, within an error expressed as a fraction of the expected value.
    fn pct_eql(value: f64, expected: f64, pct_error: f64) {
        apx_eql(value, expected, pct_error * expected);
    }

    #[pg_test]
    fn test_tdigest_aggregate() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test (data DOUBLE PRECISION)", None, None)
                .unwrap();
            client
                .update(
                    "INSERT INTO test SELECT generate_series(0.01, 100, 0.01)",
                    None,
                    None,
                )
                .unwrap();

            let sanity = client
                .update("SELECT COUNT(*) FROM test", None, None)
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap();
            assert_eq!(10000, sanity.unwrap());

            client
                .update(
                    "CREATE VIEW digest AS \
                SELECT tdigest(100, data) FROM test",
                    None,
                    None,
                )
                .unwrap();

            let (min, max, count) = client
                .update(
                    "SELECT \
                    min_val(tdigest), \
                    max_val(tdigest), \
                    num_vals(tdigest) \
                    FROM digest",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_three::<f64, f64, f64>()
                .unwrap();

            apx_eql(min.unwrap(), 0.01, 0.000001);
            apx_eql(max.unwrap(), 100.0, 0.000001);
            apx_eql(count.unwrap(), 10000.0, 0.000001);

            let (min2, max2, count2) = client
                .update(
                    "SELECT \
                    tdigest->min_val(), \
                    tdigest->max_val(), \
                    tdigest->num_vals() \
                    FROM digest",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_three::<f64, f64, f64>()
                .unwrap();

            assert_eq!(min2, min);
            assert_eq!(max2, max);
            assert_eq!(count2, count);

            let (mean, mean2) = client
                .update(
                    "SELECT \
                    mean(tdigest), \
                    tdigest -> mean()
                    FROM digest",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_two::<f64, f64>()
                .unwrap();

            apx_eql(mean.unwrap(), 50.005, 0.0001);
            assert_eq!(mean, mean2);

            for i in 0..=100 {
                let value = i as f64;
                let quantile = value / 100.0;

                let (est_val, est_quant) = client
                    .update(
                        &format!(
                            "SELECT
                            approx_percentile({}, tdigest), \
                            approx_percentile_rank({}, tdigest) \
                            FROM digest",
                            quantile, value
                        ),
                        None,
                        None,
                    )
                    .unwrap()
                    .first()
                    .get_two::<f64, f64>()
                    .unwrap();

                if i == 0 {
                    pct_eql(est_val.unwrap(), 0.01, 1.0);
                    apx_eql(est_quant.unwrap(), quantile, 0.0001);
                } else {
                    pct_eql(est_val.unwrap(), value, 1.0);
                    pct_eql(est_quant.unwrap(), quantile, 1.0);
                }

                let (est_val2, est_quant2) = client
                    .update(
                        &format!(
                            "SELECT
                            tdigest->approx_percentile({}), \
                            tdigest->approx_percentile_rank({}) \
                            FROM digest",
                            quantile, value
                        ),
                        None,
                        None,
                    )
                    .unwrap()
                    .first()
                    .get_two::<f64, f64>()
                    .unwrap();
                assert_eq!(est_val2, est_val);
                assert_eq!(est_quant2, est_quant);
            }
        });
    }

    #[pg_test]
    fn test_tdigest_small_count() {
        Spi::connect(|mut client| {
            let estimate = client
                .update(
                    "SELECT \
                    approx_percentile(\
                        0.99, \
                        tdigest(100, data)) \
                    FROM generate_series(1, 100) data;",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one()
                .unwrap();

            assert_eq!(estimate, Some(99.5));
        });
    }

    #[pg_test]
    fn serialization_matches() {
        let mut t = InternalTDigest::new_with_size(10);
        let vals = vec![1.0, 1.0, 1.0, 2.0, 1.0, 1.0];
        for v in vals {
            t = t.merge_unsorted(vec![v]);
        }
        let pgt = TDigest::from_internal_tdigest(&t);
        let mut si = StringInfo::new();
        pgt.output(&mut si);
        assert_eq!(t.format_for_postgres(), si.to_string());
    }

    #[pg_test]
    fn test_tdigest_io() {
        Spi::connect(|mut client| {
            let output = client
                .update(
                    "SELECT \
                tdigest(100, data)::text \
                FROM generate_series(1, 100) data;",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();

            let expected = "(version:1,buckets:88,max_buckets:100,count:100,sum:5050,min:1,max:100,centroids:[(mean:1,weight:1),(mean:2,weight:1),(mean:3,weight:1),(mean:4,weight:1),(mean:5,weight:1),(mean:6,weight:1),(mean:7,weight:1),(mean:8,weight:1),(mean:9,weight:1),(mean:10,weight:1),(mean:11,weight:1),(mean:12,weight:1),(mean:13,weight:1),(mean:14,weight:1),(mean:15,weight:1),(mean:16,weight:1),(mean:17,weight:1),(mean:18,weight:1),(mean:19,weight:1),(mean:20,weight:1),(mean:21,weight:1),(mean:22,weight:1),(mean:23,weight:1),(mean:24,weight:1),(mean:25,weight:1),(mean:26,weight:1),(mean:27,weight:1),(mean:28,weight:1),(mean:29,weight:1),(mean:30,weight:1),(mean:31,weight:1),(mean:32,weight:1),(mean:33,weight:1),(mean:34,weight:1),(mean:35,weight:1),(mean:36,weight:1),(mean:37,weight:1),(mean:38,weight:1),(mean:39,weight:1),(mean:40,weight:1),(mean:41,weight:1),(mean:42,weight:1),(mean:43,weight:1),(mean:44,weight:1),(mean:45,weight:1),(mean:46,weight:1),(mean:47,weight:1),(mean:48,weight:1),(mean:49,weight:1),(mean:50,weight:1),(mean:51,weight:1),(mean:52.5,weight:2),(mean:54.5,weight:2),(mean:56.5,weight:2),(mean:58.5,weight:2),(mean:60.5,weight:2),(mean:62.5,weight:2),(mean:64,weight:1),(mean:65.5,weight:2),(mean:67.5,weight:2),(mean:69,weight:1),(mean:70.5,weight:2),(mean:72,weight:1),(mean:73.5,weight:2),(mean:75,weight:1),(mean:76,weight:1),(mean:77.5,weight:2),(mean:79,weight:1),(mean:80,weight:1),(mean:81.5,weight:2),(mean:83,weight:1),(mean:84,weight:1),(mean:85,weight:1),(mean:86,weight:1),(mean:87,weight:1),(mean:88,weight:1),(mean:89,weight:1),(mean:90,weight:1),(mean:91,weight:1),(mean:92,weight:1),(mean:93,weight:1),(mean:94,weight:1),(mean:95,weight:1),(mean:96,weight:1),(mean:97,weight:1),(mean:98,weight:1),(mean:99,weight:1),(mean:100,weight:1)])";

            assert_eq!(output, Some(expected.into()));

            let estimate = client
                .update(
                    &format!("SELECT approx_percentile(0.90, '{}'::tdigest)", expected),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one()
                .unwrap();
            assert_eq!(estimate, Some(90.5));
        });
    }

    #[pg_test]
    fn test_tdigest_byte_io() {
        unsafe {
            use std::ptr;
            let state = tdigest_trans_inner(None, 100, Some(14.0), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(18.0), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(22.7), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(39.42), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(-43.0), ptr::null_mut());

            let mut control = state.unwrap();
            let buffer = tdigest_serialize(Inner::from(control.clone()).internal().unwrap());
            let buffer = pgrx::varlena::varlena_to_byte_slice(buffer.0.cast_mut_ptr());

            let expected = [
                1, 1, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 69, 192, 1, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 44, 64, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50, 64, 1, 0,
                0, 0, 0, 0, 0, 0, 51, 51, 51, 51, 51, 179, 54, 64, 1, 0, 0, 0, 0, 0, 0, 0, 246, 40,
                92, 143, 194, 181, 67, 64, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 144,
                194, 245, 40, 92, 143, 73, 64, 5, 0, 0, 0, 0, 0, 0, 0, 246, 40, 92, 143, 194, 181,
                67, 64, 0, 0, 0, 0, 0, 128, 69, 192,
            ];
            assert_eq!(buffer, expected);

            let expected = pgrx::varlena::rust_byte_slice_to_bytea(&expected);
            let mut new_state =
                tdigest_deserialize_inner(bytea(pg_sys::Datum::from(expected.as_ptr())));

            assert_eq!(new_state.build(), control.build());
        }
    }

    #[pg_test]
    fn test_tdigest_compound_agg() {
        Spi::connect(|mut client| {
            client
                .update(
                    "CREATE TABLE new_test (device INTEGER, value DOUBLE PRECISION)",
                    None,
                    None,
                )
                .unwrap();
            client.update("INSERT INTO new_test SELECT dev, dev - v FROM generate_series(1,10) dev, generate_series(0, 1.0, 0.01) v", None, None).unwrap();

            let sanity = client
                .update("SELECT COUNT(*) FROM new_test", None, None)
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap();
            assert_eq!(Some(1010), sanity);

            client
                .update(
                    "CREATE VIEW digests AS \
                SELECT device, tdigest(20, value) \
                FROM new_test \
                GROUP BY device",
                    None,
                    None,
                )
                .unwrap();

            client
                .update(
                    "CREATE VIEW composite AS \
                SELECT tdigest(tdigest) \
                FROM digests",
                    None,
                    None,
                )
                .unwrap();

            client
                .update(
                    "CREATE VIEW base AS \
                SELECT tdigest(20, value) \
                FROM new_test",
                    None,
                    None,
                )
                .unwrap();

            let value = client
                .update(
                    "SELECT \
                    approx_percentile(0.9, tdigest) \
                    FROM base",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap();

            let test_value = client
                .update(
                    "SELECT \
                approx_percentile(0.9, tdigest) \
                    FROM composite",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap();

            apx_eql(test_value.unwrap(), value.unwrap(), 0.1);
            apx_eql(test_value.unwrap(), 9.0, 0.1);
        });
    }
}
