
use pgx::*;

use flat_serialize::*;

use std::convert::TryInto;

use crate::{
    build, flatten,
    pg_type,
    ron_inout_funcs,
};

pub mod toolkit_experimental {
    pub use super::*;
    varlena_type!(AccessorApproxRank);
    varlena_type!(AccessorApproxPercentile);
    varlena_type!(AccessorNumVals);
    varlena_type!(AccessorMean);
    varlena_type!(AccessorError);
    varlena_type!(AccessorMin);
    varlena_type!(AccessorMax);
    varlena_type!(AccessorAverage);

    varlena_type!(AccessorAverageX);
    varlena_type!(AccessorAverageY);
    varlena_type!(AccessorSum);
    varlena_type!(AccessorSumX);
    varlena_type!(AccessorSumY);
    varlena_type!(AccessorSlope);
    varlena_type!(AccessorStdDev);
    varlena_type!(AccessorStdDevX);
    varlena_type!(AccessorStdDevY);
    varlena_type!(AccessorVariance);
    varlena_type!(AccessorVarianceX);
    varlena_type!(AccessorVarianceY);
    varlena_type!(AccessorSkewness);
    varlena_type!(AccessorSkewnessX);
    varlena_type!(AccessorSkewnessY);
    varlena_type!(AccessorKurtosis);
    varlena_type!(AccessorKurtosisX);
    varlena_type!(AccessorKurtosisY);
    varlena_type!(AccessorCorr);
    varlena_type!(AccessorIntercept);
    varlena_type!(AccessorXIntercept);
    varlena_type!(AccessorDeterminationCoeff);
    varlena_type!(AccessorCovar);

    varlena_type!(AccessorDistinctCount);
    varlena_type!(AccessorStdError);
}

pg_type! {
    #[derive(Debug)]
    struct AccessorApproxPercentile {
        percentile: f64,
    }
}

ron_inout_funcs!(AccessorApproxPercentile);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="approx_percentile")]
pub fn accessor_approx_percentile(
    percentile: f64,
) -> toolkit_experimental::AccessorApproxPercentile<'static> {
    build!{
        AccessorApproxPercentile {
            percentile: percentile,
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorApproxRank {
        value: f64,
    }
}

ron_inout_funcs!(AccessorApproxRank);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="approx_percentile_rank")]
pub fn accessor_approx_rank(
    value: f64,
) -> toolkit_experimental::AccessorApproxRank<'static> {
    build!{
        AccessorApproxRank {
            value: value,
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorNumVals {
    }
}

ron_inout_funcs!(AccessorNumVals);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="num_vals")]
pub fn accessor_num_vals(
) -> toolkit_experimental::AccessorNumVals<'static> {
    build!{
        AccessorNumVals {
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorMean {
    }
}

ron_inout_funcs!(AccessorMean);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="mean")]
pub fn accessor_mean(
) -> toolkit_experimental::AccessorMean<'static> {
    build!{
        AccessorMean {
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorError {
    }
}

ron_inout_funcs!(AccessorError);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="error")]
pub fn accessor_error(
) -> toolkit_experimental::AccessorError<'static> {
    build!{
        AccessorError {
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorMin {
    }
}

ron_inout_funcs!(AccessorMin);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="min_val")]
pub fn accessor_min(
) -> toolkit_experimental::AccessorMin<'static> {
    build!{
        AccessorMin {
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorMax {
    }
}

ron_inout_funcs!(AccessorMax);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="max_val")]
pub fn accessor_max(
) -> toolkit_experimental::AccessorMax<'static> {
    build!{
        AccessorMax {
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorAverage {
    }
}

ron_inout_funcs!(AccessorAverage);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="average")]
pub fn accessor_average(
) -> toolkit_experimental::AccessorAverage<'static> {
    build!{
        AccessorAverage {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorAverageX {
    }
}

ron_inout_funcs!(AccessorAverageX);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="average_x")]
pub fn accessor_average_x(
) -> toolkit_experimental::AccessorAverageX<'static> {
    build!{
        AccessorAverageX {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorAverageY {
    }
}

ron_inout_funcs!(AccessorAverageY);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="average_y")]
pub fn accessor_average_y(
) -> toolkit_experimental::AccessorAverageY<'static> {
    build!{
        AccessorAverageY {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorSum {
    }
}

ron_inout_funcs!(AccessorSum);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="sum")]
pub fn accessor_sum(
) -> toolkit_experimental::AccessorSum<'static> {
    build!{
        AccessorSum {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorSumX {
    }
}

ron_inout_funcs!(AccessorSumX);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="sum_x")]
pub fn accessor_sum_x(
) -> toolkit_experimental::AccessorSumX<'static> {
    build!{
        AccessorSumX {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorSumY {
    }
}

ron_inout_funcs!(AccessorSumY);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="sum_y")]
pub fn accessor_sum_y(
) -> toolkit_experimental::AccessorSumY<'static> {
    build!{
        AccessorSumY {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorSlope {
    }
}

ron_inout_funcs!(AccessorSlope);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="slope")]
pub fn accessor_slope(
) -> toolkit_experimental::AccessorSlope<'static> {
    build!{
        AccessorSlope {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorStdDev<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorStdDev);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="stddev")]
pub fn accessor_stddev(
    method: default!(String, "sample"),
) -> toolkit_experimental::AccessorStdDev<'static> {
    //TODO validate `method`?
    unsafe {
        flatten!{
            AccessorStdDev {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorStdDevX<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorStdDevX);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="stddev_x")]
pub fn accessor_stddev_x(
    method: default!(String, "sample"),
) -> toolkit_experimental::AccessorStdDevX<'static> {
    //TODO validate `method`?
    unsafe {
        flatten!{
            AccessorStdDevX {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorStdDevY<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorStdDevY);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="stddev_y")]
pub fn accessor_stddev_y(
    method: default!(String, "sample"),
) -> toolkit_experimental::AccessorStdDevY<'static> {
    //TODO validate `method`?
    unsafe {
        flatten!{
            AccessorStdDevY {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorVariance<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorVariance);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="variance")]
pub fn accessor_variance(
    method: default!(String, "sample"),
) -> toolkit_experimental::AccessorVariance<'static> {
    //TODO validate `method`?
    unsafe {
        flatten!{
            AccessorVariance {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorVarianceX<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorVarianceX);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="variance_x")]
pub fn accessor_variance_x(
    method: default!(String, "sample"),
) -> toolkit_experimental::AccessorVarianceX<'static> {
    //TODO validate `method`?
    unsafe {
        flatten!{
            AccessorVarianceX {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorVarianceY<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorVarianceY);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="variance_y")]
pub fn accessor_variance_y(
    method: default!(String, "sample"),
) -> toolkit_experimental::AccessorVarianceY<'static> {
    //TODO validate `method`?
    unsafe {
        flatten!{
            AccessorVarianceY {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorSkewness {
    }
}

ron_inout_funcs!(AccessorSkewness);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="skewness")]
pub fn accessor_skewness(
) -> toolkit_experimental::AccessorSkewness<'static> {
    build!{
        AccessorSkewness {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorSkewnessX {
    }
}

ron_inout_funcs!(AccessorSkewnessX);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="skewness_x")]
pub fn accessor_skewness_x(
) -> toolkit_experimental::AccessorSkewnessX<'static> {
    build!{
        AccessorSkewnessX {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorSkewnessY {
    }
}

ron_inout_funcs!(AccessorSkewnessY);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="skewness_y")]
pub fn accessor_skewness_y(
) -> toolkit_experimental::AccessorSkewnessY<'static> {
    build!{
        AccessorSkewnessY {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorKurtosis {
    }
}

ron_inout_funcs!(AccessorKurtosis);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="kurtosis")]
pub fn accessor_kurtosis(
) -> toolkit_experimental::AccessorKurtosis<'static> {
    build!{
        AccessorKurtosis {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorKurtosisX {
    }
}

ron_inout_funcs!(AccessorKurtosisX);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="kurtosis_x")]
pub fn accessor_kurtosis_x(
) -> toolkit_experimental::AccessorKurtosisX<'static> {
    build!{
        AccessorKurtosisX {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorKurtosisY {
    }
}

ron_inout_funcs!(AccessorKurtosisY);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="kurtosis_y")]
pub fn accessor_kurtosis_y(
) -> toolkit_experimental::AccessorKurtosisY<'static> {
    build!{
        AccessorKurtosisY {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorCorr {
    }
}

ron_inout_funcs!(AccessorCorr);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="corr")]
pub fn accessor_corr(
) -> toolkit_experimental::AccessorCorr<'static> {
    build!{
        AccessorCorr {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorIntercept {
    }
}

ron_inout_funcs!(AccessorIntercept);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="intercept")]
pub fn accessor_intercept(
) -> toolkit_experimental::AccessorIntercept<'static> {
    build!{
        AccessorIntercept {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorXIntercept {
    }
}

ron_inout_funcs!(AccessorXIntercept);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="x_intercept")]
pub fn accessor_x_intercept(
) -> toolkit_experimental::AccessorXIntercept<'static> {
    build!{
        AccessorXIntercept {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorDeterminationCoeff {
    }
}

ron_inout_funcs!(AccessorDeterminationCoeff);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="determination_coeff")]
pub fn accessor_determination_coeff(
) -> toolkit_experimental::AccessorDeterminationCoeff<'static> {
    build!{
        AccessorDeterminationCoeff {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorCovar<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

// FIXME string IO
ron_inout_funcs!(AccessorCovar);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="covariance")]
pub fn accessor_covar(
    method: default!(String, "sample"),
) -> toolkit_experimental::AccessorCovar<'static> {
    //TODO validate `method`?
    unsafe {
        flatten!{
            AccessorCovar {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorDistinctCount {
    }
}

ron_inout_funcs!(AccessorDistinctCount);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="distinct_count")]
pub fn accessor_distinct_count(
) -> toolkit_experimental::AccessorDistinctCount<'static> {
    build!{
        AccessorDistinctCount {
        }
    }
}


pg_type! {
    #[derive(Debug)]
    struct AccessorStdError {
    }
}

ron_inout_funcs!(AccessorStdError);

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental" name="stderror")]
pub fn accessor_stderror(
) -> toolkit_experimental::AccessorStdError<'static> {
    build!{
        AccessorStdError {
        }
    }
}
