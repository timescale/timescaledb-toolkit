
use counter_agg::range::I64Range;
use pgx::*;

use flat_serialize::*;

use std::convert::TryInto;

use crate::{
    build, flatten,
    pg_type,
    ron_inout_funcs,
};

#[pg_schema]
pub mod toolkit_experimental {
    pub use super::*;

    pg_type! {
        #[derive(Debug)]
        struct AccessorApproxPercentile {
            percentile: f64,
        }
    }

    ron_inout_funcs!(AccessorApproxPercentile);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="approx_percentile")]
    pub fn accessor_approx_percentile(
        percentile: f64,
    ) -> toolkit_experimental::AccessorApproxPercentile<'static> {
        build!{
            AccessorApproxPercentile {
                percentile,
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="approx_percentile_rank")]
    pub fn accessor_approx_rank(
        value: f64,
    ) -> toolkit_experimental::AccessorApproxRank<'static> {
        build!{
            AccessorApproxRank {
                value,
            }
        }
    }

    pg_type! {
        #[derive(Debug)]
        struct AccessorNumVals {
        }
    }

    ron_inout_funcs!(AccessorNumVals);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="num_vals")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="mean")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="error")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="min_val")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="max_val")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="average")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="average_x")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="average_y")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="sum")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="sum_x")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="sum_y")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="slope")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="stddev")]
    pub fn accessor_stddev(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorStdDev<'static> {
        let _ = crate::stats_agg::method_kind(method);
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="stddev_x")]
    pub fn accessor_stddev_x(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorStdDevX<'static> {
        let _ = crate::stats_agg::method_kind(method);
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="stddev_y")]
    pub fn accessor_stddev_y(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorStdDevY<'static> {
        let _ = crate::stats_agg::method_kind(method);
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="variance")]
    pub fn accessor_variance(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorVariance<'static> {
        let _ = crate::stats_agg::method_kind(method);
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="variance_x")]
    pub fn accessor_variance_x(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorVarianceX<'static> {
        let _ = crate::stats_agg::method_kind(method);
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="variance_y")]
    pub fn accessor_variance_y(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorVarianceY<'static> {
        let _ = crate::stats_agg::method_kind(method);
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
        struct AccessorSkewness<'input>  {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    ron_inout_funcs!(AccessorSkewness);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="skewness")]
    pub fn accessor_skewness(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorSkewness<'static> {
        let _ = crate::stats_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorSkewness {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorSkewnessX<'input>  {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    ron_inout_funcs!(AccessorSkewnessX);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="skewness_x")]
    pub fn accessor_skewness_x(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorSkewnessX<'static> {
        let _ = crate::stats_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorSkewnessX {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorSkewnessY<'input>  {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    ron_inout_funcs!(AccessorSkewnessY);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="skewness_y")]
    pub fn accessor_skewness_y(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorSkewnessY<'static> {
        let _ = crate::stats_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorSkewnessY {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorKurtosis<'input>  {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    ron_inout_funcs!(AccessorKurtosis);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="kurtosis")]
    pub fn accessor_kurtosis(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorKurtosis<'static> {
        let _ = crate::stats_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorKurtosis {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorKurtosisX<'input>  {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    ron_inout_funcs!(AccessorKurtosisX);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="kurtosis_x")]
    pub fn accessor_kurtosis_x(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorKurtosisX<'static> {
        let _ = crate::stats_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorKurtosisX {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorKurtosisY<'input>  {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    ron_inout_funcs!(AccessorKurtosisY);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="kurtosis_y")]
    pub fn accessor_kurtosis_y(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorKurtosisY<'static> {
        let _ = crate::stats_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorKurtosisY {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorCorr {
        }
    }

    ron_inout_funcs!(AccessorCorr);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="corr")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="intercept")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="x_intercept")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="determination_coeff")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="covariance")]
    pub fn accessor_covar(
        method: default!(&str, "'sample'"),
    ) -> toolkit_experimental::AccessorCovar<'static> {
        let _ = crate::stats_agg::method_kind(method);
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="distinct_count")]
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

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="stderror")]
    pub fn accessor_stderror(
    ) -> toolkit_experimental::AccessorStdError<'static> {
        build!{
            AccessorStdError {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorDelta {
        }
    }

    ron_inout_funcs!(AccessorDelta);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="delta")]
    pub fn accessor_delta(
    ) -> toolkit_experimental::AccessorDelta<'static> {
        build!{
            AccessorDelta {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorTimeDelta {
        }
    }

    ron_inout_funcs!(AccessorTimeDelta);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="time_delta")]
    pub fn accessor_time_delta(
    ) -> toolkit_experimental::AccessorTimeDelta<'static> {
        build!{
            AccessorTimeDelta {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorRate {
        }
    }

    ron_inout_funcs!(AccessorRate);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="rate")]
    pub fn accessor_rate(
    ) -> toolkit_experimental::AccessorRate<'static> {
        build!{
            AccessorRate {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorIRateLeft {
        }
    }

    ron_inout_funcs!(AccessorIRateLeft);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="irate_left")]
    pub fn accessor_irate_left(
    ) -> toolkit_experimental::AccessorIRateLeft<'static> {
        build!{
            AccessorIRateLeft {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorIRateRight {
        }
    }

    ron_inout_funcs!(AccessorIRateRight);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="irate_right")]
    pub fn accessor_irate_right(
    ) -> toolkit_experimental::AccessorIRateRight<'static> {
        build!{
            AccessorIRateRight {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorIDeltaLeft {
        }
    }

    ron_inout_funcs!(AccessorIDeltaLeft);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="idelta_left")]
    pub fn accessor_idelta_left(
    ) -> toolkit_experimental::AccessorIDeltaLeft<'static> {
        build!{
            AccessorIDeltaLeft {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorIDeltaRight {
        }
    }

    ron_inout_funcs!(AccessorIDeltaRight);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="idelta_right")]
    pub fn accessor_idelta_right(
    ) -> toolkit_experimental::AccessorIDeltaRight<'static> {
        build!{
            AccessorIDeltaRight {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorNumElements {
        }
    }

    ron_inout_funcs!(AccessorNumElements);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="num_elements")]
    pub fn accessor_num_elements(
    ) -> toolkit_experimental::AccessorNumElements<'static> {
        build!{
            AccessorNumElements {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorNumChanges {
        }
    }

    ron_inout_funcs!(AccessorNumChanges);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="num_changes")]
    pub fn accessor_num_changes(
    ) -> toolkit_experimental::AccessorNumChanges<'static> {
        build!{
            AccessorNumChanges {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorNumResets {
        }
    }

    ron_inout_funcs!(AccessorNumResets);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="num_resets")]
    pub fn accessor_num_resets(
    ) -> toolkit_experimental::AccessorNumResets<'static> {
        build!{
            AccessorNumResets {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorZeroTime {
        }
    }

    ron_inout_funcs!(AccessorZeroTime);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="counter_zero_time")]
    pub fn accessor_zero_time(
    ) -> toolkit_experimental::AccessorZeroTime<'static> {
        build!{
            AccessorZeroTime {
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorExtrapolatedDelta<'input> {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    //FIXME string IO
    ron_inout_funcs!(AccessorExtrapolatedDelta);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="extrapolated_delta")]
    pub fn accessor_extrapolated_delta(
        method: &str,
    ) -> toolkit_experimental::AccessorExtrapolatedDelta<'static> {
        let _ = crate::counter_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorExtrapolatedDelta {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorExtrapolatedRate<'input> {
            len: u32,
            bytes: [u8; self.len],
        }
    }

    //FIXME string IO
    ron_inout_funcs!(AccessorExtrapolatedRate);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="extrapolated_rate")]
    pub fn accessor_extrapolated_rate(
        method: &str,
    ) -> toolkit_experimental::AccessorExtrapolatedRate<'static> {
        let _ = crate::counter_agg::method_kind(method);
        unsafe {
            flatten!{
                AccessorExtrapolatedRate {
                    len: method.len().try_into().unwrap(),
                    bytes: method.as_bytes().into(),
                }
            }
        }
    }


    pg_type! {
        #[derive(Debug)]
        struct AccessorWithBounds {
            lower: i64,
            upper: i64,
            range_null: u8,
            lower_present: u8,
            upper_present: u8,
        }
    }

    ron_inout_funcs!(AccessorWithBounds);

    #[pg_extern(immutable, parallel_safe, schema="toolkit_experimental", name="with_bounds")]
    pub fn accessor_with_bounds(
        bounds: crate::raw::tstzrange,
    ) -> toolkit_experimental::AccessorWithBounds<'static> {
        let range = unsafe { crate::range::get_range(bounds.0 as *mut pg_sys::varlena) };
        let mut accessor = build!{
            AccessorWithBounds {
                lower: 0,
                upper: 0,
                range_null: 0,
                lower_present: 0,
                upper_present: 0,
            }
        };
        match range {
            None => accessor.range_null = 1,
            Some(range) => {
                if let Some(left) = range.left() {
                    accessor.lower_present = 1;
                    accessor.lower = left;
                }
                if let Some(right) = range.right() {
                    accessor.upper_present = 1;
                    accessor.upper = right;
                }
            },
        }
        accessor
    }

    impl<'i> AccessorWithBounds<'i> {
        pub fn bounds(&self) -> I64Range {
            if self.range_null != 0{
                return I64Range::infinite();
            }

            I64Range::new(
                (self.lower_present != 0).then(|| self.lower),
                (self.upper_present != 0).then(|| self.upper),
            )
        }
    }
}
