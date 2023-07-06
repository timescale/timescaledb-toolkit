use std::convert::TryInto as _;

use pgrx::*;

use counter_agg::range::I64Range;

use crate::{build, flatten, pg_type, ron_inout_funcs};

macro_rules! accessor {
    (
        $name: ident (
            $($field:ident : $typ: tt),* $(,)?
        )
    ) => {
        ::paste::paste! {
            $crate::pg_type!{
                // TODO Move into pg_type as we don't care to vary it.
                #[derive(Debug)]
                struct [<Accessor $name:camel>] {
                $($field: $typ,)*
                }
            }
            $crate::ron_inout_funcs!([<Accessor $name:camel>]);
        }
        accessor_fn_impl! { $name( $( $field: $typ),* ) }
    };
}

macro_rules! accessor_fn_impl {
    (
        $name: ident (
            $( $field:ident : $typ: tt ),*
                $(,)?
        )
    ) => {
        ::paste::paste!{
            #[pg_extern(immutable, parallel_safe, name = "" $name "")]
            fn [<accessor_ $name >](
                $( $field: $typ ),*
            ) -> [<Accessor $name:camel>]<'static> {
                $crate::build! {
                    [<Accessor $name:camel>] {
                        $( $field ),*
                    }
                }
            }
        }
    };
}

accessor! { approx_percentile(
    percentile: f64,
) }

accessor! { approx_percentile_rank(
    value: f64,
) }

accessor! { num_vals() }
accessor! { mean() }
accessor! { error() }
accessor! { min_val() }
accessor! { max_val() }
accessor! { average() }
accessor! { average_x() }
accessor! { average_y() }
accessor! { sum() }
accessor! { sum_x() }
accessor! { sum_y() }
accessor! { slope() }
accessor! { corr() }
accessor! { intercept() }
accessor! { x_intercept() }
accessor! { determination_coeff() }
accessor! { distinct_count() }
accessor! { stderror() }
accessor! { delta() }
accessor! { time_delta() }
accessor! { rate() }
accessor! { irate_left() }
accessor! { irate_right() }
accessor! { idelta_left() }
accessor! { idelta_right() }
accessor! { num_elements() }
accessor! { num_changes() }
accessor! { num_resets() }
accessor! { counter_zero_time() }
accessor! { first_val() }
accessor! { last_val() }
accessor! { first_time() }
accessor! { last_time() }
accessor! { open() }
accessor! { close() }
accessor! { high() }
accessor! { low() }
accessor! { open_time() }
accessor! { high_time() }
accessor! { low_time() }
accessor! { close_time() }
accessor! { live_ranges() }
accessor! { dead_ranges() }
accessor! { uptime() }
accessor! { downtime() }
accessor! { into_values() }
accessor! { into_array() }
accessor! { into_int_values() }
accessor! { state_timeline() }
accessor! { state_int_timeline() }
accessor! { num_live_ranges() }
accessor! { num_gaps() }
accessor! { topn() }
// The rest are more complex, with String or other challenges.  Leaving alone for now.

pg_type! {
    #[derive(Debug)]
    struct AccessorLiveAt {
        time: u64,
    }
}

ron_inout_funcs!(AccessorLiveAt);

#[pg_extern(immutable, parallel_safe, name = "live_at")]
pub fn accessor_live_at(ts: crate::raw::TimestampTz) -> AccessorLiveAt<'static> {
    unsafe {
        flatten! {
            AccessorLiveAt {
                time: ts.0.value() as u64,
            }
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

#[pg_extern(immutable, parallel_safe, name = "stddev")]
pub fn accessor_stddev(method: default!(&str, "'sample'")) -> AccessorStdDev<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "stddev_x")]
pub fn accessor_stddev_x(method: default!(&str, "'sample'")) -> AccessorStdDevX<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "stddev_y")]
pub fn accessor_stddev_y(method: default!(&str, "'sample'")) -> AccessorStdDevY<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "variance")]
pub fn accessor_variance(method: default!(&str, "'sample'")) -> AccessorVariance<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "variance_x")]
pub fn accessor_variance_x(method: default!(&str, "'sample'")) -> AccessorVarianceX<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "variance_y")]
pub fn accessor_variance_y(method: default!(&str, "'sample'")) -> AccessorVarianceY<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "skewness")]
pub fn accessor_skewness(method: default!(&str, "'sample'")) -> AccessorSkewness<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "skewness_x")]
pub fn accessor_skewness_x(method: default!(&str, "'sample'")) -> AccessorSkewnessX<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "skewness_y")]
pub fn accessor_skewness_y(method: default!(&str, "'sample'")) -> AccessorSkewnessY<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "kurtosis")]
pub fn accessor_kurtosis(method: default!(&str, "'sample'")) -> AccessorKurtosis<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "kurtosis_x")]
pub fn accessor_kurtosis_x(method: default!(&str, "'sample'")) -> AccessorKurtosisX<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "kurtosis_y")]
pub fn accessor_kurtosis_y(method: default!(&str, "'sample'")) -> AccessorKurtosisY<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorKurtosisY {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
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

#[pg_extern(immutable, parallel_safe, name = "covariance")]
pub fn accessor_covar(method: default!(&str, "'sample'")) -> AccessorCovar<'static> {
    let _ = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorCovar {
                len: method.len().try_into().unwrap(),
                bytes: method.as_bytes().into(),
            }
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

#[pg_extern(immutable, parallel_safe, name = "extrapolated_delta")]
pub fn accessor_extrapolated_delta(method: &str) -> AccessorExtrapolatedDelta<'static> {
    let _ = crate::counter_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "extrapolated_rate")]
pub fn accessor_extrapolated_rate(method: &str) -> AccessorExtrapolatedRate<'static> {
    let _ = crate::counter_agg::method_kind(method);
    unsafe {
        flatten! {
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

#[pg_extern(immutable, parallel_safe, name = "with_bounds")]
pub fn accessor_with_bounds(bounds: crate::raw::tstzrange) -> AccessorWithBounds<'static> {
    let range = unsafe { crate::range::get_range(bounds.0.cast_mut_ptr()) };
    let mut accessor = build! {
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
            if let Some(left) = range.left {
                accessor.lower_present = 1;
                accessor.lower = left;
            }
            if let Some(right) = range.right {
                accessor.upper_present = 1;
                accessor.upper = right;
            }
        }
    }
    accessor
}

impl AccessorWithBounds<'_> {
    pub fn bounds(&self) -> Option<I64Range> {
        if self.range_null != 0 {
            return None;
        }

        I64Range {
            left: (self.lower_present != 0).then(|| self.lower),
            right: (self.upper_present != 0).then(|| self.upper),
        }
        .into()
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorUnnest {
    }
}

ron_inout_funcs!(AccessorUnnest);

// Note that this should be able to replace the timescale_experimental.unnest function
// and related object in src/timevector/pipeline/expansion.rs
#[pg_extern(immutable, parallel_safe, name = "unnest")]
pub fn accessor_unnest() -> AccessorUnnest<'static> {
    build! {
        AccessorUnnest {
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorIntegral<'input> {
        len: u32,
        bytes: [u8; self.len],
    }
}

// FIXME string IO
ron_inout_funcs!(AccessorIntegral);

#[pg_extern(immutable, parallel_safe, name = "integral")]
pub fn accessor_integral(unit: default!(&str, "'second'")) -> AccessorIntegral<'static> {
    unsafe {
        flatten! {
            AccessorIntegral {
                len: unit.len().try_into().unwrap(),
                bytes: unit.as_bytes().into(),
            }
        }
    }
}

// Note we also have a AccessorTopn which is similar to this but doesn't store the count
pg_type! {
    #[derive(Debug)]
    struct AccessorTopNCount {
        count: i64,
    }
}

ron_inout_funcs!(AccessorTopNCount);

#[pg_extern(immutable, parallel_safe, name = "topn")]
pub fn accessor_topn_count(count: i64) -> AccessorTopNCount<'static> {
    unsafe {
        flatten! {
            AccessorTopNCount {
                count
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorMaxFrequencyInt {
        value: i64,
    }
}

ron_inout_funcs!(AccessorMaxFrequencyInt);

#[pg_extern(immutable, parallel_safe, name = "max_frequency")]
pub fn accessor_max_frequency_int(value: i64) -> AccessorMaxFrequencyInt<'static> {
    unsafe {
        flatten! {
            AccessorMaxFrequencyInt {
                value
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorMinFrequencyInt {
        value: i64,
    }
}

ron_inout_funcs!(AccessorMinFrequencyInt);

#[pg_extern(immutable, parallel_safe, name = "min_frequency")]
pub fn accessor_min_frequency_int(value: i64) -> AccessorMinFrequencyInt<'static> {
    unsafe {
        flatten! {
            AccessorMinFrequencyInt {
                value
            }
        }
    }
}

pg_type! {
    struct AccessorPercentileArray<'input> {
        len: u64,
        percentile: [f64; self.len],
    }
}

ron_inout_funcs!(AccessorPercentileArray);

#[pg_extern(immutable, name = "approx_percentiles")]
pub fn accessor_percentiles(unit: Vec<f64>) -> AccessorPercentileArray<'static> {
    unsafe {
        flatten! {
            AccessorPercentileArray{
                len: unit.len().try_into().unwrap(),
                percentile: unit.into(),
            }
        }
    }
}
