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
            ) -> [<Accessor $name:camel>] {
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
pub fn accessor_live_at(ts: crate::raw::TimestampTz) -> AccessorLiveAt {
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
    struct AccessorStdDev {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorStdDev);

#[pg_extern(immutable, parallel_safe, name = "stddev")]
pub fn accessor_stddev(method: default!(&str, "'sample'")) -> AccessorStdDev {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorStdDev {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorStdDevX {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorStdDevX);

#[pg_extern(immutable, parallel_safe, name = "stddev_x")]
pub fn accessor_stddev_x(method: default!(&str, "'sample'")) -> AccessorStdDevX {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorStdDevX {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorStdDevY {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorStdDevY);

#[pg_extern(immutable, parallel_safe, name = "stddev_y")]
pub fn accessor_stddev_y(method: default!(&str, "'sample'")) -> AccessorStdDevY {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorStdDevY {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorVariance {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorVariance);

#[pg_extern(immutable, parallel_safe, name = "variance")]
pub fn accessor_variance(method: default!(&str, "'sample'")) -> AccessorVariance {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorVariance {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorVarianceX {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorVarianceX);

#[pg_extern(immutable, parallel_safe, name = "variance_x")]
pub fn accessor_variance_x(method: default!(&str, "'sample'")) -> AccessorVarianceX {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorVarianceX {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorVarianceY {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorVarianceY);

#[pg_extern(immutable, parallel_safe, name = "variance_y")]
pub fn accessor_variance_y(method: default!(&str, "'sample'")) -> AccessorVarianceY {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorVarianceY {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorSkewness {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorSkewness);

#[pg_extern(immutable, parallel_safe, name = "skewness")]
pub fn accessor_skewness(method: default!(&str, "'sample'")) -> AccessorSkewness {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorSkewness {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorSkewnessX {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorSkewnessX);

#[pg_extern(immutable, parallel_safe, name = "skewness_x")]
pub fn accessor_skewness_x(method: default!(&str, "'sample'")) -> AccessorSkewnessX {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorSkewnessX {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorSkewnessY {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorSkewnessY);

#[pg_extern(immutable, parallel_safe, name = "skewness_y")]
pub fn accessor_skewness_y(method: default!(&str, "'sample'")) -> AccessorSkewnessY {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorSkewnessY {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorKurtosis {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorKurtosis);

#[pg_extern(immutable, parallel_safe, name = "kurtosis")]
pub fn accessor_kurtosis(method: default!(&str, "'sample'")) -> AccessorKurtosis {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorKurtosis {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorKurtosisX {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorKurtosisX);

#[pg_extern(immutable, parallel_safe, name = "kurtosis_x")]
pub fn accessor_kurtosis_x(method: default!(&str, "'sample'")) -> AccessorKurtosisX {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorKurtosisX {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorKurtosisY {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorKurtosisY);

#[pg_extern(immutable, parallel_safe, name = "kurtosis_y")]
pub fn accessor_kurtosis_y(method: default!(&str, "'sample'")) -> AccessorKurtosisY {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorKurtosisY {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorCovar {
        method: crate::stats_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorCovar);

#[pg_extern(immutable, parallel_safe, name = "covariance")]
pub fn accessor_covar(method: default!(&str, "'sample'")) -> AccessorCovar {
    let method_enum = crate::stats_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorCovar {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorExtrapolatedDelta {
        method: crate::counter_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorExtrapolatedDelta);

#[pg_extern(immutable, parallel_safe, name = "extrapolated_delta")]
pub fn accessor_extrapolated_delta(method: &str) -> AccessorExtrapolatedDelta {
    let method_enum = crate::counter_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorExtrapolatedDelta {
                method: method_enum,
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorExtrapolatedRate {
        method: crate::counter_agg::Method,
    }
}

//FIXME string IO
ron_inout_funcs!(AccessorExtrapolatedRate);

#[pg_extern(immutable, parallel_safe, name = "extrapolated_rate")]
pub fn accessor_extrapolated_rate(method: &str) -> AccessorExtrapolatedRate {
    let method_enum = crate::counter_agg::method_kind(method);
    unsafe {
        flatten! {
            AccessorExtrapolatedRate {
                method: method_enum,
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
pub fn accessor_with_bounds(bounds: crate::raw::tstzrange) -> AccessorWithBounds {
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

impl AccessorWithBounds {
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
pub fn accessor_unnest() -> AccessorUnnest {
    build! {
        AccessorUnnest {
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorIntegral {
        len: u8,
        bytes: [u8; 16],
    }
}

// FIXME string IO
ron_inout_funcs!(AccessorIntegral);

#[pg_extern(immutable, parallel_safe, name = "integral")]
pub fn accessor_integral(unit: default!(&str, "'second'")) -> AccessorIntegral {
    if unit.len() > 16 {
        pgrx::error!(
            "Time unit string too long: {} characters (max 16)",
            unit.len()
        );
    }

    let mut bytes = [0u8; 16];
    let unit_bytes = unit.as_bytes();
    bytes[..unit_bytes.len()].copy_from_slice(unit_bytes);

    unsafe {
        flatten! {
            AccessorIntegral {
                len: unit.len() as u8,
                bytes,
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
pub fn accessor_topn_count(count: i64) -> AccessorTopNCount {
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
pub fn accessor_max_frequency_int(value: i64) -> AccessorMaxFrequencyInt {
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
pub fn accessor_min_frequency_int(value: i64) -> AccessorMinFrequencyInt {
    unsafe {
        flatten! {
            AccessorMinFrequencyInt {
                value
            }
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct AccessorPercentileArray {
        len: u64,
        percentile: [f64; 32],
    }
}

ron_inout_funcs!(AccessorPercentileArray);

#[pg_extern(immutable, name = "approx_percentiles")]
pub fn accessor_percentiles(unit: Vec<f64>) -> AccessorPercentileArray {
    if unit.len() > 32 {
        pgrx::error!("Too many percentiles: {} (max 32)", unit.len());
    }

    let mut percentile = [0.0f64; 32];
    for (i, &val) in unit.iter().enumerate() {
        percentile[i] = val;
    }

    unsafe {
        flatten! {
            AccessorPercentileArray {
                len: unit.len() as u64,
                percentile,
            }
        }
    }
}
