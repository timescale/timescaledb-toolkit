
use pgx::*;

use flat_serialize::*;

use crate::{
    build,
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