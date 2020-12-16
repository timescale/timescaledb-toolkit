mod tdigest;
mod palloc;
mod aggregate_utils;

pub use crate::tdigest::{
    TDigestTransState,
    tdigest_trans,
    tdigest_final,
    tdigest_combine,
    tdigest_serialize,
    tdigest_deserialize,
    tsTDigest,
    tdigest_quantile,
    tdigest_quantile_at_value,
    tdigest_count,
    tdigest_sum,
    tdigest_min,
    tdigest_max,
    tdigest_mean,
};

pgx::pg_module_magic!();

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
