#![allow(clippy::modulo_one)]   // flat_serialize! alignment checks hit this for any single byte field (of which all pg_types! have two by default)
#![allow(clippy::extra_unused_lifetimes)]  // some disagreement between clippy and the rust compiler about when lifetime are and are not needed
#![allow(clippy::not_unsafe_ptr_arg_deref)]  // every function calling in_aggregate_context should be unsafe

pub mod accessors;
pub mod tdigest;
pub mod hyperloglog;
pub mod uddsketch;
pub mod time_weighted_average;
pub mod asap;
pub mod lttb;
pub mod counter_agg;
pub mod gauge_agg;
pub mod range;
pub mod state_aggregate;
pub mod stats_agg;
pub mod utilities;
pub mod time_series;
pub mod frequency;

mod palloc;
mod aggregate_utils;
mod type_builder;
mod serialization;
mod stabilization_tests;
mod stabilization_info;
mod raw;
mod datum_utils;
mod pg_any_element;

#[cfg(any(test, feature = "pg_test"))]
mod aggregate_builder_tests;

use pgx::*;

pgx::pg_module_magic!();

#[pg_guard]
pub extern "C" fn _PG_init() {
    // Nothing to do here
}

extension_sql!(r#"GRANT USAGE ON SCHEMA toolkit_experimental TO PUBLIC;"#,
    name = "final_grant",
    finalize,
);


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
