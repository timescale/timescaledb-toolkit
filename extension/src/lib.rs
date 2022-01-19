#![allow(clippy::modulo_one)]   // flat_serialize! alignment checks hit this for any single byte field (of which all pg_types! have two by default)
#![allow(clippy::extra_unused_lifetimes)]  // some disagreement between clippy and the rust compiler about when lifetime are and are not needed

pub mod accessors;
pub mod tdigest;
pub mod hyperloglog;
pub mod uddsketch;
pub mod time_weighted_average;
pub mod asap;
pub mod lttb;
pub mod counter_agg;
pub mod range;
pub mod stats_agg;
pub mod utilities;
pub mod time_series;
pub mod frequency;

mod palloc;
mod aggregate_utils;
mod type_builder;
mod serialization;
mod schema_test;
mod raw;
mod datum_utils;

#[cfg(any(test, feature = "pg_test"))]
mod aggregate_builder_tests;

// This should be last so we don't run our warning trigger on when
// installing this extension
pub mod zz_triggers;

use pgx::*;

pgx::pg_module_magic!();

static EXPERIMENTAL_ENABLED: GucSetting<bool> = GucSetting::new(false);

#[pg_guard]
pub extern "C" fn _PG_init() {
    GucRegistry::define_bool_guc(
        "timescaledb_toolkit_acknowledge_auto_drop",
        "enable creation of auto-dropping objects using experimental timescaledb_toolkit_features",
        "experimental features are very unstable, and objects \
            depending on them will be automatically deleted on extension update",
        &EXPERIMENTAL_ENABLED,
        //TODO should this be superuser?
        GucContext::Userset,
    );
}

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
