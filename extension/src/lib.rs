
pub mod triggers;

pub mod tdigest;
pub mod hyperloglog;
pub mod uddsketch;
pub mod time_weighted_average;
pub mod asap;
pub mod lttb;

mod palloc;
mod aggregate_utils;
mod type_builder;
mod serialization;
mod schema_test;

use pgx::*;

pgx::pg_module_magic!();

static EXPERIMENTAL_ENABLED: GucSetting<bool> = GucSetting::new(false);

#[pg_guard]
pub extern "C" fn _PG_init() {
    GucRegistry::define_bool_guc(
        "timescale_analytics_acknowledge_auto_drop",
        "enable creation of auto-dropping objects using experimental timescale_analytics_features",
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
