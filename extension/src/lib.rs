// so we can allow very new Clippy lints
#![allow(unknown_lints)]
// flat_serialize! alignment checks hit this for any single byte field (of which all pg_types! have two by default)
#![allow(clippy::modulo_one)]
// some disagreement between clippy and the rust compiler about when lifetime are and are not needed
#![allow(clippy::extra_unused_lifetimes)]
// every function calling in_aggregate_context should be unsafe
#![allow(clippy::not_unsafe_ptr_arg_deref)]
// since 0.5 pgrx requires non-elided lifetimes on extern functions: https://github.com/tcdi/pgrx/issues/721
#![allow(clippy::needless_lifetimes)]
// triggered by pg_extern macros
#![allow(clippy::useless_conversion)]
// caused by pgrx
#![allow(clippy::unnecessary_lazy_evaluations)]
// clippy triggers an internal complier error checking this
#![allow(clippy::unnecessary_literal_unwrap)]

#![allow(warnings)]

pub mod accessors;
pub mod asap;
pub mod candlestick;
pub mod counter_agg;
pub mod countminsketch;
pub mod frequency;
pub mod gauge_agg;
pub mod heartbeat_agg;
pub mod hyperloglog;
pub mod lttb;
pub mod nmost;
pub mod range;
pub mod saturation;
pub(crate) mod serialization;
pub mod state_aggregate;
pub mod stats_agg;
pub mod tdigest;
pub mod time_vector;
pub mod time_weighted_average;
pub mod uddsketch;
pub mod utilities;

mod aggregate_utils;
mod datum_utils;
mod duration;
mod palloc;
mod pg_any_element;
mod raw;
mod stabilization_info;
mod stabilization_tests;
mod type_builder;

#[cfg(any(test, feature = "pg_test"))]
mod aggregate_builder_tests;

use pgrx::*;

pgrx::pg_module_magic!();

#[pg_guard]
pub extern "C" fn _PG_init() {
    // Nothing to do here
}

extension_sql!(
    r#"GRANT USAGE ON SCHEMA toolkit_experimental TO PUBLIC;"#,
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
