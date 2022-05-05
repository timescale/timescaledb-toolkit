//! These data structures are stable, meaning they may not change even in
//! their layout in memory, as the raw bytes-in-memory are serialized and
//! exchanged by PostgreSQL.
//!
//! Note that [MetricSummary] is already in violation, as it does not lock in
//! a memory representation and the Rust project makes no guarantees to
//! preserve this across releases of the compiler.  We should bump its
//! serialization version and repr(C) the new one.

use serde::{Deserialize, Serialize};

use stats_agg::stats2d::StatsSummary2D;
use time_series::TSPoint;

use crate::range::I64Range;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
// TODO Our one serialization test (counter_byte_io) passes, but is that just luck?
//#[repr(C)]
pub struct MetricSummary {
    pub first: TSPoint,
    pub second: TSPoint,
    pub penultimate: TSPoint,
    pub last: TSPoint,
    pub reset_sum: f64,
    pub num_resets: u64,
    pub num_changes: u64,
    pub stats: StatsSummary2D,
    pub bounds: Option<I64Range>,
}

impl From<super::MetricSummary> for MetricSummary {
    fn from(range: super::MetricSummary) -> Self {
        Self {
            first: range.first,
            second: range.second,
            penultimate: range.penultimate,
            last: range.last,
            reset_sum: range.reset_sum,
            num_resets: range.num_resets,
            num_changes: range.num_changes,
            stats: range.stats,
            bounds: if range.bounds.is_infinite() { None } else { Some(range.bounds) },
        }
    }
}

impl From<MetricSummary> for super::MetricSummary {
    fn from(stable: MetricSummary) -> Self {
        Self {
            first: stable.first,
            second: stable.second,
            penultimate: stable.penultimate,
            last: stable.last,
            reset_sum: stable.reset_sum,
            num_resets: stable.num_resets,
            num_changes: stable.num_changes,
            stats: stable.stats,
            bounds: stable.bounds.unwrap_or_else(I64Range::infinite),
        }
    }
}
