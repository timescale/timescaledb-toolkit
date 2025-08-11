use pgrx::*;

use crate::{
    counter_agg::{CounterSummary, CounterSummaryData, MetricSummary},
    datum_utils::interval_to_ms,
    pg_type, ron_inout_funcs,
};

use tspoint::TSPoint;

pg_type! {
    struct CounterInterpolatedRateAccessor {
        timestamp : i64,
        interval : i64,
        prev : CounterSummaryData,
        next : CounterSummaryData,
        flags : u64,
    }
}

ron_inout_funcs!(CounterInterpolatedRateAccessor);

#[pg_extern(immutable, parallel_safe, name = "interpolated_rate")]
fn counter_interpolated_rate_accessor(
    start: crate::raw::TimestampTz,
    duration: crate::raw::Interval,
    prev: Option<CounterSummary>,
    next: Option<CounterSummary>,
) -> CounterInterpolatedRateAccessor {
    fn empty_summary() -> Option<CounterSummary> {
        let tmp = TSPoint { ts: 0, val: 0.0 };
        let tmp = MetricSummary::new(&tmp, None);
        let tmp = CounterSummary::from_internal_counter_summary(tmp);
        Some(tmp)
    }

    let flags = u64::from(prev.is_some()) + if next.is_some() { 2 } else { 0 };
    let prev = prev.or_else(empty_summary).unwrap().0;
    let next = next.or_else(empty_summary).unwrap().0;
    let interval = interval_to_ms(&start, &duration);
    crate::build! {
        CounterInterpolatedRateAccessor {
            timestamp : start.into(),
            interval,
            prev,
            next,
            flags,
        }
    }
}

pg_type! {
    struct CounterInterpolatedDeltaAccessor {
        timestamp : i64,
        interval : i64,
        prev : CounterSummaryData,
        next : CounterSummaryData,
        flags : u64,
    }
}

ron_inout_funcs!(CounterInterpolatedDeltaAccessor);

#[pg_extern(immutable, parallel_safe, name = "interpolated_delta")]
fn counter_interpolated_delta_accessor(
    start: crate::raw::TimestampTz,
    duration: crate::raw::Interval,
    prev: Option<CounterSummary>,
    next: Option<CounterSummary>,
) -> CounterInterpolatedDeltaAccessor {
    fn empty_summary() -> Option<CounterSummary> {
        let tmp = TSPoint { ts: 0, val: 0.0 };
        let tmp = MetricSummary::new(&tmp, None);
        let tmp = CounterSummary::from_internal_counter_summary(tmp);
        Some(tmp)
    }

    let flags = u64::from(prev.is_some()) + if next.is_some() { 2 } else { 0 };
    let prev = prev.or_else(empty_summary).unwrap().0;
    let next = next.or_else(empty_summary).unwrap().0;
    let interval = interval_to_ms(&start, &duration);
    crate::build! {
        CounterInterpolatedDeltaAccessor {
            timestamp : start.into(),
            interval,
            prev,
            next,
            flags,
        }
    }
}
