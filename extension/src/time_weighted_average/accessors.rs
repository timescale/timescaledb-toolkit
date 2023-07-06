use pgrx::*;

use crate::time_weighted_average::DurationUnit;
use crate::{
    datum_utils::interval_to_ms,
    flatten, pg_type, ron_inout_funcs,
    time_weighted_average::{TimeWeightMethod, TimeWeightSummary, TimeWeightSummaryData},
};

use tspoint::TSPoint;

pg_type! {
    struct TimeWeightInterpolatedAverageAccessor {
        timestamp : i64,
        interval : i64,
        prev : TimeWeightSummaryData,
        pad : [u8;3],
        flags : u32,
        next : TimeWeightSummaryData,
    }
}

ron_inout_funcs!(TimeWeightInterpolatedAverageAccessor);

#[pg_extern(immutable, parallel_safe, name = "interpolated_average")]
fn time_weight_interpolated_average_accessor<'a>(
    start: crate::raw::TimestampTz,
    duration: crate::raw::Interval,
    prev: default!(Option<TimeWeightSummary<'a>>, "NULL"),
    next: default!(Option<TimeWeightSummary<'a>>, "NULL"),
) -> TimeWeightInterpolatedAverageAccessor<'static> {
    fn empty_summary<'b>() -> Option<TimeWeightSummary<'b>> {
        Some(unsafe {
            flatten!(TimeWeightSummary {
                first: TSPoint { ts: 0, val: 0.0 },
                last: TSPoint { ts: 0, val: 0.0 },
                weighted_sum: 0.0,
                method: TimeWeightMethod::LOCF,
            })
        })
    }

    let flags = u32::from(prev.is_some()) + if next.is_some() { 2 } else { 0 };
    let prev = prev.or_else(empty_summary).unwrap().0;
    let next = next.or_else(empty_summary).unwrap().0;
    let interval = interval_to_ms(&start, &duration);
    crate::build! {
        TimeWeightInterpolatedAverageAccessor {
            timestamp : start.into(),
            interval,
            prev,
            pad : [0,0,0],
            flags,
            next,
        }
    }
}

pg_type! {
    #[derive(Debug)]
    struct TimeWeightInterpolatedIntegralAccessor {
        start : i64,
        interval : i64,
        prev : TimeWeightSummaryData,
        pad : [u8;3],
        unit : u32,
        flags: u64,
        next : TimeWeightSummaryData,
    }
}

ron_inout_funcs!(TimeWeightInterpolatedIntegralAccessor);

#[pg_extern(immutable, parallel_safe, name = "interpolated_integral")]
fn time_weight_interpolated_integral_accessor<'a>(
    start: crate::raw::TimestampTz,
    interval: crate::raw::Interval,
    prev: default!(Option<TimeWeightSummary<'a>>, "NULL"),
    next: default!(Option<TimeWeightSummary<'a>>, "NULL"),
    unit: default!(String, "'second'"),
) -> TimeWeightInterpolatedIntegralAccessor<'static> {
    fn empty_summary<'b>() -> Option<TimeWeightSummary<'b>> {
        Some(unsafe {
            flatten!(TimeWeightSummary {
                first: TSPoint { ts: 0, val: 0.0 },
                last: TSPoint { ts: 0, val: 0.0 },
                weighted_sum: 0.0,
                method: TimeWeightMethod::LOCF,
            })
        })
    }

    let unit = match DurationUnit::from_str(&unit) {
        Some(unit) => unit.microseconds(),
        None => pgrx::error!(
            "Unrecognized duration unit: {}. Valid units are: usecond, msecond, second, minute, hour",
            unit,
        ),
    };
    let flags = u64::from(prev.is_some()) + if next.is_some() { 2 } else { 0 };
    let prev = prev.or_else(empty_summary).unwrap().0;
    let next = next.or_else(empty_summary).unwrap().0;
    let interval = interval_to_ms(&start, &interval);
    crate::build! {
        TimeWeightInterpolatedIntegralAccessor {
            start: start.into(),
            interval,
            prev,
            pad : [0,0,0],
            unit,
            flags,
            next,
        }
    }
}
