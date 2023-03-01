use pgx::*;

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
    prev: Option<TimeWeightSummary<'a>>,
    next: Option<TimeWeightSummary<'a>>,
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
