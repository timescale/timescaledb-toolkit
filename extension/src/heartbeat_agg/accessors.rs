use pgrx::*;

use crate::{
    flatten,
    heartbeat_agg::{HeartbeatAgg, HeartbeatAggData},
    pg_type, ron_inout_funcs,
};

fn empty_agg<'a>() -> HeartbeatAgg<'a> {
    unsafe {
        flatten!(HeartbeatAgg {
            start_time: 0,
            end_time: 0,
            last_seen: 0,
            interval_len: 0,
            num_intervals: 0,
            interval_starts: vec!().into(),
            interval_ends: vec!().into(),
        })
    }
}

pg_type! {
    struct HeartbeatInterpolatedUptimeAccessor<'input> {
        has_prev : u64,
        prev : HeartbeatAggData<'input>,
    }
}

ron_inout_funcs!(HeartbeatInterpolatedUptimeAccessor);

#[pg_extern(immutable, parallel_safe, name = "interpolated_uptime")]
fn heartbeat_agg_interpolated_uptime_accessor<'a>(
    prev: Option<HeartbeatAgg<'a>>,
) -> HeartbeatInterpolatedUptimeAccessor<'a> {
    let has_prev = u64::from(prev.is_some());
    let prev = prev.unwrap_or_else(empty_agg).0;

    crate::build! {
        HeartbeatInterpolatedUptimeAccessor {
            has_prev,
            prev,
        }
    }
}

impl<'a> HeartbeatInterpolatedUptimeAccessor<'a> {
    pub fn pred(&self) -> Option<HeartbeatAgg<'a>> {
        if self.has_prev == 0 {
            None
        } else {
            Some(self.prev.clone().into())
        }
    }
}

pg_type! {
    struct HeartbeatInterpolatedDowntimeAccessor<'input> {
        has_prev : u64,
        prev : HeartbeatAggData<'input>,
    }
}

ron_inout_funcs!(HeartbeatInterpolatedDowntimeAccessor);

#[pg_extern(immutable, parallel_safe, name = "interpolated_downtime")]
fn heartbeat_agg_interpolated_downtime_accessor<'a>(
    prev: Option<HeartbeatAgg<'a>>,
) -> HeartbeatInterpolatedDowntimeAccessor<'a> {
    let has_prev = u64::from(prev.is_some());
    let prev = prev.unwrap_or_else(empty_agg).0;

    crate::build! {
        HeartbeatInterpolatedDowntimeAccessor {
            has_prev,
            prev,
        }
    }
}

impl<'a> HeartbeatInterpolatedDowntimeAccessor<'a> {
    pub fn pred(&self) -> Option<HeartbeatAgg<'a>> {
        if self.has_prev == 0 {
            None
        } else {
            Some(self.prev.clone().into())
        }
    }
}

pg_type! {
    struct HeartbeatInterpolateAccessor<'input> {
        has_prev : u64,
        prev : HeartbeatAggData<'input>,
    }
}

ron_inout_funcs!(HeartbeatInterpolateAccessor);

#[pg_extern(immutable, parallel_safe, name = "interpolate")]
fn heartbeat_agg_interpolate_accessor<'a>(
    prev: Option<HeartbeatAgg<'a>>,
) -> HeartbeatInterpolateAccessor<'a> {
    let has_prev = u64::from(prev.is_some());
    let prev = prev.unwrap_or_else(empty_agg).0;

    crate::build! {
        HeartbeatInterpolateAccessor {
            has_prev,
            prev,
        }
    }
}

impl<'a> HeartbeatInterpolateAccessor<'a> {
    pub fn pred(&self) -> Option<HeartbeatAgg<'a>> {
        if self.has_prev == 0 {
            None
        } else {
            Some(self.prev.clone().into())
        }
    }
}

pg_type! {
    struct HeartbeatTrimToAccessor {
        start : i64,
        end : i64,
    }
}

ron_inout_funcs!(HeartbeatTrimToAccessor);

// Note that this is unable to take only a duration, as we don't have the functionality to store
// an interval in PG format and are unable to convert it to an int without a reference time.
// This is a difference from the inline function.
#[pg_extern(immutable, parallel_safe, name = "trim_to")]
fn heartbeat_agg_trim_to_accessor(
    start: crate::raw::TimestampTz,
    duration: default!(Option<crate::raw::Interval>, "NULL"),
) -> HeartbeatTrimToAccessor<'static> {
    let end = duration
        .map(|intv| crate::datum_utils::ts_interval_sum_to_ms(&start, &intv))
        .unwrap_or(0);
    let start = i64::from(start);

    crate::build! {
        HeartbeatTrimToAccessor {
            start,
            end,
        }
    }
}
