use pgx::*;

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
