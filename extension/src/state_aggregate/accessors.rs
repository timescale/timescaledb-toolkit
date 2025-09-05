use crate::{
    datum_utils::interval_to_ms,
    pg_type,
    raw::{Interval, TimestampTz},
    ron_inout_funcs,
    state_aggregate::*,
};

pg_type! {
    struct AccessorInterpolatedStateTimeline<'input> {
        start: i64,
        interval: i64,
        prev: StateAggData<'input>,
        prev_present: bool,
    }
}
ron_inout_funcs!(AccessorInterpolatedStateTimeline<'input>);

#[pg_extern(immutable, parallel_safe, name = "interpolated_state_timeline")]
fn accessor_state_agg_interpolated_interpolated_state_timeline<'a>(
    start: TimestampTz,
    interval: Interval,
    prev: Option<StateAgg<'a>>,
) -> AccessorInterpolatedStateTimeline<'a> {
    crate::build! {
        AccessorInterpolatedStateTimeline {
            interval: interval_to_ms(&start, &interval),
            start: start.into(),
            prev_present: prev.is_some(),
            prev: prev.unwrap_or_else(|| StateAgg::empty(false)).0,
        }
    }
}

pg_type! {
    struct AccessorInterpolatedStateIntTimeline<'input> {
        start: i64,
        interval: i64,
        prev: StateAggData<'input>,
        prev_present: bool,
    }
}
ron_inout_funcs!(AccessorInterpolatedStateIntTimeline<'input>);

#[pg_extern(immutable, parallel_safe, name = "interpolated_state_int_timeline")]
fn accessor_state_agg_interpolated_interpolated_state_int_timeline<'a>(
    start: TimestampTz,
    interval: Interval,
    prev: Option<StateAgg<'a>>,
) -> AccessorInterpolatedStateIntTimeline<'a> {
    crate::build! {
        AccessorInterpolatedStateIntTimeline {
            interval: interval_to_ms(&start, &interval),
            start: start.into(),
            prev_present: prev.is_some(),
            prev: prev.unwrap_or_else(|| StateAgg::empty(false)).0,
        }
    }
}

// weird ordering is needed for alignment
pg_type! {
    struct AccessorInterpolatedDurationIn<'input> {
        start: i64,
        interval: i64,
        state_len: u32,
        padding_2: [u8; 4],
        prev: StateAggData<'input>,
        state_bytes: [u8; self.state_len],
        prev_present: bool,
    }
}
ron_inout_funcs!(AccessorInterpolatedDurationIn<'input>);
pg_type! {
    struct AccessorInterpolatedDurationInInt<'input> {
        start: i64,
        interval: i64,
        state: i64,
        prev_present: bool,
        padding_2: [u8; 7],
        prev: StateAggData<'input>,
    }
}
ron_inout_funcs!(AccessorInterpolatedDurationInInt<'input>);

#[pg_extern(immutable, parallel_safe, name = "interpolated_duration_in")]
fn accessor_state_agg_interpolated_interpolated_duration_in<'a>(
    state: String,
    start: TimestampTz,
    interval: Interval,
    prev: Option<StateAgg<'a>>,
) -> AccessorInterpolatedDurationIn<'a> {
    crate::build! {
        AccessorInterpolatedDurationIn {
            state_len: state.len().try_into().unwrap(),
            state_bytes: state.into_bytes().into(),
            interval: interval_to_ms(&start, &interval),
            start: start.into(),
            prev_present: prev.is_some(),
            prev: prev.unwrap_or_else(|| StateAgg::empty(false)).0,
            padding_2: Default::default(),
        }
    }
}
#[pg_extern(immutable, parallel_safe, name = "interpolated_duration_in")]
fn accessor_state_agg_interpolated_interpolated_duration_in_int<'a>(
    state: i64,
    start: TimestampTz,
    interval: Interval,
    prev: Option<StateAgg<'a>>,
) -> AccessorInterpolatedDurationInInt<'a> {
    crate::build! {
        AccessorInterpolatedDurationInInt {
            state,
            interval: interval_to_ms(&start, &interval),
            start: start.into(),
            prev_present: prev.is_some(),
            prev: prev.unwrap_or_else(|| StateAgg::empty(false)).0,
            padding_2: Default::default(),
        }
    }
}

// weird ordering is needed for alignment
pg_type! {
    struct AccessorInterpolatedStatePeriods<'input> {
        start: i64,
        interval: i64,
        state_len: u32,
        padding_2: [u8; 4],
        prev: StateAggData<'input>,
        state_bytes: [u8; self.state_len],
        prev_present: bool,
    }
}
ron_inout_funcs!(AccessorInterpolatedStatePeriods<'input>);
pg_type! {
    struct AccessorInterpolatedStatePeriodsInt<'input> {
        start: i64,
        interval: i64,
        state: i64,
        prev_present: bool,
        padding_2: [u8; 7],
        prev: StateAggData<'input>,
    }
}
ron_inout_funcs!(AccessorInterpolatedStatePeriodsInt<'input>);

#[pg_extern(immutable, parallel_safe, name = "interpolated_state_periods")]
fn accessor_state_agg_interpolated_interpolated_state_periods<'a>(
    state: String,
    start: TimestampTz,
    interval: Interval,
    prev: Option<StateAgg<'a>>,
) -> AccessorInterpolatedStatePeriods<'a> {
    crate::build! {
        AccessorInterpolatedStatePeriods {
            state_len: state.len().try_into().unwrap(),
            state_bytes: state.into_bytes().into(),
            interval: interval_to_ms(&start, &interval),
            start: start.into(),
            prev_present: prev.is_some(),
            prev: prev.unwrap_or_else(|| StateAgg::empty(false)).0,
            padding_2: Default::default(),
        }
    }
}
#[pg_extern(immutable, parallel_safe, name = "interpolated_state_periods")]
fn accessor_state_agg_interpolated_interpolated_state_periods_int<'a>(
    state: i64,
    start: TimestampTz,
    interval: Interval,
    prev: Option<StateAgg<'a>>,
) -> AccessorInterpolatedStatePeriodsInt<'a> {
    crate::build! {
        AccessorInterpolatedStatePeriodsInt {
            state,
            interval: interval_to_ms(&start, &interval),
            start: start.into(),
            prev_present: prev.is_some(),
            prev: prev.unwrap_or_else(|| StateAgg::empty(false)).0,
            padding_2: Default::default(),
        }
    }
}

pg_type! {
    struct AccessorDurationIn<'input> {
        state_len: u32,
        state_bytes: [u8; self.state_len],
    }
}
ron_inout_funcs!(AccessorDurationIn<'input>);
pg_type! {
    struct AccessorDurationInInt {
        state: i64,
    }
}
ron_inout_funcs!(AccessorDurationInInt);

#[pg_extern(immutable, parallel_safe, name = "duration_in")]
fn accessor_state_agg_duration_in(state: String) -> AccessorDurationIn<'static> {
    crate::build! {
        AccessorDurationIn {
            state_len: state.len().try_into().unwrap(),
            state_bytes: state.into_bytes().into(),
        }
    }
}
#[pg_extern(immutable, parallel_safe, name = "duration_in")]
fn accessor_state_agg_duration_in_int(state: i64) -> AccessorDurationInInt {
    crate::build! {
        AccessorDurationInInt {
            state,
        }
    }
}

pg_type! {
    struct AccessorStatePeriods<'input> {
        state_len: u32,
        state_bytes: [u8; self.state_len],
    }
}
ron_inout_funcs!(AccessorStatePeriods<'input>);
pg_type! {
    struct AccessorStatePeriodsInt {
        state: i64,
    }
}
ron_inout_funcs!(AccessorStatePeriodsInt);

#[pg_extern(immutable, parallel_safe, name = "state_periods")]
fn accessor_state_agg_state_periods<'a>(state: String) -> AccessorStatePeriods<'static> {
    crate::build! {
        AccessorStatePeriods {
            state_len: state.len().try_into().unwrap(),
            state_bytes: state.into_bytes().into(),
        }
    }
}
#[pg_extern(immutable, parallel_safe, name = "state_periods")]
fn accessor_state_agg_state_periods_int(state: i64) -> AccessorStatePeriodsInt {
    crate::build! {
        AccessorStatePeriodsInt {
            state,
        }
    }
}

pg_type! {
    struct AccessorDurationInRange<'input> {
        state_len: u32,
        padding_2: [u8; 4],
        start: i64,
        interval: i64,
        state_bytes: [u8; self.state_len],
    }
}
ron_inout_funcs!(AccessorDurationInRange<'input>);
pg_type! {
    struct AccessorDurationInRangeInt {
        state: i64,
        start: i64,
        interval: i64,
    }
}
ron_inout_funcs!(AccessorDurationInRangeInt);

#[pg_extern(immutable, parallel_safe, name = "duration_in")]
fn accessor_state_agg_duration_in_range(
    state: String,
    start: TimestampTz,
    interval: default!(Option<crate::raw::Interval>, "NULL"),
) -> AccessorDurationInRange<'static> {
    let interval = interval
        .map(|interval| crate::datum_utils::interval_to_ms(&start, &interval))
        .unwrap_or(NO_INTERVAL_MARKER);
    let start = start.into();
    crate::build! {
        AccessorDurationInRange {
            state_len: state.len().try_into().unwrap(),
            state_bytes: state.into_bytes().into(),
            padding_2: [0; 4],
            start, interval
        }
    }
}
#[pg_extern(immutable, parallel_safe, name = "duration_in")]
fn accessor_state_agg_duration_in_range_int(
    state: i64,
    start: TimestampTz,
    interval: default!(Option<crate::raw::Interval>, "NULL"),
) -> AccessorDurationInRangeInt {
    let interval = interval
        .map(|interval| crate::datum_utils::interval_to_ms(&start, &interval))
        .unwrap_or(NO_INTERVAL_MARKER);
    let start = start.into();
    crate::build! {
        AccessorDurationInRangeInt {
            state,
            start, interval
        }
    }
}

pg_type! {
    struct AccessorStateAt {
        time: i64,
    }
}
ron_inout_funcs!(AccessorStateAt);

#[pg_extern(immutable, parallel_safe, name = "state_at")]
fn accessor_state_agg_state_at(time: TimestampTz) -> AccessorStateAt {
    crate::build! {
        AccessorStateAt {
            time: time.into(),
        }
    }
}

pg_type! {
    struct AccessorStateAtInt {
        time: i64,
    }
}
ron_inout_funcs!(AccessorStateAtInt);

#[pg_extern(immutable, parallel_safe, name = "state_at_int")]
fn accessor_state_agg_state_at_int(time: TimestampTz) -> AccessorStateAtInt {
    crate::build! {
        AccessorStateAtInt {
            time: time.into(),
        }
    }
}
