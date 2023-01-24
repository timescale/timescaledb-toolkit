//! SELECT duration_in('STOPPED', states) as run_time, duration_in('ERROR', states) as error_time FROM (
//!   SELECT state_agg(time, state) as states FROM ...
//! );
//!
//! Currently requires loading all data into memory in order to sort it by time.

#![allow(non_camel_case_types)]

use pgx::{iter::TableIterator, *};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use aggregate_builder::aggregate;
use flat_serialize::*;
use flat_serialize_macro::FlatSerializable;

use crate::{
    flatten,
    palloc::{Inner, Internal},
    pg_type,
    raw::{bytea, TimestampTz},
    ron_inout_funcs,
};

use toolkit_experimental::{StateAgg, TimelineAgg};

pub mod rollup;

/// The data of a state.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[repr(C)]
enum MaterializedState {
    String(String),
    Integer(i64),
}
impl MaterializedState {
    fn entry(&self, states: &mut String) -> StateEntry {
        match self {
            Self::Integer(i) => StateEntry { a: i64::MAX, b: *i },
            Self::String(s) => StateEntry::from_str(states, s),
        }
    }
    fn existing_entry(&self, states: &str) -> StateEntry {
        match self {
            Self::Integer(i) => StateEntry { a: i64::MAX, b: *i },
            Self::String(s) => StateEntry::from_existing_str(states, s),
        }
    }
    fn try_existing_entry(&self, states: &str) -> Option<StateEntry> {
        Some(match self {
            Self::Integer(i) => StateEntry { a: i64::MAX, b: *i },
            Self::String(s) => StateEntry::try_from_existing_str(states, s)?,
        })
    }
}

/// A stored state entry. Needs a `states` string to be interpreted.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, FlatSerializable, Serialize, Deserialize,
)]
#[repr(C)]
pub struct StateEntry {
    a: i64,
    b: i64,
}
impl StateEntry {
    fn from_integer(int: i64) -> Self {
        Self {
            a: i64::MAX,
            b: int,
        }
    }
    fn from_str(states: &mut String, new_state: &str) -> Self {
        let (a, b) = if let Some(bounds) = states
            .find(new_state)
            .map(|idx| (idx as i64, (idx + new_state.len()) as i64))
        {
            bounds
        } else {
            let bounds = (states.len() as i64, (states.len() + new_state.len()) as i64);
            states.push_str(new_state);
            bounds
        };
        Self { a, b }
    }
    fn from_existing_str(states: &str, state: &str) -> Self {
        if let Some(val) = Self::try_from_existing_str(states, state) {
            val
        } else {
            panic!("Tried to get state that doesn't exist: {}", state)
        }
    }
    fn try_from_existing_str(states: &str, state: &str) -> Option<Self> {
        states
            .find(state)
            .map(|idx| (idx as i64, (idx + state.len()) as i64))
            .map(|bounds| Self {
                a: bounds.0,
                b: bounds.1,
            })
    }

    fn materialize(&self, states: &str) -> MaterializedState {
        if self.a == i64::MAX {
            MaterializedState::Integer(self.b)
        } else {
            MaterializedState::String(states[self.a as usize..self.b as usize].to_string())
        }
    }

    fn as_str(self, states: &str) -> &str {
        assert!(self.a != i64::MAX, "Tried to get non-string state");
        &states[self.a as usize..self.b as usize]
    }

    fn as_integer(self) -> i64 {
        assert!(self.a == i64::MAX, "Tried to get non-integer state");
        self.b
    }
}

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct StateAgg<'input> {
            states_len: u64, // TODO JOSH this and durations_len can be 32
            durations_len: u64,
            durations: [DurationInState; self.durations_len],
            combined_durations_len: u64,
            combined_durations: [TimeInState; self.combined_durations_len],
            first_time: i64,
            last_time: i64,
            first_state: u32,
            last_state: u32,  // first/last state are idx into durations, keep together for alignment
            states: [u8; self.states_len],
            from_timeline_agg: bool,
            integer_states: bool,
        }
    }

    pg_type! {
        #[derive(Debug)]
        struct TimelineAgg<'input> {
            state_agg: StateAggData<'input>,
        }
    }

    impl StateAgg<'_> {
        pub(super) fn empty(from_timeline_agg: bool, integer_states: bool) -> Self {
            unsafe {
                flatten!(StateAgg {
                    states_len: 0,
                    states: Slice::Slice(&[]),
                    durations_len: 0,
                    durations: Slice::Slice(&[]),
                    combined_durations: Slice::Slice(&[]),
                    combined_durations_len: 0,
                    first_time: 0,
                    last_time: 0,
                    first_state: 0,
                    last_state: 0,
                    from_timeline_agg,
                    integer_states,
                })
            }
        }

        pub(super) fn new(
            states: String,
            durations: Vec<DurationInState>,
            first: Option<Record>,
            last: Option<Record>,
            combined_durations: Option<Vec<TimeInState>>,
            integer_states: bool,
        ) -> Self {
            let from_timeline_agg = combined_durations.is_some();
            if durations.is_empty() {
                assert!(
                    first.is_none()
                        && last.is_none()
                        && states.is_empty()
                        && combined_durations.map(|v| v.is_empty()).unwrap_or(true)
                );

                return Self::empty(from_timeline_agg, integer_states);
            }

            assert!(first.is_some() && last.is_some());
            let first = first.unwrap();
            let last = last.unwrap();
            let states_len = states.len() as u64;
            let durations_len = durations.len() as u64;
            let mut first_state = durations.len();
            let mut last_state = durations.len();

            // Find first and last state
            for (i, d) in durations.iter().enumerate() {
                let s = d.state.materialize(&states);
                if s == first.state {
                    first_state = i;
                    if last_state < durations.len() {
                        break;
                    }
                }
                if s == last.state {
                    last_state = i;
                    if first_state < durations.len() {
                        break;
                    }
                }
            }
            assert!(first_state < durations.len() && last_state < durations.len());

            let combined_durations = combined_durations.unwrap_or_default();

            unsafe {
                flatten!(StateAgg {
                    states_len,
                    states: states.into_bytes().into(),
                    durations_len,
                    durations: (&*durations).into(),
                    combined_durations: (&*combined_durations).into(),
                    combined_durations_len: combined_durations.len() as u64,
                    first_time: first.time,
                    last_time: last.time,
                    first_state: first_state as u32,
                    last_state: last_state as u32,
                    from_timeline_agg,
                    integer_states,
                })
            }
        }

        pub fn get(&self, state: StateEntry) -> Option<i64> {
            let materialized = state.materialize(self.states_as_str());
            for record in self.durations.iter() {
                if record.state.materialize(self.states_as_str()) == materialized {
                    return Some(record.duration);
                }
            }
            None
        }

        pub(super) fn states_as_str(&self) -> &str {
            let states: &[u8] = self.states.as_slice();
            // SAFETY: came from a String in `new` a few lines up
            unsafe { std::str::from_utf8_unchecked(states) }
        }

        pub(super) fn interpolate(
            &self,
            interval_start: i64,
            interval_len: i64,
            prev: Option<StateAgg>,
        ) -> StateAgg {
            if self.durations.is_empty() {
                pgx::error!("unable to interpolate interval on state aggregate with no data");
            }
            if let Some(ref prev) = prev {
                assert_eq!(
                    prev.integer_states, self.integer_states,
                    "can't interpolate between aggs with different state types"
                );
            }

            let mut states = std::str::from_utf8(self.states.as_slice())
                .unwrap()
                .to_string();
            let mut durations: Vec<DurationInState> = self.durations.iter().collect();

            let mut combined_durations = if self.from_timeline_agg {
                Some(self.combined_durations.iter().collect::<Vec<_>>())
            } else {
                None
            };

            let first = match prev {
                Some(prev) if interval_start < self.first_time => {
                    if prev.last_state < prev.durations.len() as u32 {
                        let start_interval = self.first_time - interval_start;
                        let start_state = &prev.durations.as_slice()[prev.last_state as usize]
                            .state
                            .materialize(prev.states_as_str());

                        // update durations
                        let state = match durations
                            .iter_mut()
                            .find(|x| x.state.materialize(&states) == *start_state)
                        {
                            Some(dis) => {
                                dis.duration += start_interval;
                                dis.state
                            }
                            None => {
                                let state = start_state.entry(&mut states);
                                durations.push(DurationInState {
                                    duration: start_interval,
                                    state,
                                });
                                state
                            }
                        };

                        // update combined_durations
                        if let Some(combined_durations) = combined_durations.as_mut() {
                            // extend last duration
                            let first_cd = combined_durations
                                .first_mut()
                                .expect("poorly formed TimelineAgg, length mismatch");
                            let first_cd_state = first_cd.state.materialize(&states);
                            if first_cd_state == *start_state {
                                first_cd.start_time -= start_interval;
                            } else {
                                combined_durations.insert(
                                    0,
                                    TimeInState {
                                        start_time: interval_start,
                                        end_time: self.first_time,
                                        state,
                                    },
                                );
                            };
                        };

                        Record {
                            state: start_state.clone(),
                            time: interval_start,
                        }
                    } else {
                        pgx::error!("unable to interpolate interval on state aggregate where previous agg has no data")
                    }
                }
                _ => Record {
                    state: self.durations.as_slice()[self.first_state as usize]
                        .state
                        .materialize(&states),
                    time: self.first_time,
                },
            };

            let last = if interval_start + interval_len > self.last_time {
                let last_interval = interval_start + interval_len - self.last_time;
                match durations.get_mut(self.last_state as usize) {
                    None => pgx::error!("poorly formed StateAgg, last_state out of starts"),
                    Some(dis) => {
                        dis.duration += last_interval;
                        if let Some(combined_durations) = combined_durations.as_mut() {
                            // extend last duration
                            combined_durations
                                .last_mut()
                                .expect("poorly formed TimelineAgg, length mismatch")
                                .end_time += last_interval;
                        };
                        Record {
                            state: dis.state.materialize(&states),
                            time: interval_start + interval_len,
                        }
                    }
                }
            } else {
                Record {
                    state: self.durations.as_slice()[self.last_state as usize]
                        .state
                        .materialize(&states),
                    time: self.last_time,
                }
            };

            StateAgg::new(
                states,
                durations,
                Some(first),
                Some(last),
                combined_durations,
                self.integer_states,
            )
        }

        pub fn assert_int<'a>(&self) {
            assert!(
                self.0.integer_states,
                "Expected integer state, found string state"
            );
        }
        pub fn assert_str<'a>(&self) {
            assert!(
                !self.0.integer_states,
                "Expected string state, found integer state"
            );
        }
    }

    impl<'input> TimelineAgg<'input> {
        pub fn new(state_agg: StateAgg) -> Self {
            unsafe {
                flatten!(TimelineAgg {
                    state_agg: state_agg.0,
                })
            }
        }

        pub fn as_state_agg(self) -> StateAgg<'input> {
            unsafe { self.0.state_agg.flatten() }
        }

        pub fn assert_int<'a>(&self) {
            assert!(
                self.0.state_agg.integer_states,
                "State must have integer values for this function"
            );
        }
        pub fn assert_str<'a>(&self) {
            assert!(
                !self.0.state_agg.integer_states,
                "State must have string values for this function"
            );
        }
    }

    ron_inout_funcs!(StateAgg);
    ron_inout_funcs!(TimelineAgg);
}

fn state_trans_inner(
    state: Option<StateAggTransState>,
    ts: TimestampTz,
    value: Option<MaterializedState>,
    integer_states: bool,
) -> Option<StateAggTransState> {
    let value = match value {
        None => return state,
        Some(value) => value,
    };
    let mut state = state.unwrap_or_else(|| StateAggTransState::new(integer_states));
    state.record(value, ts.into());
    Some(state)
}
#[aggregate]
impl toolkit_experimental::state_agg {
    type State = StateAggTransState;

    const PARALLEL_SAFE: bool = true;

    fn transition(
        state: Option<State>,
        #[sql_type("timestamptz")] ts: TimestampTz,
        #[sql_type("text")] value: Option<String>,
    ) -> Option<State> {
        state_trans_inner(state, ts, value.map(MaterializedState::String), false)
    }

    fn combine(a: Option<&State>, b: Option<&State>) -> Option<State> {
        match (a, b) {
            (None, None) => None,
            (None, Some(only)) | (Some(only), None) => Some(only.clone()),
            (Some(a), Some(b)) => {
                let (mut a, mut b) = (a.clone(), b.clone());
                a.append(&mut b);
                Some(a)
            }
        }
    }

    fn serialize(state: &mut State) -> bytea {
        crate::do_serialize!(state)
    }

    fn deserialize(bytes: bytea) -> State {
        crate::do_deserialize!(bytes, StateAggTransState)
    }

    fn finally(state: Option<&mut State>) -> Option<StateAgg<'static>> {
        state.map(|s| {
            let mut states = String::new();
            let mut durations: Vec<DurationInState> = vec![];
            let (map, first, last) = s.make_duration_map_and_bounds();
            for (state, duration) in map {
                durations.push(DurationInState {
                    duration,
                    state: state.entry(&mut states),
                });
            }
            StateAgg::new(states, durations, first, last, None, s.integer_states)
        })
    }
}

extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.state_agg(
        ts timestamptz,
        value bigint
    ) (
        stype = internal,
        sfunc = toolkit_experimental.state_agg_int_trans,
        finalfunc = toolkit_experimental.state_agg_finally_fn_outer,
        parallel = safe,
        serialfunc = toolkit_experimental.state_agg_serialize_fn_outer,
        deserialfunc = toolkit_experimental.state_agg_deserialize_fn_outer,
        combinefunc = toolkit_experimental.state_agg_combine_fn_outer
    );",
    name = "state_agg_bigint",
    requires = [
        state_agg_int_trans,
        state_agg_finally_fn_outer,
        state_agg_serialize_fn_outer,
        state_agg_deserialize_fn_outer,
        state_agg_combine_fn_outer
    ],
);
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn state_agg_int_trans(
    __inner: pgx::Internal,
    ts: TimestampTz,
    value: Option<i64>,
    __fcinfo: pg_sys::FunctionCallInfo,
) -> Option<pgx::Internal> {
    // expanded from #[aggregate] transition function
    use crate::palloc::{Inner, InternalAsValue, ToInternal};
    type State = StateAggTransState;
    unsafe {
        let mut __inner: Option<Inner<Option<State>>> = __inner.to_inner();
        let inner: Option<State> = match &mut __inner {
            None => None,
            Some(inner) => Option::take(&mut **inner),
        };
        let state: Option<State> = inner;
        crate::aggregate_utils::in_aggregate_context(__fcinfo, || {
            let result = state_trans_inner(state, ts, value.map(MaterializedState::Integer), true);
            let state: Option<State> = result;
            __inner = match (__inner, state) {
                (None, None) => None,
                (None, state @ Some(..)) => Some(state.into()),
                (Some(mut inner), state) => {
                    *inner = state;
                    Some(inner)
                }
            };
            __inner.internal()
        })
    }
}

#[aggregate]
impl toolkit_experimental::timeline_agg {
    type State = StateAggTransState;

    const PARALLEL_SAFE: bool = true;

    fn transition(
        state: Option<State>,
        #[sql_type("timestamptz")] ts: TimestampTz,
        #[sql_type("text")] value: Option<String>,
    ) -> Option<State> {
        state_agg::transition(state, ts, value)
    }

    fn combine(a: Option<&State>, b: Option<&State>) -> Option<State> {
        state_agg::combine(a, b)
    }

    fn serialize(state: &mut State) -> bytea {
        state_agg::serialize(state)
    }

    fn deserialize(bytes: bytea) -> State {
        state_agg::deserialize(bytes)
    }

    fn finally(state: Option<&mut State>) -> Option<TimelineAgg<'static>> {
        state.map(|s| {
            let mut states = String::new();
            let mut durations: Vec<DurationInState> = vec![];
            let (map, first, last) = s.make_duration_map_and_bounds();
            for (state, duration) in map {
                let state = state.entry(&mut states);
                durations.push(DurationInState { duration, state });
            }

            let mut merged_durations: Vec<TimeInState> = Vec::new();
            let mut last_record_state = None;
            for record in s.records.drain(..) {
                if last_record_state
                    .clone()
                    .map(|last| last != record.state)
                    .unwrap_or(true)
                {
                    if let Some(prev) = merged_durations.last_mut() {
                        prev.end_time = record.time;
                    }
                    merged_durations.push(TimeInState {
                        start_time: record.time,
                        end_time: 0,
                        state: record.state.entry(&mut states),
                    });
                    last_record_state = Some(record.state);
                }
            }
            if let Some(last_time_in_state) = merged_durations.last_mut() {
                last_time_in_state.end_time = last.as_ref().unwrap().time;
            }

            TimelineAgg::new(StateAgg::new(
                states,
                durations,
                first,
                last,
                Some(merged_durations),
                s.integer_states,
            ))
        })
    }
}

extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.timeline_agg(
        ts timestamptz,
        value bigint
    ) (
        stype = internal,
        sfunc = toolkit_experimental.timeline_agg_int_trans,
        finalfunc = toolkit_experimental.timeline_agg_finally_fn_outer,
        parallel = safe,
        serialfunc = toolkit_experimental.timeline_agg_serialize_fn_outer,
        deserialfunc = toolkit_experimental.timeline_agg_deserialize_fn_outer,
        combinefunc = toolkit_experimental.timeline_agg_combine_fn_outer
    );",
    name = "timeline_agg_bigint",
    requires = [
        timeline_agg_int_trans,
        timeline_agg_finally_fn_outer,
        timeline_agg_serialize_fn_outer,
        timeline_agg_deserialize_fn_outer,
        timeline_agg_combine_fn_outer
    ],
);
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn timeline_agg_int_trans(
    __inner: pgx::Internal,
    ts: TimestampTz,
    value: Option<i64>,
    __fcinfo: pg_sys::FunctionCallInfo,
) -> Option<pgx::Internal> {
    // expanded from #[aggregate] transition function
    use crate::palloc::{Inner, InternalAsValue, ToInternal};
    type State = StateAggTransState;
    unsafe {
        let mut __inner: Option<Inner<Option<State>>> = __inner.to_inner();
        let inner: Option<State> = match &mut __inner {
            None => None,
            Some(inner) => Option::take(&mut **inner),
        };
        let state: Option<State> = inner;
        crate::aggregate_utils::in_aggregate_context(__fcinfo, || {
            let result = state_trans_inner(state, ts, value.map(MaterializedState::Integer), true);
            let state: Option<State> = result;
            __inner = match (__inner, state) {
                (None, None) => None,
                (None, state @ Some(..)) => Some(state.into()),
                (Some(mut inner), state) => {
                    *inner = state;
                    Some(inner)
                }
            };
            __inner.internal()
        })
    }
}

// Intermediate state kept in postgres.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StateAggTransState {
    records: Vec<Record>,
    integer_states: bool,
}

impl StateAggTransState {
    fn new(integer_states: bool) -> Self {
        Self {
            records: vec![],
            integer_states,
        }
    }

    fn record(&mut self, state: MaterializedState, time: i64) {
        self.records.push(Record { state, time });
    }

    fn append(&mut self, other: &mut Self) {
        self.records.append(&mut other.records)
    }

    fn sort_records(&mut self) {
        self.records.sort_by(|a, b| {
            if a.time == b.time {
                // TODO JOSH do we care about instantaneous state changes?
                //           an alternative is to drop duplicate timestamps
                if a.state != b.state {
                    // TODO use human-readable timestamp
                    panic!(
                        "state cannot be both {:?} and {:?} at {}",
                        a.state, b.state, a.time
                    )
                }
                std::cmp::Ordering::Equal
            } else {
                a.time.cmp(&b.time)
            }
        });
    }

    /// Use accumulated state, sort, and return tuple of map of states to durations along with first and last record.
    fn make_duration_map_and_bounds(
        &mut self,
    ) -> (
        std::collections::HashMap<MaterializedState, i64>,
        Option<Record>,
        Option<Record>,
    ) {
        self.sort_records();
        let (first, last) = (self.records.first(), self.records.last());
        let first = first.cloned();
        let last = last.cloned();
        let mut duration_state = DurationState::new();
        for record in &self.records {
            duration_state.handle_record(record.state.clone(), record.time);
        }
        duration_state.finalize();
        // TODO BRIAN sort this by decreasing duration will make it easier to implement a TopN states
        (duration_state.durations, first, last)
    }
}

fn duration_in_inner<'a>(
    state: Option<StateEntry>,
    aggregate: Option<StateAgg<'a>>,
    range: Option<(TimestampTz, TimestampTz)>,
) -> crate::raw::Interval {
    let time: i64 = if let Some((start, end)) = range {
        let (start, end) = (start.into(), end.into());
        assert!(end >= start, "End time must be after start time");
        if let (Some(state), Some(agg)) = (state, aggregate) {
            let state = state.materialize(agg.states_as_str());
            let mut total = 0;
            for tis in agg.combined_durations.iter() {
                if tis.state.materialize(agg.states_as_str()) == state {
                    let tis_start_time = i64::max(tis.start_time, start);
                    let tis_end_time = i64::min(tis.end_time, end);
                    if tis_end_time >= start && tis_start_time <= end {
                        let amount = tis_end_time - tis_start_time;
                        assert!(amount >= 0, "incorrectly ordered times");
                        total += amount;
                    }
                }
            }
            total
        } else {
            0
        }
    } else {
        state.and_then(|state| aggregate?.get(state)).unwrap_or(0)
    };
    let interval = pg_sys::Interval {
        time,
        ..Default::default()
    };
    let interval: *const pg_sys::Interval = to_palloc(interval);
    // Now we have a valid Interval in at least one sense.  But we have the
    // microseconds in the `time` field and `day` and `month` are both 0,
    // which is legal.  However, directly converting one of these to TEXT
    // comes out quite ugly if the number of microseconds is greater than 1 day:
    //   8760:02:00
    // Should be:
    //   365 days 00:02:00
    // How does postgresql do it?  It happens in src/backend/utils/adt/timestamp.c:timestamp_mi:
    //  result->time = dt1 - dt2;
    //  result = DatumGetIntervalP(DirectFunctionCall1(interval_justify_hours,
    //                                                 IntervalPGetDatum(result)));
    // So if we want the same behavior, we need to call interval_justify_hours too:
    let function_args = vec![Some(pg_sys::Datum::from(interval))];
    unsafe { pgx::direct_function_call(pg_sys::interval_justify_hours, function_args) }
        .expect("interval_justify_hours does not return None")
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn duration_in<'a>(state: String, aggregate: Option<StateAgg<'a>>) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_str()
    };
    duration_in_inner(
        aggregate.as_ref().and_then(|aggregate| {
            StateEntry::try_from_existing_str(aggregate.states_as_str(), &state)
        }),
        aggregate,
        None,
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_int<'a>(state: i64, aggregate: Option<StateAgg<'a>>) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_int()
    };
    duration_in_inner(Some(StateEntry::from_integer(state)), aggregate, None)
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_tl<'a>(
    state: String,
    aggregate: Option<TimelineAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_str()
    };
    duration_in(state, aggregate.map(TimelineAgg::as_state_agg))
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_tl_int<'a>(
    state: i64,
    aggregate: Option<TimelineAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_int()
    };
    duration_in_inner(
        Some(StateEntry::from_integer(state)),
        aggregate.map(TimelineAgg::as_state_agg),
        None,
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_range<'a>(
    state: String,
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    end: default!(TimestampTz, "'infinity'"),
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_str()
    };
    let aggregate = aggregate.map(TimelineAgg::as_state_agg);
    duration_in_inner(
        aggregate.as_ref().and_then(|aggregate| {
            StateEntry::try_from_existing_str(aggregate.states_as_str(), &state)
        }),
        aggregate,
        Some((start, end)),
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_range_int<'a>(
    state: i64,
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    end: default!(TimestampTz, "'infinity'"),
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_int()
    };
    duration_in_inner(
        Some(StateEntry::from_integer(state)),
        aggregate.map(TimelineAgg::as_state_agg),
        Some((start, end)),
    )
}

fn interpolated_duration_in_inner<'a>(
    state: Option<MaterializedState>,
    aggregate: Option<StateAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> crate::raw::Interval {
    match aggregate {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(aggregate) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            let new_agg = aggregate.interpolate(start.into(), interval, prev);
            let state_entry =
                state.and_then(|state| state.try_existing_entry(new_agg.states_as_str()));
            duration_in_inner(state_entry, Some(new_agg), None)
        }
    }
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_duration_in<'a>(
    state: String,
    aggregate: Option<StateAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_str()
    };
    interpolated_duration_in_inner(
        Some(MaterializedState::String(state)),
        aggregate,
        start,
        interval,
        prev,
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "interpolated_duration_in",
    schema = "toolkit_experimental"
)]
pub fn interpolated_duration_in_tl<'a>(
    state: String,
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimelineAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_str()
    };
    interpolated_duration_in(
        state,
        aggregate.map(TimelineAgg::as_state_agg),
        start,
        interval,
        prev.map(TimelineAgg::as_state_agg),
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    schema = "toolkit_experimental",
    name = "interpolated_duration_in"
)]
pub fn interpolated_duration_in_int<'a>(
    state: i64,
    aggregate: Option<StateAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_int()
    };
    interpolated_duration_in_inner(
        Some(MaterializedState::Integer(state)),
        aggregate,
        start,
        interval,
        prev,
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "interpolated_duration_in",
    schema = "toolkit_experimental"
)]
pub fn interpolated_duration_in_tl_int<'a>(
    state: i64,
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimelineAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_int()
    };
    interpolated_duration_in_int(
        state,
        aggregate.map(TimelineAgg::as_state_agg),
        start,
        interval,
        prev.map(TimelineAgg::as_state_agg),
    )
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn into_values<'a>(
    agg: StateAgg<'a>,
) -> TableIterator<'a, (pgx::name!(state, String), pgx::name!(duration, i64))> {
    agg.assert_str();
    let states: String = agg.states_as_str().to_owned();
    TableIterator::new(
        agg.durations
            .clone()
            .into_iter()
            .map(move |record| (record.state.as_str(&states).to_string(), record.duration)),
    )
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn into_int_values<'a>(
    agg: StateAgg<'a>,
) -> TableIterator<'a, (pgx::name!(state, i64), pgx::name!(duration, i64))> {
    agg.assert_int();
    TableIterator::new(
        agg.durations
            .clone()
            .into_iter()
            .map(move |record| (record.state.as_integer(), record.duration))
            .collect::<Vec<_>>()
            .into_iter(), // make map panic now instead of at iteration time
    )
}
#[pg_extern(
    immutable,
    parallel_safe,
    name = "into_values",
    schema = "toolkit_experimental"
)]
pub fn into_values_tl<'a>(
    aggregate: TimelineAgg<'a>,
) -> TableIterator<'a, (pgx::name!(state, String), pgx::name!(duration, i64))> {
    aggregate.assert_str();
    into_values(aggregate.as_state_agg())
}
#[pg_extern(
    immutable,
    parallel_safe,
    name = "into_int_values",
    schema = "toolkit_experimental"
)]
pub fn into_values_tl_int<'a>(
    aggregate: TimelineAgg<'a>,
) -> TableIterator<'a, (pgx::name!(state, i64), pgx::name!(duration, i64))> {
    aggregate.assert_int();
    into_int_values(aggregate.as_state_agg())
}

fn state_timeline_inner<'a>(
    agg: StateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    assert!(
        agg.from_timeline_agg,
        "state_timeline can only be called on a state_agg built from timeline_agg"
    );
    let states: String = agg.states_as_str().to_owned();
    TableIterator::new(
        agg.combined_durations
            .clone()
            .into_iter()
            .map(move |record| {
                (
                    record.state.as_str(&states).to_string(),
                    TimestampTz::from(record.start_time),
                    TimestampTz::from(record.end_time),
                )
            }),
    )
}
fn state_int_timeline_inner<'a>(
    agg: StateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    assert!(
        agg.from_timeline_agg,
        "state_timeline can only be called on a state_agg built from timeline_agg"
    );
    TableIterator::new(
        agg.combined_durations
            .clone()
            .into_iter()
            .map(move |record| {
                (
                    record.state.as_integer(),
                    TimestampTz::from(record.start_time),
                    TimestampTz::from(record.end_time),
                )
            })
            .collect::<Vec<_>>()
            .into_iter(), // make map panic now instead of at iteration time
    )
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn state_timeline<'a>(
    agg: TimelineAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_str();
    state_timeline_inner(agg.as_state_agg())
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn state_int_timeline<'a>(
    agg: TimelineAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_int();
    state_int_timeline_inner(agg.as_state_agg())
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_state_timeline<'a>(
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimelineAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_str()
    };
    match aggregate {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(aggregate) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            TableIterator::new(
                state_timeline_inner(aggregate.as_state_agg().interpolate(
                    start.into(),
                    interval,
                    prev.map(TimelineAgg::as_state_agg),
                ))
                .collect::<Vec<_>>()
                .into_iter(),
            )
        }
    }
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_int_state_timeline<'a>(
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimelineAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_int()
    };
    match aggregate {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(aggregate) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            TableIterator::new(
                state_int_timeline_inner(aggregate.as_state_agg().interpolate(
                    start.into(),
                    interval,
                    prev.map(TimelineAgg::as_state_agg),
                ))
                .collect::<Vec<_>>()
                .into_iter(),
            )
        }
    }
}

fn state_periods_inner<'a>(
    state: MaterializedState,
    agg: StateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    assert!(
        agg.from_timeline_agg,
        "state_periods can only be called on a state_agg built from timeline_agg"
    );
    let states: String = agg.states_as_str().to_owned();
    TableIterator::new(
        agg.combined_durations
            .clone()
            .into_iter()
            .filter_map(move |record| {
                if record.state.materialize(&states) == state {
                    Some((
                        TimestampTz::from(record.start_time),
                        TimestampTz::from(record.end_time),
                    ))
                } else {
                    None
                }
            }),
    )
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn state_periods<'a>(
    state: String,
    agg: TimelineAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_str();
    let agg = agg.as_state_agg();
    state_periods_inner(MaterializedState::String(state), agg)
}
#[pg_extern(
    immutable,
    parallel_safe,
    schema = "toolkit_experimental",
    name = "state_periods"
)]
pub fn state_int_periods<'a>(
    state: i64,
    agg: TimelineAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_int();
    state_periods_inner(MaterializedState::Integer(state), agg.as_state_agg())
}

fn interpolated_state_periods_inner<'a>(
    state: MaterializedState,
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimelineAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    match aggregate {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(aggregate) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            TableIterator::new(
                state_periods_inner(
                    state,
                    aggregate.as_state_agg().interpolate(
                        start.into(),
                        interval,
                        prev.map(TimelineAgg::as_state_agg),
                    ),
                )
                .collect::<Vec<_>>()
                .into_iter(),
            )
        }
    }
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_state_periods<'a>(
    state: String,
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimelineAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_str()
    };
    interpolated_state_periods_inner(
        MaterializedState::String(state),
        aggregate,
        start,
        interval,
        prev,
    )
}
#[pg_extern(
    immutable,
    parallel_safe,
    schema = "toolkit_experimental",
    name = "interpolated_state_periods"
)]
pub fn interpolated_state_periods_int<'a>(
    state: i64,
    aggregate: Option<TimelineAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<TimelineAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref aggregate) = aggregate {
        aggregate.assert_int()
    };
    interpolated_state_periods_inner(
        MaterializedState::Integer(state),
        aggregate,
        start,
        interval,
        prev,
    )
}

#[derive(Clone, Debug, Deserialize, Eq, FlatSerializable, PartialEq, Serialize)]
#[repr(C)]
pub struct DurationInState {
    duration: i64,
    state: StateEntry,
}

#[derive(Clone, Debug, Deserialize, Eq, FlatSerializable, PartialEq, Serialize)]
#[repr(C)]
pub struct TimeInState {
    start_time: i64,
    end_time: i64,
    state: StateEntry,
}

struct DurationState {
    last_state: Option<(MaterializedState, i64)>,
    durations: std::collections::HashMap<MaterializedState, i64>,
}
impl DurationState {
    fn new() -> Self {
        Self {
            last_state: None,
            durations: std::collections::HashMap::new(),
        }
    }

    fn handle_record(&mut self, state: MaterializedState, time: i64) {
        match self.last_state.take() {
            None => self.last_state = Some((state, time)),
            Some((last_state, last_time)) => {
                debug_assert!(time >= last_time);
                self.last_state = Some((state, time));
                match self.durations.get_mut(&last_state) {
                    None => {
                        self.durations.insert(last_state, time - last_time);
                    }
                    Some(duration) => {
                        let this_duration = time - last_time;
                        let new_duration = *duration + this_duration;
                        *duration = new_duration;
                    }
                }
            }
        }
    }

    // It's possible that our last seen state was unique, in which case we'll have to
    // add a 0 duration entry so that we can handle rollup and interpolation calls
    fn finalize(&mut self) {
        if let Some((last_state, _)) = self.last_state.take() {
            self.durations.entry(last_state).or_insert(0);
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Record {
    state: MaterializedState,
    time: i64,
}

fn to_palloc<T>(value: T) -> *const T {
    unsafe {
        let ptr = pg_sys::palloc(std::mem::size_of::<T>()) as *mut T;
        *ptr = value;
        ptr
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::sync::atomic::Ordering::Relaxed;

    use super::*;
    use pgx_macros::pg_test;

    macro_rules! select_one {
        ($client:expr, $stmt:expr, $type:ty) => {
            $client
                .select($stmt, None, None)
                .first()
                .get_one::<$type>()
                .unwrap()
        };
    }

    #[pg_test]
    fn one_state_one_change() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                None,
                None,
            );
            assert_eq!(
                "365 days 00:02:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days 00:02:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.timeline_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn two_states_two_changes() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                None,
                None,
            );

            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn two_states_three_changes() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-01-01 00:02:00+00', 'one'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                None,
                None,
            );

            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );

            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.timeline_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.timeline_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn out_of_order_times() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:02:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                None,
                None,
            );

            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn same_state_twice() {
        // TODO Do we care?  Could be that states are recorded not only when they change but
        // also at regular intervals even when they don't?
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'one'),
                    ('2020-01-01 00:02:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                None,
                None,
            );
            assert_eq!(
                "00:02:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn duration_in_two_states_two_changes() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                None,
                None,
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn same_state_twice_last() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-01-01 00:02:00+00', 'two')
                "#,
                None,
                None,
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn combine_using_muchos_data() {
        state_agg::counters::reset();
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"
insert into test values ('2020-01-01 00:00:00+00', 'one');
insert into test select '2020-01-02 UTC'::timestamptz + make_interval(days=>v), 'two' from generate_series(1,300000) v;
insert into test select '2020-01-02 UTC'::timestamptz + make_interval(days=>v), 'three' from generate_series(300001,600000) v;
insert into test select '2020-01-02 UTC'::timestamptz + make_interval(days=>v), 'four' from generate_series(600001,900000) v;
                "#,
                None,
                None,
            );
            assert_eq!(
                "2 days",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM test",
                    &str
                )
            );
        });
        assert!(state_agg::counters::COMBINE_NONE.load(Relaxed) == 0); // TODO untested
        assert!(state_agg::counters::COMBINE_A.load(Relaxed) == 0); // TODO untested
        assert!(state_agg::counters::COMBINE_B.load(Relaxed) > 0); // tested
        assert!(state_agg::counters::COMBINE_BOTH.load(Relaxed) > 0); // tested
    }

    // TODO This doesn't work under github actions.  Do we run with multiple
    //   CPUs there?  If not, that would surely make a big difference.
    // TODO use EXPLAIN to figure out how it differs when run under github actions
    // #[pg_test]
    #[allow(dead_code)]
    fn combine_using_settings() {
        state_agg::counters::reset();
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-03 00:00:00+00', 'two')
                "#,
                None,
                None,
            );
            assert_eq!(
                "2 days",
                select_one!(
                    client,
                    r#"
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
SET parallel_leader_participation = off;
SET enable_indexonlyscan = off;
SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state))::TEXT FROM (
    SELECT * FROM test
    UNION ALL SELECT * FROM test
    UNION ALL SELECT * FROM test
    UNION ALL SELECT * FROM test) u
                "#,
                    &str
                )
            );
        });
        assert!(state_agg::counters::COMBINE_NONE.load(Relaxed) == 0); // TODO untested
        assert!(state_agg::counters::COMBINE_A.load(Relaxed) == 0); // TODO untested
        assert!(state_agg::counters::COMBINE_B.load(Relaxed) > 0); // tested
        assert!(state_agg::counters::COMBINE_BOTH.load(Relaxed) > 0); // tested
    }

    // the sample query from the ticket
    #[pg_test]
    fn sample_query() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'START'),
                    ('2020-01-01 00:01:00+00', 'ERROR'),
                    ('2020-01-01 00:02:00+00', 'STOPPED')"#,
                None,
                None,
            );
            assert_eq!(
                client
                    .select(
                        r#"SELECT toolkit_experimental.duration_in('ERROR', states)::TEXT as error,
                                  toolkit_experimental.duration_in('START', states)::TEXT as start,
                                  toolkit_experimental.duration_in('STOPPED', states)::TEXT as stopped
                             FROM (SELECT toolkit_experimental.state_agg(ts, state) as states FROM test) as foo"#,
                        None,
                        None,
                    )
                    .first()
                    .get_three::<&str, &str, &str>(),
                (Some("00:01:00"), Some("00:01:00"), Some("00:00:00"))
            );
            assert_eq!(
                client
                    .select(
                        r#"SELECT toolkit_experimental.duration_in('ERROR', states)::TEXT as error,
                                  toolkit_experimental.duration_in('START', states)::TEXT as start,
                                  toolkit_experimental.duration_in('STOPPED', states)::TEXT as stopped
                             FROM (SELECT toolkit_experimental.timeline_agg(ts, state) as states FROM test) as foo"#,
                        None,
                        None,
                    )
                    .first()
                    .get_three::<&str, &str, &str>(),
                (Some("00:01:00"), Some("00:01:00"), Some("00:00:00"))
            );
        })
    }

    #[pg_test]
    fn interpolated_duration() {
        Spi::execute(|client| {
            client.select(
                "SET TIME ZONE 'UTC';
                CREATE TABLE inttest(time TIMESTAMPTZ, state TEXT, bucket INT);
                CREATE TABLE inttest2(time TIMESTAMPTZ, state BIGINT, bucket INT);",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO inttest VALUES
                ('2020-1-1 10:00'::timestamptz, 'one', 1),
                ('2020-1-1 12:00'::timestamptz, 'two', 1), 
                ('2020-1-1 16:00'::timestamptz, 'three', 1), 
                ('2020-1-2 2:00'::timestamptz, 'one', 2), 
                ('2020-1-2 12:00'::timestamptz, 'two', 2), 
                ('2020-1-2 20:00'::timestamptz, 'three', 2), 
                ('2020-1-3 10:00'::timestamptz, 'one', 3), 
                ('2020-1-3 12:00'::timestamptz, 'two', 3), 
                ('2020-1-3 16:00'::timestamptz, 'three', 3);
                INSERT INTO inttest2 VALUES
                ('2020-1-1 10:00'::timestamptz, 10001, 1),
                ('2020-1-1 12:00'::timestamptz, 10002, 1), 
                ('2020-1-1 16:00'::timestamptz, 10003, 1), 
                ('2020-1-2 2:00'::timestamptz, 10001, 2), 
                ('2020-1-2 12:00'::timestamptz, 10002, 2), 
                ('2020-1-2 20:00'::timestamptz, 10003, 2), 
                ('2020-1-3 10:00'::timestamptz, 10001, 3), 
                ('2020-1-3 12:00'::timestamptz, 10002, 3), 
                ('2020-1-3 16:00'::timestamptz, 10003, 3);"#,
                None,
                None,
            );

            // Interpolate time spent in state "three" each day
            let mut durations = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_duration_in(
                    'three', 
                    agg, 
                    '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.state_agg(time, state) as agg 
                    FROM inttest 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            // Day 1, in "three" from "16:00" to end of day
            assert_eq!(durations.next().unwrap()[1].value(), Some("08:00:00"));
            // Day 2, in "three" from start of day to "2:00" and "20:00" to end of day
            assert_eq!(durations.next().unwrap()[1].value(), Some("06:00:00"));
            // Day 3, in "three" from start of day to end
            assert_eq!(durations.next().unwrap()[1].value(), Some("18:00:00"));
            assert!(durations.next().is_none());

            let mut durations = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_duration_in(
                    'three', 
                    agg, 
                    '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.timeline_agg(time, state) as agg 
                    FROM inttest 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            // Day 1, in "three" from "16:00" to end of day
            assert_eq!(durations.next().unwrap()[1].value(), Some("08:00:00"));
            // Day 2, in "three" from start of day to "2:00" and "20:00" to end of day
            assert_eq!(durations.next().unwrap()[1].value(), Some("06:00:00"));
            // Day 3, in "three" from start of day to end
            assert_eq!(durations.next().unwrap()[1].value(), Some("18:00:00"));
            assert!(durations.next().is_none());

            let mut durations = client.select(
                r#"SELECT
                toolkit_experimental.interpolated_duration_in(
                    10003,
                    agg, 
                    '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.state_agg(time, state) as agg 
                    FROM inttest2
                    GROUP BY bucket ORDER BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            // Day 1, in "three" from "16:00" to end of day
            assert_eq!(durations.next().unwrap()[1].value(), Some("08:00:00"));
            // Day 2, in "three" from start of day to "2:00" and "20:00" to end of day
            assert_eq!(durations.next().unwrap()[1].value(), Some("06:00:00"));
            // Day 3, in "three" from start of day to end
            assert_eq!(durations.next().unwrap()[1].value(), Some("18:00:00"));
            assert!(durations.next().is_none());
        });
    }

    #[pg_test(
        error = "state cannot be both String(\"ERROR\") and String(\"START\") at 631152000000000"
    )]
    fn two_states_at_one_time() {
        Spi::execute(|client| {
            client.select("CREATE TABLE test(ts timestamptz, state TEXT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                        ('2020-01-01 00:00:00+00', 'START'),
                        ('2020-01-01 00:00:00+00', 'ERROR')"#,
                None,
                None,
            );
            client.select(
                "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                None,
                None,
            );
            client.select(
                "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.timeline_agg(ts, state)) FROM test",
                None,
                None,
            );
        })
    }

    #[pg_test]
    fn interpolate_introduces_state() {
        Spi::execute(|client| {
            client.select(
                "CREATE TABLE states(time TIMESTAMPTZ, state TEXT, bucket INT)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO states VALUES
                ('2020-1-1 10:00', 'starting', 1),
                ('2020-1-1 10:30', 'running', 1),
                ('2020-1-2 16:00', 'error', 2),
                ('2020-1-3 18:30', 'starting', 3),
                ('2020-1-3 19:30', 'running', 3),
                ('2020-1-4 12:00', 'stopping', 4)"#,
                None,
                None,
            );

            let mut durations = client.select(
                r#"SELECT 
                toolkit_experimental.interpolated_duration_in(
                  'running',
                  agg,
                  '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval,
                  LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.state_agg(time, state) as agg
                    FROM states
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            assert_eq!(durations.next().unwrap()[1].value(), Some("13:30:00"));
            assert_eq!(durations.next().unwrap()[1].value(), Some("16:00:00"));
            assert_eq!(durations.next().unwrap()[1].value(), Some("04:30:00"));
            assert_eq!(durations.next().unwrap()[1].value(), Some("12:00:00"));

            let mut durations = client.select(
                r#"SELECT 
                toolkit_experimental.interpolated_duration_in(
                  'running',
                  agg,
                  '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval,
                  LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.timeline_agg(time, state) as agg
                    FROM states
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            );

            assert_eq!(durations.next().unwrap()[1].value(), Some("13:30:00"));
            assert_eq!(durations.next().unwrap()[1].value(), Some("16:00:00"));
            assert_eq!(durations.next().unwrap()[1].value(), Some("04:30:00"));
            assert_eq!(durations.next().unwrap()[1].value(), Some("12:00:00"));
        })
    }
}
