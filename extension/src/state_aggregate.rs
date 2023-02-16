//! SELECT duration_in('STOPPED', states) as run_time, duration_in('ERROR', states) as error_time FROM (
//!   SELECT compact_state_agg(time, state) as states FROM ...
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

use toolkit_experimental::{CompactStateAgg, StateAgg};

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
        struct CompactStateAgg<'input> {
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
            compact: bool,
            integer_states: bool,
        }
    }

    pg_type! {
        #[derive(Debug)]
        struct StateAgg<'input> {
            compact_state_agg: CompactStateAggData<'input>,
        }
    }

    impl CompactStateAgg<'_> {
        pub(super) fn empty(compact: bool, integer_states: bool) -> Self {
            unsafe {
                flatten!(CompactStateAgg {
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
                    compact,
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
            let compact = combined_durations.is_none();
            if durations.is_empty() {
                assert!(
                    first.is_none()
                        && last.is_none()
                        && states.is_empty()
                        && combined_durations.map(|v| v.is_empty()).unwrap_or(true)
                );

                return Self::empty(compact, integer_states);
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
                flatten!(CompactStateAgg {
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
                    compact,
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
            prev: Option<CompactStateAgg>,
        ) -> CompactStateAgg {
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

            let mut combined_durations = if self.compact {
                None
            } else {
                Some(self.combined_durations.iter().collect::<Vec<_>>())
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
                                .expect("poorly formed StateAgg, length mismatch");
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
                    None => {
                        pgx::error!("poorly formed state aggregate, last_state out of starts")
                    }
                    Some(dis) => {
                        dis.duration += last_interval;
                        if let Some(combined_durations) = combined_durations.as_mut() {
                            // extend last duration
                            combined_durations
                                .last_mut()
                                .expect("poorly formed state aggregate, length mismatch")
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

            CompactStateAgg::new(
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

    impl<'input> StateAgg<'input> {
        pub fn new(compact_state_agg: CompactStateAgg) -> Self {
            unsafe {
                flatten!(StateAgg {
                    compact_state_agg: compact_state_agg.0,
                })
            }
        }

        pub fn as_compact_state_agg(self) -> CompactStateAgg<'input> {
            unsafe { self.0.compact_state_agg.flatten() }
        }

        pub fn assert_int<'a>(&self) {
            assert!(
                self.0.compact_state_agg.integer_states,
                "State must have integer values for this function"
            );
        }
        pub fn assert_str<'a>(&self) {
            assert!(
                !self.0.compact_state_agg.integer_states,
                "State must have string values for this function"
            );
        }
    }

    ron_inout_funcs!(CompactStateAgg);
    ron_inout_funcs!(StateAgg);
}

fn state_trans_inner(
    state: Option<CompactStateAggTransState>,
    ts: TimestampTz,
    value: Option<MaterializedState>,
    integer_states: bool,
) -> Option<CompactStateAggTransState> {
    let value = match value {
        None => return state,
        Some(value) => value,
    };
    let mut state = state.unwrap_or_else(|| CompactStateAggTransState::new(integer_states));
    state.record(value, ts.into());
    Some(state)
}
#[aggregate]
impl toolkit_experimental::compact_state_agg {
    type State = CompactStateAggTransState;

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
        crate::do_deserialize!(bytes, CompactStateAggTransState)
    }

    fn finally(state: Option<&mut State>) -> Option<CompactStateAgg<'static>> {
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
            CompactStateAgg::new(states, durations, first, last, None, s.integer_states)
        })
    }
}

extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.compact_state_agg(
        ts timestamptz,
        value bigint
    ) (
        stype = internal,
        sfunc = toolkit_experimental.compact_state_agg_int_trans,
        finalfunc = toolkit_experimental.compact_state_agg_finally_fn_outer,
        parallel = safe,
        serialfunc = toolkit_experimental.compact_state_agg_serialize_fn_outer,
        deserialfunc = toolkit_experimental.compact_state_agg_deserialize_fn_outer,
        combinefunc = toolkit_experimental.compact_state_agg_combine_fn_outer
    );",
    name = "compact_state_agg_bigint",
    requires = [
        compact_state_agg_int_trans,
        compact_state_agg_finally_fn_outer,
        compact_state_agg_serialize_fn_outer,
        compact_state_agg_deserialize_fn_outer,
        compact_state_agg_combine_fn_outer
    ],
);
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn compact_state_agg_int_trans(
    __inner: pgx::Internal,
    ts: TimestampTz,
    value: Option<i64>,
    __fcinfo: pg_sys::FunctionCallInfo,
) -> Option<pgx::Internal> {
    // expanded from #[aggregate] transition function
    use crate::palloc::{Inner, InternalAsValue, ToInternal};
    type State = CompactStateAggTransState;
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
impl toolkit_experimental::state_agg {
    type State = CompactStateAggTransState;

    const PARALLEL_SAFE: bool = true;

    fn transition(
        state: Option<State>,
        #[sql_type("timestamptz")] ts: TimestampTz,
        #[sql_type("text")] value: Option<String>,
    ) -> Option<State> {
        compact_state_agg::transition(state, ts, value)
    }

    fn combine(a: Option<&State>, b: Option<&State>) -> Option<State> {
        compact_state_agg::combine(a, b)
    }

    fn serialize(state: &mut State) -> bytea {
        compact_state_agg::serialize(state)
    }

    fn deserialize(bytes: bytea) -> State {
        compact_state_agg::deserialize(bytes)
    }

    fn finally(state: Option<&mut State>) -> Option<StateAgg<'static>> {
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

            StateAgg::new(CompactStateAgg::new(
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
    type State = CompactStateAggTransState;
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
pub struct CompactStateAggTransState {
    records: Vec<Record>,
    integer_states: bool,
}

impl CompactStateAggTransState {
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
    aggregate: Option<CompactStateAgg<'a>>,
    state: Option<StateEntry>,
    range: Option<(i64, Option<i64>)>, // start and interval
) -> crate::raw::Interval {
    let time: i64 = if let Some((start, interval)) = range {
        let end = if let Some(interval) = interval {
            assert!(interval >= 0, "Interval must not be negative");
            start + interval
        } else {
            i64::MAX
        };
        assert!(end >= start, "End time must be after start time");
        if let (Some(state), Some(agg)) = (state, aggregate) {
            assert!(
                !agg.0.compact,
                "unreachable: interval specified for compact aggregate"
            );

            let state = state.materialize(agg.states_as_str());
            let mut total = 0;
            for tis in agg.combined_durations.iter() {
                let tis_start_time = i64::max(tis.start_time, start);
                let tis_end_time = i64::min(tis.end_time, end);
                if tis_start_time > end {
                    // combined_durations is sorted, so after this point there can't be any more
                    break;
                };
                if tis_end_time >= start && tis.state.materialize(agg.states_as_str()) == state {
                    let amount = tis_end_time - tis_start_time;
                    assert!(amount >= 0, "incorrectly ordered times");
                    total += amount;
                }
            }
            total
        } else {
            0
        }
    } else {
        state.and_then(|state| aggregate?.get(state)).unwrap_or(0)
    };
    time.into()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn duration_in<'a>(agg: Option<CompactStateAgg<'a>>, state: String) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_str()
    };
    let state = agg
        .as_ref()
        .and_then(|agg| StateEntry::try_from_existing_str(agg.states_as_str(), &state));
    duration_in_inner(agg, state, None)
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_int<'a>(agg: Option<CompactStateAgg<'a>>, state: i64) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_int()
    };
    duration_in_inner(agg, Some(StateEntry::from_integer(state)), None)
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_tl<'a>(agg: Option<StateAgg<'a>>, state: String) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_str()
    };
    duration_in(agg.map(StateAgg::as_compact_state_agg), state)
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_tl_int<'a>(agg: Option<StateAgg<'a>>, state: i64) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_int()
    };
    duration_in_inner(
        agg.map(StateAgg::as_compact_state_agg),
        Some(StateEntry::from_integer(state)),
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
    agg: Option<StateAgg<'a>>,
    state: String,
    start: TimestampTz,
    interval: default!(Option<crate::raw::Interval>, "NULL"),
) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_str()
    };
    let agg = agg.map(StateAgg::as_compact_state_agg);
    let interval = interval.map(|interval| crate::datum_utils::interval_to_ms(&start, &interval));
    let start = start.into();
    let state = agg
        .as_ref()
        .and_then(|agg| StateEntry::try_from_existing_str(agg.states_as_str(), &state));
    duration_in_inner(agg, state, Some((start, interval)))
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_range_int<'a>(
    agg: Option<StateAgg<'a>>,
    state: i64,
    start: TimestampTz,
    interval: default!(Option<crate::raw::Interval>, "NULL"),
) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_int()
    };
    let interval = interval.map(|interval| crate::datum_utils::interval_to_ms(&start, &interval));
    let start = start.into();
    duration_in_inner(
        agg.map(StateAgg::as_compact_state_agg),
        Some(StateEntry::from_integer(state)),
        Some((start, interval)),
    )
}

fn interpolated_duration_in_inner<'a>(
    aggregate: Option<CompactStateAgg<'a>>,
    state: Option<MaterializedState>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<CompactStateAgg<'a>>,
) -> crate::raw::Interval {
    match aggregate {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(aggregate) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            let start = start.into();
            if let Some(ref prev) = prev {
                assert!(
                    start >= prev.0.last_time,
                    "Start time cannot be before last state of previous aggregate"
                );
            };
            let range = if aggregate.compact {
                assert!(
                    start <= aggregate.first_time,
                    "For compact state aggregates, the start cannot be after the first state"
                );
                assert!(
                    (start + interval) >= aggregate.last_time,
                    "For compact state aggregates, the time range cannot be after the last state"
                );
                None
            } else {
                Some((start, Some(interval)))
            };
            let new_agg = aggregate.interpolate(start, interval, prev);
            let state_entry =
                state.and_then(|state| state.try_existing_entry(new_agg.states_as_str()));
            duration_in_inner(Some(new_agg), state_entry, range)
        }
    }
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_duration_in<'a>(
    agg: Option<CompactStateAgg<'a>>,
    state: String,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<CompactStateAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_str()
    };
    interpolated_duration_in_inner(
        agg,
        Some(MaterializedState::String(state)),
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
    agg: Option<StateAgg<'a>>,
    state: String,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_str()
    };
    interpolated_duration_in(
        agg.map(StateAgg::as_compact_state_agg),
        state,
        start,
        interval,
        prev.map(StateAgg::as_compact_state_agg),
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    schema = "toolkit_experimental",
    name = "interpolated_duration_in"
)]
pub fn interpolated_duration_in_int<'a>(
    agg: Option<CompactStateAgg<'a>>,
    state: i64,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<CompactStateAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_int()
    };
    interpolated_duration_in_inner(
        agg,
        Some(MaterializedState::Integer(state)),
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
    agg: Option<StateAgg<'a>>,
    state: i64,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> crate::raw::Interval {
    if let Some(ref agg) = agg {
        agg.assert_int()
    };
    interpolated_duration_in_int(
        agg.map(StateAgg::as_compact_state_agg),
        state,
        start,
        interval,
        prev.map(StateAgg::as_compact_state_agg),
    )
}

fn duration_in_bad_args_inner() -> ! {
    panic!("The start and interval parameters cannot be used for duration_in with a compact state aggregate")
}

#[allow(unused_variables)] // can't underscore-prefix since argument names are used by pgx
#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_bad_args<'a>(
    agg: Option<CompactStateAgg<'a>>,
    state: String,
    start: TimestampTz,
    interval: crate::raw::Interval,
) -> crate::raw::Interval {
    duration_in_bad_args_inner()
}
#[allow(unused_variables)] // can't underscore-prefix since argument names are used by pgx
#[pg_extern(
    immutable,
    parallel_safe,
    name = "duration_in",
    schema = "toolkit_experimental"
)]
pub fn duration_in_int_bad_args<'a>(
    agg: Option<CompactStateAgg<'a>>,
    state: i64,
    start: TimestampTz,
    interval: crate::raw::Interval,
) -> crate::raw::Interval {
    duration_in_bad_args_inner()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn into_values<'a>(
    agg: CompactStateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(duration, crate::raw::Interval),
    ),
> {
    agg.assert_str();
    let states: String = agg.states_as_str().to_owned();
    TableIterator::new(agg.durations.clone().into_iter().map(move |record| {
        (
            record.state.as_str(&states).to_string(),
            record.duration.into(),
        )
    }))
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn into_int_values<'a>(
    agg: CompactStateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(duration, crate::raw::Interval),
    ),
> {
    agg.assert_int();
    TableIterator::new(
        agg.durations
            .clone()
            .into_iter()
            .map(move |record| (record.state.as_integer(), record.duration.into()))
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
    agg: StateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(duration, crate::raw::Interval),
    ),
> {
    agg.assert_str();
    into_values(agg.as_compact_state_agg())
}
#[pg_extern(
    immutable,
    parallel_safe,
    name = "into_int_values",
    schema = "toolkit_experimental"
)]
pub fn into_values_tl_int<'a>(
    agg: StateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(duration, crate::raw::Interval),
    ),
> {
    agg.assert_int();
    into_int_values(agg.as_compact_state_agg())
}

fn state_timeline_inner<'a>(
    agg: CompactStateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    assert!(
        !agg.compact,
        "state_timeline can only be called on a compact_state_agg built from state_agg"
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
    agg: CompactStateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    assert!(
        !agg.compact,
        "state_timeline can only be called on a compact_state_agg built from state_agg"
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
    agg: StateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_str();
    state_timeline_inner(agg.as_compact_state_agg())
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn state_int_timeline<'a>(
    agg: StateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_int();
    state_int_timeline_inner(agg.as_compact_state_agg())
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_state_timeline<'a>(
    agg: Option<StateAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, String),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref agg) = agg {
        agg.assert_str()
    };
    match agg {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(agg) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            TableIterator::new(
                state_timeline_inner(agg.as_compact_state_agg().interpolate(
                    start.into(),
                    interval,
                    prev.map(StateAgg::as_compact_state_agg),
                ))
                .collect::<Vec<_>>()
                .into_iter(),
            )
        }
    }
}
#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_int_state_timeline<'a>(
    agg: Option<StateAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(state, i64),
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref agg) = agg {
        agg.assert_int()
    };
    match agg {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(agg) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            TableIterator::new(
                state_int_timeline_inner(agg.as_compact_state_agg().interpolate(
                    start.into(),
                    interval,
                    prev.map(StateAgg::as_compact_state_agg),
                ))
                .collect::<Vec<_>>()
                .into_iter(),
            )
        }
    }
}

fn state_periods_inner<'a>(
    state: MaterializedState,
    agg: CompactStateAgg<'a>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    assert!(
        !agg.compact,
        "state_periods can only be called on a compact_state_agg built from state_agg"
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
    agg: StateAgg<'a>,
    state: String,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_str();
    let agg = agg.as_compact_state_agg();
    state_periods_inner(MaterializedState::String(state), agg)
}
#[pg_extern(
    immutable,
    parallel_safe,
    schema = "toolkit_experimental",
    name = "state_periods"
)]
pub fn state_int_periods<'a>(
    agg: StateAgg<'a>,
    state: i64,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    agg.assert_int();
    state_periods_inner(
        MaterializedState::Integer(state),
        agg.as_compact_state_agg(),
    )
}

fn interpolated_state_periods_inner<'a>(
    aggregate: Option<StateAgg<'a>>,
    state: MaterializedState,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
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
                    aggregate.as_compact_state_agg().interpolate(
                        start.into(),
                        interval,
                        prev.map(StateAgg::as_compact_state_agg),
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
    agg: Option<StateAgg<'a>>,
    state: String,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref agg) = agg {
        agg.assert_str()
    };
    interpolated_state_periods_inner(agg, MaterializedState::String(state), start, interval, prev)
}
#[pg_extern(
    immutable,
    parallel_safe,
    schema = "toolkit_experimental",
    name = "interpolated_state_periods"
)]
pub fn interpolated_state_periods_int<'a>(
    agg: Option<StateAgg<'a>>,
    state: i64,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
) -> TableIterator<
    'a,
    (
        pgx::name!(start_time, TimestampTz),
        pgx::name!(end_time, TimestampTz),
    ),
> {
    if let Some(ref agg) = agg {
        agg.assert_int()
    };
    interpolated_state_periods_inner(
        agg,
        MaterializedState::Integer(state),
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::sync::atomic::Ordering::Relaxed;

    use super::*;
    use pgx_macros::pg_test;

    macro_rules! select_one {
        ($client:expr, $stmt:expr, $type:ty) => {
            $client
                .update($stmt, None, None)
                .unwrap()
                .first()
                .get_one::<$type>()
                .unwrap()
                .unwrap()
        };
    }

    #[pg_test]
    #[should_panic = "The start and interval parameters cannot be used for duration_in with"]
    fn duration_in_misuse_error() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            assert_eq!(
                "365 days 00:02:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one', '2020-01-01', '1 day')::TEXT FROM test",
                    &str
                )
            );
        })
    }

    #[pg_test]
    fn one_state_one_change() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                "365 days 00:02:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days 00:02:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn two_states_two_changes() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'two')::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn two_states_three_changes() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-01-01 00:02:00+00', 'one'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'two')::TEXT FROM test",
                    &str
                )
            );

            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.state_agg(ts, state), 'two')::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn out_of_order_times() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:02:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'two')::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn same_state_twice() {
        // TODO Do we care?  Could be that states are recorded not only when they change but
        // also at regular intervals even when they don't?
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'one'),
                    ('2020-01-01 00:02:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                "00:02:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'two')::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn duration_in_two_states_two_changes() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-12-31 00:02:00+00', 'end')
                "#,
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
            assert_eq!(
                "365 days 00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'two')::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn same_state_twice_last() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-01 00:01:00+00', 'two'),
                    ('2020-01-01 00:02:00+00', 'two')
                "#,
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                "00:01:00",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'two')::TEXT FROM test",
                    &str
                )
            );
        });
    }

    #[pg_test]
    fn combine_using_muchos_data() {
        compact_state_agg::counters::reset();
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client.update(
                r#"
insert into test values ('2020-01-01 00:00:00+00', 'one');
insert into test select '2020-01-02 UTC'::timestamptz + make_interval(days=>v), 'two' from generate_series(1,300000) v;
insert into test select '2020-01-02 UTC'::timestamptz + make_interval(days=>v), 'three' from generate_series(300001,600000) v;
insert into test select '2020-01-02 UTC'::timestamptz + make_interval(days=>v), 'four' from generate_series(600001,900000) v;
                "#,
                None,
                None,
            ).unwrap();
            assert_eq!(
                "2 days",
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one')::TEXT FROM test",
                    &str
                )
            );
        });
        assert!(compact_state_agg::counters::COMBINE_NONE.load(Relaxed) == 0); // TODO untested
        assert!(compact_state_agg::counters::COMBINE_A.load(Relaxed) == 0); // TODO untested
        assert!(compact_state_agg::counters::COMBINE_B.load(Relaxed) > 0); // tested
        assert!(compact_state_agg::counters::COMBINE_BOTH.load(Relaxed) > 0);
        // tested
    }

    // TODO This doesn't work under github actions.  Do we run with multiple
    //   CPUs there?  If not, that would surely make a big difference.
    // TODO use EXPLAIN to figure out how it differs when run under github actions
    // #[pg_test]
    #[allow(dead_code)]
    fn combine_using_settings() {
        compact_state_agg::counters::reset();
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'one'),
                    ('2020-01-03 00:00:00+00', 'two')
                "#,
                    None,
                    None,
                )
                .unwrap();
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
SELECT toolkit_experimental.duration_in('one', toolkit_experimental.compact_state_agg(ts, state))::TEXT FROM (
    SELECT * FROM test
    UNION ALL SELECT * FROM test
    UNION ALL SELECT * FROM test
    UNION ALL SELECT * FROM test) u
                "#,
                    &str
                )
            );
        });
        assert!(compact_state_agg::counters::COMBINE_NONE.load(Relaxed) == 0); // TODO untested
        assert!(compact_state_agg::counters::COMBINE_A.load(Relaxed) == 0); // TODO untested
        assert!(compact_state_agg::counters::COMBINE_B.load(Relaxed) > 0); // tested
        assert!(compact_state_agg::counters::COMBINE_BOTH.load(Relaxed) > 0);
        // tested
    }

    // the sample query from the ticket
    #[pg_test]
    fn sample_query() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                    ('2020-01-01 00:00:00+00', 'START'),
                    ('2020-01-01 00:01:00+00', 'ERROR'),
                    ('2020-01-01 00:02:00+00', 'STOPPED')"#,
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                client
                    .update(
                        r#"SELECT toolkit_experimental.duration_in(states, 'ERROR')::TEXT as error,
                                  toolkit_experimental.duration_in(states, 'START')::TEXT as start,
                                  toolkit_experimental.duration_in(states, 'STOPPED')::TEXT as stopped
                             FROM (SELECT toolkit_experimental.compact_state_agg(ts, state) as states FROM test) as foo"#,
                        None,
                        None,
                    )
                    .unwrap().first()
                    .get_three::<&str, &str, &str>().unwrap(),
                (Some("00:01:00"), Some("00:01:00"), Some("00:00:00"))
            );
            assert_eq!(
                client
                    .update(
                        r#"SELECT toolkit_experimental.duration_in(states, 'ERROR')::TEXT as error,
                                  toolkit_experimental.duration_in(states, 'START')::TEXT as start,
                                  toolkit_experimental.duration_in(states, 'STOPPED')::TEXT as stopped
                             FROM (SELECT toolkit_experimental.state_agg(ts, state) as states FROM test) as foo"#,
                        None,
                        None,
                    )
                    .unwrap().first()
                    .get_three::<&str, &str, &str>().unwrap(),
                (Some("00:01:00"), Some("00:01:00"), Some("00:00:00"))
            );
        })
    }

    #[pg_test]
    fn interpolated_duration() {
        Spi::connect(|mut client| {
            client
                .update(
                    "SET TIME ZONE 'UTC';
                CREATE TABLE inttest(time TIMESTAMPTZ, state TEXT, bucket INT);
                CREATE TABLE inttest2(time TIMESTAMPTZ, state BIGINT, bucket INT);",
                    None,
                    None,
                )
                .unwrap();
            client
                .update(
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
                )
                .unwrap();

            // Interpolate time spent in state "three" each day
            let mut durations = client.update(
                r#"SELECT
                toolkit_experimental.interpolated_duration_in(
                    agg, 
                    'three',
                    '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.compact_state_agg(time, state) as agg 
                    FROM inttest 
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            ).unwrap();

            // Day 1, in "three" from "16:00" to end of day
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("08:00:00")
            );
            // Day 2, in "three" from start of day to "2:00" and "20:00" to end of day
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("06:00:00")
            );
            // Day 3, in "three" from start of day to end
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("18:00:00")
            );
            assert!(durations.next().is_none());

            let mut durations = client.update(
                r#"SELECT
                toolkit_experimental.interpolated_duration_in(
                    agg,
                    'three', 
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
            ).unwrap();

            // Day 1, in "three" from "16:00" to end of day
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("08:00:00")
            );
            // Day 2, in "three" from start of day to "2:00" and "20:00" to end of day
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("06:00:00")
            );
            // Day 3, in "three" from start of day to end
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("18:00:00")
            );
            assert!(durations.next().is_none());

            let mut durations = client.update(
                r#"SELECT
                toolkit_experimental.interpolated_duration_in(
                    agg,
                    10003,
                    '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval, 
                    LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.compact_state_agg(time, state) as agg 
                    FROM inttest2
                    GROUP BY bucket ORDER BY bucket
                ) s
                ORDER BY bucket"#,
                None,
                None,
            ).unwrap();

            // Day 1, in "three" from "16:00" to end of day
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("08:00:00")
            );
            // Day 2, in "three" from start of day to "2:00" and "20:00" to end of day
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("06:00:00")
            );
            // Day 3, in "three" from start of day to end
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("18:00:00")
            );
            assert!(durations.next().is_none());
        });
    }

    #[pg_test(
        error = "state cannot be both String(\"ERROR\") and String(\"START\") at 631152000000000"
    )]
    fn two_states_at_one_time() {
        Spi::connect(|mut client| {
            client
                .update("CREATE TABLE test(ts timestamptz, state TEXT)", None, None)
                .unwrap();
            client
                .update(
                    r#"INSERT INTO test VALUES
                        ('2020-01-01 00:00:00+00', 'START'),
                        ('2020-01-01 00:00:00+00', 'ERROR')"#,
                    None,
                    None,
                )
                .unwrap();
            client.update(
                "SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'one') FROM test",
                None,
                None,
            ).unwrap();
            client.update(
                "SELECT toolkit_experimental.duration_in(toolkit_experimental.state_agg(ts, state), 'one') FROM test",
                None,
                None,
            ).unwrap();
        })
    }

    #[pg_test]
    fn interpolate_introduces_state() {
        Spi::connect(|mut client| {
            client
                .update(
                    "CREATE TABLE states(time TIMESTAMPTZ, state TEXT, bucket INT)",
                    None,
                    None,
                )
                .unwrap();
            client
                .update(
                    r#"INSERT INTO states VALUES
                ('2020-1-1 10:00', 'starting', 1),
                ('2020-1-1 10:30', 'running', 1),
                ('2020-1-2 16:00', 'error', 2),
                ('2020-1-3 18:30', 'starting', 3),
                ('2020-1-3 19:30', 'running', 3),
                ('2020-1-4 12:00', 'stopping', 4)"#,
                    None,
                    None,
                )
                .unwrap();

            let mut durations = client
                .update(
                    r#"SELECT 
                toolkit_experimental.interpolated_duration_in(
                    agg,
                    'running',
                  '2019-12-31 0:00'::timestamptz + (bucket * '1 day'::interval), '1 day'::interval,
                  LAG(agg) OVER (ORDER BY bucket)
                )::TEXT FROM (
                    SELECT bucket, toolkit_experimental.compact_state_agg(time, state) as agg
                    FROM states
                    GROUP BY bucket
                ) s
                ORDER BY bucket"#,
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("13:30:00")
            );
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("16:00:00")
            );
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("04:30:00")
            );
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("12:00:00")
            );

            let mut durations = client
                .update(
                    r#"SELECT 
                toolkit_experimental.interpolated_duration_in(
                    agg,
                    'running',
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
                )
                .unwrap();

            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("13:30:00")
            );
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("16:00:00")
            );
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("04:30:00")
            );
            assert_eq!(
                durations.next().unwrap()[1].value().unwrap(),
                Some("12:00:00")
            );
        })
    }
}
