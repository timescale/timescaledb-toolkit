use super::{toolkit_experimental::*, *};
use crate::{
    aggregate_utils::in_aggregate_context,
    palloc::{InternalAsValue, ToInternal},
};
use serde::{Deserialize, Serialize};

extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.rollup(
        value toolkit_experimental.CompactStateAgg
    ) (
        sfunc = toolkit_experimental.compact_state_agg_rollup_trans,
        stype = internal,
        finalfunc = toolkit_experimental.compact_state_agg_rollup_final,
        combinefunc = toolkit_experimental.compact_state_agg_rollup_combine,
        serialfunc = toolkit_experimental.compact_state_agg_rollup_serialize,
        deserialfunc = toolkit_experimental.compact_state_agg_rollup_deserialize,
        parallel = restricted
    );",
    name = "compact_state_agg_rollup",
    requires = [
        compact_state_agg_rollup_trans,
        compact_state_agg_rollup_final,
        compact_state_agg_rollup_combine,
        compact_state_agg_rollup_serialize,
        compact_state_agg_rollup_deserialize,
        CompactStateAgg,
    ],
);
extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.rollup(
        value toolkit_experimental.StateAgg
    ) (
        sfunc = toolkit_experimental.state_agg_rollup_trans,
        stype = internal,
        finalfunc = toolkit_experimental.state_agg_rollup_final,
        combinefunc = toolkit_experimental.compact_state_agg_rollup_combine,
        serialfunc = toolkit_experimental.compact_state_agg_rollup_serialize,
        deserialfunc = toolkit_experimental.compact_state_agg_rollup_deserialize,
        parallel = restricted
    );",
    name = "state_agg_rollup",
    requires = [
        state_agg_rollup_trans,
        state_agg_rollup_final,
        compact_state_agg_rollup_combine,
        compact_state_agg_rollup_serialize,
        compact_state_agg_rollup_deserialize,
        StateAgg,
    ],
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupTransState {
    values: Vec<OwnedCompactStateAgg>,
    compact: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OwnedCompactStateAgg {
    durations: Vec<DurationInState>,
    combined_durations: Vec<TimeInState>,
    first_time: i64,
    last_time: i64,
    first_state: u32,
    last_state: u32,
    states: Vec<u8>,
    compact: bool,
    integer_states: bool,
}

impl OwnedCompactStateAgg {
    pub fn merge(self, other: Self) -> Self {
        assert_eq!(
            self.compact, other.compact,
            "can't merge compact_state_agg and state_agg"
        );
        assert_eq!(
            self.integer_states, other.integer_states,
            "can't merge aggs with different state types"
        );

        let (earlier, later) = match self.first_time.cmp(&other.first_time) {
            Ordering::Less => (self, other),
            Ordering::Greater => (other, self),
            Ordering::Equal => panic!("can't merge overlapping aggregates (same start time)"),
        };
        assert!(
            earlier.last_time <= later.first_time,
            "can't merge overlapping aggregates"
        );
        assert_ne!(
            later.durations.len(),
            0,
            "later aggregate must be non-empty"
        );
        assert_ne!(
            earlier.durations.len(),
            0,
            "later aggregate must be non-empty"
        );

        let later_states =
            String::from_utf8(later.states.to_vec()).expect("invalid later UTF-8 states");
        let mut merged_states =
            String::from_utf8(earlier.states.to_vec()).expect("invalid earlier UTF-8 states");
        let mut merged_durations = earlier.durations.into_iter().collect::<Vec<_>>();

        let earlier_len = earlier.combined_durations.len();

        let mut added_entries = 0;
        for dis in later.durations.iter() {
            let merged_duration_to_update = merged_durations.iter_mut().find(|merged_dis| {
                merged_dis.state.materialize(&merged_states) == dis.state.materialize(&later_states)
            });
            if let Some(merged_duration_to_update) = merged_duration_to_update {
                merged_duration_to_update.duration += dis.duration;
            } else {
                let state = dis
                    .state
                    .materialize(&later_states)
                    .entry(&mut merged_states);
                merged_durations.push(DurationInState {
                    state,
                    duration: dis.duration,
                });
                added_entries += 1;
            };
        }

        let mut combined_durations = earlier
            .combined_durations
            .into_iter()
            .chain(later.combined_durations.into_iter().map(|tis| {
                let state = tis
                    .state
                    .materialize(&later_states)
                    .existing_entry(&merged_states);
                TimeInState { state, ..tis }
            }))
            .collect::<Vec<_>>();

        let gap = later.first_time - earlier.last_time;
        assert!(gap >= 0);
        merged_durations[earlier.last_state as usize].duration += gap;

        // ensure combined_durations covers the whole range of time
        if !earlier.compact {
            if combined_durations[earlier_len - 1]
                .state
                .materialize(&merged_states)
                == combined_durations[earlier_len]
                    .state
                    .materialize(&merged_states)
            {
                combined_durations[earlier_len - 1].end_time =
                    combined_durations.remove(earlier_len).end_time;
            } else {
                combined_durations[earlier_len - 1].end_time =
                    combined_durations[earlier_len].start_time;
            }
        }

        let merged_states = merged_states.into_bytes();
        OwnedCompactStateAgg {
            states: merged_states,
            durations: merged_durations,
            combined_durations,

            first_time: earlier.first_time,
            last_time: later.last_time,
            first_state: earlier.first_state,
            last_state: added_entries + later.last_state,

            // these values are always the same for both
            compact: earlier.compact,
            integer_states: earlier.integer_states,
        }
    }
}

impl<'a> From<OwnedCompactStateAgg> for CompactStateAgg<'a> {
    fn from(owned: OwnedCompactStateAgg) -> CompactStateAgg<'a> {
        unsafe {
            flatten!(CompactStateAgg {
                states_len: owned.states.len() as u64,
                states: (&*owned.states).into(),
                durations_len: owned.durations.len() as u64,
                durations: (&*owned.durations).into(),
                combined_durations: (&*owned.combined_durations).into(),
                combined_durations_len: owned.combined_durations.len() as u64,
                first_time: owned.first_time,
                last_time: owned.last_time,
                first_state: owned.first_state,
                last_state: owned.last_state,
                compact: owned.compact,
                integer_states: owned.integer_states,
            })
        }
    }
}

impl<'a> From<CompactStateAgg<'a>> for OwnedCompactStateAgg {
    fn from(agg: CompactStateAgg<'a>) -> OwnedCompactStateAgg {
        OwnedCompactStateAgg {
            states: agg.states.iter().collect::<Vec<_>>(),
            durations: agg.durations.iter().collect::<Vec<_>>(),
            combined_durations: agg.combined_durations.iter().collect::<Vec<_>>(),
            first_time: agg.first_time,
            last_time: agg.last_time,
            first_state: agg.first_state,
            last_state: agg.last_state,
            compact: agg.compact,
            integer_states: agg.integer_states,
        }
    }
}

impl RollupTransState {
    fn merge(&mut self) {
        self.values = self
            .values
            .drain(..)
            .reduce(|a, b| a.merge(b))
            .map(|val| vec![val])
            .unwrap_or_else(Vec::new);
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn compact_state_agg_rollup_trans<'a>(
    state: Internal,
    next: Option<CompactStateAgg<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    compact_state_agg_rollup_trans_inner(unsafe { state.to_inner() }, next, fcinfo).internal()
}

pub fn compact_state_agg_rollup_trans_inner<'a>(
    state: Option<Inner<RollupTransState>>,
    next: Option<CompactStateAgg<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<RollupTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, next) {
            (None, None) => None,
            (None, Some(next)) => Some(
                RollupTransState {
                    values: vec![next.into()],
                    compact: false,
                }
                .into(),
            ),
            (Some(state), None) => Some(state),
            (Some(mut state), Some(next)) => {
                state.values.push(next.into());
                Some(state)
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn state_agg_rollup_trans<'a>(
    state: Internal,
    next: Option<StateAgg<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    compact_state_agg_rollup_trans_inner(
        unsafe { state.to_inner() },
        next.map(StateAgg::as_compact_state_agg),
        fcinfo,
    )
    .internal()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn compact_state_agg_rollup_final<'a>(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<CompactStateAgg<'a>> {
    compact_state_agg_rollup_final_inner(unsafe { state.to_inner() }, fcinfo)
}

fn compact_state_agg_rollup_final_inner<'a>(
    state: Option<Inner<RollupTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<CompactStateAgg<'a>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state.clone(),
            };
            state.merge();
            assert!(state.values.len() == 1);
            let agg: Option<OwnedCompactStateAgg> = state.values.drain(..).next().unwrap().into();
            agg.map(Into::into)
        })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
fn state_agg_rollup_final<'a>(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StateAgg<'a>> {
    state_agg_rollup_final_inner(unsafe { state.to_inner() }, fcinfo)
}

fn state_agg_rollup_final_inner<'a>(
    state: Option<Inner<RollupTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StateAgg<'a>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state.clone(),
            };
            state.merge();
            assert!(state.values.len() == 1);
            let agg: Option<OwnedCompactStateAgg> = state.values.drain(..).next().unwrap().into();
            agg.map(Into::into).map(StateAgg::new)
        })
    }
}

#[pg_extern(immutable, parallel_safe, strict, schema = "toolkit_experimental")]
pub fn compact_state_agg_rollup_serialize(state: Internal) -> bytea {
    let mut state: Inner<RollupTransState> = unsafe { state.to_inner().unwrap() };
    state.merge();
    crate::do_serialize!(state)
}

#[pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn compact_state_agg_rollup_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    compact_state_agg_rollup_deserialize_inner(bytes).internal()
}
pub fn compact_state_agg_rollup_deserialize_inner(bytes: bytea) -> Inner<RollupTransState> {
    let t: RollupTransState = crate::do_deserialize!(bytes, RollupTransState);
    t.into()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn compact_state_agg_rollup_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe {
        compact_state_agg_rollup_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo)
            .internal()
    }
}

#[allow(clippy::redundant_clone)] // clone is needed so we don't mutate shared memory
pub fn compact_state_agg_rollup_combine_inner(
    state1: Option<Inner<RollupTransState>>,
    state2: Option<Inner<RollupTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<RollupTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (Some(x), None) => Some(x.clone().into()),
            (None, Some(x)) => Some(x.clone().into()),
            (Some(x), Some(y)) => {
                let compact = x.compact;
                assert_eq!(
                    compact, y.compact,
                    "trying to merge compact and non-compact state aggs, this should be unreachable"
                );
                let values = x
                    .values
                    .iter()
                    .chain(y.values.iter())
                    .map(Clone::clone)
                    .collect::<Vec<_>>();
                let trans_state = RollupTransState { values, compact };
                Some(trans_state.clone().into())
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    #[should_panic = "can't merge overlapping aggregates"]
    fn merge_range_full_overlap() {
        let mut outer: OwnedCompactStateAgg = CompactStateAgg::empty(false, false).into();
        outer.first_time = 10;
        outer.last_time = 50;

        let mut inner: OwnedCompactStateAgg = CompactStateAgg::empty(false, false).into();
        inner.first_time = 20;
        inner.last_time = 30;

        inner.merge(outer);
    }

    #[pg_test]
    #[should_panic = "can't merge overlapping aggregates"]
    fn merge_range_partial_overlap() {
        let mut r1: OwnedCompactStateAgg = CompactStateAgg::empty(false, false).into();
        r1.first_time = 10;
        r1.last_time = 50;

        let mut r2: OwnedCompactStateAgg = CompactStateAgg::empty(false, false).into();
        r2.first_time = 20;
        r2.last_time = 50;

        r2.merge(r1);
    }
}
