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
        combinefunc = state_agg_rollup_combine,
        serialfunc = state_agg_rollup_serialize,
        deserialfunc = state_agg_rollup_deserialize,
        parallel = restricted
    );",
    name = "compact_state_agg_rollup",
    requires = [
        compact_state_agg_rollup_trans,
        compact_state_agg_rollup_final,
        state_agg_rollup_combine,
        state_agg_rollup_serialize,
        state_agg_rollup_deserialize,
        CompactStateAgg,
    ],
);
extension_sql!(
    "CREATE AGGREGATE rollup(
        value StateAgg
    ) (
        sfunc = state_agg_rollup_trans,
        stype = internal,
        finalfunc = state_agg_rollup_final,
        combinefunc = state_agg_rollup_combine,
        serialfunc = state_agg_rollup_serialize,
        deserialfunc = state_agg_rollup_deserialize,
        parallel = restricted
    );",
    name = "state_agg_rollup",
    requires = [
        state_agg_rollup_trans,
        state_agg_rollup_final,
        state_agg_rollup_combine,
        state_agg_rollup_serialize,
        state_agg_rollup_deserialize,
        StateAgg,
    ],
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupTransState {
    values: Vec<OwnedCompactStateAgg>,
    compact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

        let (earlier, later) = match self.cmp(&other) {
            Ordering::Less => (self, other),
            Ordering::Greater => (other, self),
            Ordering::Equal => panic!(
                "can't merge overlapping aggregates (same start time: {})",
                self.first_time
            ),
        };

        assert!(
            earlier.last_time <= later.first_time,
            "can't merge overlapping aggregates (earlier={}-{}, later={}-{})",
            earlier.first_time,
            earlier.last_time,
            later.first_time,
            later.last_time,
        );
        assert_ne!(
            later.durations.len(),
            0,
            "later aggregate must be non-empty"
        );
        assert_ne!(
            earlier.durations.len(),
            0,
            "earlier aggregate must be non-empty"
        );

        let later_states =
            String::from_utf8(later.states.to_vec()).expect("invalid later UTF-8 states");
        let mut merged_states =
            String::from_utf8(earlier.states.to_vec()).expect("invalid earlier UTF-8 states");
        let mut merged_durations = earlier.durations.into_iter().collect::<Vec<_>>();

        let earlier_len = earlier.combined_durations.len();

        let mut merged_last_state = None;
        for (later_idx, dis) in later.durations.iter().enumerate() {
            let materialized_dis = dis.state.materialize(&later_states);
            let merged_duration_info =
                merged_durations
                    .iter_mut()
                    .enumerate()
                    .find(|(_, merged_dis)| {
                        merged_dis.state.materialize(&merged_states) == materialized_dis
                    });

            let merged_idx =
                if let Some((merged_idx, merged_duration_to_update)) = merged_duration_info {
                    merged_duration_to_update.duration += dis.duration;
                    merged_idx
                } else {
                    let state = materialized_dis.entry(&mut merged_states);
                    merged_durations.push(DurationInState {
                        state,
                        duration: dis.duration,
                    });
                    merged_durations.len() - 1
                };

            if later_idx == later.last_state as usize {
                // this is the last state
                merged_last_state = Some(merged_idx);
            };
        }
        let merged_last_state =
            merged_last_state.expect("later last_state not in later.durations") as u32;

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
        merged_durations
            .get_mut(earlier.last_state as usize)
            .expect("earlier.last_state doesn't point to a state")
            .duration += gap;

        // ensure combined_durations covers the whole range of time
        if !earlier.compact {
            if combined_durations
                .get_mut(earlier_len - 1)
                .expect("invalid combined_durations: nothing at end of earlier")
                .state
                .materialize(&merged_states)
                == combined_durations
                    .get(earlier_len)
                    .expect("invalid combined_durations: nothing at start of earlier")
                    .state
                    .materialize(&merged_states)
            {
                combined_durations
                    .get_mut(earlier_len - 1)
                    .expect("invalid combined_durations (nothing at earlier_len - 1, equal)")
                    .end_time = combined_durations.remove(earlier_len).end_time;
            } else {
                combined_durations
                    .get_mut(earlier_len - 1)
                    .expect("invalid combined_durations (nothing at earlier_len - 1, not equal)")
                    .end_time = combined_durations
                    .get(earlier_len)
                    .expect("invalid combined_durations (nothing at earlier_len, not equal)")
                    .start_time;
            }
        }

        let merged_states = merged_states.into_bytes();
        OwnedCompactStateAgg {
            states: merged_states,
            durations: merged_durations,
            combined_durations,

            first_time: earlier.first_time,
            last_time: later.last_time,
            first_state: earlier.first_state, // indexes into earlier durations are same for merged_durations
            last_state: merged_last_state,

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

impl PartialOrd for OwnedCompactStateAgg {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OwnedCompactStateAgg {
    fn cmp(&self, other: &Self) -> Ordering {
        // compare using first time (OwnedCompactStateAgg::merge will handle any overlap)
        self.first_time.cmp(&other.first_time)
    }
}

impl RollupTransState {
    fn merge(&mut self) {
        // OwnedCompactStateAgg::merge can't merge overlapping aggregates
        self.values.sort();
        self.values = self
            .values
            .drain(..)
            .reduce(|a, b| a.merge(b))
            .map(|val| vec![val])
            .unwrap_or_else(Vec::new);
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn compact_state_agg_rollup_trans(
    state: Internal,
    next: Option<CompactStateAgg>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    compact_state_agg_rollup_trans_inner(unsafe { state.to_inner() }, next, fcinfo).internal()
}

pub fn compact_state_agg_rollup_trans_inner(
    state: Option<Inner<RollupTransState>>,
    next: Option<CompactStateAgg>,
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

#[pg_extern(immutable, parallel_safe)]
pub fn state_agg_rollup_trans(
    state: Internal,
    next: Option<StateAgg>,
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
fn compact_state_agg_rollup_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<CompactStateAgg<'static>> {
    compact_state_agg_rollup_final_inner(unsafe { state.to_inner() }, fcinfo)
}

fn compact_state_agg_rollup_final_inner(
    state: Option<Inner<RollupTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<CompactStateAgg<'static>> {
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

#[pg_extern(immutable, parallel_safe)]
fn state_agg_rollup_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StateAgg<'static>> {
    state_agg_rollup_final_inner(unsafe { state.to_inner() }, fcinfo)
}

fn state_agg_rollup_final_inner(
    state: Option<Inner<RollupTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StateAgg<'static>> {
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

#[pg_extern(immutable, parallel_safe, strict)]
pub fn state_agg_rollup_serialize(state: Internal) -> bytea {
    let mut state: Inner<RollupTransState> = unsafe { state.to_inner().unwrap() };
    state.merge();
    crate::do_serialize!(state)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn state_agg_rollup_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    state_agg_rollup_deserialize_inner(bytes).internal()
}
pub fn state_agg_rollup_deserialize_inner(bytes: bytea) -> Inner<RollupTransState> {
    let t: RollupTransState = crate::do_deserialize!(bytes, RollupTransState);
    t.into()
}

#[pg_extern(immutable, parallel_safe)]
pub fn state_agg_rollup_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe {
        state_agg_rollup_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal()
    }
}

#[allow(clippy::redundant_clone)] // clone is needed so we don't mutate shared memory
pub fn state_agg_rollup_combine_inner(
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
    use pgrx_macros::pg_test;

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

    #[test]
    fn merges_compact_aggs_correctly() {
        let s1 = OwnedCompactStateAgg {
            durations: vec![
                DurationInState {
                    duration: 500,
                    state: StateEntry::from_integer(5_552),
                },
                DurationInState {
                    duration: 400,
                    state: StateEntry::from_integer(5_551),
                },
            ],
            combined_durations: vec![],
            first_time: 100,
            last_time: 1000,
            first_state: 1,
            last_state: 0,
            states: vec![],
            compact: true,
            integer_states: true,
        };
        let s2 = OwnedCompactStateAgg {
            durations: vec![
                DurationInState {
                    duration: 500,
                    state: StateEntry::from_integer(5_552),
                },
                DurationInState {
                    duration: 400,
                    state: StateEntry::from_integer(5_551),
                },
            ],
            combined_durations: vec![],
            first_time: 1000 + 12345,
            last_time: 1900 + 12345,
            first_state: 1,
            last_state: 0,
            states: vec![],
            compact: true,
            integer_states: true,
        };
        let s3 = OwnedCompactStateAgg {
            durations: vec![
                DurationInState {
                    duration: 500,
                    state: StateEntry::from_integer(5_552),
                },
                DurationInState {
                    duration: 400,
                    state: StateEntry::from_integer(5_551),
                },
            ],
            combined_durations: vec![],
            first_time: 1900 + 12345,
            last_time: 1900 + 12345 + 900,
            first_state: 1,
            last_state: 0,
            states: vec![],
            compact: true,
            integer_states: true,
        };
        let expected = OwnedCompactStateAgg {
            durations: vec![
                DurationInState {
                    duration: 500 * 3 + 12345,
                    state: StateEntry::from_integer(5_552),
                },
                DurationInState {
                    duration: 400 * 3,
                    state: StateEntry::from_integer(5_551),
                },
            ],
            combined_durations: vec![],
            first_time: 100,
            last_time: 1900 + 12345 + 900,
            first_state: 1,
            last_state: 0,
            states: vec![],
            compact: true,
            integer_states: true,
        };
        let merged = s1.clone().merge(s2.clone().merge(s3.clone()));
        assert_eq!(merged, expected);
        let merged = s3.clone().merge(s2.clone().merge(s1.clone()));
        assert_eq!(merged, expected);

        let mut trans_state = RollupTransState {
            values: vec![s1.clone(), s2.clone(), s3.clone()],
            compact: true,
        };
        trans_state.merge();
        assert_eq!(trans_state.values.len(), 1);
        assert_eq!(trans_state.values[0], expected.clone());

        let mut trans_state = RollupTransState {
            values: vec![s3.clone(), s1.clone(), s2.clone()],
            compact: true,
        };
        trans_state.merge();
        assert_eq!(trans_state.values.len(), 1);
        assert_eq!(trans_state.values[0], expected.clone());
    }
}
