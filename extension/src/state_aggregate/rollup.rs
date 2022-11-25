use super::*;
use crate::aggregate_utils::in_aggregate_context;

extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.rollup(
        value toolkit_experimental.StateAgg
    ) (
        sfunc = toolkit_experimental.state_agg_rollup_trans,
        stype = toolkit_experimental.StateAgg,
        finalfunc = toolkit_experimental.state_agg_rollup_final
    );",
    name = "state_agg_rollup",
    requires = [
        // TODO: depend on state_agg somehow?
        state_agg_rollup_trans,
        state_agg_rollup_final,
    ],
);

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn state_agg_rollup_trans<'a>(
    state: Option<toolkit_experimental::StateAgg<'a>>,
    val: toolkit_experimental::StateAgg<'a>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::StateAgg<'a>> {
    Some(unsafe {
        in_aggregate_context(fcinfo, || match state {
            None => val.into(),
            Some(state) => state.merge(&val),
        })
    })
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn state_agg_rollup_final<'a>(
    state: toolkit_experimental::StateAgg<'a>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<StateAgg<'a>> {
    unsafe { in_aggregate_context(fcinfo, || Some(state.in_current_context())) }
}

extension_sql!(
    "CREATE AGGREGATE toolkit_experimental.rollup(
        value toolkit_experimental.TimelineAgg
    ) (
        sfunc = toolkit_experimental.timeline_agg_rollup_trans,
        stype = toolkit_experimental.StateAgg,
        finalfunc = toolkit_experimental.timeline_agg_rollup_final
    );",
    name = "timeline_agg_rollup",
    requires = [
        // TODO: depend on state_agg somehow?
        timeline_agg_rollup_trans,
        timeline_agg_rollup_final,
    ],
);

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timeline_agg_rollup_trans<'a>(
    state: Option<toolkit_experimental::StateAgg<'a>>,
    val: toolkit_experimental::TimelineAgg<'a>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::StateAgg<'a>> {
    Some(unsafe {
        in_aggregate_context(fcinfo, || match state {
            None => val.as_state_agg().into(),
            Some(state) => state.merge(&val.as_state_agg()),
        })
    })
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn timeline_agg_rollup_final<'a>(
    state: toolkit_experimental::StateAgg<'a>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TimelineAgg<'a>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            Some(TimelineAgg::new(state).in_current_context())
        })
    }
}
