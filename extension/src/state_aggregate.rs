//! SELECT duration_in('STOPPED', states) as run_time, duration_in('ERROR', states) as error_time FROM (
//!   SELECT state_agg(time, state) as states FROM ...
//! );
//!
//! Currently requires loading all data into memory in order to sort it by time.

#![allow(non_camel_case_types)]

use pgx::*;
use serde::{Deserialize, Serialize};

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

use toolkit_experimental::StateAgg;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct StateAgg<'input> {
            states_len: u64, // TODO JOSH this and durations_len can be 32
            durations_len: u64,
            durations: [DurationInState; self.durations_len],
            states: [u8; self.states_len],
        }
    }

    impl StateAgg<'_> {
        pub fn new(states: String, durations: Vec<DurationInState>) -> Self {
            let states_len = states.len() as u64;
            let durations_len = durations.len() as u64;
            unsafe {
                flatten!(StateAgg {
                    states_len,
                    states: states.into_bytes().into(),
                    durations_len,
                    durations: (&*durations).into(),
                })
            }
        }

        pub fn get(&self, state: &str) -> Option<i64> {
            for record in self.durations.iter() {
                if self.state_str(&record) == state {
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

        fn state_str(&self, record: &DurationInState) -> &str {
            let beg = record.state_beg as usize;
            let end = record.state_end as usize;
            &self.states_as_str()[beg..end]
        }
    }

    ron_inout_funcs!(StateAgg);
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
        let value = match value {
            None => return state,
            Some(value) => value,
        };
        let mut state = state.unwrap_or_else(StateAggTransState::new);
        state.record(value, ts.into());
        Some(state)
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
            for (state, duration) in s.drain_to_duration_map() {
                let state_beg = states.len() as u32;
                let state_end = state_beg + state.len() as u32;
                states.push_str(&state);
                durations.push(DurationInState {
                    duration,
                    state_beg,
                    state_end,
                });
            }
            StateAgg::new(states, durations)
        })
    }
}

// Intermediate state kept in postgres.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct StateAggTransState {
    records: Vec<Record>,
}

impl StateAggTransState {
    fn new() -> Self {
        Self { records: vec![] }
    }

    fn record(&mut self, state: String, time: i64) {
        self.records.push(Record { state, time });
    }

    fn append(&mut self, other: &mut Self) {
        self.records.append(&mut other.records)
    }

    /// Drain accumulated state, sort, and return map of states to durations.
    fn drain_to_duration_map(&mut self) -> std::collections::HashMap<String, i64> {
        self.records.sort_by(|a, b| {
            if a.time == b.time {
                // TODO JOSH do we care about instantaneous state changes?
                //           an alternative is to drop duplicate timestamps
                if a.state != b.state {
                    // TODO use human-readable timestamp
                    panic!(
                        "state cannot be both {} and {} at {}",
                        a.state, b.state, a.time
                    )
                }
                std::cmp::Ordering::Equal
            } else {
                a.time.cmp(&b.time)
            }
        });
        let mut duration_state = DurationState::new();
        for record in self.records.drain(..) {
            duration_state.handle_record(record.state, record.time);
        }
        // TODO BRIAN sort this by decreasing duration will make it easier to implement a TopN states
        duration_state.durations
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn duration_in(state: String, aggregate: Option<StateAgg>) -> i64 {
    aggregate
        .map(|aggregate| aggregate.get(&state))
        .flatten()
        .unwrap_or(0)
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn into_values(
    agg: StateAgg<'_>,
) -> impl std::iter::Iterator<Item = (name!(state, String), name!(duration, i64))> + '_ {
    let states: String = agg.states_as_str().to_owned();
    agg.durations.clone().into_iter().map(move |record| {
        let beg = record.state_beg as usize;
        let end = record.state_end as usize;
        (states[beg..end].to_owned(), record.duration)
    })
}

#[derive(Clone, Debug, Deserialize, Eq, FlatSerializable, PartialEq, Serialize)]
#[repr(C)]
pub struct DurationInState {
    duration: i64, // TODO BRIAN is i64 or u64 the right type
    state_beg: u32,
    state_end: u32,
}

struct DurationState {
    last_state: Option<(String, i64)>,
    durations: std::collections::HashMap<String, i64>,
}
impl DurationState {
    fn new() -> Self {
        Self {
            last_state: None,
            durations: std::collections::HashMap::new(),
        }
    }

    fn handle_record(&mut self, state: String, time: i64) {
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
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Record {
    state: String,
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
                31536120000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
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
                60000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
            assert_eq!(
                31536060000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
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
                31536060000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
            assert_eq!(
                60000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
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
                31536060000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
            assert_eq!(
                60000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
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
                120000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
            assert_eq!(
                31536000000000,
                select_one!(
                    client,
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state)) FROM test",
                    i64));
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
            let result: i64 = client
                .select(
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                    None,
                    None,
                )
                .first()
                .get_one()
                .unwrap();
            assert_eq!(60000000, result);
            let result: i64 = client
                .select(
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state)) FROM test",
                    None,
                    None,
                )
                .first()
                .get_one()
                .unwrap();
            assert_eq!(31536060000000, result);
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
            let result: i64 = client
                .select(
                    "SELECT toolkit_experimental.duration_in('two', toolkit_experimental.state_agg(ts, state)) FROM test",
                    None,
                    None,
                )
                .first()
                .get_one()
                .unwrap();
            assert_eq!(60000000, result);
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
            let result: i64 = client
                .select(
                    "SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM test",
                    None,
                    None,
                )
                .first()
                .get_one()
                .unwrap();
            assert_eq!(172800000000, result);
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
            let result: i64 = client
                .select(
                    r#"
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
SET parallel_leader_participation = off;
SET enable_indexonlyscan = off;
SELECT toolkit_experimental.duration_in('one', toolkit_experimental.state_agg(ts, state)) FROM (
    SELECT * FROM test
    UNION ALL SELECT * FROM test
    UNION ALL SELECT * FROM test
    UNION ALL SELECT * FROM test) u
                "#,
                    None,
                    None,
                )
                .first()
                .get_one()
                .unwrap();
            assert_eq!(172800000000, result);
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
                        r#"SELECT toolkit_experimental.duration_in('ERROR', states) as error,
                                  toolkit_experimental.duration_in('START', states) as start,
                                  toolkit_experimental.duration_in('STOPPED', states) as stopped
                             FROM (SELECT toolkit_experimental.state_agg(ts, state) as states FROM test) as foo"#,
                        None,
                        None,
                    )
                    .first()
                    .get_three::<i64, i64, i64>(),
                (Some(60000000), Some(60000000), Some(0))
            );
        })
    }

    // TODO why doesn't this catch the error under github actions?
    //  https://github.com/timescale/timescaledb-toolkit/runs/4943786692?check_suite_focus=true
    // Retrieving Tests
    // Running 98 tests
    // test `two_states_at_one_time` failed with
    // db error: ERROR: state cannot be both ERROR and START at 631152000000000
    // test combine_using_muchos_data ... ok
    //#[pg_test(error = "state cannot be both ERROR and START at 631152000000000")]
    #[allow(dead_code)]
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
        })
    }
}
