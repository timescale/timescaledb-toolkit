//! SELECT duration_in('STOPPED', states) as run_time, duration_in('ERROR', states) as error_time FROM (
//!   SELECT state_agg(time, state) as states FROM ...
//! );
//!
//! Currently requires loading all data into memory in order to sort it by time.

#![allow(non_camel_case_types)]

use pgx::{iter::TableIterator, *};
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
            first_time: i64,
            last_time: i64,
            first_state: u32,
            last_state: u32,  // first/last state are idx into durations, keep together for alignment
            states: [u8; self.states_len],
        }
    }

    impl StateAgg<'_> {
        pub(super) fn new(
            states: String,
            durations: Vec<DurationInState>,
            first: Option<Record>,
            last: Option<Record>,
        ) -> Self {
            if durations.is_empty() {
                assert!(first.is_none() && last.is_none() && states.is_empty());

                return unsafe {
                    flatten!(StateAgg {
                        states_len: 0,
                        states: Slice::Slice(&[]),
                        durations_len: 0,
                        durations: Slice::Slice(&[]),
                        first_time: 0,
                        last_time: 0,
                        first_state: 0,
                        last_state: 0,
                    })
                };
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
                let s = &states[d.state_beg as usize..d.state_end as usize];
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

            unsafe {
                flatten!(StateAgg {
                    states_len,
                    states: states.into_bytes().into(),
                    durations_len,
                    durations: (&*durations).into(),
                    first_time: first.time,
                    last_time: last.time,
                    first_state: first_state as u32,
                    last_state: last_state as u32,
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

        pub(super) fn interpolate(
            &self,
            interval_start: i64,
            interval_len: i64,
            prev: Option<StateAgg>,
            has_next: bool,
        ) -> StateAgg {
            if self.durations.is_empty() {
                pgx::error!("unable to interpolate interval on state aggregate with no data");
            }

            let mut states = std::str::from_utf8(self.states.as_slice())
                .unwrap()
                .to_string();
            let mut durations: Vec<DurationInState> = self.durations.iter().collect();

            let first = match prev {
                Some(prev) if interval_start < self.first_time => {
                    if prev.last_state < prev.durations.len() as u32 {
                        let start_interval = self.first_time - interval_start;
                        let start_state =
                            prev.state_str(&prev.durations.as_slice()[prev.last_state as usize]);
                        match durations.iter_mut().find(|x| {
                            states[x.state_beg as usize..x.state_end as usize].eq(start_state)
                        }) {
                            Some(dis) => dis.duration += start_interval,
                            None => {
                                durations.push(DurationInState {
                                    duration: start_interval,
                                    state_beg: states.len() as u32,
                                    state_end: (states.len() + start_state.len()) as u32,
                                });
                                states += start_state;
                            }
                        };

                        Record {
                            state: start_state.to_string(),
                            time: interval_start,
                        }
                    } else {
                        pgx::error!("unable to interpolate interval on state aggregate where previous agg has no data")
                    }
                }
                _ => Record {
                    state: self
                        .state_str(&self.durations.as_slice()[self.first_state as usize])
                        .to_string(),
                    time: self.first_time,
                },
            };

            let last = if interval_start + interval_len > self.last_time && has_next {
                let last_interval = interval_start + interval_len - self.last_time;
                match durations.get_mut(self.last_state as usize) {
                    None => pgx::error!("poorly formed StateAgg, last_state out of starts"),
                    Some(dis) => {
                        dis.duration += last_interval;
                        Record {
                            state: states[dis.state_beg as usize..dis.state_end as usize]
                                .to_string(),
                            time: interval_start + interval_len,
                        }
                    }
                }
            } else {
                Record {
                    state: self
                        .state_str(&self.durations.as_slice()[self.last_state as usize])
                        .to_string(),
                    time: self.last_time,
                }
            };

            StateAgg::new(states, durations, Some(first), Some(last))
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
            let (map, first, last) = s.drain_to_duration_map_and_bounds();
            for (state, duration) in map {
                let state_beg = states.len() as u32;
                let state_end = state_beg + state.len() as u32;
                states.push_str(&state);
                durations.push(DurationInState {
                    duration,
                    state_beg,
                    state_end,
                });
            }
            StateAgg::new(states, durations, first, last)
        })
    }
}

// Intermediate state kept in postgres.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

    /// Drain accumulated state, sort, and return tuple of map of states to durations along with first and last record.
    fn drain_to_duration_map_and_bounds(
        &mut self,
    ) -> (
        std::collections::HashMap<String, i64>,
        Option<Record>,
        Option<Record>,
    ) {
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
        let (first, last) = (self.records.first(), self.records.last());
        let first = first.cloned();
        let last = last.cloned();
        let mut duration_state = DurationState::new();
        for record in self.records.drain(..) {
            duration_state.handle_record(record.state, record.time);
        }
        duration_state.finalize();
        // TODO BRIAN sort this by decreasing duration will make it easier to implement a TopN states
        (duration_state.durations, first, last)
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn duration_in<'a>(state: String, aggregate: Option<StateAgg<'a>>) -> crate::raw::Interval {
    let time: i64 = aggregate
        .and_then(|aggregate| aggregate.get(&state))
        .unwrap_or(0);
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
    let function_args = vec![Some(pgx::Datum::from(interval))];
    unsafe { pgx::direct_function_call(pg_sys::interval_justify_hours, function_args) }
        .expect("interval_justify_hours does not return None")
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn interpolated_duration_in<'a>(
    state: String,
    aggregate: Option<StateAgg<'a>>,
    start: TimestampTz,
    interval: crate::raw::Interval,
    prev: Option<StateAgg<'a>>,
    next: Option<StateAgg<'a>>,
) -> crate::raw::Interval {
    match aggregate {
        None => pgx::error!(
            "when interpolating data between grouped data, all groups must contain some data"
        ),
        Some(aggregate) => {
            let interval = crate::datum_utils::interval_to_ms(&start, &interval);
            duration_in(
                state,
                Some(aggregate.interpolate(start.into(), interval, prev, next.is_some())),
            )
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn into_values<'a>(
    agg: StateAgg<'a>,
) -> TableIterator<'a, (pgx::name!(state, String), pgx::name!(duration, i64))> {
    let states: String = agg.states_as_str().to_owned();
    TableIterator::new(agg.durations.clone().into_iter().map(move |record| {
        let beg = record.state_beg as usize;
        let end = record.state_end as usize;
        (states[beg..end].to_owned(), record.duration)
    }))
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
    state: String,
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
        })
    }

    #[pg_test]
    fn interpolated_duration() {
        Spi::execute(|client| {
            client.select(
                "CREATE TABLE inttest(time TIMESTAMPTZ, state TEXT, bucket INT)",
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
                ('2020-1-3 16:00'::timestamptz, 'three', 3)"#,
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
                    LAG(agg) OVER (ORDER BY bucket), 
                    LEAD(agg) OVER (ORDER BY bucket)
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
            // Day 3, in "three" from start of day to "10:00"; end in that state, but no following point
            assert_eq!(durations.next().unwrap()[1].value(), Some("10:00:00"));
            assert!(durations.next().is_none());
        });
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
                  LAG(agg) OVER (ORDER BY bucket),
                  LEAD(agg) OVER (ORDER BY bucket)
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
        })
    }
}
