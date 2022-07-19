use pgx::*;
use serde::{Deserialize, Serialize};

use aggregate_builder::aggregate;
use flat_serialize::*;
use flat_serialize_macro::FlatSerializable;

use crate::{
    datum_utils::interval_to_ms,
    flatten,
    palloc::{Inner, Internal},
    pg_type,
    raw::{bytea, TimestampTz},
    ron_inout_funcs,
};

use toolkit_experimental::FunnelAgg;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct FunnelAgg<'input> {
            names_len: u64,
            events_len: u64,
            events: [FunnelEvent; self.events_len],
            names: [u8; self.names_len],
        }
    }

    impl FunnelAgg<'_> {
        pub fn new(names: String, events: Vec<FunnelEvent>) -> Self {
            let names_len = names.len() as u64;
            let events_len = events.len() as u64;
            unsafe {
                flatten!(FunnelAgg {
                    names_len,
                    names: names.into_bytes().into(),
                    events_len,
                    events: (&*events).into(),
                })
            }
        }

        pub fn names_as_str(&self) -> &str {
            let names: &[u8] = self.names.as_slice();
            // SAFETY: came from a String in `new` a few lines up
            unsafe { std::str::from_utf8_unchecked(names) }
        }

        pub fn event_name(&self, event: &FunnelEvent) -> &str {
            let beg = event.name_beg as usize;
            let end = event.name_end as usize;
            &self.names_as_str()[beg..end]
        }
    }

    ron_inout_funcs!(FunnelAgg);
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize, FlatSerializable)]
#[repr(C)]
pub struct FunnelEvent {
    time: i64,
    handle: Handle,
    name_beg: u32,
    name_end: u32,
    _pad: u32,
}

#[aggregate]
impl toolkit_experimental::funnel_agg {
    type State = FunnelAggTransState;

    const PARALLEL_SAFE: bool = true;

    fn transition(
        state: Option<State>,
        #[sql_type("integer")] handle: Handle,
        #[sql_type("text")] event: Option<String>,
        #[sql_type("timestamptz")] ts: TimestampTz,
    ) -> Option<State> {
        let event = match event {
            None => return state,
            Some(event) => event,
        };
        let mut state = state.unwrap_or_else(FunnelAggTransState::new);
        state.record(handle, event, ts.into());
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
        crate::do_deserialize!(bytes, FunnelAggTransState)
    }

    fn finally(state: Option<&mut State>) -> Option<FunnelAgg<'static>> {
        state.map(|s| {
            let mut events = vec![];
            let mut event_names = String::new();
            let mut packer = StringPacker::new(&mut event_names);
            {
                let mut by_handle = std::collections::HashMap::new();
                for event in s.drain(..) {
                    let entry = by_handle.entry(event.handle).or_insert_with(Vec::new);
                    entry.push((event.event, event.time));
                }
                for (handle, mut events_for_handle) in by_handle.drain() {
                    for (event_name, time) in events_for_handle.drain(..) {
                        let (name_beg, name_end) = packer.maybe_push(event_name);
                        events.push(FunnelEvent {
                            handle,
                            name_beg: name_beg as u32,
                            name_end: name_end as u32,
                            time,
                            ..Default::default()
                        });
                    }
                }
            }
            FunnelAgg::new(event_names, events)
        })
    }
}

// Intermediate state kept in postgres.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct FunnelAggTransState {
    events: Vec<TransEvent>,
}

impl FunnelAggTransState {
    fn new() -> Self {
        Self { events: vec![] }
    }

    fn record(&mut self, handle: Handle, event: String, time: i64) {
        self.events.push(TransEvent {
            handle,
            event,
            time,
        });
    }

    fn append(&mut self, other: &mut Self) {
        self.events.append(&mut other.events)
    }

    fn drain<R: std::ops::RangeBounds<usize>>(&mut self, range: R) -> std::vec::Drain<TransEvent> {
        self.events.drain(range)
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn consecutive(
    event1_name: String,
    event2_name: String,
    agg: FunnelAgg<'_>,
) -> impl std::iter::Iterator<Item = Handle> + '_ {
    // TODO Implement as Iterator rather than buffering results first.
    let mut result = vec![];
    struct Scratch {
        hit: bool,
        last_event_name: Option<String>,
    }
    let mut scratch = std::collections::HashMap::new();
    for event in agg.events.iter() {
        let scratch = scratch.entry(event.handle).or_insert(Scratch {
            hit: false,
            last_event_name: None,
        });
        if scratch.hit {
            continue;
        }
        let name = agg.event_name(&event);
        // First, check this event against event2_name if we already found event1.
        if let Some(last_event_name) = &scratch.last_event_name {
            if name == event2_name && last_event_name == &event1_name {
                result.push(event.handle);
                scratch.hit = true;
                continue;
            }
        }
        // Second, check this event against event1_name.  Second because we
        // may be looking for two occurrences of the same event, in which case
        // we need the last event1 time, not THIS one, for event2 checking.
        scratch.last_event_name = Some(name.to_owned());
    }
    result.into_iter()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn within_interval(
    event1_name: String,
    event2_name: String,
    interval: crate::raw::Interval,
    agg: FunnelAgg<'_>,
) -> impl std::iter::Iterator<Item = Handle> + '_ {
    // TODO Implement as Iterator rather than buffering results first.
    let mut result = vec![];
    struct Scratch {
        hit: bool,
        last_event1_time: Option<i64>,
    }
    let mut scratch = std::collections::HashMap::new();
    for event in agg.events.iter() {
        let scratch = scratch.entry(event.handle).or_insert(Scratch {
            hit: false,
            last_event1_time: None,
        });
        if scratch.hit {
            continue;
        }
        let name = agg.event_name(&event);
        // First, check this event against event2_name if we already found event1.
        if let Some(last_event1_time) = scratch.last_event1_time {
            if name == event2_name {
                let interval_ms = interval_to_ms(&TimestampTz::from(last_event1_time), &interval);
                if event.time - last_event1_time <= interval_ms {
                    result.push(event.handle);
                    scratch.hit = true;
                    continue;
                }
            }
        }
        // Second, check this event against event1_name.  Second because we
        // may be looking for two occurrences of the same event, in which case
        // we need the last event1 time, not THIS one, for event2 checking.
        if name == event1_name {
            scratch.last_event1_time = Some(event.time);
        }
    }
    result.into_iter()
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "into_values",
    schema = "toolkit_experimental"
)]
pub fn funnel_into_values(
    agg: FunnelAgg<'_>,
) -> impl std::iter::Iterator<
    Item = (
        name!(handle, Handle),
        name!(event, String),
        name!(time, TimestampTz),
    ),
> + '_ {
    let event_names: String = agg.names_as_str().to_owned();
    agg.events.clone().into_iter().map(move |event| {
        let beg = event.name_beg as usize;
        let end = event.name_end as usize;
        (
            event.handle,
            event_names[beg..end].to_owned(),
            event.time.into(),
        )
    })
}

type Handle = i32;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
struct TransEvent {
    handle: Handle,
    event: String,
    time: i64,
}

// TODO Move out of this file.
// TODO Share with state_agg which has an older duplicating form of this.
/// Pack strings into a single String buffer without repeating them.
/// Useful if many copies of a string need to be referenced, but does require
/// that two copies of each are stored while packing the buffer.
struct StringPacker<'a> {
    buffer: &'a mut String,
    index: std::collections::HashMap<String, (usize, usize)>,
}
impl<'a> StringPacker<'a> {
    pub fn new(buffer: &'a mut String) -> Self {
        Self {
            buffer,
            index: std::collections::HashMap::new(),
        }
    }

    pub fn maybe_push(&mut self, s: String) -> (usize, usize) {
        match self.index.entry(s) {
            std::collections::hash_map::Entry::Occupied(i) => *i.get(),
            std::collections::hash_map::Entry::Vacant(i) => {
                let beg = self.buffer.len();
                let end = beg + i.key().len();
                self.buffer.push_str(i.key());
                *i.insert((beg, end))
            }
        }
    }
}
