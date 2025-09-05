use pgrx::iter::TableIterator;
use pgrx::*;

use crate::{
    accessors::{
        AccessorDeadRanges, AccessorDowntime, AccessorLiveAt, AccessorLiveRanges, AccessorNumGaps,
        AccessorNumLiveRanges, AccessorUptime,
    },
    aggregate_utils::in_aggregate_context,
    datum_utils::interval_to_ms,
    flatten,
    palloc::{Inner, InternalAsValue, ToInternal},
    pg_type,
    raw::{Interval, TimestampTz},
    ron_inout_funcs,
};

use std::cmp::{max, min};

mod accessors;

use accessors::{
    HeartbeatInterpolateAccessor, HeartbeatInterpolatedDowntimeAccessor,
    HeartbeatInterpolatedUptimeAccessor, HeartbeatTrimToAccessor,
};

const BUFFER_SIZE: usize = 1000; // How many values to absorb before consolidating

// Given the lack of a good range map class, or efficient predecessor operation on btrees,
// the trans state will simply collect points and then process them in batches
pub struct HeartbeatTransState {
    start: i64,
    end: i64,
    last: i64,
    interval_len: i64,
    buffer: Vec<i64>,
    liveness: Vec<(i64, i64)>, // sorted array of non-overlapping (start_time, end_time)
}

impl HeartbeatTransState {
    pub fn new(start: i64, end: i64, interval: i64) -> Self {
        assert!(end - start > interval, "all points passed to heartbeat agg must occur in the 'agg_duration' interval after 'agg_start'");
        HeartbeatTransState {
            start,
            end,
            last: i64::MIN,
            interval_len: interval,
            buffer: vec![],
            liveness: vec![],
        }
    }

    pub fn insert(&mut self, time: i64) {
        assert!(time >= self.start && time < self.end, "all points passed to heartbeat agg must occur in the 'agg_duration' interval after 'agg_start'");
        if self.buffer.len() >= BUFFER_SIZE {
            self.process_batch();
        }
        self.buffer.push(time);
    }

    pub fn process_batch(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.buffer.sort_unstable();

        if self.last < *self.buffer.last().unwrap() {
            self.last = *self.buffer.last().unwrap();
        }

        let mut new_intervals = vec![];

        let mut start = *self.buffer.first().unwrap();
        let mut bound = start + self.interval_len;

        for heartbeat in std::mem::take(&mut self.buffer).into_iter() {
            if heartbeat <= bound {
                bound = heartbeat + self.interval_len;
            } else {
                new_intervals.push((start, bound));
                start = heartbeat;
                bound = start + self.interval_len;
            }
        }
        new_intervals.push((start, bound));

        if self.liveness.is_empty() {
            std::mem::swap(&mut self.liveness, &mut new_intervals);
        } else {
            self.combine_intervals(new_intervals)
        }
    }

    // In general we shouldn't need to change these creation time parameters, but if
    // we're combining with another interval this may be necessary.
    fn extend_covered_interval(&mut self, new_start: i64, new_end: i64) {
        assert!(new_start <= self.start && new_end >= self.end); // this is guaranteed by the combine function
        self.start = new_start;

        // extend last range if able
        if self.end < new_end && self.last + self.interval_len > self.end {
            assert!(!self.liveness.is_empty()); // above condition should be impossible without liveness data

            let last_interval = self.liveness.last_mut().unwrap();
            last_interval.1 = min(self.last + self.interval_len, new_end);
        }
        self.end = new_end;
    }

    fn combine_intervals(&mut self, new_intervals: Vec<(i64, i64)>) {
        // Optimized path for nonoverlapping, ordered inputs
        if self.last < new_intervals.first().unwrap().0 {
            let mut new_intervals = new_intervals.into_iter();

            // Grab the first new interval to check for overlap with the existing data
            let first_new = new_intervals.next().unwrap();

            if self.liveness.last().unwrap().1 >= first_new.0 {
                // Note that the bound of the new interval must be >= the old bound
                self.liveness.last_mut().unwrap().1 = first_new.1;
            } else {
                self.liveness.push(first_new);
            }

            for val in new_intervals {
                self.liveness.push(val);
            }
            return;
        }

        let new_intervals = new_intervals.into_iter();
        let old_intervals = std::mem::take(&mut self.liveness).into_iter();

        // In the following while let block, test and control are used to track our two interval iterators.
        // We will swap them back and forth to try to keep control as the iterator which has provided the current bound.
        let mut test = new_intervals.peekable();
        let mut control = old_intervals.peekable();

        while let Some(interval) = if let Some((start1, _)) = control.peek() {
            if let Some((start2, _)) = test.peek() {
                let (start, mut bound) = if start1 < start2 {
                    control.next().unwrap()
                } else {
                    std::mem::swap(&mut test, &mut control);
                    control.next().unwrap()
                };

                while test.peek().is_some() && test.peek().unwrap().0 <= bound {
                    let (_, new_bound) = test.next().unwrap();
                    if new_bound > bound {
                        std::mem::swap(&mut test, &mut control);
                        bound = new_bound;
                    }
                }

                Some((start, bound))
            } else {
                control.next()
            }
        } else {
            test.next()
        } {
            self.liveness.push(interval)
        }
    }

    pub fn combine(&mut self, mut other: HeartbeatTransState) {
        assert!(self.interval_len == other.interval_len); // Nicer error would be nice here
        self.process_batch();
        other.process_batch();

        let min_start = min(self.start, other.start);
        let max_end = max(self.end, other.end);
        self.extend_covered_interval(min_start, max_end);
        other.extend_covered_interval(min_start, max_end);

        self.combine_intervals(other.liveness);
        self.last = max(self.last, other.last);
    }
}

#[cfg(any(test, feature = "pg_test"))]
impl HeartbeatTransState {
    pub fn get_buffer(&self) -> &Vec<i64> {
        &self.buffer
    }
    pub fn get_liveness(&self) -> &Vec<(i64, i64)> {
        &self.liveness
    }
}

pg_type! {
    #[derive(Debug)]
    struct HeartbeatAgg<'input>
    {
        start_time : i64,
        end_time : i64,
        last_seen : i64,
        interval_len : i64,
        num_intervals : u64,
        interval_starts : [i64; self.num_intervals],
        interval_ends : [i64; self.num_intervals],
    }
}

ron_inout_funcs!(HeartbeatAgg<'input>);

impl HeartbeatAgg<'_> {
    fn trim_to(self, start: Option<i64>, end: Option<i64>) -> HeartbeatAgg<'static> {
        if (start.is_some() && start.unwrap() < self.start_time)
            || (end.is_some() && end.unwrap() > self.end_time)
        {
            error!("Can not query beyond the original aggregate bounds");
        }

        let mut starts: Vec<i64> = vec![];
        let mut ends: Vec<i64> = vec![];
        for i in 0..self.num_intervals as usize {
            starts.push(self.interval_starts.slice()[i]);
            ends.push(self.interval_ends.slice()[i]);
        }

        let low_idx = if let Some(start) = start {
            let mut idx = 0;
            while idx < self.num_intervals as usize && ends[idx] < start {
                idx += 1;
            }
            if starts[idx] < start {
                starts[idx] = start;
            }
            idx
        } else {
            0
        };

        let mut new_last = None;
        let high_idx = if let Some(end) = end {
            if self.num_intervals > 0 {
                let mut idx = self.num_intervals as usize - 1;
                while idx > low_idx && starts[idx] > end {
                    idx -= 1;
                }
                new_last = Some(ends[idx] - self.interval_len);
                if ends[idx] > end {
                    if end < new_last.unwrap() {
                        new_last = Some(end);
                    }
                    ends[idx] = end;
                }
                idx
            } else {
                self.num_intervals as usize - 1
            }
        } else {
            self.num_intervals as usize - 1
        };

        unsafe {
            flatten!(HeartbeatAgg {
                start_time: start.unwrap_or(self.start_time),
                end_time: end.unwrap_or(self.end_time),
                last_seen: new_last.unwrap_or(self.last_seen),
                interval_len: self.interval_len,
                num_intervals: (high_idx - low_idx + 1) as u64,
                interval_starts: starts[low_idx..=high_idx].into(),
                interval_ends: ends[low_idx..=high_idx].into(),
            })
        }
    }

    fn sum_live_intervals(self) -> i64 {
        let starts = self.interval_starts.as_slice();
        let ends = self.interval_ends.as_slice();
        let mut sum = 0;
        for i in 0..self.num_intervals as usize {
            sum += ends[i] - starts[i];
        }
        sum
    }

    fn interpolate_start(&mut self, pred: &Self) {
        // only allow interpolation of non-overlapping ranges
        assert!(pred.end_time <= self.start_time);
        let pred_end = pred.last_seen + self.interval_len;

        if pred_end <= self.start_time {
            return;
        }

        // If first range already covers (start_time, pred_end) return
        if self
            .interval_starts
            .as_slice()
            .first()
            .filter(|v| **v == self.start_time)
            .is_some()
            && self
                .interval_ends
                .as_slice()
                .first()
                .filter(|v| **v >= pred_end)
                .is_some()
        {
            return;
        }

        if self
            .interval_starts
            .as_slice()
            .first()
            .filter(|v| **v <= pred_end)
            .is_some()
        {
            self.interval_starts.as_owned()[0] = self.start_time;
        } else {
            let start = self.start_time;
            self.interval_starts.as_owned().insert(0, start);
            self.interval_ends.as_owned().insert(0, pred_end);
            self.num_intervals += 1;
        }
    }
}

#[pg_extern]
pub fn live_ranges(
    agg: HeartbeatAgg<'static>,
) -> TableIterator<'static, (name!(start, TimestampTz), name!(end, TimestampTz))> {
    let starts = agg.interval_starts.clone();
    let ends = agg.interval_ends.clone();
    TableIterator::new(
        starts
            .into_iter()
            .map(|x| x.into())
            .zip(ends.into_iter().map(|x| x.into())),
    )
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_live_ranges(
    sketch: HeartbeatAgg<'static>,
    _accessor: AccessorLiveRanges,
) -> TableIterator<'static, (name!(start, TimestampTz), name!(end, TimestampTz))> {
    live_ranges(sketch)
}

#[pg_extern]
pub fn dead_ranges(
    agg: HeartbeatAgg<'static>,
) -> TableIterator<'static, (name!(start, TimestampTz), name!(end, TimestampTz))> {
    if agg.num_intervals == 0 {
        return TableIterator::new(std::iter::once((
            agg.start_time.into(),
            agg.end_time.into(),
        )));
    }

    // Dead ranges are the opposite of the intervals stored in the aggregate
    let mut starts = agg.interval_ends.clone().into_vec();
    let mut ends = agg.interval_starts.clone().into_vec();

    // Fix the first point depending on whether the aggregate starts in a live or dead range
    if ends[0] == agg.start_time {
        ends.remove(0);
    } else {
        starts.insert(0, agg.start_time);
    }

    // Fix the last point depending on whether the aggregate starts in a live or dead range
    if *starts.last().unwrap() == agg.end_time {
        starts.pop();
    } else {
        ends.push(agg.end_time);
    }

    TableIterator::new(
        starts
            .into_iter()
            .map(|x| x.into())
            .zip(ends.into_iter().map(|x| x.into())),
    )
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_dead_ranges(
    sketch: HeartbeatAgg<'static>,
    _accessor: AccessorDeadRanges,
) -> TableIterator<'static, (name!(start, TimestampTz), name!(end, TimestampTz))> {
    dead_ranges(sketch)
}

#[pg_extern]
pub fn uptime(agg: HeartbeatAgg<'static>) -> Interval {
    agg.sum_live_intervals().into()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_uptime(
    sketch: HeartbeatAgg<'static>,
    _accessor: AccessorUptime,
) -> Interval {
    uptime(sketch)
}

#[pg_extern]
pub fn interpolated_uptime(
    agg: HeartbeatAgg<'static>,
    pred: Option<HeartbeatAgg<'static>>,
) -> Interval {
    uptime(interpolate_heartbeat_agg(agg, pred))
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_interpolated_uptime(
    sketch: HeartbeatAgg<'static>,
    accessor: HeartbeatInterpolatedUptimeAccessor<'static>,
) -> Interval {
    interpolated_uptime(sketch, accessor.pred())
}

#[pg_extern]
pub fn downtime(agg: HeartbeatAgg<'static>) -> Interval {
    (agg.end_time - agg.start_time - agg.sum_live_intervals()).into()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_downtime(
    sketch: HeartbeatAgg<'static>,
    _accessor: AccessorDowntime,
) -> Interval {
    downtime(sketch)
}

#[pg_extern]
pub fn interpolated_downtime(
    agg: HeartbeatAgg<'static>,
    pred: Option<HeartbeatAgg<'static>>,
) -> Interval {
    downtime(interpolate_heartbeat_agg(agg, pred))
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_interpolated_downtime(
    sketch: HeartbeatAgg<'static>,
    accessor: HeartbeatInterpolatedDowntimeAccessor<'static>,
) -> Interval {
    interpolated_downtime(sketch, accessor.pred())
}

#[pg_extern]
pub fn live_at(agg: HeartbeatAgg<'static>, test: TimestampTz) -> bool {
    let test = i64::from(test);

    if test < agg.start_time || test > agg.end_time {
        error!("unable to test for liveness outside of a heartbeat_agg's covered range")
    }

    if agg.num_intervals == 0 {
        return false;
    }

    let mut start_iter = agg.interval_starts.iter().enumerate().peekable();
    while let Some((idx, val)) = start_iter.next() {
        if test < val {
            // Only possible if test shows up before first interval
            return false;
        }
        if let Some((_, next_val)) = start_iter.peek() {
            if test < *next_val {
                return test < agg.interval_ends.as_slice()[idx];
            }
        }
    }
    // Fall out the loop if test > start of last interval
    test < *agg.interval_ends.as_slice().last().unwrap()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_live_at(
    sketch: HeartbeatAgg<'static>,
    accessor: AccessorLiveAt,
) -> bool {
    let ts = TimestampTz(accessor.time.into());
    live_at(sketch, ts)
}

#[pg_extern(name = "interpolate")]
fn interpolate_heartbeat_agg(
    agg: HeartbeatAgg<'static>,
    pred: Option<HeartbeatAgg<'static>>,
) -> HeartbeatAgg<'static> {
    let mut r = agg.clone();
    if let Some(pred) = pred {
        r.interpolate_start(&pred);
    }
    r
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_interpolate(
    sketch: HeartbeatAgg<'static>,
    accessor: HeartbeatInterpolateAccessor<'static>,
) -> HeartbeatAgg<'static> {
    interpolate_heartbeat_agg(sketch, accessor.pred())
}

#[pg_extern]
pub fn num_live_ranges(agg: HeartbeatAgg<'static>) -> i64 {
    agg.num_intervals as i64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_num_live_ranges(
    agg: HeartbeatAgg<'static>,
    _accessor: AccessorNumLiveRanges,
) -> i64 {
    num_live_ranges(agg)
}

#[pg_extern]
pub fn num_gaps(agg: HeartbeatAgg<'static>) -> i64 {
    if agg.num_intervals == 0 {
        return 1;
    }
    let mut count = agg.num_intervals - 1;
    if agg.interval_starts.slice()[0] != agg.start_time {
        count += 1;
    }
    if agg.interval_ends.slice()[agg.num_intervals as usize - 1] != agg.end_time {
        count += 1;
    }
    count as i64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_num_gaps(agg: HeartbeatAgg<'static>, _accessor: AccessorNumGaps) -> i64 {
    num_gaps(agg)
}

#[pg_extern]
pub fn trim_to(
    agg: HeartbeatAgg<'static>,
    start: default!(Option<crate::raw::TimestampTz>, "NULL"),
    duration: default!(Option<crate::raw::Interval>, "NULL"),
) -> HeartbeatAgg<'static> {
    if let Some(start) = start {
        let end = duration.map(|intv| crate::datum_utils::ts_interval_sum_to_ms(&start, &intv));
        agg.trim_to(Some(i64::from(start)), end)
    } else {
        let end = duration.map(|intv| {
            crate::datum_utils::ts_interval_sum_to_ms(&TimestampTz::from(agg.start_time), &intv)
        });
        agg.trim_to(None, end)
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_heartbeat_agg_trim_to(
    agg: HeartbeatAgg<'static>,
    accessor: HeartbeatTrimToAccessor,
) -> HeartbeatAgg<'static> {
    let end = if accessor.end == 0 {
        None
    } else {
        Some(accessor.end)
    };
    agg.trim_to(Some(accessor.start), end)
}

impl From<HeartbeatAgg<'static>> for HeartbeatTransState {
    fn from(agg: HeartbeatAgg<'static>) -> Self {
        HeartbeatTransState {
            start: agg.start_time,
            end: agg.end_time,
            last: agg.last_seen,
            interval_len: agg.interval_len,
            buffer: vec![],
            liveness: agg
                .interval_starts
                .iter()
                .zip(agg.interval_ends.iter())
                .collect(),
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn heartbeat_trans(
    state: Internal,
    heartbeat: TimestampTz,
    start: TimestampTz,
    length: Interval,
    liveness_duration: Interval,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    heartbeat_trans_inner(
        unsafe { state.to_inner() },
        heartbeat,
        start,
        length,
        liveness_duration,
        fcinfo,
    )
    .internal()
}
pub fn heartbeat_trans_inner(
    state: Option<Inner<HeartbeatTransState>>,
    heartbeat: TimestampTz,
    start: TimestampTz,
    length: Interval,
    liveness_duration: Interval,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<HeartbeatTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = state.unwrap_or_else(|| {
                let length = interval_to_ms(&start, &length);
                let interval = interval_to_ms(&start, &liveness_duration);
                let start = start.into();
                HeartbeatTransState::new(start, start + length, interval).into()
            });
            state.insert(heartbeat.into());
            Some(state)
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn heartbeat_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<HeartbeatAgg<'static>> {
    heartbeat_final_inner(unsafe { state.to_inner() }, fcinfo)
}
pub fn heartbeat_final_inner(
    state: Option<Inner<HeartbeatTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<HeartbeatAgg<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            state.map(|mut s| {
                s.process_batch();
                let (starts, mut ends): (Vec<i64>, Vec<i64>) =
                    s.liveness.clone().into_iter().unzip();

                // Trim last interval to end of aggregate's range
                if let Some(last) = ends.last_mut() {
                    if *last > s.end {
                        *last = s.end;
                    }
                }

                flatten!(HeartbeatAgg {
                    start_time: s.start,
                    end_time: s.end,
                    last_seen: s.last,
                    interval_len: s.interval_len,
                    num_intervals: starts.len() as u64,
                    interval_starts: starts.into(),
                    interval_ends: ends.into(),
                })
            })
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn heartbeat_rollup_trans(
    state: Internal,
    value: Option<HeartbeatAgg<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    heartbeat_rollup_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn heartbeat_rollup_trans_inner(
    state: Option<Inner<HeartbeatTransState>>,
    value: Option<HeartbeatAgg<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<HeartbeatTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (a, None) => a,
            (None, Some(a)) => Some(HeartbeatTransState::from(a).into()),
            (Some(mut a), Some(b)) => {
                a.combine(b.into());
                Some(a)
            }
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE heartbeat_agg(\n\
        heartbeat TIMESTAMPTZ, agg_start TIMESTAMPTZ, agg_duration INTERVAL, heartbeat_liveness INTERVAL\n\
    ) (\n\
        sfunc = heartbeat_trans,\n\
        stype = internal,\n\
        finalfunc = heartbeat_final\n\
    );\n\
",
    name = "heartbeat_agg",
    requires = [
        heartbeat_trans,
        heartbeat_final,
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        HeartbeatAgg\n\
    ) (\n\
        sfunc = heartbeat_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = heartbeat_final\n\
    );\n\
",
    name = "heartbeat_agg_rollup",
    requires = [heartbeat_rollup_trans, heartbeat_final,],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    pub fn test_heartbeat_trans_state() {
        let mut state = HeartbeatTransState::new(0, 500, 10);
        state.insert(100);
        state.insert(200);
        state.insert(250);
        state.insert(220);
        state.insert(210);
        state.insert(300);

        assert_eq!(state.get_buffer().len(), 6);

        state.process_batch();
        assert_eq!(state.get_buffer().len(), 0);

        let mut it = state.get_liveness().iter();
        assert_eq!(*it.next().unwrap(), (100, 110));
        assert_eq!(*it.next().unwrap(), (200, 230));
        assert_eq!(*it.next().unwrap(), (250, 260));
        assert_eq!(*it.next().unwrap(), (300, 310));
        assert!(it.next().is_none());

        state.insert(400);
        state.insert(350);
        state.process_batch();

        let mut it = state.get_liveness().iter();
        assert_eq!(*it.next().unwrap(), (100, 110));
        assert_eq!(*it.next().unwrap(), (200, 230));
        assert_eq!(*it.next().unwrap(), (250, 260));
        assert_eq!(*it.next().unwrap(), (300, 310));
        assert_eq!(*it.next().unwrap(), (350, 360));
        assert_eq!(*it.next().unwrap(), (400, 410));
        assert!(it.next().is_none());

        state.insert(80);
        state.insert(190);
        state.insert(210);
        state.insert(230);
        state.insert(240);
        state.insert(310);
        state.insert(395);
        state.insert(408);
        state.process_batch();

        let mut it = state.get_liveness().iter();
        assert_eq!(*it.next().unwrap(), (80, 90));
        assert_eq!(*it.next().unwrap(), (100, 110));
        assert_eq!(*it.next().unwrap(), (190, 260));
        assert_eq!(*it.next().unwrap(), (300, 320));
        assert_eq!(*it.next().unwrap(), (350, 360));
        assert_eq!(*it.next().unwrap(), (395, 418));
        assert!(it.next().is_none());
    }

    #[pg_test]
    pub fn test_heartbeat_agg() {
        Spi::connect_mut(|client| {
            client.update("SET TIMEZONE to UTC", None, &[]).unwrap();

            client
                .update("CREATE TABLE liveness(heartbeat TIMESTAMPTZ)", None, &[])
                .unwrap();

            client
                .update(
                    "INSERT INTO liveness VALUES
                ('01-01-2020 0:2:20 UTC'),
                ('01-01-2020 0:10 UTC'),
                ('01-01-2020 0:17 UTC'),
                ('01-01-2020 0:30 UTC'),
                ('01-01-2020 0:35 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:35 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:50:30 UTC'),
                ('01-01-2020 1:00 UTC'),
                ('01-01-2020 1:08 UTC'),
                ('01-01-2020 1:18 UTC'),
                ('01-01-2020 1:28 UTC'),
                ('01-01-2020 1:38:01 UTC'),
                ('01-01-2020 1:40 UTC'),
                ('01-01-2020 1:40:01 UTC'),
                ('01-01-2020 1:50:01 UTC'),
                ('01-01-2020 1:57 UTC'),
                ('01-01-2020 1:59:50 UTC')
            ",
                    None,
                    &[],
                )
                .unwrap();

            let mut result = client.update(
                "SELECT live_ranges(heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness", None, &[]).unwrap();

            let mut arrow_result = client.update(
                "SELECT (heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') -> live_ranges())::TEXT
                FROM liveness", None, &[]).unwrap();

            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 00:02:20+00\",\"2020-01-01 00:27:00+00\")"
            );
            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 00:30:00+00\",\"2020-01-01 00:50:00+00\")"
            );
            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 00:50:30+00\",\"2020-01-01 01:38:00+00\")"
            );
            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 01:38:01+00\",\"2020-01-01 02:00:00+00\")"
            );
            assert!(result.next().is_none());
            assert!(arrow_result.next().is_none());

            let mut result = client.update(
                "SELECT dead_ranges(heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness", None, &[]).unwrap();

            let mut arrow_result = client.update(
                "SELECT (heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') -> dead_ranges())::TEXT
                FROM liveness", None, &[]).unwrap();

            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 00:00:00+00\",\"2020-01-01 00:02:20+00\")"
            );
            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 00:27:00+00\",\"2020-01-01 00:30:00+00\")"
            );
            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 00:50:00+00\",\"2020-01-01 00:50:30+00\")"
            );
            let test = arrow_result.next().unwrap()[1]
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                test
            );
            assert_eq!(
                test,
                "(\"2020-01-01 01:38:00+00\",\"2020-01-01 01:38:01+00\")"
            );
            assert!(result.next().is_none());
            assert!(arrow_result.next().is_none());

            let result = client
                .update(
                    "SELECT uptime(heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            assert_eq!("01:54:09", result);

            let result = client.update(
                "SELECT (heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') -> uptime())::TEXT
                FROM liveness", None, &[]).unwrap().first().get_one::<String>().unwrap().unwrap();
            assert_eq!("01:54:09", result);

            let result = client
                .update(
                    "SELECT downtime(heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            assert_eq!("00:05:51", result);

            let result = client.update(
                "SELECT (heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') -> downtime())::TEXT
                FROM liveness", None, &[]).unwrap().first().get_one::<String>().unwrap().unwrap();
            assert_eq!("00:05:51", result);

            let (result1, result2, result3) =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT live_at(agg, '01-01-2020 00:01:00 UTC')::TEXT, 
                    live_at(agg, '01-01-2020 00:05:00 UTC')::TEXT,
                    live_at(agg, '01-01-2020 00:30:00 UTC')::TEXT FROM agg", None, &[])
                .unwrap().first()
                .get_three::<String, String, String>().unwrap();

            let result4 =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT live_at(agg, '01-01-2020 01:38:00 UTC')::TEXT FROM agg", None, &[])
                .unwrap().first()
                .get_one::<String>().unwrap();

            assert_eq!(result1.unwrap(), "false"); // outside ranges
            assert_eq!(result2.unwrap(), "true"); // inside ranges
            assert_eq!(result3.unwrap(), "true"); // first point of range
            assert_eq!(result4.unwrap(), "false"); // last point of range

            let (result1, result2, result3) =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT (agg -> live_at('01-01-2020 00:01:00 UTC'))::TEXT, 
                    (agg -> live_at('01-01-2020 00:05:00 UTC'))::TEXT,
                    (agg -> live_at('01-01-2020 00:30:00 UTC'))::TEXT FROM agg", None, &[])
                .unwrap().first()
                .get_three::<String, String, String>().unwrap();

            let result4 =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT (agg -> live_at('01-01-2020 01:38:00 UTC'))::TEXT FROM agg", None, &[])
                .unwrap().first()
                .get_one::<String>().unwrap();

            assert_eq!(result1.unwrap(), "false"); // outside ranges
            assert_eq!(result2.unwrap(), "true"); // inside ranges
            assert_eq!(result3.unwrap(), "true"); // first point of range
            assert_eq!(result4.unwrap(), "false"); // last point of range

            let (result1, result2) =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT num_live_ranges(agg), num_gaps(agg) FROM agg", None, &[])
                .unwrap().first()
                .get_two::<i64, i64>().unwrap();

            assert_eq!(result1.unwrap(), 4);
            assert_eq!(result2.unwrap(), 4);

            let (result1, result2) =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT agg->num_live_ranges(), agg->num_gaps() FROM agg", None, &[])
                .unwrap().first()
                .get_two::<i64, i64>().unwrap();

            assert_eq!(result1.unwrap(), 4);
            assert_eq!(result2.unwrap(), 4);
        })
    }

    #[pg_test]
    pub fn test_heartbeat_rollup() {
        Spi::connect_mut(|client| {
            client.update("SET TIMEZONE to UTC", None, &[]).unwrap();

            client
                .update(
                    "CREATE TABLE heartbeats(time timestamptz, batch timestamptz)",
                    None,
                    &[],
                )
                .unwrap();

            client.update(
                "INSERT INTO heartbeats VALUES
                    ('01-01-2020 3:02:20 UTC'::timestamptz, '01-01-2020 3:00:00 UTC'::timestamptz),
                    ('01-01-2020 3:03:10 UTC'::timestamptz, '01-01-2020 3:00:00 UTC'::timestamptz),
                    ('01-01-2020 3:04:07 UTC'::timestamptz, '01-01-2020 3:00:00 UTC'::timestamptz),
                    ('01-01-2020 7:19:20 UTC'::timestamptz, '01-01-2020 7:00:00 UTC'::timestamptz),
                    ('01-01-2020 7:39:20 UTC'::timestamptz, '01-01-2020 7:00:00 UTC'::timestamptz),
                    ('01-01-2020 7:59:20 UTC'::timestamptz, '01-01-2020 7:00:00 UTC'::timestamptz),
                    ('01-01-2020 8:00:10 UTC'::timestamptz, '01-01-2020 8:00:00 UTC'::timestamptz),
                    ('01-01-2020 8:59:10 UTC'::timestamptz, '01-01-2020 8:00:00 UTC'::timestamptz),
                    ('01-01-2020 23:34:20 UTC'::timestamptz, '01-01-2020 23:00:00 UTC'::timestamptz),
                    ('01-01-2020 23:37:20 UTC'::timestamptz, '01-01-2020 23:00:00 UTC'::timestamptz),
                    ('01-01-2020 23:38:05 UTC'::timestamptz, '01-01-2020 23:00:00 UTC'::timestamptz),
                    ('01-01-2020 23:39:00 UTC'::timestamptz, '01-01-2020 23:00:00 UTC'::timestamptz)",
                None,
                &[],
            ).unwrap();

            let result = client
                .update(
                    "WITH aggs AS (
                    SELECT heartbeat_agg(time, batch, '1h', '1m')
                    FROM heartbeats 
                    GROUP BY batch
                ) SELECT rollup(heartbeat_agg)::TEXT FROM aggs",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            assert_eq!("(version:1,start_time:631162800000000,end_time:631238400000000,last_seen:631237140000000,interval_len:60000000,num_intervals:7,interval_starts:[631162940000000,631178360000000,631179560000000,631180760000000,631184350000000,631236860000000,631237040000000],interval_ends:[631163107000000,631178420000000,631179620000000,631180870000000,631184410000000,631236920000000,631237200000000])", result);
        })
    }

    #[pg_test]
    pub fn test_heartbeat_combining_rollup() {
        Spi::connect_mut(|client| {
            client.update("SET TIMEZONE to UTC", None, &[]).unwrap();

            client
                .update("CREATE TABLE aggs(agg heartbeatagg)", None, &[])
                .unwrap();

            client
                .update(
                    "INSERT INTO aggs SELECT heartbeat_agg(hb, '01-01-2020 UTC', '1h', '10m')
                FROM (VALUES
                    ('01-01-2020 0:2:20 UTC'::timestamptz),
                    ('01-01-2020 0:10 UTC'::timestamptz),
                    ('01-01-2020 0:17 UTC'::timestamptz),
                    ('01-01-2020 0:30 UTC'::timestamptz),
                    ('01-01-2020 0:35 UTC'::timestamptz),
                    ('01-01-2020 0:40 UTC'::timestamptz),
                    ('01-01-2020 0:50:30 UTC'::timestamptz)
                ) AS _(hb)",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "INSERT INTO aggs SELECT heartbeat_agg(hb, '01-01-2020 0:30 UTC', '1h', '10m')
                    FROM (VALUES
                    ('01-01-2020 0:35 UTC'::timestamptz),
                    ('01-01-2020 0:40 UTC'::timestamptz),
                    ('01-01-2020 0:40 UTC'::timestamptz),
                    ('01-01-2020 1:08 UTC'::timestamptz),
                    ('01-01-2020 1:18 UTC'::timestamptz)
                ) AS _(hb)",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "INSERT INTO aggs SELECT heartbeat_agg(hb, '01-01-2020 1:00 UTC', '1h', '10m')
                FROM (VALUES
                    ('01-01-2020 1:00 UTC'::timestamptz),
                    ('01-01-2020 1:28 UTC'::timestamptz),
                    ('01-01-2020 1:38:01 UTC'::timestamptz),
                    ('01-01-2020 1:40 UTC'::timestamptz),
                    ('01-01-2020 1:40:01 UTC'::timestamptz),
                    ('01-01-2020 1:50:01 UTC'::timestamptz),
                    ('01-01-2020 1:57 UTC'::timestamptz),
                    ('01-01-2020 1:59:50 UTC'::timestamptz)
                ) AS _(hb)",
                    None,
                    &[],
                )
                .unwrap();

            let mut result = client
                .update(
                    "SELECT dead_ranges(rollup(agg))::TEXT
                FROM aggs",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:00:00+00\",\"2020-01-01 00:02:20+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:27:00+00\",\"2020-01-01 00:30:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:50:00+00\",\"2020-01-01 00:50:30+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 01:38:00+00\",\"2020-01-01 01:38:01+00\")"
            );
            assert!(result.next().is_none());
        });
    }

    #[pg_test]
    pub fn test_heartbeat_trim_to() {
        Spi::connect_mut(|client| {
            client.update("SET TIMEZONE to UTC", None, &[]).unwrap();

            client
                .update("CREATE TABLE liveness(heartbeat TIMESTAMPTZ)", None, &[])
                .unwrap();

            client
                .update(
                    "INSERT INTO liveness VALUES
                ('01-01-2020 0:2:20 UTC'),
                ('01-01-2020 0:10 UTC'),
                ('01-01-2020 0:17 UTC'),
                ('01-01-2020 0:30 UTC'),
                ('01-01-2020 0:35 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:35 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:50:30 UTC'),
                ('01-01-2020 1:00 UTC'),
                ('01-01-2020 1:08 UTC'),
                ('01-01-2020 1:18 UTC'),
                ('01-01-2020 1:28 UTC'),
                ('01-01-2020 1:38:01 UTC'),
                ('01-01-2020 1:40 UTC'),
                ('01-01-2020 1:40:01 UTC'),
                ('01-01-2020 1:50:01 UTC'),
                ('01-01-2020 1:57 UTC'),
                ('01-01-2020 1:59:50 UTC')
            ",
                    None,
                    &[],
                )
                .unwrap();

            let (result1, result2, result3) =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness),
                    trimmed AS (SELECT trim_to(agg, '01-01-2020 0:30 UTC', '1h') AS agg FROM agg)
                    SELECT uptime(agg)::TEXT, num_gaps(agg), live_at(agg, '01-01-2020 0:50:25 UTC')::TEXT FROM trimmed", None, &[])
                .unwrap().first()
                .get_three::<String, i64, String>().unwrap();

            assert_eq!(result1.unwrap(), "00:59:30");
            assert_eq!(result2.unwrap(), 1);
            assert_eq!(result3.unwrap(), "false");

            let (result1, result2, result3) =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness),
                    trimmed AS (SELECT trim_to(agg, duration=>'30m') AS agg FROM agg)
                    SELECT uptime(agg)::TEXT, num_gaps(agg), live_at(agg, '01-01-2020 0:20:25 UTC')::TEXT FROM trimmed", None, &[])
                .unwrap().first()
                .get_three::<String, i64, String>().unwrap();

            assert_eq!(result1.unwrap(), "00:24:40");
            assert_eq!(result2.unwrap(), 2);
            assert_eq!(result3.unwrap(), "true");

            let (result1, result2, result3) =
                client.update(
                    "WITH agg AS (SELECT heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT agg -> trim_to('01-01-2020 1:40:00 UTC'::timestamptz) -> num_gaps(),
                    (agg -> trim_to('01-01-2020 00:50:00 UTC'::timestamptz, '30s') -> uptime())::TEXT,
                    agg -> trim_to('01-01-2020 00:28:00 UTC'::timestamptz, '22m15s') -> num_live_ranges() FROM agg", None, &[])
                .unwrap().first()
                .get_three::<i64, String, i64>().unwrap();

            assert_eq!(result1.unwrap(), 0);
            assert_eq!(result2.unwrap(), "00:00:00");
            assert_eq!(result3.unwrap(), 1);
        });
    }

    #[pg_test]
    pub fn test_heartbeat_agg_interpolation() {
        Spi::connect_mut(|client| {
            client.update("SET TIMEZONE to UTC", None, &[]).unwrap();

            client
                .update(
                    "CREATE TABLE liveness(heartbeat TIMESTAMPTZ, start TIMESTAMPTZ)",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "INSERT INTO liveness VALUES
                ('01-01-2020 0:2:20 UTC', '01-01-2020 0:0 UTC'),
                ('01-01-2020 0:10 UTC', '01-01-2020 0:0 UTC'),
                ('01-01-2020 0:17 UTC', '01-01-2020 0:0 UTC'),
                ('01-01-2020 0:30 UTC', '01-01-2020 0:30 UTC'),
                ('01-01-2020 0:35 UTC', '01-01-2020 0:30 UTC'),
                ('01-01-2020 0:40 UTC', '01-01-2020 0:30 UTC'),
                ('01-01-2020 0:35 UTC', '01-01-2020 0:30 UTC'),
                ('01-01-2020 0:40 UTC', '01-01-2020 0:30 UTC'),
                ('01-01-2020 0:40 UTC', '01-01-2020 0:30 UTC'),
                ('01-01-2020 0:50:30 UTC', '01-01-2020 0:30 UTC'),
                ('01-01-2020 1:00:30 UTC', '01-01-2020 1:00 UTC'),
                ('01-01-2020 1:08 UTC', '01-01-2020 1:00 UTC'),
                ('01-01-2020 1:18 UTC', '01-01-2020 1:00 UTC'),
                ('01-01-2020 1:28 UTC', '01-01-2020 1:00 UTC'),
                ('01-01-2020 1:38:01 UTC', '01-01-2020 1:30 UTC'),
                ('01-01-2020 1:40 UTC', '01-01-2020 1:30 UTC'),
                ('01-01-2020 1:40:01 UTC', '01-01-2020 1:30 UTC'),
                ('01-01-2020 1:50:01 UTC', '01-01-2020 1:30 UTC'),
                ('01-01-2020 1:57 UTC', '01-01-2020 1:30 UTC'),
                ('01-01-2020 1:59:50 UTC', '01-01-2020 1:30 UTC')
            ",
                    None,
                    &[],
                )
                .unwrap();

            let mut result = client
                .update(
                    "WITH s AS (
                    SELECT start,
                        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg 
                    FROM liveness 
                    GROUP BY start),
                t AS (
                    SELECT start,
                        interpolate(agg, LAG (agg) OVER (ORDER BY start)) AS agg 
                    FROM s)
                SELECT downtime(agg)::TEXT FROM t;",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:05:20"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:30"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:00"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:01"
            );
            assert!(result.next().is_none());

            let mut result = client
                .update(
                    "WITH s AS (
                    SELECT start,
                        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg 
                    FROM liveness 
                    GROUP BY start),
                t AS (
                    SELECT start,
                        interpolate(agg, LAG (agg) OVER (ORDER BY start)) AS agg 
                    FROM s)
                SELECT live_ranges(agg)::TEXT FROM t;",
                    None,
                    &[],
                )
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:02:20+00\",\"2020-01-01 00:27:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:30:00+00\",\"2020-01-01 00:50:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:50:30+00\",\"2020-01-01 01:00:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 01:00:00+00\",\"2020-01-01 01:30:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 01:30:00+00\",\"2020-01-01 01:38:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 01:38:01+00\",\"2020-01-01 02:00:00+00\")"
            );
            assert!(result.next().is_none());

            let mut result = client
                .update(
                    "WITH s AS (
                    SELECT start,
                        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg 
                    FROM liveness 
                    GROUP BY start),
                t AS (
                    SELECT start,
                        agg -> interpolate(LAG (agg) OVER (ORDER BY start)) AS agg 
                    FROM s)
                SELECT live_ranges(agg)::TEXT FROM t;",
                    None,
                    &[],
                )
                .unwrap();
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:02:20+00\",\"2020-01-01 00:27:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:30:00+00\",\"2020-01-01 00:50:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 00:50:30+00\",\"2020-01-01 01:00:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 01:00:00+00\",\"2020-01-01 01:30:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 01:30:00+00\",\"2020-01-01 01:38:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "(\"2020-01-01 01:38:01+00\",\"2020-01-01 02:00:00+00\")"
            );
            assert!(result.next().is_none());

            let mut result = client
                .update(
                    "WITH s AS (
                    SELECT start,
                        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg 
                    FROM liveness 
                    GROUP BY start)
                SELECT interpolated_uptime(agg, LAG (agg) OVER (ORDER BY start))::TEXT
                FROM s",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:24:40"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:29:30"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:30:00"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:29:59"
            );
            assert!(result.next().is_none());

            let mut result = client
                .update(
                    "WITH s AS (
                    SELECT start,
                        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg 
                    FROM liveness 
                    GROUP BY start)
                SELECT (agg -> interpolated_uptime(LAG (agg) OVER (ORDER BY start)))::TEXT
                FROM s",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:24:40"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:29:30"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:30:00"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:29:59"
            );
            assert!(result.next().is_none());

            let mut result = client
                .update(
                    "WITH s AS (
                    SELECT start,
                        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg 
                    FROM liveness 
                    GROUP BY start)
                SELECT interpolated_downtime(agg, LAG (agg) OVER (ORDER BY start))::TEXT
                FROM s",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:05:20"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:30"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:00"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:01"
            );
            assert!(result.next().is_none());

            let mut result = client
                .update(
                    "WITH s AS (
                    SELECT start,
                        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg 
                    FROM liveness 
                    GROUP BY start)
                SELECT (agg -> interpolated_downtime(LAG (agg) OVER (ORDER BY start)))::TEXT
                FROM s",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:05:20"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:30"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:00"
            );
            assert_eq!(
                result.next().unwrap()[1]
                    .value::<String>()
                    .unwrap()
                    .unwrap(),
                "00:00:01"
            );
            assert!(result.next().is_none());
        })
    }

    #[pg_test]
    fn test_heartbeat_agg_text_io() {
        Spi::connect_mut(|client| {
            client.update("SET TIMEZONE to UTC", None, &[]).unwrap();

            client
                .update("CREATE TABLE liveness(heartbeat TIMESTAMPTZ)", None, &[])
                .unwrap();

            client
                .update(
                    "INSERT INTO liveness VALUES
                ('01-01-2020 0:2:20 UTC'),
                ('01-01-2020 0:10 UTC'),
                ('01-01-2020 0:17 UTC')
            ",
                    None,
                    &[],
                )
                .unwrap();

            let output = client
                .update(
                    "SELECT heartbeat_agg(heartbeat, '01-01-2020', '30m', '5m')::TEXT
                    FROM liveness;",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();

            let expected = "(version:1,start_time:631152000000000,end_time:631153800000000,last_seen:631153020000000,interval_len:300000000,num_intervals:3,interval_starts:[631152140000000,631152600000000,631153020000000],interval_ends:[631152440000000,631152900000000,631153320000000])";

            assert_eq!(output, Some(expected.into()));

            let estimate = client
                .update(
                    &format!("SELECT uptime('{expected}'::heartbeatagg)::TEXT"),
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(estimate.unwrap().as_str(), "00:15:00");
        });
    }

    #[pg_test]
    fn test_heartbeat_agg_byte_io() {
        use std::ptr;

        // Create a heartbeat agg from 0 to 250 with intervals from 40-50, 60-85, and 100-110
        let state = heartbeat_trans_inner(
            None,
            40.into(),
            0.into(),
            250.into(),
            10.into(),
            ptr::null_mut(),
        );
        let state = heartbeat_trans_inner(
            state,
            60.into(),
            0.into(),
            250.into(),
            10.into(),
            ptr::null_mut(),
        );
        let state = heartbeat_trans_inner(
            state,
            65.into(),
            0.into(),
            250.into(),
            10.into(),
            ptr::null_mut(),
        );
        let state = heartbeat_trans_inner(
            state,
            75.into(),
            0.into(),
            250.into(),
            10.into(),
            ptr::null_mut(),
        );
        let state = heartbeat_trans_inner(
            state,
            100.into(),
            0.into(),
            250.into(),
            10.into(),
            ptr::null_mut(),
        );

        let agg = heartbeat_final_inner(state, ptr::null_mut())
            .expect("failed to build finalized heartbeat_agg");
        let serial = agg.to_pg_bytes();

        let expected = [
            128, 1, 0, 0, // header
            1, // version
            0, 0, 0, // padding
            0, 0, 0, 0, 0, 0, 0, 0, // start_time
            250, 0, 0, 0, 0, 0, 0, 0, // end_time
            100, 0, 0, 0, 0, 0, 0, 0, // last_seen
            10, 0, 0, 0, 0, 0, 0, 0, // interval_len
            3, 0, 0, 0, 0, 0, 0, 0, // num_intervals
            40, 0, 0, 0, 0, 0, 0, 0, // interval_starts[0]
            60, 0, 0, 0, 0, 0, 0, 0, // interval_starts[1]
            100, 0, 0, 0, 0, 0, 0, 0, // interval_starts[2]
            50, 0, 0, 0, 0, 0, 0, 0, // interval_ends[0]
            85, 0, 0, 0, 0, 0, 0, 0, // interval_ends[1]
            110, 0, 0, 0, 0, 0, 0, 0, // interval_ends[2]
        ];
        assert_eq!(serial, expected);
    }

    #[pg_test]
    fn test_rollup_overlap() {
        Spi::connect_mut(|client| {
            client.update("SET TIMEZONE to UTC", None, &[]).unwrap();

            client
                .update(
                    "CREATE TABLE poc(ts TIMESTAMPTZ, batch TIMESTAMPTZ)",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "INSERT INTO poc VALUES
                    ('1-1-2020 0:50 UTC', '1-1-2020 0:00 UTC'),
                    ('1-1-2020 1:10 UTC', '1-1-2020 0:00 UTC'),
                    ('1-1-2020 1:00 UTC', '1-1-2020 1:00 UTC')",
                    None,
                    &[],
                )
                .unwrap();

            let output = client
                .update(
                    "WITH rollups AS (
                        SELECT heartbeat_agg(ts, batch, '2h', '20m') 
                        FROM poc 
                        GROUP BY batch 
                        ORDER BY batch
                    )
                    SELECT live_ranges(rollup(heartbeat_agg))::TEXT 
                    FROM rollups",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();

            let expected = "(\"2020-01-01 00:50:00+00\",\"2020-01-01 01:30:00+00\")";

            assert_eq!(output, Some(expected.into()));
        });
    }
}
