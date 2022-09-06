use serde::{Deserialize, Serialize};
use stats_agg::{stats2d::StatsSummary2D, XYPair};
use std::fmt;
use tspoint::TSPoint;

pub mod range;

#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq, Eq)]
pub enum CounterError {
    OrderError,
    BoundsInvalid,
}

// TODO Intent is for this to be immutable with mutations going through (and
//  internal consistency protected by) the builders below.  But, we allow raw
//  access to the extension to allow it to (de)serialize, so the separation is
//  but a fiction for now.  If the only consequence of corruption is
//  nonsensical results rather than unsound behavior, garbage in garbage out.
//  But much better if we can validate at deserialization.  We can do that in
//  the builder if we want.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MetricSummary {
    // TODO invariants?
    pub first: TSPoint,
    pub second: TSPoint,
    pub penultimate: TSPoint,
    pub last: TSPoint,
    // Invariants:
    // - num_changes > 0 if num_resets > 0
    // - num_resets > 0 if num_changes > 0
    // - reset_sum > 0 if num_resets > 0
    // - num_resets > 0 if reset_sum > 0
    pub reset_sum: f64,
    pub num_resets: u64,
    pub num_changes: u64,
    // TODO Protect from deserialization?  Is there any risk other than giving
    //  nonsensical results?  If so, maybe it's fine to just accept garbage
    //  out upon garbage in.
    pub stats: StatsSummary2D,
    // TODO See TODOs in I64Range about protecting from deserialization.
    pub bounds: Option<range::I64Range>,
}

// Note that this can lose fidelity with the timestamp, but it would only lose it in the microseconds,
// this is likely okay in most applications. However, if you need better regression analysis at the subsecond level,
// you can always subtract a common near value from all your times, then add it back in, the regression analysis will be unchanged.
// Note that convert the timestamp into seconds rather than microseconds here so that the slope and any other regression analysis, is done on a per-second basis.
// For instance, the slope will be the per-second slope, not the per-microsecond slope. The x intercept value will need to be converted back to microseconds so you get a timestamp out.
fn ts_to_xy(pt: TSPoint) -> XYPair {
    XYPair {
        x: to_seconds(pt.ts as f64),
        y: pt.val,
    }
}

fn to_seconds(t: f64) -> f64 {
    t / 1_000_000_f64 // by default postgres timestamps have microsecond precision
}

/// MetricSummary tracks monotonically increasing counters that may reset, ie every time the value decreases
/// it is treated as a reset of the counter and the previous value is added to the "true value" of the
/// counter at that timestamp.
impl MetricSummary {
    pub fn new(pt: &TSPoint, bounds: Option<range::I64Range>) -> MetricSummary {
        let mut n = MetricSummary {
            first: *pt,
            second: *pt,
            penultimate: *pt,
            last: *pt,
            reset_sum: 0.0,
            num_resets: 0,
            num_changes: 0,
            stats: StatsSummary2D::new(),
            bounds,
        };
        n.stats.accum(ts_to_xy(*pt)).unwrap();
        n
    }

    fn reset(&mut self, incoming: &TSPoint) {
        if incoming.val < self.last.val {
            self.reset_sum += self.last.val;
            self.num_resets += 1;
        }
    }

    // expects time-ordered input
    fn add_point(&mut self, incoming: &TSPoint) -> Result<(), CounterError> {
        if incoming.ts < self.last.ts {
            return Err(CounterError::OrderError);
        }
        //TODO: test this
        if incoming.ts == self.last.ts {
            // if two points are equal we only use the first we see
            // see discussion at https://github.com/timescale/timescaledb-toolkit/discussions/65
            return Ok(());
        }
        // right now we treat a counter reset that goes to exactly zero as a change (not sure that's correct, but it seems defensible)
        // These values are not rounded, so direct comparison is valid.
        if incoming.val != self.last.val {
            self.num_changes += 1;
        }
        if self.first == self.second {
            self.second = *incoming;
        }
        self.penultimate = self.last;
        self.last = *incoming;
        let mut incoming_xy = ts_to_xy(*incoming);
        incoming_xy.y += self.reset_sum;
        self.stats.accum(incoming_xy).unwrap();
        Ok(())
    }

    fn single_value(&self) -> bool {
        self.last == self.first
    }

    // combining can only happen for disjoint time ranges
    fn combine(&mut self, incoming: &MetricSummary) -> Result<(), CounterError> {
        // this requires that self comes before incoming in time order
        if self.last.ts >= incoming.first.ts {
            return Err(CounterError::OrderError);
        }

        // These values are not rounded, so direct comparison is valid.
        if self.last.val != incoming.first.val {
            self.num_changes += 1;
        }

        if incoming.single_value() {
            self.penultimate = self.last;
        } else {
            self.penultimate = incoming.penultimate;
        }
        if self.single_value() {
            self.second = incoming.first;
        }
        let mut stats = incoming.stats;
        // have to offset based on our reset_sum, including the amount we added based on any resets that happened at the boundary (but before we add in the incoming reset_sum)
        stats
            .offset(XYPair {
                x: 0.0,
                y: self.reset_sum,
            })
            .unwrap();
        self.last = incoming.last;
        self.reset_sum += incoming.reset_sum;
        self.num_resets += incoming.num_resets;
        self.num_changes += incoming.num_changes;

        self.stats = self.stats.combine(stats).unwrap();
        self.bounds_extend(incoming.bounds);
        Ok(())
    }

    pub fn time_delta(&self) -> f64 {
        to_seconds((self.last.ts - self.first.ts) as f64)
    }

    pub fn delta(&self) -> f64 {
        self.last.val + self.reset_sum - self.first.val
    }

    pub fn rate(&self) -> Option<f64> {
        if self.single_value() {
            return None;
        }
        Some(self.delta() / self.time_delta())
    }

    pub fn idelta_left(&self) -> f64 {
        //check for counter reset
        if self.second.val >= self.first.val {
            self.second.val - self.first.val
        } else {
            self.second.val // counter reset assumes it reset at the previous point, so we just return the second point
        }
    }

    pub fn idelta_right(&self) -> f64 {
        //check for counter reset
        if self.last.val >= self.penultimate.val {
            self.last.val - self.penultimate.val
        } else {
            self.last.val
        }
    }

    pub fn irate_left(&self) -> Option<f64> {
        if self.single_value() {
            None
        } else {
            Some(self.idelta_left() / to_seconds((self.second.ts - self.first.ts) as f64))
        }
    }

    pub fn irate_right(&self) -> Option<f64> {
        if self.single_value() {
            None
        } else {
            Some(self.idelta_right() / to_seconds((self.last.ts - self.penultimate.ts) as f64))
        }
    }

    pub fn bounds_valid(&self) -> bool {
        match self.bounds {
            None => true, // unbounded contains everything
            Some(b) => b.contains(self.last.ts) && b.contains(self.first.ts),
        }
    }

    fn bounds_extend(&mut self, in_bounds: Option<range::I64Range>) {
        match (self.bounds, in_bounds) {
            (None, _) => self.bounds = in_bounds,
            (_, None) => {}
            (Some(mut a), Some(b)) => {
                a.extend(&b);
                self.bounds = Some(a);
            }
        };
    }

    // based on:  https://github.com/timescale/promscale_extension/blob/d51a0958442f66cb78d38b584a10100f0d278298/src/lib.rs#L208,
    // which is based on:     // https://github.com/prometheus/prometheus/blob/e5ffa8c9a08a5ee4185271c8c26051ddc1388b7a/promql/functions.go#L59
    pub fn prometheus_delta(&self) -> Result<Option<f64>, CounterError> {
        if self.bounds.is_none() || !self.bounds_valid() || self.bounds.unwrap().has_infinite() {
            return Err(CounterError::BoundsInvalid);
        }
        //must have at least 2 values
        if self.single_value() || self.bounds.unwrap().is_singleton() {
            //technically, the is_singleton check is redundant, it's included for clarity (any singleton bound that is valid can only be one point)
            return Ok(None);
        }

        let mut result_val = self.delta();

        // all calculated durations in seconds in Prom implementation, so we'll do that here.
        // we can unwrap all of the bounds accesses as they are guaranteed to be there from the checks above
        let mut duration_to_start =
            to_seconds((self.first.ts - self.bounds.unwrap().left.unwrap()) as f64);

        /* bounds stores [L,H), but Prom takes the duration using the inclusive range [L, H-1ms]. Subtract an extra ms, ours is in microseconds. */
        let duration_to_end =
            to_seconds((self.bounds.unwrap().right.unwrap() - self.last.ts - 1_000) as f64);
        let sampled_interval = self.time_delta();
        let avg_duration_between_samples = sampled_interval / (self.stats.n - 1) as f64; // don't have to worry about divide by zero because we know we have at least 2 values from the above.

        // we don't want to extrapolate to negative counter values, so we calculate the duration to the zero point of the counter (based on what we know here) and set that as duration_to_start if it's smaller than duration_to_start
        if result_val > 0.0 && self.first.val >= 0.0 {
            let duration_to_zero = sampled_interval * (self.first.val / result_val);
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero;
            }
        }

        // If the first/last samples are close to the boundaries of the range,
        // extrapolate the result. This is as we expect that another sample
        // will exist given the spacing between samples we've seen thus far,
        // with an allowance for noise.
        // Otherwise, we extrapolate to one half the avg distance between samples...
        // this was empirically shown to be good for certain things and was discussed at length in: https://github.com/prometheus/prometheus/pull/1161

        let extrapolation_threshold = avg_duration_between_samples * 1.1;
        let mut extrapolate_to_interval = sampled_interval;

        if duration_to_start < extrapolation_threshold {
            extrapolate_to_interval += duration_to_start
        } else {
            extrapolate_to_interval += avg_duration_between_samples / 2.0
        }

        if duration_to_end < extrapolation_threshold {
            extrapolate_to_interval += duration_to_end
        } else {
            extrapolate_to_interval += avg_duration_between_samples / 2.0
        }
        result_val *= extrapolate_to_interval / sampled_interval;
        Ok(Some(result_val))
    }

    pub fn prometheus_rate(&self) -> Result<Option<f64>, CounterError> {
        let delta = self.prometheus_delta()?;
        if delta.is_none() {
            return Ok(None);
        }
        let delta = delta.unwrap();
        let bounds = self.bounds.unwrap(); // if we got through delta without error then we have bounds
                                           /* bounds stores [L,H), but Prom takes the duration using the inclusive range [L, H-1ms]. So subtract an extra ms from the duration*/
        let duration = bounds.duration().unwrap() - 1_000;
        if duration <= 0 {
            return Ok(None); // if we have a total duration under a ms, it's less than prom could deal with so we return none.
        }
        Ok(Some(delta / to_seconds(duration as f64))) // don't have to deal with 0 case because that is checked in delta as well (singleton)
    }
}

impl fmt::Display for CounterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            CounterError::OrderError => write!(
                f,
                "out of order points: points must be submitted in time-order"
            ),
            CounterError::BoundsInvalid => write!(f, "cannot calculate delta without valid bounds"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct GaugeSummaryBuilder(MetricSummary);

impl GaugeSummaryBuilder {
    pub fn new(pt: &TSPoint, bounds: Option<range::I64Range>) -> Self {
        Self(MetricSummary::new(pt, bounds))
    }

    /// expects time-ordered input
    pub fn add_point(&mut self, incoming: &TSPoint) -> Result<(), CounterError> {
        self.0.add_point(incoming)
    }

    /// combining can only happen for disjoint time ranges
    pub fn combine(&mut self, incoming: &MetricSummary) -> Result<(), CounterError> {
        self.0.combine(incoming)
    }

    pub fn set_bounds(&mut self, bounds: Option<range::I64Range>) {
        self.0.bounds = bounds;
    }

    pub fn build(self) -> MetricSummary {
        self.0
    }

    pub fn first(&self) -> &TSPoint {
        &self.0.first
    }

    // TODO build method should check validity rather than caller
    pub fn bounds_valid(&self) -> bool {
        self.0.bounds_valid()
    }
}

impl From<MetricSummary> for GaugeSummaryBuilder {
    fn from(summary: MetricSummary) -> Self {
        Self(summary)
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct CounterSummaryBuilder(MetricSummary);

impl CounterSummaryBuilder {
    pub fn new(pt: &TSPoint, bounds: Option<range::I64Range>) -> Self {
        Self(MetricSummary::new(pt, bounds))
    }

    /// expects time-ordered input
    pub fn add_point(&mut self, incoming: &TSPoint) -> Result<(), CounterError> {
        self.0.reset(incoming);
        self.0.add_point(incoming)
    }

    /// combining can only happen for disjoint time ranges
    pub fn combine(&mut self, incoming: &MetricSummary) -> Result<(), CounterError> {
        self.0.reset(&incoming.first);
        self.0.combine(incoming)
    }

    pub fn set_bounds(&mut self, bounds: Option<range::I64Range>) {
        self.0.bounds = bounds;
    }

    pub fn build(self) -> MetricSummary {
        self.0
    }

    pub fn first(&self) -> &TSPoint {
        &self.0.first
    }

    // TODO build method should check validity rather than caller
    pub fn bounds_valid(&self) -> bool {
        self.0.bounds_valid()
    }
}

impl From<MetricSummary> for CounterSummaryBuilder {
    fn from(summary: MetricSummary) -> Self {
        Self(summary)
    }
}
