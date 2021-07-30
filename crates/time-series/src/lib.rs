
use serde::{Deserialize, Serialize, ser::SerializeStruct};

use flat_serialize_macro::FlatSerializable;

use std::{borrow::Cow, ffi::CStr};

#[derive(Clone, Copy, PartialEq, Debug, FlatSerializable)]
#[repr(C)]
pub struct TSPoint {
    pub ts: i64,
    pub val: f64,
}

#[derive(Debug, PartialEq)]
pub enum TSPointError {
    TimesEqualInterpolate,
}

impl TSPoint {
    pub fn interpolate_linear(&self, p2: &TSPoint, ts: i64) -> Result<f64, TSPointError> {
        if self.ts == p2.ts {
            return Err(TSPointError::TimesEqualInterpolate);
        }
        // using point slope form of a line iteratively y = y2 - y1 / (x2 - x1) * (x - x1) + y1
        let duration = (p2.ts - self.ts) as f64; // x2 - x1
        let dinterp = (ts - self.ts) as f64; // x - x1
        Ok((p2.val - self.val) * dinterp / duration + self.val)
    }
}

impl Serialize for TSPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        if serializer.is_human_readable() {
            // FIXME ugly hack to use postgres functions in an non-postgres library
            extern "C" {
                fn _ts_toolkit_encode_timestamptz(dt: i64, buf: &mut [u8; 128]);
            }
            let mut ts = [0; 128];
            unsafe {
                _ts_toolkit_encode_timestamptz(self.ts, &mut ts);
            }
            let end = ts.iter().position(|c| *c == 0).unwrap();
            let ts = CStr::from_bytes_with_nul(&ts[..end+1]).unwrap();
            let ts = ts.to_str().unwrap();
            let mut point = serializer.serialize_struct("TSPoint", 2)?;
            point.serialize_field("ts", &ts)?;
            point.serialize_field("val", &self.val)?;
            point.end()
        } else {
            let mut point = serializer.serialize_struct("TSPoint", 2)?;
            point.serialize_field("ts", &self.ts)?;
            point.serialize_field("val", &self.val)?;
            point.end()
        }
    }
}

impl<'de> Deserialize<'de> for TSPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {

        use std::fmt;
        use serde::de::{self, Visitor, SeqAccess, MapAccess};
        struct TsPointVisitor{ text_timestamp: bool }

        // FIXME ugly hack to use postgres functions in an non-postgres library
        extern "C" {
            // this is only going to be used to communicate with a rust lib we compile with this one
            #[allow(improper_ctypes)]
            fn _ts_toolkit_decode_timestamptz(text: &str) -> i64;
        }

        impl<'de> Visitor<'de> for TsPointVisitor {
            type Value = TSPoint;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct TSPoint")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<TSPoint, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let ts = if self.text_timestamp {
                    let text: &str = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    unsafe {
                        _ts_toolkit_decode_timestamptz(text)
                    }
                } else {
                    seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?
                };
                let val = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(TSPoint{ ts, val })
            }

            fn visit_map<V>(self, mut map: V) -> Result<TSPoint, V::Error>
            where
                V: MapAccess<'de>,
            {
                #[derive(Deserialize)]
                #[serde(field_identifier, rename_all = "lowercase")]
                enum Field { Ts, Val }
                let mut ts = None;
                let mut val = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Ts => {
                            if ts.is_some() {
                                return Err(de::Error::duplicate_field("ts"));
                            }
                            ts = if self.text_timestamp {
                                let text: &str = map.next_value()?;
                                unsafe {
                                    Some(_ts_toolkit_decode_timestamptz(text))
                                }
                            } else {
                                Some(map.next_value()?)
                            };
                        }
                        Field::Val => {
                            if val.is_some() {
                                return Err(de::Error::duplicate_field("val"));
                            }
                            val = Some(map.next_value()?);
                        }
                    }
                }
                let ts = ts.ok_or_else(|| de::Error::missing_field("ts"))?;
                let val = val.ok_or_else(|| de::Error::missing_field("val"))?;
                Ok(TSPoint{ ts, val })
            }
        }
        const FIELDS: &'static [&'static str] = &["ts", "val"];

        let visitor = TsPointVisitor { text_timestamp: deserializer.is_human_readable() };
        deserializer.deserialize_struct("TSPoint", FIELDS, visitor)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExplicitTimeSeries {
    pub ordered: bool,
    pub points: Vec<TSPoint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NormalTimeSeries {
    pub start_ts: i64,
    pub step_interval: i64,    // ts delta between values
    pub values: Vec<f64>
}

// Normal timeseries, but may be missing values.  First and last values are required.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GappyNormalTimeSeries {
    pub start_ts: i64,
    pub step_interval: i64,    // ts delta between values
    pub count: u64,            // num values + num gaps
    pub present: Vec<u64>,     // bitmap, 0 = gap...
    pub values: Vec<f64>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TimeSeries {
    Explicit(ExplicitTimeSeries),
    Normal(NormalTimeSeries),
    GappyNormal(GappyNormalTimeSeries),
}

pub enum TimeSeriesError {
    OrderedDataExpected,
    InsufficientDataToExtrapolate,
}

pub enum GapfillMethod {
    LOCF,
    Linear,
    Nearest,  // Defaults to left on tie
}

impl GapfillMethod {
    // Adds the given number of points to the end of a non-empty NormalTimeSeries
    fn fill_normalized_series_gap(&self, series: &mut NormalTimeSeries, points: i32, post_gap_val: f64) {
        assert!(!series.values.is_empty());
        let last_val = *series.values.last().unwrap();
        for i in 1..=points {
            match self {
                GapfillMethod::LOCF => series.values.push(last_val),
                GapfillMethod::Linear => series.values.push(last_val + (post_gap_val - last_val) * i as f64 / (points + 1) as f64),
                GapfillMethod::Nearest => series.values.push(if i <= (points + 1) / 2 {last_val} else {post_gap_val}),
            }
        }
    }
}

impl GapfillMethod {
    // Determine a value to the left of a given point or two (for linear) using the given gapfill method
    // TODO: this returns the first value for LOCF, which probabaly isn't correct, technically this function shouldn't be valid for LOCF,
    pub fn predict_left(&self, target_time: i64, first: TSPoint, second: Option<TSPoint>) -> TSPoint {
        TSPoint {
            ts: target_time,
            val: match self {
                GapfillMethod::LOCF => first.val,
                GapfillMethod::Nearest => first.val,
                GapfillMethod::Linear => {
                    let second = match second {
                        Some(v) => v,
                        None => panic!{"Unable to predict left point without two values to interpolate from"},
                    };
                    let slope = (first.val - second.val) / (first.ts - second.ts) as f64;
                    first.val - slope * (first.ts - target_time) as f64
                },
            }
        }
    }

    // Determine a value to the right of a given point or two (for linear) using the given gapfill method
    pub fn predict_right(&self, target_time: i64, last: TSPoint, penultimate: Option<TSPoint>) -> TSPoint {
        TSPoint {
            ts: target_time,
            val: match self {
                GapfillMethod::LOCF => last.val,
                GapfillMethod::Nearest => last.val,
                GapfillMethod::Linear => {
                    let penultimate = match penultimate {
                        Some(v) => v,
                        None => panic!{"Unable to predict right point without two values to interpolate from"},
                    };
                    let slope = (last.val - penultimate.val) / (last.ts - penultimate.ts) as f64;
                    last.val + slope * (target_time - last.ts) as f64
                },
            }
        }
    }

    // Given a target time and the immediate points to either side, provide the missing point
    pub fn gapfill(&self, target_time: i64, left: TSPoint, right: TSPoint) -> TSPoint {
        TSPoint {
            ts: target_time,
            val: match self {
                GapfillMethod::LOCF => left.val,
                GapfillMethod::Nearest => if target_time - left.ts <= right.ts - target_time {left.val} else {right.val},
                GapfillMethod::Linear => {
                    let slope = (right.val - left.val) / (right.ts - left.ts) as f64;
                    left.val + slope * (target_time - left.ts) as f64
                },
            }
        }
    }
}

impl ExplicitTimeSeries {
    pub fn sort(&mut self) {
        if !self.ordered {
            self.points.sort_unstable_by_key(|p| p.ts);
            self.ordered = true;
        }
    }

    // This function will normalize a time range by averaging the values in `downsample_interval`
    // sized buckets.  Any gaps will be filled via the given method and will use the downsampled
    // values as the relevant points for LOCF or interpolation.
    pub fn downsample_and_gapfill_to_normal_form(&self, downsample_interval: i64, gapfill_method: GapfillMethod) -> Result<NormalTimeSeries, TimeSeriesError> {
        if !self.ordered {
            return Err(TimeSeriesError::OrderedDataExpected);
        }
        if self.points.len() < 2 || self.points.last().unwrap().ts - self.points.first().unwrap().ts < downsample_interval {
            return Err(TimeSeriesError::InsufficientDataToExtrapolate);
        }

        let mut result = NormalTimeSeries {
            start_ts: self.points.first().unwrap().ts,
            step_interval: downsample_interval,
            values: Vec::<f64>::new(),
        };

        let mut bound = self.points.first().unwrap().ts + downsample_interval;
        let mut sum = 0.0;
        let mut count = 0;
        let mut gap_count = 0;
        for pt in self.points.iter() {
            if pt.ts < bound {
                sum += pt.val;
                count += 1;
            } else {
                assert!(count != 0);
                let new_val = sum / count as f64;
                // If we missed any intervals prior to the current one, fill in the gap here
                if gap_count != 0 {
                    gapfill_method.fill_normalized_series_gap(&mut result, gap_count, new_val);
                    gap_count = 0;
                }
                result.values.push(new_val);
                sum = pt.val;
                count = 1;
                bound += downsample_interval;
                // If the current point doesn't go in the bucket immediately following the one
                // we just created, update the bound until we find the correct bucket and track
                // the number of empty buckets we skip over
                while bound < pt.ts {
                    bound += downsample_interval;
                    gap_count += 1;
                }
            }
        }
        // This will handle the last interval, since we always exit the above loop in the middle
        // of accumulating an interval
        assert!(count > 0);
        let new_val = sum / count as f64;
        if gap_count != 0 {
            gapfill_method.fill_normalized_series_gap(&mut result, gap_count, new_val);
        }
        result.values.push(sum / count as f64);
        Ok(result)
    }
}

impl GappyNormalTimeSeries {
    pub fn is_present(&self, index: u64) -> bool {
        let outer = index / 64;
        let inner = index % 64;

        return self.present[outer as usize] & ((1 as u64) << inner) != 0;
    }

    pub fn add_gap(&mut self) {
        self.add_next_present(false);
    }

    pub fn has_next_value(&mut self) {
        self.add_next_present(true);
    }

    fn add_next_present(&mut self, is_present: bool) {
        let idx = self.count;
        let val = if is_present { 1 } else { 0 };
        self.count += 1;
        if idx % 64 == 0 {
            self.present.push(val);
        } else if is_present {
            self.present[(idx / 64) as usize] ^= 1 << (idx % 64);
        }
    }

    pub fn has_value(&self, time: i64) -> bool {
        if time < self.start_ts || (time - self.start_ts) % self.step_interval != 0 {
            return false;
        }
        let index = ((time - self.start_ts) / self.step_interval) as u64;
        if index > self.count {
            return false;
        }
        self.is_present(index)
    }

    pub fn fill_to(&mut self, time: i64) {
        let mut next = self.start_ts + self.count as i64 * self.step_interval;
        while next < time {
            self.add_gap();
            next += self.step_interval;
        }
        assert_eq!(next, time);
    }
}

#[derive(Clone)]
pub struct GappyNormalTimeSeriesIterator<'a> {
    container: &'a GappyNormalTimeSeries,
    next_time_idx: u64,
    next_value_idx: u64,
}

impl<'a> Iterator for GappyNormalTimeSeriesIterator<'a> {
    type Item = TSPoint;

    fn next(&mut self) -> Option<TSPoint> {
        if self.next_time_idx >= self.container.count {
            None
        } else {
            assert!(self.next_value_idx < self.container.values.len() as u64);
            while !self.container.is_present(self.next_time_idx) {
                self.next_time_idx += 1;
            }
            let val = self.container.values[self.next_value_idx as usize];
            let ts = self.container.start_ts + self.next_time_idx as i64 * self.container.step_interval;
            self.next_time_idx += 1;
            self.next_value_idx += 1;
            Some(TSPoint{ts, val})
        }
    }
}

impl TimeSeries {
    pub fn new_explicit_series() -> TimeSeries {
        TimeSeries::Explicit(
            ExplicitTimeSeries {
                ordered: true,
                points: vec![],
            }
        )
    }

    pub fn new_normal_series(start: TSPoint, interval: i64) -> TimeSeries {
        TimeSeries::Normal(
            NormalTimeSeries {
                start_ts: start.ts,
                step_interval: interval,
                values: vec![start.val]
            }
        )
    }

    pub fn new_gappy_normal_series(start: TSPoint, interval: i64) -> TimeSeries {
        TimeSeries::GappyNormal(
            GappyNormalTimeSeries {
                start_ts: start.ts,
                step_interval: interval,
                count: 1,
                values: vec![start.val],
                present: vec![1],
            }
        )
    }

    pub fn add_point(&mut self, point: TSPoint) {
        match self {
            TimeSeries::Explicit(series) => {
                series.ordered = series.points.is_empty() || series.ordered && point.ts >= series.points.last().unwrap().ts;
                series.points.push(point);
            },
            TimeSeries::Normal(normal) => {
                // TODO: return error rather than assert
                assert_eq!(normal.start_ts + normal.values.len() as i64 * normal.step_interval, point.ts);
                normal.values.push(point.val);
            },
            TimeSeries::GappyNormal(series) => {
                // TODO: return error rather than assert
                assert!(point.ts >= series.start_ts + (series.step_interval * series.count as i64) && (point.ts - series.start_ts) % series.step_interval == 0);
                series.fill_to(point.ts);
                series.has_next_value();
                series.values.push(point.val);
            }
        }
    }

    pub fn sort(&mut self) {
        match self {
            TimeSeries::Explicit(series) => {
                series.sort();
            },
            TimeSeries::Normal(_) => (),
            TimeSeries::GappyNormal(_) => (),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item=TSPoint> + '_> {
        match self {
            TimeSeries::Explicit(series) => Box::new(series.points.iter().cloned()),
            TimeSeries::Normal(NormalTimeSeries { start_ts, step_interval, values }) => {
                let mut next_ts = *start_ts;
                let iter = values.iter().cloned().map(move |val| {
                    let ts = next_ts;
                    next_ts += *step_interval;
                    TSPoint{ts, val}
                });
                Box::new(iter)
            },
            TimeSeries::GappyNormal(series) => {
                Box::new(GappyNormalTimeSeriesIterator {container: series, next_time_idx: 0, next_value_idx: 0})
            }
        }
    }

    pub fn num_vals(&self) -> usize {
        match &self {
            TimeSeries::Explicit(explicit) => explicit.points.len(),
            TimeSeries::Normal(normal) => normal.values.len(),
            TimeSeries::GappyNormal(normal) => normal.values.len(),
        }
    }

    // Combines two TimeSeries and returns the result.
    pub fn combine(first: &TimeSeries, second: &TimeSeries) -> TimeSeries {
        if first.num_vals() == 0 {
            return second.clone();
        }
        if second.num_vals() == 0 {
            return first.clone();
        }

        // If two explicit series are sorted and disjoint, return a sorted explicit series
        if let (TimeSeries::Explicit(first), TimeSeries::Explicit(second)) = (&first, &second) {
            if first.ordered && second.ordered {
                if first.points.last().unwrap().ts < second.points[0].ts {
                    let mut new = first.clone();
                    new.points.extend(second.points.iter());
                    return TimeSeries::Explicit(new);
                }

                if second.points.last().unwrap().ts < first.points[0].ts {
                    let mut new = second.clone();
                    new.points.extend(first.points.iter());
                    return TimeSeries::Explicit(new);
                }
            }
        };

        // If the series are adjacent normal series, combine them into a larger normal series
        let ordered = if let (TimeSeries::Normal(first), TimeSeries::Normal(second)) = (&first, &second) {
            if first.step_interval == second.step_interval {
                if second.start_ts == first.start_ts + first.values.len() as i64 * first.step_interval {
                    let mut new = first.clone();
                    new.values.extend(second.values.iter());
                    return TimeSeries::Normal(new);
                }
                if first.start_ts == second.start_ts + second.values.len() as i64 * second.step_interval {
                    let mut new = second.clone();
                    new.values.extend(first.values.iter());
                    return TimeSeries::Normal(new);
                }
            }

            first.start_ts + (first.values.len() - 1) as i64 * first.step_interval < second.start_ts
        } else {
            false
        };

        // In all other cases, just return a new explicit series containing all the points from both series
        let mut new = ExplicitTimeSeries{ordered, points: vec![]};
        new.points.extend(first.iter());
        new.points.extend(second.iter());
        TimeSeries::Explicit(new)
    }

    pub fn first(&self) -> Option<TSPoint> {
        if self.num_vals() == 0 {
            None
        } else {
            match self {
                TimeSeries::Explicit(series) => Some(series.points[0]),
                TimeSeries::Normal(NormalTimeSeries { start_ts, values, ..}) => Some(TSPoint{ts: *start_ts, val: values[0]}),
                TimeSeries::GappyNormal(GappyNormalTimeSeries { start_ts, values, ..}) => Some(TSPoint{ts: *start_ts, val: values[0]}),
            }
        }
    }

    pub fn last(&self) -> Option<TSPoint> {
        if self.num_vals() == 0 {
            None
        } else {
            match self {
                TimeSeries::Explicit(series) => Some(*series.points.last().unwrap()),
                TimeSeries::Normal(NormalTimeSeries { start_ts, step_interval, values }) => Some(TSPoint{ts: *start_ts + step_interval * (values.len() as i64 - 1), val: *values.last().unwrap()}),
                TimeSeries::GappyNormal(GappyNormalTimeSeries { start_ts, step_interval, values, count, .. }) => Some(TSPoint{ts: *start_ts + step_interval * (count - 1) as i64, val: *values.last().unwrap()}),
            }
        }
    }
}

impl<'a> From<&'a TimeSeries> for Cow<'a, [TSPoint]> {
    fn from (series : &'a TimeSeries) -> Cow<'a, [TSPoint]> {
        match series {
            TimeSeries::Explicit(series) => Cow::Borrowed(&series.points[..]),
            _ => unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_interpolate(){
        let p1 = TSPoint{ts: 1, val: 1.0};
        let p2 = TSPoint{ts: 3, val: 3.0};
        assert_eq!(p1.interpolate_linear(&p2, 2).unwrap(), 2.0);
        assert_eq!(p1.interpolate_linear(&p2, 3).unwrap(), 3.0);
        assert_eq!(p1.interpolate_linear(&p2, 4).unwrap(), 4.0);
        assert_eq!(p1.interpolate_linear(&p2, 0).unwrap(), 0.0);
        assert_eq!(p1.interpolate_linear(&p1, 2).unwrap_err(), TSPointError::TimesEqualInterpolate);
    }

    #[test]
    fn test_series_combine() {
        let a1 = TSPoint{ts: 1, val: 1.0};
        let a2 = TSPoint{ts: 3, val: 3.0};
        let a3 = TSPoint{ts: 4, val: 4.0};
        let a4 = TSPoint{ts: 7, val: 7.0};

        let b1 = TSPoint{ts: 2, val: 2.0};
        let b2 = TSPoint{ts: 5, val: 5.0};
        let b3 = TSPoint{ts: 8, val: 8.0};
        let b4 = TSPoint{ts: 6, val: 6.0};

        let mut a = TimeSeries::new_explicit_series();
        a.add_point(a1);
        a.add_point(a2);
        a.add_point(a3);
        a.add_point(a4);

        let mut b = TimeSeries::new_explicit_series();
        b.add_point(b1);
        b.add_point(b2);
        b.add_point(b3);
        b.add_point(b4);

        let c = TimeSeries::combine(&a, &b);
        assert_eq!(8, c.num_vals());

        let mut dup_check = 0;
        for point in c.iter() {
            assert!(point.ts > 0 && point.ts < 9);
            assert_eq!(point.ts as f64, point.val);
            assert!(1 << point.ts & dup_check == 0);
            dup_check |= 1 << point.ts;
        }
    }

    #[test]
    fn test_sorted_series_combine() {
        let mut a = TimeSeries::new_explicit_series();
        a.add_point(TSPoint{ts: 2, val: 2.0});
        a.add_point(TSPoint{ts: 5, val: 2.0});
        a.add_point(TSPoint{ts: 10, val: 2.0});
        a.add_point(TSPoint{ts: 15, val: 2.0});

        let mut b = TimeSeries::new_explicit_series();
        b.add_point(TSPoint{ts: 20, val: 2.0});
        b.add_point(TSPoint{ts: 25, val: 2.0});
        b.add_point(TSPoint{ts: 30, val: 2.0});
        b.add_point(TSPoint{ts: 35, val: 2.0});

        let mut c = TimeSeries::new_explicit_series();
        c.add_point(TSPoint{ts: 31, val: 2.0});
        c.add_point(TSPoint{ts: 36, val: 2.0});
        c.add_point(TSPoint{ts: 40, val: 2.0});
        c.add_point(TSPoint{ts: 45, val: 2.0});

        let ab = TimeSeries::combine(&a, &b);
        assert_eq!(8, ab.num_vals());
        assert!(if let TimeSeries::Explicit(inner) = ab {inner.ordered} else {false});

        let ca = TimeSeries::combine(&c, &a);
        assert_eq!(8, ca.num_vals());
        assert!(if let TimeSeries::Explicit(inner) = ca {inner.ordered} else {false});

        let bc = TimeSeries::combine(&b, &c);
        assert_eq!(8, bc.num_vals());
        assert!(!(if let TimeSeries::Explicit(inner) = bc {inner.ordered} else {false}));
    }

    #[test]
    fn test_normal_series_combine() {
        let a = TimeSeries::Normal(
            NormalTimeSeries {
                start_ts: 5,
                step_interval: 5,
                values: vec![1.0, 2.0, 3.0, 4.0]
            }
        );
        let b = TimeSeries::Normal(
            NormalTimeSeries {
                start_ts: 25,
                step_interval: 5,
                values: vec![5.0, 6.0, 7.0, 8.0]
            }
        );
        let c = TimeSeries::Normal(
            NormalTimeSeries {
                start_ts: 30,
                step_interval: 5,
                values: vec![6.0, 7.0, 8.0, 9.0]
            }
        );
        let d = TimeSeries::Normal(
            NormalTimeSeries {
                start_ts: 25,
                step_interval: 6,
                values: vec![5.0, 6.0, 7.0, 8.0]
            }
        );

        let ab = TimeSeries::combine(&a, &b);
        assert_eq!(8, ab.num_vals());
        assert!(matches!(ab, TimeSeries::Normal(_)));

        let ba = TimeSeries::combine(&b, &a);
        assert_eq!(8, ba.num_vals());
        assert!(matches!(ba, TimeSeries::Normal(_)));

        let ca = TimeSeries::combine(&c, &a);
        assert_eq!(8, ca.num_vals());
        assert!(!matches!(ca, TimeSeries::Normal(_)));

        let ad = TimeSeries::combine(&a, &d);
        assert_eq!(8, ad.num_vals());
        assert!(!matches!(ad, TimeSeries::Normal(_)));
        assert!(if let TimeSeries::Explicit(inner) = ad {inner.ordered} else {false});
    }
}
