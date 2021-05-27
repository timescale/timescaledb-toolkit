
use serde::{Deserialize, Serialize};

use std::borrow::Cow;

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TimeSeries {
    Explicit(ExplicitTimeSeries),
    Normal(NormalTimeSeries)
}

pub enum TimeSeriesError {
    OrderedDataExpected,
    InsufficientDataToExtrapolate,
}

pub enum GapfillMethod {
    LOCF,
    Linear,
}

impl GapfillMethod {
    // Adds the given number of points to the end of a non-empty NormalTimeSeries
    fn fill_normalized_series_gap(&self, series: &mut NormalTimeSeries, points: i32, post_gap_val: f64) {
        assert!(!series.values.is_empty());
        let last_val = *series.values.last().unwrap();
        for i in 1..=points {
            match self {
                GapfillMethod::LOCF => series.values.push(last_val),
                GapfillMethod::Linear => series.values.push(last_val + (post_gap_val - last_val) * i as f64 / (points + 1) as f64)
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

impl TimeSeries {
    pub fn new_explicit_series() -> TimeSeries {
        TimeSeries::Explicit(
            ExplicitTimeSeries {
                ordered: true,
                points: vec![],
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
            }
        }
    }

    pub fn sort(&mut self) {
        match self {
            TimeSeries::Explicit(series) => {
                series.sort();
            },
            TimeSeries::Normal(_) => ()
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
}
