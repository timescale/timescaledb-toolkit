
use pgx::*;
use asap::*;
use pg_sys::{Datum, TimestampTz};
use serde::{Deserialize, Serialize};
use std::slice;

use crate::{
    aggregate_utils::in_aggregate_context, flatten, json_inout_funcs, palloc::Internal, pg_type,
};

use flat_serialize::*;

use time_weighted_average::tspoint::TSPoint;

// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
mod timescale_analytics_experimental {
    pub(crate) use super::*;
    extension_sql!(r#"
        CREATE SCHEMA IF NOT EXISTS timescale_analytics_experimental;
    "#);
}

// This is included for debug purposes and probably should not leave experimental
#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn asap_smooth_raw(
    data: Vec<f64>,
    resolution: i32,
) -> Vec<f64> {
    asap_smooth(&data, resolution as u32)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExplicitTimeSeries {
    ordered: bool,
    points: Vec<TSPoint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NormalTimeSeries {
    start_ts: i64,
    step_interval: i64,    // ts delta between values 
    values: Vec<f64>
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
}

// TODO: Can we have a single time-series object which can store either an
// explicit or normal timeseries (without being stupidly inefficient)
extension_sql!(r#"
CREATE TYPE timescale_analytics_experimental.NormalizedTimeSeries;
"#);

pg_type! {
    #[derive(Debug)]
    struct NormalizedTimeSeries {
        start_ts: i64,
        step_interval: i64,
        num_vals: u64,  // required to be aligned
        values: [f64; self.num_vals],
    }
}

json_inout_funcs!(NormalizedTimeSeries);

impl<'input> NormalizedTimeSeries<'input> {
    #[allow(dead_code)]
    fn to_normal_time_series(&self) -> NormalTimeSeries {
        NormalTimeSeries {
            start_ts: *self.start_ts,
            step_interval: *self.step_interval,
            values: self.values.to_vec(),
        }
    }

    fn from_normal_time_series(series: &NormalTimeSeries) -> NormalizedTimeSeries<'input> {
        unsafe {
            flatten!(
                NormalizedTimeSeries {
                    start_ts: &series.start_ts,
                    step_interval: &series.step_interval,
                    num_vals: &(series.values.len() as u64),
                    values: &series.values,
                }
            )
        }
    }
}

extension_sql!(r#"
CREATE OR REPLACE FUNCTION
    timescale_analytics_experimental.NormalizedTimeSeries_in(cstring)
RETURNS timescale_analytics_experimental.NormalizedTimeSeries
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C
AS 'MODULE_PATHNAME', 'normalizedtimeseries_in_wrapper';

CREATE OR REPLACE FUNCTION
    timescale_analytics_experimental.NormalizedTimeSeries_out(timescale_analytics_experimental.NormalizedTimeSeries)
RETURNS CString
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C
AS 'MODULE_PATHNAME', 'normalizedtimeseries_out_wrapper';

CREATE TYPE timescale_analytics_experimental.NormalizedTimeSeries (
    INTERNALLENGTH = variable,
    INPUT = timescale_analytics_experimental.NormalizedTimeSeries_in,
    OUTPUT = timescale_analytics_experimental.NormalizedTimeSeries_out,
    STORAGE = extended
);
"#);

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn unnest_series(
    series: timescale_analytics_experimental::NormalizedTimeSeries,
) -> impl std::iter::Iterator<Item = (name!(time,TimestampTz),name!(value,f64))> + '_ {
    (0..*series.num_vals).map(move |i| {
        let num_steps = i as i64;
        let step_interval = *series.step_interval;
        (*series.start_ts + num_steps * step_interval, series.values[i as usize])
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ASAPTransState {
    ts: TimeSeries,
    resolution: i32,
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn asap_trans(
    state: Option<Internal<ASAPTransState>>,
    ts: Option<pg_sys::TimestampTz>,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<ASAPTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint { ts, val },
            };

            match state {
                None => {
                    Some(ASAPTransState {
                            ts: TimeSeries::Explicit(
                                ExplicitTimeSeries {
                                    ordered: true,
                                    points: vec![p],
                                },
                            ),
                            resolution
                        }.into()
                    )
                }
                Some(mut s) => {
                    s.ts.add_point(p);
                    Some(s)
                }
            }
        })
    }
}

fn find_downsample_interval(series: &ExplicitTimeSeries, resolution: i64) -> i64 {
    assert!(series.ordered);
    
    // First candidate is simply the total range divided into even size buckets
    let candidate = (series.points.last().unwrap().ts - series.points.first().unwrap().ts) / resolution;

    // Problem with this approach is ASAP appears to deliver much rougher graphs if buckets
    // don't contain an equal number of points.  We try to adjust for this by truncating the
    // downsample_interval to a multiple of the average delta, unfortunately this is very
    // susceptible to gaps in the data.  So instead of the average delta, we use the median.
    let mut diffs = vec!(0; (series.points.len() - 1) as usize);
    for i in 1..series.points.len() as usize {
        diffs[i-1] = series.points[i].ts - series.points[i-1].ts;
    }
    diffs.sort();
    let median = diffs[diffs.len() / 2];
    candidate / median * median  // Truncate candidate to a multiple of median
}

#[pg_extern(schema = "timescale_analytics_experimental")]
fn asap_final(
    state: Option<Internal<ASAPTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<timescale_analytics_experimental::NormalizedTimeSeries<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state.clone(),
            };

            if let TimeSeries::Explicit(mut series) = state.ts {
                series.sort();

                // In following the ASAP reference implementation, we only downsample if the number
                // of points is at least twice the resolution.  Otherwise we keep the number of
                // points, but still normalize them to equal sized buckets.
                let normal = if series.points.len() >= 2 * state.resolution as usize {
                    let downsample_interval = find_downsample_interval(&series, state.resolution as i64);
                    series.downsample_and_gapfill_to_normal_form(downsample_interval, GapfillMethod::Linear)
                } else {
                    series.downsample_and_gapfill_to_normal_form((series.points.last().unwrap().ts - series.points.first().unwrap().ts) / series.points.len() as i64, GapfillMethod::Linear)
                };
                let mut normal = match normal {
                    Ok(series) => series,
                    Err(TimeSeriesError::InsufficientDataToExtrapolate) => panic!("Not enough data to generate a smoothed representation"),
                    Err(_) => unreachable!()
                };

                // Drop the last value to match the reference implementation
                normal.values.pop();

                let mut result = NormalTimeSeries {start_ts: normal.start_ts,
                    step_interval: 0,
                    values: asap_smooth(&normal.values, state.resolution as u32)
                };
                // Set the step interval for the asap result so that it covers the same interval
                // as the passed in data
                result.step_interval = normal.step_interval * normal.values.len() as i64 / result.values.len() as i64;
                Some(NormalizedTimeSeries::from_normal_time_series(&result))
            } else {
                panic!("Unexpected timeseries format encountered");
            }
        })
    }
}


// Aggregate on only values (assumes aggregation over ordered normalized timestamp)
extension_sql!(r#"
CREATE AGGREGATE timescale_analytics_experimental.asap_smooth(ts TIMESTAMPTZ, value DOUBLE PRECISION, resolution INT) (
    sfunc = timescale_analytics_experimental.asap_trans,
    stype = internal,
    finalfunc = timescale_analytics_experimental.asap_final
);
"#);

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_asap() {
        Spi::execute(|client| {
            client.select("CREATE TABLE asap_test (date timestamptz, value DOUBLE PRECISION)", None, None);

            // Create a table with some cyclic data
            client.select("insert into asap_test select '2020-1-1 UTC'::timestamptz + make_interval(days=>foo), 10 + 5 * cos(foo) from generate_series(0,1000) foo", None, None);
            // Gap from [1001,1040] then continue cycle
            client.select("insert into asap_test select '2020-1-1 UTC'::timestamptz + make_interval(days=>foo), 10 + 5 * cos(foo) from generate_series(1041,2000) foo", None, None);
            // Values in [2001,2200] are 2 less than normal
            client.select("insert into asap_test select '2020-1-1 UTC'::timestamptz + make_interval(days=>foo), 8 + 5 * cos(foo) from generate_series(2001,2200) foo", None, None);
            // And fill out to 3000
            client.select("insert into asap_test select '2020-1-1 UTC'::timestamptz + make_interval(days=>foo), 10 + 5 * cos(foo) from generate_series(2201,3000) foo", None, None);

            // Smoothing to resolution 100 gives us 95 points so our hole should be around index 32-33
            // and our decreased values should be around 64-72.  However, since the output is
            // rolling averages, expect these values to impact the results around these ranges as well.

            client.select("create table asap_vals as SELECT * FROM timescale_analytics_experimental.unnest_series((SELECT timescale_analytics_experimental.asap_smooth(date, value, 100) FROM asap_test ))", None, None);
            
            let sanity = client.select("SELECT COUNT(*) FROM asap_vals", None, None).first()
                .get_one::<i32>().unwrap();
            assert_eq!(sanity, 95);

            // First check that our smoothed values away from our impacted ranges are about 10
            let test_val = client
                .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 5", None, None)
                .first()
                .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() < 0.05);
            let test_val = client
                .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 20", None, None)
                .first()
                .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() < 0.05);
            let test_val = client
                .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 55", None, None)
                .first()
                .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() < 0.05);
            let test_val = client
                .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 85", None, None)
                .first()
                .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() < 0.05);

            // There's not too much we can assume about our gap, since it's only one or two data point at our resolution, and they'll be filled with the linear interpolation of the left and right sides and then taken as part of a moving average with the surrounding points.  We will just check that the values are a bit away from 10 around this range.
            let test_val = client
            .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 29", None, None)
            .first()
            .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() > 0.1);
            let test_val = client
            .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 33", None, None)
            .first()
            .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() > 0.1);

            // Finally check that our points near our decreased range are significantly lower.  We don't expect these to necessarily get down to 8 due to the rolling average, but they should be closer to 8 than 10 in the middle of the range.
            let test_val = client
            .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 68", None, None)
            .first()
            .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() > (8.0 - test_val).abs());
            let test_val = client
            .select("SELECT value FROM asap_vals ORDER BY time LIMIT 1 OFFSET 70", None, None)
            .first()
            .get_one::<f64>().unwrap();
            assert!((10.0 - test_val).abs() > (8.0 - test_val).abs());
        });
    }
}
