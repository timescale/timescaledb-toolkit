
use pgx::*;
use asap::*;
use serde::{Deserialize, Serialize};

use crate::{
    aggregate_utils::in_aggregate_context, palloc::{Internal, InternalAsValue, Inner, ToInternal},
};

use time_series::TSPoint;

use crate::time_series::{Timevector, TimevectorData, SeriesType};

// This is included for debug purposes and probably should not leave experimental
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn asap_smooth_raw(
    data: Vec<f64>,
    resolution: i32,
) -> Vec<f64> {
    asap_smooth(&data, resolution as u32)
}

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
mod toolkit_experimental {
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ASAPTransState {
    ts: Vec<TSPoint>,
    sorted: bool,
    resolution: i32,
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn asap_trans(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    asap_trans_internal(unsafe{ state.to_inner() }, ts, val, resolution, fcinfo).internal()
}
pub fn asap_trans_internal(
    state: Option<Inner<ASAPTransState>>,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<ASAPTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint { ts: ts.into(), val },
            };

            match state {
                None => {
                    Some(ASAPTransState {
                            ts: vec![p],
                            sorted: true,
                            resolution,
                        }.into()
                    )
                }
                Some(mut s) => {
                    s.add_point(p);
                    Some(s)
                }
            }
        })
    }
}

impl ASAPTransState {
    fn add_point(&mut self, point: TSPoint) {
        self.ts.push(point);
        if let Some(window) = self.ts.windows(2).last() {
            if window[0].ts > window[1].ts {
                self.sorted = false
            }
        }
    }
}

fn find_downsample_interval(points: &[TSPoint], resolution: i64) -> i64 {
    // debug_assert!(points.is_sorted_by_key(|p| p.ts));

    // First candidate is simply the total range divided into even size buckets
    let candidate = (points.last().unwrap().ts - points.first().unwrap().ts) / resolution;

    // Problem with this approach is ASAP appears to deliver much rougher graphs if buckets
    // don't contain an equal number of points.  We try to adjust for this by truncating the
    // downsample_interval to a multiple of the average delta, unfortunately this is very
    // susceptible to gaps in the data.  So instead of the average delta, we use the median.
    let mut diffs = vec![0; (points.len() - 1) as usize];
    for i in 1..points.len() as usize {
        diffs[i-1] = points[i].ts - points[i-1].ts;
    }
    diffs.sort_unstable();
    let median = diffs[diffs.len() / 2];
    candidate / median * median  // Truncate candidate to a multiple of median
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn asap_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_series::toolkit_experimental::Timevector<'static>> {
    asap_final_inner(unsafe{ state.to_inner() }, fcinfo)
}
fn asap_final_inner(
    state: Option<Inner<ASAPTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_series::toolkit_experimental::Timevector<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state.clone(),
            };

            let mut points = state.ts;
            if !state.sorted {
                points.sort_by_key(|p| p.ts);
            }
            // In following the ASAP reference implementation, we only downsample if the number
            // of points is at least twice the resolution.  Otherwise we keep the number of
            // points, but still normalize them to equal sized buckets.
            let downsample_interval = if points.len() >= 2 * state.resolution as usize {
                find_downsample_interval(&points, state.resolution as i64)
            } else {
                (points.last().unwrap().ts - points.first().unwrap().ts) / points.len() as i64
            };
            let mut normal = downsample_and_gapfill_to_normal_form(&points, downsample_interval);
            let start_ts = points.first().unwrap().ts;

            // Drop the last value to match the reference implementation
            normal.pop();
            let values = asap_smooth(&normal, state.resolution as u32);

            Some(crate::build! {
                Timevector {
                    series: SeriesType::Normal {
                        start_ts,
                        // Set the step interval for the asap result so that it covers the same interval
                        // as the passed in data
                        step_interval: downsample_interval * normal.len() as i64 / values.len() as i64,
                        num_vals: values.len() as _,
                        values: values.into(),
                    }
                }
            })
        })
    }
}

#[pg_extern(name="asap_smooth", schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn asap_on_timevector(
    mut series: crate::time_series::toolkit_experimental::Timevector<'static>,
    resolution: i32
) -> Option<crate::time_series::toolkit_experimental::Timevector<'static>> {
    // TODO: implement this using zero copy (requires sort, find_downsample_interval, and downsample_and_gapfill on Timevector)
    let needs_sort = matches!(&series.series, SeriesType::Explicit{..});
    let start_ts;
    let downsample_interval;
    let mut normal = match &mut series.series {
        SeriesType::Explicit { points, .. } | SeriesType::Sorted { points, .. }
        => {
            if needs_sort {
                points.as_owned().sort_by_key(|p| p.ts);
            }
            // TODO points.make_slice()?
            downsample_interval = if points.len() >= 2 * resolution as usize {
                find_downsample_interval(points.as_slice(), resolution as i64)
            } else {
                (points.as_slice().last().unwrap().ts - points.as_slice().first().unwrap().ts) / points.len() as i64
            };
            let normal = downsample_and_gapfill_to_normal_form(points.as_slice(), downsample_interval);
            start_ts = points.as_slice().first().unwrap().ts;
            normal
        },
        SeriesType::Normal { start_ts: start, step_interval, values, .. } => {
            start_ts = *start;
            downsample_interval = *step_interval;
            values.clone().into_vec()
        },
        SeriesType::GappyNormal { .. } =>
            panic!("Series must be gapfilled before running asap smoothing"),
    };

    // Drop the last value to match the reference implementation
    normal.pop();

    let result = asap_smooth(&normal, resolution as u32);

    Some(crate::build! {
        Timevector {
            series: SeriesType::Normal {
                start_ts,
                // Set the step interval for the asap result so that it covers the same interval
                // as the passed in data
                step_interval: downsample_interval * normal.len() as i64 / result.len() as i64,
                num_vals: result.len() as _,
                values: result.into(),
            }
        }
    })
}

// Adds the given number of points to the end of a non-empty NormalTimevector
pub fn fill_normalized_series_gap(values: &mut Vec<f64>, points: i32, post_gap_val: f64) {
    assert!(!values.is_empty());
    let last_val = *values.last().unwrap();
    for i in 1..=points {
        values.push(last_val + (post_gap_val - last_val) * i as f64 / (points + 1) as f64);
    }
}

fn downsample_and_gapfill_to_normal_form(
    points: &[TSPoint],
    downsample_interval: i64
) -> Vec<f64> {
    if points.len() < 2 || points.last().unwrap().ts - points.first().unwrap().ts < downsample_interval {
        panic!("Not enough data to generate a smoothed representation")
    }
    //TODO can we right-size?
    let mut values = vec![];
    let mut bound = points.first().unwrap().ts + downsample_interval;
    let mut sum = 0.0;
    let mut count = 0;
    let mut gap_count = 0;
    for pt in points.iter() {
        if pt.ts < bound {
            sum += pt.val;
            count += 1;
        } else {
            assert!(count != 0);
            let new_val = sum / count as f64;
            // If we missed any intervals prior to the current one, fill in the gap here
            if gap_count != 0 {
                fill_normalized_series_gap(&mut values, gap_count, new_val);
                gap_count = 0;
            }
            values.push(new_val);
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
        fill_normalized_series_gap(&mut values, gap_count, new_val);
    }
    values.push(sum / count as f64);
    values
}

// Aggregate on only values (assumes aggregation over ordered normalized timestamp)
extension_sql!("\n\
    CREATE AGGREGATE toolkit_experimental.asap_smooth(ts TIMESTAMPTZ, value DOUBLE PRECISION, resolution INT)\n\
    (\n\
        sfunc = toolkit_experimental.asap_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.asap_final\n\
    );\n",
name = "asap_agg",
requires = [asap_trans, asap_final],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_asap() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
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

            client.select("create table asap_vals as SELECT * FROM toolkit_experimental.unnest((SELECT toolkit_experimental.asap_smooth(date, value, 100) FROM asap_test ))", None, None);

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

            // Now compare the asap aggregate to asap run on a timevector aggregate
            client.select(
                "create table asap_vals2 as
                SELECT *
                FROM toolkit_experimental.unnest(
                    (SELECT toolkit_experimental.asap_smooth(
                        (SELECT toolkit_experimental.timevector(date, value) FROM asap_test),
                        100)
                    )
                )", None, None);

            let delta = client
                .select(
                    "SELECT count(*)
                    FROM asap_vals r1 FULL OUTER JOIN asap_vals2 r2 ON r1 = r2
                    WHERE r1 IS NULL OR r2 IS NULL;" , None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(delta.unwrap(), 0);
        });
    }
}
