
use pgx::*;

use flat_serialize_macro::FlatSerializable;

use serde::{Deserialize, Serialize};

use super::*;

type Interval = pg_sys::Datum;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, FlatSerializable)]
#[repr(u64)]
pub enum ResampleMethod {
    Average,
    WeightedAverage,
    Nearest,
    TrailingAverage,
}

impl ResampleMethod {
    pub fn process(&self, vals: &[TSPoint], leading_edge: i64, interval: i64) -> TSPoint {
        match self {
            ResampleMethod::Average | ResampleMethod::TrailingAverage => {
                let ts = if *self == ResampleMethod::TrailingAverage {
                    leading_edge
                } else {
                    leading_edge + interval / 2
                };
                let mut sum = 0.0;
                for TSPoint{val, ..} in vals.iter() {
                    sum += val;
                }
                TSPoint{ts, val: sum / vals.len() as f64}
            }
            ResampleMethod::WeightedAverage => {
                let target = leading_edge + interval / 2;
                let mut sum = 0.0;
                let mut wsum  = 0.0;
                for TSPoint{ts, val} in vals.iter() {
                    let weight = 1.0 - ((ts - target).abs() as f64 / (interval as f64 / 2.0));
                    let weight = 0.1 + 0.9 * weight;  // use 0.1 as minimum weight to bound max_weight/min_weight to 10 (also fixes potential div0)
                    sum += val * weight;
                    wsum += weight;
                }
                TSPoint{ts: target, val: sum / wsum as f64}
            }
            ResampleMethod::Nearest => {
                let target = leading_edge + interval / 2;
                let mut closest = i64::MAX;
                let mut result = 0.0;
                for TSPoint{ts, val} in vals.iter() {
                    let distance = (ts - target).abs();
                    if distance < closest {
                        closest = distance;
                        result = *val;
                    } else if distance == closest {
                        result = (result + val) / 2.0;
                    }
                }
                TSPoint{ts: target, val: result}
            }
        }
    }
}

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="resample_to_rate",
    schema="toolkit_experimental"
)]
pub fn resample_pipeline_element<'p, 'e>(
    resample_method: String,
    interval: Interval,
    snap_to_rate: bool,
) -> toolkit_experimental::UnstableTimeseriesPipeline<'e> {
    unsafe {
        let interval = interval as *const pg_sys::Interval;
        if (*interval).day > 0 || (*interval).month > 0 {
            panic!("downsample intervals are currently restricted to stable units (hours or smaller)");
        }
        let interval = (*interval).time;

        let resample_method = match resample_method.to_lowercase().as_str() {
            "average" => ResampleMethod::Average,
            "weighted_average" => ResampleMethod::WeightedAverage,
            "nearest" => ResampleMethod::Nearest,
            "trailing_average" => ResampleMethod::TrailingAverage,
            _ => panic!("Invalid downsample method")
        };

        Element::ResampleToRate {
            interval,
            resample_method,
            snap_to_rate: if snap_to_rate {1} else {0},
        }.flatten()
    }
}

fn determine_offset_from_rate(first_timestamp: i64, rate: i64, snap_to_rate: bool, method: &ResampleMethod) -> i64 {
    let result = if snap_to_rate {
        0
    } else {
        first_timestamp % rate
    };

    match method {
        ResampleMethod::Average | ResampleMethod::Nearest | ResampleMethod::WeightedAverage => result - rate / 2,
        ResampleMethod::TrailingAverage => result,
    }
}

pub fn resample_to_rate<'s>(
    series: &toolkit_experimental::TimeSeries<'s>,
    element: &toolkit_experimental::Element
) -> toolkit_experimental::TimeSeries<'s> {
    let (interval, method, snap) = match element {
        Element::ResampleToRate{interval, resample_method, snap_to_rate} => (interval, resample_method, snap_to_rate),
        _ => panic!("Downsample evaluator called on incorrect pipeline element")
    };
    let interval = *interval;
    let snap = *snap == 1;

    let mut result = None;
    let mut current = None;
    let mut points = Vec::new();
    let mut offset_from_rate = None;

    for point in series.iter() {
        let TSPoint{ts, ..} = point;
        if offset_from_rate.is_none() {
            offset_from_rate = Some(determine_offset_from_rate(ts, interval, snap, method));
        }

        let target = (ts - offset_from_rate.unwrap()) / interval * interval + offset_from_rate.unwrap();
        if current != Some(target) {
            if current.is_some() {
                let TSPoint { ts, val } = method.process(&points, current.unwrap(), interval);
                match &mut result {
                    None => result = Some(GappyTimeSeriesBuilder::new(ts, interval, val)),
                    Some(series) => series.push_point(ts, val),
                }
            }

            current = Some(target);
            points.clear();
        }
        points.push(point);
    }

    let TSPoint { ts, val } = method.process(&points, current.unwrap(), interval);
    match &mut result {
        None => result = Some(GappyTimeSeriesBuilder::new(ts, interval, val)),
        Some(series) => series.push_point(ts, val),
    }

    let result = result.unwrap();
    build! {
        TimeSeries {
            series: SeriesType::GappyNormalSeries {
                start_ts: result.start_ts,
                step_interval: result.step_interval,
                num_vals: result.values.len() as _,
                count: result.count,
                values: result.values.into(),
                present: result.present.into(),
            }
        }
    }
}

struct GappyTimeSeriesBuilder {
    pub start_ts: i64,
    pub step_interval: i64,    // ts delta between values
    pub count: u64,            // num values + num gaps
    pub present: Vec<u64>,     // bitmap, 0 = gap...
    pub values: Vec<f64>
}

impl GappyTimeSeriesBuilder {
    fn new(start_time: i64, step_interval: i64, first_value: f64) -> Self {
        Self {
            start_ts: start_time,
            step_interval,
            count: 1,
            present: vec![1],
            values: vec![first_value],
        }
    }

    fn push_point(&mut self, time: i64, value: f64) {
        // TODO
        // assert!(point.ts >= series.start_ts + (series.step_interval * series.count as i64) && (point.ts - series.start_ts) % series.step_interval == 0);
        self.add_gap_until(time);
        self.push_present_bit(true);
        self.values.push(value);
    }

    fn add_gap_until(&mut self, time: i64) {
        let mut next = self.start_ts + self.count as i64 * self.step_interval;
        while next < time {
            self.push_present_bit(false);
            next += self.step_interval;
        }
        assert_eq!(next, time);
    }

    fn push_present_bit(&mut self, is_present: bool) {
        let idx = self.count;
        let val = if is_present { 1 } else { 0 };
        self.count += 1;
        if idx % 64 == 0 {
            self.present.push(val);
        } else if is_present {
            self.present[(idx / 64) as usize] ^= 1 << (idx % 64);
        }
    }
}


#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_pipeline_resample() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select(
                "CREATE TABLE resample_pipe (series timeseries)",
                None,
                None
            );
            client.select(
                "INSERT INTO resample_pipe \
                SELECT timeseries(time, val) FROM ( \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(1,11,0.1) foo \
                ) bar",
                None,
                None
            );

            let val = client.select(
                "SELECT (series -> resample_to_rate('average', '240 hours', true))::TEXT FROM resample_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.5779},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":6.30572},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.430009999999999},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":8.75585},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.22552},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":7.30756},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":5.20521},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":7.51113},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":10.0221}\
            ]");

            let val = client.select(
                "SELECT (series -> resample_to_rate('trailing_average', '240 hours', false))::TEXT FROM resample_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-11 00:00:00+00\",\"val\":10.5779},\
                {\"ts\":\"2020-01-21 00:00:00+00\",\"val\":6.30572},\
                {\"ts\":\"2020-01-31 00:00:00+00\",\"val\":5.430009999999999},\
                {\"ts\":\"2020-02-10 00:00:00+00\",\"val\":8.75585},\
                {\"ts\":\"2020-02-20 00:00:00+00\",\"val\":13.22552},\
                {\"ts\":\"2020-03-01 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-11 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-21 00:00:00+00\",\"val\":7.30756},\
                {\"ts\":\"2020-03-31 00:00:00+00\",\"val\":5.20521},\
                {\"ts\":\"2020-04-10 00:00:00+00\",\"val\":7.51113},\
                {\"ts\":\"2020-04-20 00:00:00+00\",\"val\":10.0221}\
            ]");

            let val = client.select(
                "SELECT (series -> resample_to_rate('trailing_average', '240 hours', true))::TEXT FROM resample_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-06 00:00:00+00\",\"val\":11.793660000000001},\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":8.22446},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":5.2914699999999995},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":6.68741},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":11.12889},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":14.53243},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":13.768830000000003},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":9.54011},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":5.73418},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":5.850160000000001},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":8.80205}\
            ]");

            let val = client.select(
                "SELECT (series -> resample_to_rate('weighted_average', '240 hours', true))::TEXT FROM resample_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.38865781818182},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":6.115898545454545},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.414132363636364},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":8.928520727272726},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.427980727272729},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.775747636363638},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.732629818181818},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":7.096518181818182},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":5.129781818181818},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":7.640666181818182},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":10.0221}\
            ]");

            let val = client.select(
                "SELECT (series -> resample_to_rate('NEAREST' ,'240 hours', true))::TEXT FROM resample_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.3536},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":5.9942},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.3177},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":8.946},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.5433},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.8829},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.7331},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":6.9899},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":5.0141},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":7.6223},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":10.0221}\
            ]");
        });
    }
}
