
use std::convert::TryInto;

use pgx::*;

use flat_serialize_macro::FlatSerializable;

use serde::{Deserialize, Serialize};

use super::*;

use crate::{
    json_inout_funcs, pg_type, flatten,
};

// TODO once we start stabilizing elements, create a type
//      `TimeseriesPipelineElement` and move stable variants to that.
pg_type! {
    #[derive(Debug)]
    struct UnstableTimeseriesPipelineElement {
        element: enum Element {
            kind: u64,
            LTTB: 1 {
                resolution: u64,
            },
            ResampleToRate: 2 {
                interval: i64,
                resample_method: ResampleMethod,
                snap_to_rate: i64, // padded bool
            },
            FillHoles: 3 {
                fill_method: FillMethod,
            },
        },
    }
}

json_inout_funcs!(UnstableTimeseriesPipelineElement);

// TODO once we start stabilizing elements, create a type TimeseriesPipeline
//      stable elements will create a stable pipeline, but adding an unstable
//      element to a stable pipeline will create an unstable pipeline
type USPED = UnstableTimeseriesPipelineElementData;
pg_type! {
    #[derive(Debug)]
    struct UnstableTimeseriesPipeline<'input> {
        num_elements: u64,
        elements: [USPED; self.num_elements],
    }
}

json_inout_funcs!(UnstableTimeseriesPipeline);

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
pub mod toolkit_experimental {
    pub(crate) use super::*;
    varlena_type!(UnstableTimeseriesPipeline);
    varlena_type!(UnstableTimeseriesPipelineElement);
}

pub enum MaybeOwnedTs<'s> {
    Borrowed(TimeSeries<'s>),
    Owned(TimeSeries<'static>),
}

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn run_pipeline<'s, 'p>(
    timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
) -> toolkit_experimental::TimeSeries<'static> {
    let mut timeseries = MaybeOwnedTs::Borrowed(timeseries);
    for element in pipeline.elements.iter() {
        let element = element.element;
        let new_timeseries = execute_pipeline_element(&mut timeseries, &element);
        if let Some(series) = new_timeseries {
            timeseries = MaybeOwnedTs::Owned(series)
        }
    }
    match timeseries {
        MaybeOwnedTs::Borrowed(series) => series.in_current_context(),
        MaybeOwnedTs::Owned(series) => series,
    }
}

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn run_pipeline_element<'s, 'p>(
    timeseries: toolkit_experimental::TimeSeries<'s>,
    element: toolkit_experimental::UnstableTimeseriesPipelineElement<'p>,
) -> toolkit_experimental::TimeSeries<'static> {
    let owned_timeseries = execute_pipeline_element(&mut MaybeOwnedTs::Borrowed(timeseries), &element.element);
    if let Some(timeseries) = owned_timeseries {
        return timeseries
    }
    return timeseries.in_current_context()
}

// TODO need cow-like for timeseries input
pub fn execute_pipeline_element<'s, 'e>(
    timeseries: &mut MaybeOwnedTs<'s>,
    element: &Element
) -> Option<toolkit_experimental::TimeSeries<'static>> {
    use MaybeOwnedTs::{Borrowed, Owned};

    match (element, timeseries) {
        (Element::LTTB{resolution}, Borrowed(timeseries)) => {
            return Some(crate::lttb::lttb_ts(*timeseries, *resolution as _))
        }
        (Element::LTTB{resolution}, Owned(timeseries)) => {
            return Some(crate::lttb::lttb_ts(*timeseries, *resolution as _))
        }
        (Element::ResampleToRate{..}, Borrowed(timeseries)) => {
            return Some(resample_to_rate(timeseries, &element));
        }
        (Element::ResampleToRate{..}, Owned(timeseries)) => {
            return Some(resample_to_rate(timeseries, &element));
        }
        (Element::FillHoles{..}, Borrowed(timeseries)) => {
            return Some(fill_holes(timeseries, &element));
        }
        (Element::FillHoles{..}, Owned(timeseries)) => {
            return Some(fill_holes(timeseries, &element));
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn build_unstable_pipepine<'s, 'p>(
    first: toolkit_experimental::UnstableTimeseriesPipelineElement<'s>,
    second: toolkit_experimental::UnstableTimeseriesPipelineElement<'p>,
) -> toolkit_experimental::UnstableTimeseriesPipeline<'static> {
    unsafe {
        let elements: Vec<_> = vec!(first.flatten().0, second.flatten().0);
        flatten! {
            UnstableTimeseriesPipeline {
                num_elements: 2,
                elements: (&*elements).into(),
            }
        }
    }
}

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn add_unstable_element<'p, 'e>(
    pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    element: toolkit_experimental::UnstableTimeseriesPipelineElement<'e>,
) -> toolkit_experimental::UnstableTimeseriesPipeline<'p> {
    unsafe {
        let elements: Vec<_> = pipeline.elements.iter().chain(Some(element.flatten().0)).collect();
        flatten! {
            UnstableTimeseriesPipeline {
                num_elements: elements.len().try_into().unwrap(),
                elements: (&*elements).into(),
            }
        }
    }
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(r#"
CREATE OPERATOR |> (
    PROCEDURE=toolkit_experimental."run_pipeline",
    LEFTARG=toolkit_experimental.TimeSeries,
    RIGHTARG=toolkit_experimental.UnstableTimeseriesPipeline
);

CREATE OPERATOR |> (
    PROCEDURE=toolkit_experimental."run_pipeline_element",
    LEFTARG=toolkit_experimental.TimeSeries,
    RIGHTARG=toolkit_experimental.UnstableTimeseriesPipelineElement
);

CREATE OPERATOR |> (
    PROCEDURE=toolkit_experimental."build_unstable_pipepine",
    LEFTARG=toolkit_experimental.UnstableTimeseriesPipelineElement,
    RIGHTARG=toolkit_experimental.UnstableTimeseriesPipelineElement
);

CREATE OPERATOR |> (
    PROCEDURE=toolkit_experimental."add_unstable_element",
    LEFTARG=toolkit_experimental.UnstableTimeseriesPipeline,
    RIGHTARG=toolkit_experimental.UnstableTimeseriesPipelineElement
);
"#);

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="lttb",
    schema="toolkit_experimental"
)]
pub fn lttb_pipeline_element<'p, 'e>(
    resolution: i32,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    unsafe {
        flatten!(
            UnstableTimeseriesPipelineElement {
                element: Element::LTTB {
                    resolution: resolution.try_into().unwrap(),
                }
            }
        )
    }
}

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
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
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

        flatten!(
            UnstableTimeseriesPipelineElement {
                element: Element::ResampleToRate {
                    interval,
                    resample_method,
                    snap_to_rate: if snap_to_rate {1} else {0},
                }
            }
        )
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

fn resample_to_rate(
    series: &toolkit_experimental::TimeSeries, 
    element: &toolkit_experimental::Element
) -> toolkit_experimental::TimeSeries<'static> {
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
                let new_pt = method.process(&points, current.unwrap(), interval);
                match result {
                    None => result = Some(InternalTimeSeries::new_gappy_normal_series(new_pt, interval)),
                    Some(ref mut series) => series.add_point(new_pt),
                }
            }
            
            current = Some(target);
            points.clear();
        }
        points.push(point);
    }

    let new_pt = method.process(&points, current.unwrap(), interval);
    match result {
        None => result = Some(InternalTimeSeries::new_gappy_normal_series(new_pt, interval)),
        Some(ref mut series) => series.add_point(new_pt),
    }

    TimeSeries::from_internal_time_series(&result.unwrap())
}

// TODO: there are one or two other gapfill objects in this extension, these should be unified
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, FlatSerializable)]
#[repr(u64)]
pub enum FillMethod {
    LOCF,
    Interpolate,
}

impl FillMethod {
    pub fn process<'s>(&self, series: &TimeSeries<'s>) -> MaybeOwnedTs<'s> {
        unsafe {
            match series.series {
                SeriesType::GappyNormalSeries{start_ts, step_interval, count, present, values, ..} => {
                    match self {
                        FillMethod::LOCF => {
                            let mut results = Vec::new();
                            let mut last_val = 0.0;
                            let mut vidx = 0;

                            for pidx in 0..count {
                                if present[pidx as usize / 64] & 1 << (pidx % 64) != 0 {
                                    last_val = values[vidx];
                                    vidx += 1;
                                } 
                                results.push(last_val);
                            }

                            MaybeOwnedTs::Owned(
                                flatten!(
                                    TimeSeries {
                                        series : SeriesType::NormalSeries {
                                            start_ts,
                                            step_interval,
                                            num_vals: count,
                                            values: &results,
                                        }
                                    }
                                )
                            )
                        }
                        FillMethod::Interpolate => {
                            let mut iter = series.iter();
                            let mut prev = iter.next().unwrap();
                            let mut results = vec!(prev.val);

                            for point in iter {
                                let points = (point.ts - prev.ts) / step_interval;
                                for p in 1..=points {
                                    results.push(prev.val + (point.val - prev.val) * (p as f64 / points as f64));
                                }
                                prev = point;
                            }

                            MaybeOwnedTs::Owned(
                                flatten!(
                                    TimeSeries {
                                        series : SeriesType::NormalSeries {
                                            start_ts,
                                            step_interval,
                                            num_vals: count,
                                            values: &results,
                                        }
                                    }
                                )
                            )
                        }
                    }
                }

                SeriesType::NormalSeries{..} => {
                    MaybeOwnedTs::Borrowed(*series)
                }

                _ => panic!("Gapfill not currently implemented for explicit timeseries")
            }
        }
    }
}

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="fill_holes",
    schema="toolkit_experimental"
)]
pub fn nullfill_pipeline_element<'e> (
    fill_method: String,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    unsafe {
        let fill_method = match fill_method.to_lowercase().as_str() {
            "locf" => FillMethod::LOCF,
            "interpolate" => FillMethod::Interpolate,
            "linear" => FillMethod::Interpolate,
            _ => panic!("Invalid downsample method")
        };

        flatten!(
            UnstableTimeseriesPipelineElement {
                element: Element::FillHoles {
                    fill_method
                }
            }
        )
    }
}

fn fill_holes(
    series: &toolkit_experimental::TimeSeries, 
    element: &toolkit_experimental::Element
) -> toolkit_experimental::TimeSeries<'static> {
    let method = match element {
        Element::FillHoles{fill_method: gapfill_method} => gapfill_method,
        _ => panic!("Gapfill evaluator called on incorrect pipeline element")
    };

    match method.process(series) {
        MaybeOwnedTs::Owned(series) => series,
        MaybeOwnedTs::Borrowed(series) => series.in_current_context(),
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_pipeline_lttb() {
        Spi::execute(|client| {
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select(
                "CREATE TABLE lttb_pipe (series timeseries)",
                None,
                None
            );
            client.select(
                "INSERT INTO lttb_pipe \
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
                "SELECT (series |> lttb(17))::TEXT FROM lttb_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-11 00:00:00+00\",\"val\":12.7015},\
                {\"ts\":\"2020-01-13 00:00:00+00\",\"val\":11.8117},\
                {\"ts\":\"2020-01-22 00:00:00+00\",\"val\":7.4757},\
                {\"ts\":\"2020-01-28 00:00:00+00\",\"val\":5.4796},\
                {\"ts\":\"2020-02-03 00:00:00+00\",\"val\":5.0626},\
                {\"ts\":\"2020-02-09 00:00:00+00\",\"val\":6.3703},\
                {\"ts\":\"2020-02-14 00:00:00+00\",\"val\":8.4633},\
                {\"ts\":\"2020-02-24 00:00:00+00\",\"val\":13.1734},\
                {\"ts\":\"2020-03-01 00:00:00+00\",\"val\":14.8008},\
                {\"ts\":\"2020-03-07 00:00:00+00\",\"val\":14.7511},\
                {\"ts\":\"2020-03-13 00:00:00+00\",\"val\":13.0417},\
                {\"ts\":\"2020-03-23 00:00:00+00\",\"val\":8.3042},\
                {\"ts\":\"2020-03-29 00:00:00+00\",\"val\":5.9445},\
                {\"ts\":\"2020-04-04 00:00:00+00\",\"val\":5.0015},\
                {\"ts\":\"2020-04-10 00:00:00+00\",\"val\":5.8046},\
                {\"ts\":\"2020-04-14 00:00:00+00\",\"val\":7.195},\
                {\"ts\":\"2020-04-20 00:00:00+00\",\"val\":10.0221}\
            ]");

            let val = client.select(
                "SELECT (series |> lttb(8))::TEXT FROM lttb_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-11 00:00:00+00\",\"val\":12.7015},\
                {\"ts\":\"2020-01-27 00:00:00+00\",\"val\":5.7155},\
                {\"ts\":\"2020-02-06 00:00:00+00\",\"val\":5.5162},\
                {\"ts\":\"2020-02-27 00:00:00+00\",\"val\":14.1735},\
                {\"ts\":\"2020-03-09 00:00:00+00\",\"val\":14.3469},\
                {\"ts\":\"2020-03-30 00:00:00+00\",\"val\":5.6728},\
                {\"ts\":\"2020-04-09 00:00:00+00\",\"val\":5.554},\
                {\"ts\":\"2020-04-20 00:00:00+00\",\"val\":10.0221}\
            ]");

            let val = client.select(
                "SELECT (series |> lttb(8) |> lttb(8))::TEXT FROM lttb_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-11 00:00:00+00\",\"val\":12.7015},\
                {\"ts\":\"2020-01-27 00:00:00+00\",\"val\":5.7155},\
                {\"ts\":\"2020-02-06 00:00:00+00\",\"val\":5.5162},\
                {\"ts\":\"2020-02-27 00:00:00+00\",\"val\":14.1735},\
                {\"ts\":\"2020-03-09 00:00:00+00\",\"val\":14.3469},\
                {\"ts\":\"2020-03-30 00:00:00+00\",\"val\":5.6728},\
                {\"ts\":\"2020-04-09 00:00:00+00\",\"val\":5.554},\
                {\"ts\":\"2020-04-20 00:00:00+00\",\"val\":10.0221}\
            ]");

            let val = client.select(
                "SELECT (series |> (lttb(8) |> lttb(8) |> lttb(8)))::TEXT FROM lttb_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-11 00:00:00+00\",\"val\":12.7015},\
                {\"ts\":\"2020-01-27 00:00:00+00\",\"val\":5.7155},\
                {\"ts\":\"2020-02-06 00:00:00+00\",\"val\":5.5162},\
                {\"ts\":\"2020-02-27 00:00:00+00\",\"val\":14.1735},\
                {\"ts\":\"2020-03-09 00:00:00+00\",\"val\":14.3469},\
                {\"ts\":\"2020-03-30 00:00:00+00\",\"val\":5.6728},\
                {\"ts\":\"2020-04-09 00:00:00+00\",\"val\":5.554},\
                {\"ts\":\"2020-04-20 00:00:00+00\",\"val\":10.0221}\
            ]");

            let val = client.select(
                "SELECT (series |> resample_to_rate('average', '240 hours', true))::TEXT FROM lttb_pipe",
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
                "SELECT (series |> resample_to_rate('trailing_average', '240 hours', false))::TEXT FROM lttb_pipe",
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
                "SELECT (series |> resample_to_rate('trailing_average', '240 hours', true))::TEXT FROM lttb_pipe",
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
                "SELECT (series |> resample_to_rate('weighted_average', '240 hours', true))::TEXT FROM lttb_pipe",
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
                "SELECT (series |> resample_to_rate('NEAREST' ,'240 hours', true))::TEXT FROM lttb_pipe",
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

    #[pg_test]
    fn test_pipeline_gapfill() {
        Spi::execute(|client| {
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select(
                "CREATE TABLE gappy_series(time timestamptz, value double precision)",
                None,
                None
            );
            client.select(
                "INSERT INTO gappy_series \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(1,5,0.1) foo",
                None,
                None
            );


            client.select(
                "INSERT INTO gappy_series \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(5.5,8,0.1) foo",
                None,
                None
            );
            
            client.select(
                "INSERT INTO gappy_series \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(11,13,0.1) foo",
                None,
                None
            );

            let val = client.select(
                "SELECT (timeseries(time, value) |> resample_to_rate('average', '240 hours', true))::TEXT FROM gappy_series",
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
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.679616666666666},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":12.10525},\
                {\"ts\":\"2020-05-05 00:00:00+00\",\"val\":14.76376},\
                {\"ts\":\"2020-05-15 00:00:00+00\",\"val\":14.5372}\
            ]");


            let val = client.select(
                "SELECT (timeseries(time, value) |> resample_to_rate('average', '240 hours', true) |> fill_holes('LOCF'))::TEXT FROM gappy_series",
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
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.679616666666666},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":12.10525},\
                {\"ts\":\"2020-05-05 00:00:00+00\",\"val\":14.76376},\
                {\"ts\":\"2020-05-15 00:00:00+00\",\"val\":14.5372}\
            ]");

            let val = client.select(
                "SELECT (timeseries(time, value) |> resample_to_rate('average', '240 hours', true) |> fill_holes('interpolate'))::TEXT FROM gappy_series",
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
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.679616666666666},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":10.216683333333332},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":11.160966666666667},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":12.10525},\
                {\"ts\":\"2020-05-05 00:00:00+00\",\"val\":14.76376},\
                {\"ts\":\"2020-05-15 00:00:00+00\",\"val\":14.5372}\
            ]");
        });
    }
}