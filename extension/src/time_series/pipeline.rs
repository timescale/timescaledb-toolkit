
use std::convert::TryInto;

use pgx::*;

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
            Downsample: 2 {
                interval: i64,
                downsample_method: i64,
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
        (Element::Downsample{..}, Borrowed(timeseries)) => {
            return Some(downsample(timeseries, &element));
        }
        (Element::Downsample{..}, Owned(timeseries)) => {
            return Some(downsample(timeseries, &element));
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

pub enum DownsampleMethod {
    Average,
    WeightedAverage,
    Nearest,
}

impl From<i64> for DownsampleMethod {
    fn from(item: i64) -> Self {
        match item {
            1 => DownsampleMethod::Average,
            2 => DownsampleMethod::WeightedAverage,
            3 => DownsampleMethod::Nearest,
            _ => panic!("Invalid downsample method")
        }
    }
}

impl Into<i64> for DownsampleMethod {
    fn into(self) -> i64 {
        match self {
            DownsampleMethod::Average => 1,
            DownsampleMethod::WeightedAverage => 2,
            DownsampleMethod::Nearest => 3,
        }
    }
}

impl DownsampleMethod {
    pub fn process(&self, vals: &std::vec::Vec<TSPoint>, target: i64, interval: i64) -> TSPoint {
        match self {
            DownsampleMethod::Average => {
                let mut sum = 0.0;
                for TSPoint{val, ..} in vals.iter() {
                    sum += val;
                }
                TSPoint{ts: target, val: sum / vals.len() as f64}
            }
            DownsampleMethod::WeightedAverage => {
                let mut sum = 0.0;
                let mut wsum  = 0.0;
                for TSPoint{ts, val} in vals.iter() {
                    let weight = (ts - target).abs() as f64 / interval as f64 / 2.0;
                    sum += val * weight;
                    wsum += weight;
                }
                TSPoint{ts: target, val: sum / wsum as f64}
            }
            DownsampleMethod::Nearest => {
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
    name="downsample",
    schema="toolkit_experimental"
)]
pub fn downsample_pipeline_element_default<'p, 'e>(
    interval: Interval,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    downsample_pipeline_element(interval, "average".to_string())
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="downsample",
    schema="toolkit_experimental"
)]
pub fn downsample_pipeline_element<'p, 'e>(
    interval: Interval,
    downsample_method: String,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    unsafe {
        let interval = interval as *const pg_sys::Interval;
        if (*interval).day > 0 || (*interval).month > 0 {
            panic!("downsample intervals are currently restricted to stable units (hours or smaller)");
        }
        let interval = (*interval).time;

        let downsample_method = match downsample_method.to_lowercase().as_str() {
            "average" => DownsampleMethod::Average,
            "weighted_average" => DownsampleMethod::WeightedAverage,
            "nearest" => DownsampleMethod::Nearest,
            _ => panic!("Invalid downsample method")
        };

        flatten!(
            UnstableTimeseriesPipelineElement {
                element: Element::Downsample {
                    interval,
                    downsample_method: downsample_method.into()
                }
            }
        )
    }
}

fn downsample(
    series: &toolkit_experimental::TimeSeries, 
    element: &toolkit_experimental::Element
) -> toolkit_experimental::TimeSeries<'static> {
    let (interval, method) = match element {
        Element::Downsample{interval, downsample_method} => (interval, downsample_method),
        _ => panic!("Downsample evaluator called on incorrect pipeline element")
    };
    let interval = *interval;
    let method = DownsampleMethod::from(*method);

    let mut result = None;
    let mut current = None;
    let mut points = Vec::new();
    for point in series.iter() {
        let TSPoint{ts, ..} = point;
        let target = (ts + (interval / 2) as i64 - 1) / interval as i64 * interval as i64;
        if current != Some(target) {
            if current != None {
                let new_pt = method.process(&points, current.unwrap(), interval);
                match result {
                    None => result = Some(InternalTimeSeries::new_gappy_normal_series(new_pt, interval)),
                    Some(ref mut series) => series.add_point(new_pt),
                }
            }
            
            current = Some(target);
            points = Vec::new();
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
                "SELECT (series |> downsample('240 hours'))::TEXT FROM lttb_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-06 00:00:00+00\",\"val\":12.7015},\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.09967},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":6.018800000000001},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.598180000000001},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":9.22451},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.56377},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.626499999999998},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.435550000000001},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":6.92475},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":5.24124},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":7.93288}\
            ]");

            let val = client.select(
                "SELECT (series |> downsample('240 hours', 'weighted_average'))::TEXT FROM lttb_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-06 00:00:00+00\",\"val\":12.7015},\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":9.852736000000002},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":5.963883999999999},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.785756000000001},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":9.482128000000001},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.654568000000001},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.467004000000003},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.172388},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":6.79988},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":5.369460000000001},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":8.196308}\
            ]");

            let val = client.select(
                "SELECT (series |> downsample('240 hours', 'NEAREST'))::TEXT FROM lttb_pipe",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-06 00:00:00+00\",\"val\":12.7015},\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.3536},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":5.9942},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.3177},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":8.946},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.5433},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.8829},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.7331},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":6.9899},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":5.0141},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":7.6223}\
            ]");
        });
    }
}