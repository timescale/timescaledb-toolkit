
mod fill_holes;
mod resample_to_rate;
mod sort;
mod delta;

use std::convert::TryInto;

use pgx::*;

use super::*;

use crate::{
    json_inout_funcs, pg_type, flatten,
};

use fill_holes::{
    fill_holes,
    FillMethod,
};

use resample_to_rate::{
    resample_to_rate,
    ResampleMethod,
};

use sort::sort_timeseries;
use delta::timeseries_delta;

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
            Sort: 4 {
            },
            Delta: 5 {
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
        (Element::Sort{..}, Borrowed(timeseries)) => {
            return Some(sort_timeseries(timeseries));
        }
        (Element::Sort{..}, Owned(timeseries)) => {
            return Some(sort_timeseries(timeseries));
        }
        (Element::Delta{..}, Borrowed(timeseries)) => {
            return Some(timeseries_delta(timeseries));
        }
        (Element::Delta{..}, Owned(timeseries)) => {
            return Some(timeseries_delta(timeseries));
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
        });
    }
}