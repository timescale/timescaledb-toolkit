
mod fill_holes;
mod resample_to_rate;
mod sort;
mod delta;
mod map;
mod arithmetic;

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

use map::{
    map_series_element,
    check_user_function_type,
    apply_to_series,
};

use crate::serialization::PgProcId;

// TODO once we start stabilizing elements, create a type TimeseriesPipeline
//      stable elements will create a stable pipeline, but adding an unstable
//      element to a stable pipeline will create an unstable pipeline
pg_type! {
    #[derive(Debug)]
    struct UnstableTimeseriesPipeline<'input> {
        num_elements: u64,
        elements: [Element; self.num_elements],
    }
}

flat_serialize_macro::flat_serialize! {
    #[derive(Debug)]
    #[derive(serde::Serialize, serde::Deserialize)]
    enum Element {
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
        MapData: 6 {
            // FIXME serialize/deserialize as `name(type)`
            function: PgProcId,
        },
        MapSeries: 7 {
            // FIXME serialize/deserialize as `name(type)`
            function: PgProcId,
        },
        Arithmetic: 8 {
            function: arithmetic::Function,
            rhs: f64,
        }
    }
}

impl Element {
    pub fn flatten<'a>(self) -> UnstableTimeseriesPipeline<'a> {
        let slice = &[self][..];
        unsafe {
            flatten! {
                UnstableTimeseriesPipeline {
                    num_elements: 1,
                    elements: slice.into(),
                }
            }
        }
    }
}

impl From<Element> for UnstableTimeseriesPipeline<'_> {
    fn from(element: Element) -> Self {
        build! {
            UnstableTimeseriesPipeline {
                num_elements: 1,
                elements: vec![element].into(),
            }
        }
    }
}

json_inout_funcs!(UnstableTimeseriesPipeline);

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
pub mod toolkit_experimental {
    pub(crate) use super::*;
    varlena_type!(UnstableTimeseriesPipeline);
}

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn run_pipeline<'s, 'p>(
    mut timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
) -> toolkit_experimental::TimeSeries<'static> {
    for element in pipeline.elements.iter() {
        timeseries = execute_pipeline_element(timeseries, &element);
    }
    timeseries.in_current_context()
}

// TODO need cow-like for timeseries input
pub fn execute_pipeline_element<'s, 'e>(
    timeseries: TimeSeries<'s>,
    element: &Element
) -> TimeSeries<'s> {
    match element {
        Element::LTTB{resolution} =>
            return crate::lttb::lttb_ts(timeseries, *resolution as _),
        Element::ResampleToRate{..} =>
            return resample_to_rate(&timeseries, &element),
        Element::FillHoles{..} =>
            return fill_holes(timeseries, &element),
        Element::Sort{..} =>
            return sort_timeseries(timeseries),
        Element::Delta{..} =>
            return timeseries_delta(&timeseries),
        Element::MapData { function } =>
            return map::apply_to(timeseries, function.0),
        Element::MapSeries { function } =>
            return map::apply_to_series(timeseries, function.0),
        Element::Arithmetic{ function, rhs } =>
            return arithmetic::apply(timeseries, *function, *rhs),
    }
}

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn add_unstable_element<'p, 'e>(
    mut pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    element: toolkit_experimental::UnstableTimeseriesPipeline<'e>,
) -> toolkit_experimental::UnstableTimeseriesPipeline<'p> {
    pipeline.elements.as_owned().extend(element.elements.iter());
    pipeline.num_elements = pipeline.elements.len().try_into().unwrap();
    pipeline
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
    PROCEDURE=toolkit_experimental."add_unstable_element",
    LEFTARG=toolkit_experimental.UnstableTimeseriesPipeline,
    RIGHTARG=toolkit_experimental.UnstableTimeseriesPipeline
);
"#);

#[pg_extern(stable, parallel_safe, schema="toolkit_experimental")]
pub fn run_user_pipeline_element<'s, 'p>(
    timeseries: toolkit_experimental::TimeSeries<'s>,
    function: pg_sys::regproc,
) -> toolkit_experimental::TimeSeries<'static> {
    check_user_function_type(function);
    apply_to_series(timeseries, function).in_current_context()
}

#[pg_extern(stable, parallel_safe, schema="toolkit_experimental")]
pub fn build_unstable_user_pipeline<'s, 'p>(
    first: pg_sys::regproc,
    second: pg_sys::regproc,
) -> toolkit_experimental::UnstableTimeseriesPipeline<'static> {
    let elements: Vec<_> = vec![
        map_series_element(first),
        map_series_element(second),
    ];
    build! {
        UnstableTimeseriesPipeline {
            num_elements: 2,
            elements: elements.into(),
        }
    }
}

#[pg_extern(stable, parallel_safe, schema="toolkit_experimental")]
pub fn add_user_pipeline_element<'p, 'e>(
    pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    function: pg_sys::regproc,
) -> toolkit_experimental::UnstableTimeseriesPipeline<'p> {
    let elements: Vec<_> = pipeline.elements.iter()
        .chain(Some(map_series_element(function)))
        .collect();
    build! {
        UnstableTimeseriesPipeline {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// if we use `|>` for both this and and the regular timeseries elements trying
// to do `series |> 'custom_element'` gets an ambiguous operator error
// `timeseries |> unknown` is not unique. For now we just use a different
// operator for user-defined pipeline elements. In the future we could consider
// changing the element input function to fallback to checking if the input is
// a regproc if it doesn't recognize it; the formats should be different enough
// that there's no risk of collision...
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(r#"
CREATE OPERATOR |>> (
    PROCEDURE=toolkit_experimental."run_user_pipeline_element",
    LEFTARG=toolkit_experimental.TimeSeries,
    RIGHTARG=regproc
);

CREATE OPERATOR |>> (
    PROCEDURE=toolkit_experimental."build_unstable_user_pipeline",
    LEFTARG=regproc,
    RIGHTARG=regproc
);

CREATE OPERATOR |>> (
    PROCEDURE=toolkit_experimental."add_user_pipeline_element",
    LEFTARG=toolkit_experimental.UnstableTimeseriesPipeline,
    RIGHTARG=regproc
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
) -> toolkit_experimental::UnstableTimeseriesPipeline<'e> {
    Element::LTTB {
        resolution: resolution.try_into().unwrap(),
    }.flatten()
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_pipeline_lttb() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
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
