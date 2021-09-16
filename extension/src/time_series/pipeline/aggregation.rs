
use std::mem::replace;

use pgx::*;

use super::*;

use crate::{
    json_inout_funcs, pg_type, build,
    stats_agg::{InternalStatsSummary1D, StatsSummary1D},
};


pg_type! {
    #[derive(Debug)]
    struct PipelineThenStatsAgg<'input> {
        num_elements: u64,
        elements: [Element; self.num_elements],
    }
}

json_inout_funcs!(PipelineThenStatsAgg);

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
pub mod toolkit_experimental {
    pub(crate) use super::*;
    varlena_type!(PipelineThenStatsAgg);
}



#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn run_pipeline_then_stats_agg<'s, 'p>(
    mut timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::PipelineThenStatsAgg<'p>,
) -> toolkit_experimental::StatsSummary1D<'static> {
    timeseries = run_pipeline_elements(timeseries, pipeline.elements.iter());
    let mut stats = InternalStatsSummary1D::new();
    for TSPoint{ val, ..} in timeseries.iter() {
        stats.accum(val).expect("error while running stats_agg");
    }
    StatsSummary1D::from_internal(stats)
}

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn finalize_with_stats_agg<'p, 'e>(
    mut pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    then_stats_agg: toolkit_experimental::PipelineThenStatsAgg<'e>,
) -> toolkit_experimental::PipelineThenStatsAgg<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {flatten! {
            PipelineThenStatsAgg {
                num_elements: pipeline.0.num_elements,
                elements: pipeline.0.elements,
            }
        }}
    }

    let mut elements = replace(pipeline.elements.as_owned(), vec![]);
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenStatsAgg {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="stats_agg",
    schema="toolkit_experimental"
)]
pub fn pipeline_stats_agg<'e>() -> toolkit_experimental::PipelineThenStatsAgg<'e> {
    build! {
        PipelineThenStatsAgg {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(r#"
CREATE OPERATOR |> (
    PROCEDURE=toolkit_experimental."run_pipeline_then_stats_agg",
    LEFTARG=toolkit_experimental.TimeSeries,
    RIGHTARG=toolkit_experimental.PipelineThenStatsAgg
);

CREATE OPERATOR |> (
    PROCEDURE=toolkit_experimental."finalize_with_stats_agg",
    LEFTARG=toolkit_experimental.UnstableTimeseriesPipeline,
    RIGHTARG=toolkit_experimental.PipelineThenStatsAgg
);
"#);

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_stats_agg_finalizer() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timeseries(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client.select(
                &format!("SELECT (series |> stats_agg())::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "{\"version\":1,\"n\":5,\"sx\":100.0,\"sx2\":250.0,\"sx3\":0.0,\"sx4\":21250.0}");
        });
    }
}