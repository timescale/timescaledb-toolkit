
use std::{iter::Iterator, mem::replace};

use pgx::*;

use super::*;

use crate::{
    ron_inout_funcs, pg_type, build,
};

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
pub mod toolkit_experimental {
    pub(crate) use super::*;
    varlena_type!(PipelineThenUnnest);
}

pg_type! {
    #[derive(Debug)]
    struct PipelineThenUnnest<'input> {
        num_elements: u64,
        elements: [Element; self.num_elements],
    }
}

ron_inout_funcs!(PipelineThenUnnest);

#[pg_extern(
    immutable,
    parallel_safe,
    name="unnest",
    schema="toolkit_experimental"
)]
pub fn pipeline_unnest<'e>() -> toolkit_experimental::PipelineThenUnnest<'e> {
    build! {
        PipelineThenUnnest {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_finalize_with_unnest<'p, 'e>(
    mut pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    then_stats_agg: toolkit_experimental::PipelineThenUnnest<'e>,
) -> toolkit_experimental::PipelineThenUnnest<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {flatten! {
            PipelineThenUnnest {
                num_elements: pipeline.0.num_elements,
                elements: pipeline.0.elements,
            }
        }}
    }

    let mut elements = replace(pipeline.elements.as_owned(), vec![]);
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenUnnest {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}


#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_run_pipeline_then_unnest<'s, 'p>(
    timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::PipelineThenUnnest<'p>,
) -> impl Iterator<Item = (name!(time,pg_sys::TimestampTz),name!(value,f64))>
{
    let series = run_pipeline_elements(timeseries, pipeline.elements.iter())
        .0.into_owned();
    crate::time_series::unnest_series(series.into())
}


#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_unnest_finalizer() {
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
                &format!("SELECT array_agg(val)::TEXT \
                    FROM (SELECT series -> unnest() as val FROM ({}) s) t", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "{\"(\\\"2020-01-04 00:00:00+00\\\",25)\",\"(\\\"2020-01-01 00:00:00+00\\\",10)\",\"(\\\"2020-01-03 00:00:00+00\\\",20)\",\"(\\\"2020-01-02 00:00:00+00\\\",15)\",\"(\\\"2020-01-05 00:00:00+00\\\",30)\"}");
        });
    }
}