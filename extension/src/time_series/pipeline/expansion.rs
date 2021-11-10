
use std::{iter::Iterator, mem::replace};

use pgx::*;

use super::*;

use crate::{
    ron_inout_funcs, pg_type, build,
};

use self::toolkit_experimental::{
    PipelineThenUnnest,
    PipelineThenUnnestData,
    PipelineForceMaterialize,
    PipelineForceMaterializeData,
};


#[pg_schema]
pub mod toolkit_experimental {
    pub(crate) use super::*;

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenUnnest<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenUnnest);


    pg_type! {
        #[derive(Debug)]
        struct PipelineForceMaterialize<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineForceMaterialize);
}

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
pub fn arrow_finalize_with_unnest<'p>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'p>,
    then_stats_agg: toolkit_experimental::PipelineThenUnnest<'p>,
) -> toolkit_experimental::PipelineThenUnnest<'p> {
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
pub fn arrow_run_pipeline_then_unnest<'s>(
    timevector: toolkit_experimental::Timevector<'s>,
    pipeline: toolkit_experimental::PipelineThenUnnest<'s>,
) -> impl Iterator<Item = (name!(time,crate::raw::TimestampTz),name!(value,f64))>
{
    let series = run_pipeline_elements(timevector, pipeline.elements.iter())
        .0.into_owned();
    crate::time_series::unnest(series.into())
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="materialize",
    schema="toolkit_experimental"
)]
pub fn pipeline_series<'e>() -> toolkit_experimental::PipelineForceMaterialize<'e> {
    build! {
        PipelineForceMaterialize {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_force_materialize<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_stats_agg: toolkit_experimental::PipelineForceMaterialize<'e>,
) -> toolkit_experimental::PipelineForceMaterialize<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {flatten! {
            PipelineForceMaterialize {
                num_elements: pipeline.0.num_elements,
                elements: pipeline.0.elements,
            }
        }}
    }

    let mut elements = replace(pipeline.elements.as_owned(), vec![]);
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineForceMaterialize {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_run_pipeline_then_materialize<'s, 'p>(
    timevector: toolkit_experimental::Timevector<'s>,
    pipeline: toolkit_experimental::PipelineForceMaterialize<'p>,
) -> toolkit_experimental::Timevector<'static>
{
    run_pipeline_elements(timevector, pipeline.elements.iter())
        .in_current_context()
}

#[pg_extern(
    immutable,
    parallel_safe,
    schema="toolkit_experimental"
)]
pub unsafe fn pipeline_materialize_support(input: pgx::Internal)
-> pgx::Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| unsafe {
        let new_element = PipelineForceMaterialize::from_datum(new_element, false, 0)
            .unwrap();
       arrow_force_materialize(old_pipeline, new_element).into_datum().unwrap()
    })
}

extension_sql!(r#"
ALTER FUNCTION "arrow_run_pipeline_then_materialize" SUPPORT toolkit_experimental.pipeline_materialize_support;
"#,
name="pipe_then_materialize",
requires= [pipeline_materialize_support],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

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
            let create_series = "SELECT timevector(time, value) as series FROM \
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


    #[pg_test]
    fn test_series_finalizer() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 11.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 21.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 31.0)) as v(time, value)";

            let val = client.select(
                &format!("SELECT (series -> materialize())::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[(ts:\"2020-01-04 00:00:00+00\",val:25),(ts:\"2020-01-01 00:00:00+00\",val:11),(ts:\"2020-01-03 00:00:00+00\",val:21),(ts:\"2020-01-02 00:00:00+00\",val:15),(ts:\"2020-01-05 00:00:00+00\",val:31)]");
        });
    }

    #[pg_test]
    fn test_force_materialize() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // `-> materialize()` should force materialization, but otherwise the
            // pipeline-folding optimization should proceed
            let output = client.select(
                "EXPLAIN (verbose) SELECT \
                timevector('2021-01-01'::timestamptz, 0.1) \
                -> round() -> abs() \
                -> materialize() \
                -> abs() -> round();",
                None,
                None
            ).skip(1)
                .next().unwrap()
                .by_ordinal(1).unwrap()
                .value::<String>().unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_run_pipeline(\
                    arrow_run_pipeline_then_materialize(\
                        timevector('2021-01-01 00:00:00+00'::timestamp with time zone, '0.1'::double precision), \
                        '(version:1,num_elements:2,elements:[\
                            Arithmetic(function:Round,rhs:0),Arithmetic(function:Abs,rhs:0)\
                        ])'::pipelineforcematerialize\
                    ), \
                    '(version:1,num_elements:2,elements:[\
                        Arithmetic(function:Abs,rhs:0),Arithmetic(function:Round,rhs:0)\
                    ])'::unstabletimevectorpipeline\
                )");
        });
    }
}
