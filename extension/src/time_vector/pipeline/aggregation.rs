use std::mem::take;

use pgx::*;

use counter_agg::CounterSummaryBuilder;

use super::*;

use crate::{
    accessors::{AccessorAverage, AccessorNumVals, AccessorSum},
    build,
    counter_agg::CounterSummary,
    hyperloglog::HyperLogLog,
    pg_type, ron_inout_funcs,
    stats_agg::{self, InternalStatsSummary1D, StatsSummary1D},
    uddsketch::UddSketch,
};

use self::toolkit_experimental::{
    PipelineThenAverage, PipelineThenAverageData, PipelineThenCounterAgg,
    PipelineThenCounterAggData, PipelineThenHyperLogLog, PipelineThenHyperLogLogData,
    PipelineThenNumVals, PipelineThenNumValsData, PipelineThenPercentileAgg,
    PipelineThenPercentileAggData, PipelineThenStatsAgg, PipelineThenStatsAggData, PipelineThenSum,
    PipelineThenSumData,
};

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;
    pub(crate) use crate::time_vector::pipeline::UnstableTimevectorPipeline;

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenStatsAgg<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenStatsAgg);

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenSum<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenSum);

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenAverage<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenAverage);

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenNumVals<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenNumVals);

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenCounterAgg<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenCounterAgg);

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenHyperLogLog<'input> {
            hll_size: u64,
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenHyperLogLog);

    pg_type! {
        #[derive(Debug)]
        struct PipelineThenPercentileAgg<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    ron_inout_funcs!(PipelineThenPercentileAgg);
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_run_pipeline_then_stats_agg<'a>(
    mut timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::PipelineThenStatsAgg<'a>,
) -> StatsSummary1D<'static> {
    if timevector.has_nulls() {
        panic!("Unable to compute stats aggregate over timevector containing nulls");
    }
    timevector = run_pipeline_elements(timevector, pipeline.elements.iter());
    let mut stats = InternalStatsSummary1D::new();
    for TSPoint { val, .. } in timevector.iter() {
        stats.accum(val).expect("error while running stats_agg");
    }
    StatsSummary1D::from_internal(stats)
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn finalize_with_stats_agg<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_stats_agg: toolkit_experimental::PipelineThenStatsAgg<'e>,
) -> toolkit_experimental::PipelineThenStatsAgg<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {
            flatten! {
                PipelineThenStatsAgg {
                    num_elements: pipeline.0.num_elements,
                    elements: pipeline.0.elements,
                }
            }
        };
    }

    let mut elements = take(pipeline.elements.as_owned());
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
    name = "stats_agg",
    schema = "toolkit_experimental"
)]
pub fn pipeline_stats_agg() -> toolkit_experimental::PipelineThenStatsAgg<'static> {
    build! {
        PipelineThenStatsAgg {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub unsafe fn pipeline_stats_agg_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element =
            PipelineThenStatsAgg::from_polymorphic_datum(new_element, false, pg_sys::Oid::INVALID)
                .unwrap();
        finalize_with_stats_agg(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(
    r#"
ALTER FUNCTION "arrow_run_pipeline_then_stats_agg" SUPPORT toolkit_experimental.pipeline_stats_agg_support;
"#,
    name = "pipeline_stats_agg_support",
    requires = [pipeline_stats_agg_support],
);

#[pg_extern(
    immutable,
    parallel_safe,
    name = "sum_cast",
    schema = "toolkit_experimental"
)]
pub fn sum_pipeline_element<'a>(
    accessor: AccessorSum<'a>,
) -> toolkit_experimental::PipelineThenSum {
    let _ = accessor;
    build! {
        PipelineThenSum {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

extension_sql!(
    r#"
    CREATE CAST (AccessorSum AS toolkit_experimental.PipelineThenSum)
        WITH FUNCTION toolkit_experimental.sum_cast
        AS IMPLICIT;
"#,
    name = "sum_pipe_cast",
    requires = [AccessorSum, PipelineThenSum, sum_pipeline_element],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_pipeline_then_sum<'a>(
    timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::PipelineThenSum<'a>,
) -> Option<f64> {
    let pipeline = pipeline.0;
    let pipeline = build! {
        PipelineThenStatsAgg {
            num_elements: pipeline.num_elements,
            elements: pipeline.elements,
        }
    };
    let stats_agg = arrow_run_pipeline_then_stats_agg(timevector, pipeline);
    stats_agg::stats1d_sum(stats_agg)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn finalize_with_sum<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_stats_agg: toolkit_experimental::PipelineThenSum<'e>,
) -> toolkit_experimental::PipelineThenSum<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {
            flatten! {
                PipelineThenSum {
                    num_elements: pipeline.0.num_elements,
                    elements: pipeline.0.elements,
                }
            }
        };
    }

    let mut elements = take(pipeline.elements.as_owned());
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenSum {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub unsafe fn pipeline_sum_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element =
            PipelineThenSum::from_polymorphic_datum(new_element, false, pg_sys::Oid::INVALID)
                .unwrap();
        finalize_with_sum(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

extension_sql!(
    r#"
ALTER FUNCTION "arrow_pipeline_then_sum" SUPPORT toolkit_experimental.pipeline_sum_support;
"#,
    name = "arrow_then_sum_support",
    requires = [pipeline_sum_support],
);

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn average_pipeline_element<'a>(
    accessor: AccessorAverage<'a>,
) -> toolkit_experimental::PipelineThenAverage {
    let _ = accessor;
    build! {
        PipelineThenAverage {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

extension_sql!(
    r#"
    CREATE CAST (AccessorAverage AS toolkit_experimental.PipelineThenAverage)
        WITH FUNCTION toolkit_experimental.average_pipeline_element
        AS IMPLICIT;
"#,
    name = "avg_pipe_cast",
    requires = [
        AccessorAverage,
        PipelineThenAverage,
        average_pipeline_element
    ],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_pipeline_then_average<'a>(
    timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::PipelineThenAverage<'a>,
) -> Option<f64> {
    let pipeline = pipeline.0;
    let pipeline = build! {
        PipelineThenStatsAgg {
            num_elements: pipeline.num_elements,
            elements: pipeline.elements,
        }
    };
    let stats_agg = arrow_run_pipeline_then_stats_agg(timevector, pipeline);
    stats_agg::stats1d_average(stats_agg)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn finalize_with_average<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_stats_agg: toolkit_experimental::PipelineThenAverage<'e>,
) -> toolkit_experimental::PipelineThenAverage<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {
            flatten! {
                PipelineThenAverage {
                    num_elements: pipeline.0.num_elements,
                    elements: pipeline.0.elements,
                }
            }
        };
    }

    let mut elements = take(pipeline.elements.as_owned());
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenAverage {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub unsafe fn pipeline_average_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element =
            PipelineThenAverage::from_polymorphic_datum(new_element, false, pg_sys::Oid::INVALID)
                .unwrap();
        finalize_with_average(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

extension_sql!(
    r#"
ALTER FUNCTION "arrow_pipeline_then_average" SUPPORT toolkit_experimental.pipeline_average_support;
"#,
    name = "pipe_avg_support",
    requires = [pipeline_average_support],
);

#[pg_extern(
    immutable,
    parallel_safe,
    name = "num_vals_cast",
    schema = "toolkit_experimental"
)]
pub fn num_vals_pipeline_element<'a>(
    accessor: AccessorNumVals<'a>,
) -> toolkit_experimental::PipelineThenNumVals {
    let _ = accessor;
    build! {
        PipelineThenNumVals {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

extension_sql!(
    r#"
    CREATE CAST (AccessorNumVals AS toolkit_experimental.PipelineThenNumVals)
        WITH FUNCTION toolkit_experimental.num_vals_cast
        AS IMPLICIT;
"#,
    name = "num_vals_pipe_cast",
    requires = [
        AccessorNumVals,
        PipelineThenNumVals,
        num_vals_pipeline_element
    ],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_pipeline_then_num_vals<'a>(
    timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::PipelineThenNumVals<'a>,
) -> i64 {
    run_pipeline_elements(timevector, pipeline.elements.iter()).num_vals() as _
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn finalize_with_num_vals<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_stats_agg: toolkit_experimental::PipelineThenNumVals<'e>,
) -> toolkit_experimental::PipelineThenNumVals<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {
            flatten! {
                PipelineThenNumVals {
                    num_elements: pipeline.0.num_elements,
                    elements: pipeline.0.elements,
                }
            }
        };
    }

    let mut elements = take(pipeline.elements.as_owned());
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenNumVals {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub unsafe fn pipeline_num_vals_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element =
            PipelineThenNumVals::from_polymorphic_datum(new_element, false, pg_sys::Oid::INVALID)
                .unwrap();
        finalize_with_num_vals(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

extension_sql!(
    r#"
ALTER FUNCTION "arrow_pipeline_then_num_vals" SUPPORT toolkit_experimental.pipeline_num_vals_support;
"#,
    name = "pipe_then_num_vals",
    requires = [pipeline_num_vals_support],
);

// TODO support gauge
#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_run_pipeline_then_counter_agg<'a>(
    mut timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::PipelineThenCounterAgg<'a>,
) -> Option<CounterSummary<'static>> {
    timevector = run_pipeline_elements(timevector, pipeline.elements.iter());
    if timevector.num_points() == 0 {
        return None;
    }
    let mut it = timevector.iter();
    let mut summary = CounterSummaryBuilder::new(&it.next().unwrap(), None);
    for point in it {
        summary
            .add_point(&point)
            .expect("error while running counter_agg");
    }
    Some(CounterSummary::from_internal_counter_summary(
        summary.build(),
    ))
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn finalize_with_counter_agg<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_counter_agg: toolkit_experimental::PipelineThenCounterAgg<'e>,
) -> toolkit_experimental::PipelineThenCounterAgg<'e> {
    if then_counter_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {
            flatten! {
                PipelineThenCounterAgg {
                    num_elements: pipeline.0.num_elements,
                    elements: pipeline.0.elements,
                }
            }
        };
    }

    let mut elements = take(pipeline.elements.as_owned());
    elements.extend(then_counter_agg.elements.iter());
    build! {
        PipelineThenCounterAgg {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "counter_agg",
    schema = "toolkit_experimental"
)]
pub fn pipeline_counter_agg() -> toolkit_experimental::PipelineThenCounterAgg<'static> {
    build! {
        PipelineThenCounterAgg {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub unsafe fn pipeline_counter_agg_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element = PipelineThenCounterAgg::from_polymorphic_datum(
            new_element,
            false,
            pg_sys::Oid::INVALID,
        )
        .unwrap();
        finalize_with_counter_agg(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(
    r#"
ALTER FUNCTION "arrow_run_pipeline_then_counter_agg" SUPPORT toolkit_experimental.pipeline_counter_agg_support;
"#,
    name = "pipe_then_counter_agg",
    requires = [pipeline_counter_agg_support],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_run_pipeline_then_hyperloglog<'a>(
    mut timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::PipelineThenHyperLogLog<'a>,
) -> HyperLogLog<'static> {
    timevector = run_pipeline_elements(timevector, pipeline.elements.iter());
    HyperLogLog::build_from(
        pipeline.hll_size as i32,
        PgBuiltInOids::FLOAT8OID.into(),
        None,
        timevector
            .iter()
            .map(|point| point.val.into_datum().unwrap()),
    )
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn finalize_with_hyperloglog<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_hyperloglog: toolkit_experimental::PipelineThenHyperLogLog<'e>,
) -> toolkit_experimental::PipelineThenHyperLogLog<'e> {
    if then_hyperloglog.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {
            flatten! {
                PipelineThenHyperLogLog {
                    hll_size: then_hyperloglog.hll_size,
                    num_elements: pipeline.0.num_elements,
                    elements: pipeline.0.elements,
                }
            }
        };
    }

    let mut elements = take(pipeline.elements.as_owned());
    elements.extend(then_hyperloglog.elements.iter());
    build! {
        PipelineThenHyperLogLog {
            hll_size: then_hyperloglog.hll_size,
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "hyperloglog",
    schema = "toolkit_experimental"
)]
pub fn pipeline_hyperloglog(size: i32) -> toolkit_experimental::PipelineThenHyperLogLog<'static> {
    build! {
        PipelineThenHyperLogLog {
            hll_size: size as u64,
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub unsafe fn pipeline_hyperloglog_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element = PipelineThenHyperLogLog::from_polymorphic_datum(
            new_element,
            false,
            pg_sys::Oid::INVALID,
        )
        .unwrap();
        finalize_with_hyperloglog(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(
    r#"
ALTER FUNCTION "arrow_run_pipeline_then_hyperloglog" SUPPORT toolkit_experimental.pipeline_hyperloglog_support;
"#,
    name = "pipe_then_hll",
    requires = [pipeline_hyperloglog_support],
);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_run_pipeline_then_percentile_agg<'a>(
    mut timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::PipelineThenPercentileAgg<'a>,
) -> UddSketch<'static> {
    timevector = run_pipeline_elements(timevector, pipeline.elements.iter());
    UddSketch::from_iter(timevector.into_iter().map(|p| p.val))
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn finalize_with_percentile_agg<'e>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'e>,
    then_hyperloglog: toolkit_experimental::PipelineThenPercentileAgg<'e>,
) -> toolkit_experimental::PipelineThenPercentileAgg<'e> {
    if then_hyperloglog.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {
            flatten! {
                PipelineThenPercentileAgg {
                    num_elements: pipeline.0.num_elements,
                    elements: pipeline.0.elements,
                }
            }
        };
    }

    let mut elements = take(pipeline.elements.as_owned());
    elements.extend(then_hyperloglog.elements.iter());
    build! {
        PipelineThenPercentileAgg {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name = "percentile_agg",
    schema = "toolkit_experimental"
)]
pub fn pipeline_percentile_agg() -> toolkit_experimental::PipelineThenPercentileAgg<'static> {
    build! {
        PipelineThenPercentileAgg {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub unsafe fn pipeline_percentile_agg_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element = PipelineThenPercentileAgg::from_polymorphic_datum(
            new_element,
            false,
            pg_sys::Oid::INVALID,
        )
        .unwrap();
        finalize_with_percentile_agg(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(
    r#"
ALTER FUNCTION "arrow_run_pipeline_then_percentile_agg" SUPPORT toolkit_experimental.pipeline_percentile_agg_support;
"#,
    name = "pipe_then_percentile",
    requires = [pipeline_percentile_agg_support],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_stats_agg_finalizer() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> stats_agg())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,n:5,sx:100,sx2:250,sx3:0,sx4:21250)"
            );
        });
    }

    #[pg_test]
    fn test_stats_agg_pipeline_folding() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            let output = client
                .update(
                    "EXPLAIN (verbose) SELECT \
                timevector('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> stats_agg() -> average();",
                    None,
                    None,
                )
                .unwrap()
                .nth(1)
                .unwrap()
                .get_datum_by_ordinal(1)
                .unwrap()
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(output.trim(), "Output: (\
                arrow_run_pipeline_then_stats_agg(\
                    timevector('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethenstatsagg\
                ) -> '(version:1)'::accessoraverage)");
        });
    }

    #[pg_test]
    fn test_sum_finalizer() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client
                .update(
                    &format!("SELECT (series -> sum())::TEXT FROM ({}) s", create_series),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(val.unwrap(), "100");
        });
    }

    #[pg_test]
    fn test_sum_pipeline_folding() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            let output = client
                .update(
                    "EXPLAIN (verbose) SELECT \
                timevector('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> sum();",
                    None,
                    None,
                )
                .unwrap()
                .nth(1)
                .unwrap()
                .get_datum_by_ordinal(1)
                .unwrap()
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_pipeline_then_sum(\
                    timevector('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethensum\
                )");
        });
    }

    #[pg_test]
    fn test_average_finalizer() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> average())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(val.unwrap(), "20");
        });
    }

    #[pg_test]
    fn test_average_pipeline_folding() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            let output = client
                .update(
                    "EXPLAIN (verbose) SELECT \
                timevector('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> average();",
                    None,
                    None,
                )
                .unwrap()
                .nth(1)
                .unwrap()
                .get_datum_by_ordinal(1)
                .unwrap()
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_pipeline_then_average(\
                    timevector('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethenaverage\
                )");
        });
    }

    #[pg_test]
    fn test_num_vals_finalizer() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> num_vals())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(val.unwrap(), "5");
        });
    }

    #[pg_test]
    fn test_num_vals_pipeline_folding() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            let output = client
                .update(
                    "EXPLAIN (verbose) SELECT \
                timevector('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> num_vals();",
                    None,
                    None,
                )
                .unwrap()
                .nth(1)
                .unwrap()
                .get_datum_by_ordinal(1)
                .unwrap()
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_pipeline_then_num_vals(\
                    timevector('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethennumvals\
                )");
        });
    }

    #[pg_test]
    fn test_counter_agg_finalizer() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
            (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 10.0), \
                ('2020-01-01 UTC'::TIMESTAMPTZ, 15.0), \
                ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                ('2020-01-02 UTC'::TIMESTAMPTZ, 25.0), \
                ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> sort() -> counter_agg())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(val.unwrap(), "(version:1,stats:(n:5,sx:3156624000,sx2:74649600000,sx3:0,sx4:1894671345254400000000,sy:215,sy2:2280,sy3:6720.000000000007,sy4:1788960,sxy:12960000),first:(ts:\"2020-01-01 00:00:00+00\",val:15),second:(ts:\"2020-01-02 00:00:00+00\",val:25),penultimate:(ts:\"2020-01-04 00:00:00+00\",val:10),last:(ts:\"2020-01-05 00:00:00+00\",val:30),reset_sum:45,num_resets:2,num_changes:4,bounds:(is_present:0,has_left:0,has_right:0,padding:(0,0,0,0,0),left:None,right:None))");

            let val = client.update(
                &format!("SELECT series -> sort() -> counter_agg() -> with_bounds('[2020-01-01 UTC, 2020-02-01 UTC)') -> extrapolated_delta('prometheus') FROM ({}) s", create_series),
                None,
                None
            )
                .unwrap().first()
                .get_one::<f64>().unwrap().unwrap();
            assert!((val - 67.5).abs() < f64::EPSILON);

            let output = client
                .update(
                    "EXPLAIN (verbose) SELECT \
                timevector('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> counter_agg();",
                    None,
                    None,
                )
                .unwrap()
                .nth(1)
                .unwrap()
                .get_datum_by_ordinal(1)
                .unwrap()
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_run_pipeline_then_counter_agg(\
                    timevector('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethencounteragg\
                )");
        })
    }

    #[pg_test]
    fn test_hyperloglog_finalizer() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
            (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 10.0), \
                ('2020-01-01 UTC'::TIMESTAMPTZ, 15.0), \
                ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                ('2020-01-02 UTC'::TIMESTAMPTZ, 25.0), \
                ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0), \
                ('2020-01-06 UTC'::TIMESTAMPTZ, 25.0), \
                ('2020-01-07 UTC'::TIMESTAMPTZ, 15.0), \
                ('2020-01-08 UTC'::TIMESTAMPTZ, 35.0), \
                ('2020-01-09 UTC'::TIMESTAMPTZ, 10.0), \
                ('2020-01-10 UTC'::TIMESTAMPTZ, 5.0)) as v(time, value)";

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> hyperloglog(100))::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(val.unwrap(), "(version:1,log:Sparse(num_compressed:7,element_type:FLOAT8,collation:None,compressed_bytes:28,precision:7,compressed:[136,188,20,7,8,30,244,43,72,69,89,2,72,255,97,27,72,83,248,27,200,110,35,5,8,37,85,12]))");

            let val = client
                .update(
                    &format!(
                        "SELECT series -> hyperloglog(100) -> distinct_count() FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<i32>()
                .unwrap()
                .unwrap();
            assert_eq!(val, 7);

            let output = client
                .update(
                    "EXPLAIN (verbose) SELECT \
                timevector('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> hyperloglog(100);",
                    None,
                    None,
                )
                .unwrap()
                .nth(1)
                .unwrap()
                .get_datum_by_ordinal(1)
                .unwrap()
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_run_pipeline_then_hyperloglog(\
                    timevector('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,hll_size:100,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethenhyperloglog\
                )");
        })
    }

    #[pg_test]
    fn test_percentile_agg_finalizer() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timevector(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client
                .update(
                    &format!(
                        "SELECT (series -> percentile_agg())::TEXT FROM ({}) s",
                        create_series
                    ),
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(
                val.unwrap(),
                "(version:1,\
                    alpha:0.001,\
                    max_buckets:200,\
                    num_buckets:5,\
                    compactions:0,\
                    count:5,\
                    sum:100,\
                    buckets:[\
                        (Positive(1152),1),\
                        (Positive(1355),1),\
                        (Positive(1498),1),\
                        (Positive(1610),1),\
                        (Positive(1701),1)\
                    ]\
                )",
            );
        });
    }

    #[pg_test]
    fn test_percentile_agg_pipeline_folding() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client.update(&format!("SET LOCAL search_path TO {}", sp), None, None);

            let output = client
                .update(
                    "EXPLAIN (verbose) SELECT \
                timevector('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> percentile_agg();",
                    None,
                    None,
                )
                .unwrap()
                .nth(1)
                .unwrap()
                .get_datum_by_ordinal(1)
                .unwrap()
                .value::<String>()
                .unwrap()
                .unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_run_pipeline_then_percentile_agg(\
                    timevector('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethenpercentileagg\
                )");
        });
    }
}
