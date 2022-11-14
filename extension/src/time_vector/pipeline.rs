mod aggregation;
mod arithmetic;
mod delta;
mod expansion;
mod fill_to;
mod filter;
mod lambda;
mod map;
mod sort;

use std::convert::TryInto;

use pgx::*;

use super::*;

use crate::{flatten, pg_type, ron_inout_funcs};

use fill_to::{fill_to, FillToMethod};

use delta::timevector_delta;
use sort::sort_timevector;

pub use self::toolkit_experimental::*;
use crate::serialization::PgProcId;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;
    pub use crate::time_vector::Timevector_TSTZ_F64;
    pub(crate) use lambda::toolkit_experimental::{Lambda, LambdaData};
    // TODO once we start stabilizing elements, create a type TimevectorPipeline
    //      stable elements will create a stable pipeline, but adding an unstable
    //      element to a stable pipeline will create an unstable pipeline
    pg_type! {
        #[derive(Debug)]
        struct UnstableTimevectorPipeline<'input> {
            num_elements: u64,
            elements: [Element<'input>; self.num_elements],
        }
    }

    flat_serialize_macro::flat_serialize! {
        #[derive(Debug)]
        #[derive(serde::Serialize, serde::Deserialize)]
        enum Element<'input> {
            kind: u64,
            LTTB: 1 {
                resolution: u64,
            },
            // 2 was for resample_to_rate
            // 3 was for fill_holes
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
            },
            MapLambda: 9 {
                lambda: LambdaData<'input>,
            },
            FilterLambda: 10 {
                lambda: LambdaData<'input>,
            },
            FillTo: 11 {
                interval: i64,
                fill_method: FillToMethod,
            },
        }
    }

    impl<'input> Element<'input> {
        pub fn flatten<'a>(self) -> UnstableTimevectorPipeline<'a> {
            // TODO it'd be nice not to have to allocate a vector here but
            //      `let slice = &[self][..];`
            //      gives a lifetime error I don't yet know how to solve
            let slice = vec![self].into();
            unsafe {
                flatten! {
                    UnstableTimevectorPipeline {
                        num_elements: 1,
                        elements: slice,
                    }
                }
            }
        }
    }

    impl<'e> From<Element<'e>> for UnstableTimevectorPipeline<'e> {
        fn from(element: Element<'e>) -> Self {
            build! {
                UnstableTimevectorPipeline {
                    num_elements: 1,
                    elements: vec![element].into(),
                }
            }
        }
    }

    ron_inout_funcs!(UnstableTimevectorPipeline);
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_run_pipeline<'a>(
    timevector: Timevector_TSTZ_F64<'a>,
    pipeline: toolkit_experimental::UnstableTimevectorPipeline<'a>,
) -> Timevector_TSTZ_F64<'static> {
    run_pipeline_elements(timevector, pipeline.elements.iter()).in_current_context()
}

pub fn run_pipeline_elements<'s, 'j, 'i>(
    mut timevector: Timevector_TSTZ_F64<'s>,
    pipeline: impl Iterator<Item = Element<'j>> + 'i,
) -> Timevector_TSTZ_F64<'s> {
    for element in pipeline {
        timevector = execute_pipeline_element(timevector, &element);
    }
    timevector
}

pub fn execute_pipeline_element<'s>(
    timevector: Timevector_TSTZ_F64<'s>,
    element: &Element,
) -> Timevector_TSTZ_F64<'s> {
    match element {
        Element::LTTB { resolution } => crate::lttb::lttb_ts(timevector, *resolution as _),
        Element::Sort { .. } => sort_timevector(timevector),
        Element::Delta { .. } => timevector_delta(&timevector),
        Element::MapData { function } => map::apply_to(timevector, function.0),
        Element::MapSeries { function } => map::apply_to_series(timevector, function.0),
        Element::MapLambda { lambda } => map::apply_lambda_to(timevector, lambda),
        Element::FilterLambda { lambda } => filter::apply_lambda_to(timevector, lambda),
        Element::Arithmetic { function, rhs } => arithmetic::apply(timevector, *function, *rhs),
        Element::FillTo { .. } => fill_to(timevector, element),
    }
}

// TODO is (immutable, parallel_safe) correct?
#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_add_unstable_element<'p>(
    mut pipeline: toolkit_experimental::UnstableTimevectorPipeline<'p>,
    element: toolkit_experimental::UnstableTimevectorPipeline<'p>,
) -> toolkit_experimental::UnstableTimevectorPipeline<'p> {
    pipeline.elements.as_owned().extend(element.elements.iter());
    pipeline.num_elements = pipeline.elements.len().try_into().unwrap();
    pipeline
}

#[pg_extern(
    immutable,
    parallel_safe,
    schema = "toolkit_experimental",
    name = "toolkit_pipeline_support"
)]
pub unsafe fn pipeline_support(input: Internal) -> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| {
        let new_element =
            UnstableTimevectorPipeline::from_polymorphic_datum(new_element, false, 0).unwrap();
        arrow_add_unstable_element(old_pipeline, new_element)
            .into_datum()
            .unwrap()
    })
}

pub(crate) unsafe fn pipeline_support_helper(
    input: Internal,
    make_new_pipeline: impl FnOnce(UnstableTimevectorPipeline, pg_sys::Datum) -> pg_sys::Datum,
) -> Internal {
    use std::mem::{size_of, MaybeUninit};

    let input = input.unwrap().unwrap();
    let input: *mut pg_sys::Node = input.cast_mut_ptr();
    if !pgx::is_a(input, pg_sys::NodeTag_T_SupportRequestSimplify) {
        return no_change();
    }

    let req: *mut pg_sys::SupportRequestSimplify = input.cast();

    let final_executor = (*req).fcall;
    let original_args = PgList::from_pg((*final_executor).args);
    assert_eq!(original_args.len(), 2);
    let arg1 = original_args.head().unwrap();
    let arg2 = original_args.tail().unwrap();

    let (executor_id, lhs_args) = if is_a(arg1, pg_sys::NodeTag_T_OpExpr) {
        let old_executor: *mut pg_sys::OpExpr = arg1.cast();
        ((*old_executor).opfuncid, (*old_executor).args)
    } else if is_a(arg1, pg_sys::NodeTag_T_FuncExpr) {
        let old_executor: *mut pg_sys::FuncExpr = arg1.cast();
        ((*old_executor).funcid, (*old_executor).args)
    } else {
        return no_change();
    };

    // check old_executor operator fn is 'run_pipeline' above
    static RUN_PIPELINE_OID: once_cell::sync::OnceCell<pg_sys::Oid> =
        once_cell::sync::OnceCell::new();
    match RUN_PIPELINE_OID.get() {
        Some(oid) => {
            if executor_id != *oid {
                return no_change();
            }
        }
        None => {
            let executor_fn = {
                let mut flinfo: pg_sys::FmgrInfo = MaybeUninit::zeroed().assume_init();
                pg_sys::fmgr_info(executor_id, &mut flinfo);
                flinfo.fn_addr
            };
            // FIXME this cast should not be necessary; pgx is defining the
            //       wrapper functions as
            //       `unsafe fn(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum`
            //       instead of
            //       `unsafe extern "C" fn(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum`
            //       we'll fix this upstream
            let expected_executor = arrow_run_pipeline_wrapper as usize;
            match executor_fn {
                None => return no_change(),
                // FIXME the direct comparison should work
                Some(func) if func as usize != expected_executor => return no_change(),
                Some(_) => RUN_PIPELINE_OID.get_or_init(|| executor_id),
            };
        }
    }

    let lhs_args = PgList::from_pg(lhs_args);
    assert_eq!(lhs_args.len(), 2);
    let old_series = lhs_args.head().unwrap();
    let old_const = lhs_args.tail().unwrap();

    if !is_a(old_const, pg_sys::NodeTag_T_Const) {
        return no_change();
    }

    let old_const: *mut pg_sys::Const = old_const.cast();

    if !is_a(arg2, pg_sys::NodeTag_T_Const) {
        return no_change();
    }

    let new_element_const: *mut pg_sys::Const = arg2.cast();

    let old_pipeline =
        UnstableTimevectorPipeline::from_polymorphic_datum((*old_const).constvalue, false, 0)
            .unwrap();
    let new_pipeline = make_new_pipeline(old_pipeline, (*new_element_const).constvalue);

    let new_const = pg_sys::palloc(size_of::<pg_sys::Const>()).cast();
    *new_const = *new_element_const;
    (*new_const).constvalue = new_pipeline;

    let new_executor = pg_sys::palloc(size_of::<pg_sys::FuncExpr>()).cast();
    *new_executor = *final_executor;
    let mut new_executor_args = PgList::new();
    new_executor_args.push(old_series);
    new_executor_args.push(new_const.cast());
    (*new_executor).args = new_executor_args.into_pg();

    Internal::from(Some(pg_sys::Datum::from(new_executor)))
}

// support functions are spec'd as returning NULL pointer if no simplification
// can be made
fn no_change() -> pgx::Internal {
    Internal::from(Some(pg_sys::Datum::from(
        std::ptr::null_mut::<pg_sys::Expr>(),
    )))
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(
    r#"
ALTER FUNCTION "arrow_run_pipeline" SUPPORT toolkit_experimental.toolkit_pipeline_support;
ALTER FUNCTION "arrow_add_unstable_element" SUPPORT toolkit_experimental.toolkit_pipeline_support;
"#,
    name = "pipe_support",
    requires = [pipeline_support],
);

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name = "lttb",
    schema = "toolkit_experimental"
)]
pub fn lttb_pipeline_element(
    resolution: i32,
) -> toolkit_experimental::UnstableTimevectorPipeline<'static> {
    Element::LTTB {
        resolution: resolution.try_into().unwrap(),
    }
    .flatten()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_pipeline_lttb() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select(
                "CREATE TABLE lttb_pipe (series timevector_tstz_f64)",
                None,
                None,
            );
            client.select(
                "INSERT INTO lttb_pipe \
                SELECT timevector(time, val) FROM ( \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(1,11,0.1) foo \
                ) bar",
                None,
                None
            );

            let val = client
                .select(
                    "SELECT (series -> lttb(17))::TEXT FROM lttb_pipe",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:17,flags:1,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-11 00:00:00+00\",val:12.7015),\
                (ts:\"2020-01-13 00:00:00+00\",val:11.8117),\
                (ts:\"2020-01-22 00:00:00+00\",val:7.4757),\
                (ts:\"2020-01-28 00:00:00+00\",val:5.4796),\
                (ts:\"2020-02-03 00:00:00+00\",val:5.0626),\
                (ts:\"2020-02-09 00:00:00+00\",val:6.3703),\
                (ts:\"2020-02-14 00:00:00+00\",val:8.4633),\
                (ts:\"2020-02-24 00:00:00+00\",val:13.1734),\
                (ts:\"2020-03-01 00:00:00+00\",val:14.8008),\
                (ts:\"2020-03-07 00:00:00+00\",val:14.7511),\
                (ts:\"2020-03-13 00:00:00+00\",val:13.0417),\
                (ts:\"2020-03-23 00:00:00+00\",val:8.3042),\
                (ts:\"2020-03-29 00:00:00+00\",val:5.9445),\
                (ts:\"2020-04-04 00:00:00+00\",val:5.0015),\
                (ts:\"2020-04-10 00:00:00+00\",val:5.8046),\
                (ts:\"2020-04-14 00:00:00+00\",val:7.195),\
                (ts:\"2020-04-20 00:00:00+00\",val:10.0221)\
            ],null_val:[0,0,0])"
            );

            let val = client
                .select(
                    "SELECT (series -> lttb(8))::TEXT FROM lttb_pipe",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:8,flags:1,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-11 00:00:00+00\",val:12.7015),\
                (ts:\"2020-01-27 00:00:00+00\",val:5.7155),\
                (ts:\"2020-02-06 00:00:00+00\",val:5.5162),\
                (ts:\"2020-02-27 00:00:00+00\",val:14.1735),\
                (ts:\"2020-03-09 00:00:00+00\",val:14.3469),\
                (ts:\"2020-03-30 00:00:00+00\",val:5.6728),\
                (ts:\"2020-04-09 00:00:00+00\",val:5.554),\
                (ts:\"2020-04-20 00:00:00+00\",val:10.0221)\
            ],null_val:[0])"
            );

            let val = client
                .select(
                    "SELECT (series -> lttb(8) -> lttb(8))::TEXT FROM lttb_pipe",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:8,flags:1,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-11 00:00:00+00\",val:12.7015),\
                (ts:\"2020-01-27 00:00:00+00\",val:5.7155),\
                (ts:\"2020-02-06 00:00:00+00\",val:5.5162),\
                (ts:\"2020-02-27 00:00:00+00\",val:14.1735),\
                (ts:\"2020-03-09 00:00:00+00\",val:14.3469),\
                (ts:\"2020-03-30 00:00:00+00\",val:5.6728),\
                (ts:\"2020-04-09 00:00:00+00\",val:5.554),\
                (ts:\"2020-04-20 00:00:00+00\",val:10.0221)\
            ],null_val:[0])"
            );

            let val = client
                .select(
                    "SELECT (series -> (lttb(8) -> lttb(8) -> lttb(8)))::TEXT FROM lttb_pipe",
                    None,
                    None,
                )
                .first()
                .get_one::<String>();
            assert_eq!(
                val.unwrap(),
                "(version:1,num_points:8,flags:1,internal_padding:(0,0,0),points:[\
                (ts:\"2020-01-11 00:00:00+00\",val:12.7015),\
                (ts:\"2020-01-27 00:00:00+00\",val:5.7155),\
                (ts:\"2020-02-06 00:00:00+00\",val:5.5162),\
                (ts:\"2020-02-27 00:00:00+00\",val:14.1735),\
                (ts:\"2020-03-09 00:00:00+00\",val:14.3469),\
                (ts:\"2020-03-30 00:00:00+00\",val:5.6728),\
                (ts:\"2020-04-09 00:00:00+00\",val:5.554),\
                (ts:\"2020-04-20 00:00:00+00\",val:10.0221)\
            ],null_val:[0])"
            );
        });
    }

    #[pg_test]
    fn test_pipeline_folding() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .select(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    None,
                )
                .first()
                .get_one::<String>()
                .unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            let output = client.select(
                "EXPLAIN (verbose) SELECT timevector('2021-01-01'::timestamptz, 0.1) -> round() -> abs() -> round();",
                None,
                None
            ).nth(1)
                .unwrap()
                .by_ordinal(1).unwrap()
                .value::<String>().unwrap();
            // check that it's executing as if we had input `timevector -> (round() -> abs())`
            assert_eq!(output.trim(), "Output: \
                arrow_run_pipeline(\
                    timevector('2021-01-01 00:00:00+00'::timestamp with time zone, '0.1'::double precision), \
                   '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Round,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Round,rhs:0)\
                    ])'::unstabletimevectorpipeline\
                )");
        });
    }
}
