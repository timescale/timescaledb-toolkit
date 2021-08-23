
use std::{mem::{self, ManuallyDrop, MaybeUninit}, ptr};

use pgx::*;

use super::*;

use crate::{
    flatten,
};

// TODO is (stable, parallel_safe) correct?
#[pg_extern(
    stable,
    parallel_safe,
    name="map_data",
    schema="toolkit_experimental"
)]
pub fn map_data_pipeline_element<'e>(
    function: pg_sys::regproc,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    let mut argtypes: *mut pg_sys::Oid = ptr::null_mut();
    let mut nargs: ::std::os::raw::c_int = 0;
    let rettype = unsafe {
        pg_sys::get_func_signature(function, &mut argtypes, &mut nargs)
    };

    if nargs != 1 {
        error!("invalid number of mapping function arguments, expected fn(double precision) RETURNS double precision")
    }

    if unsafe { *argtypes } != pgx::PgBuiltInOids::FLOAT8OID.value() {
        error!("invalid argument type, expected fn(double precision) RETURNS double precision")
    }

    if rettype != pgx::PgBuiltInOids::FLOAT8OID.value() {
        error!("invalid return type, expected fn(double precision) RETURNS double precision")
    }

    unsafe {
        flatten!(
            UnstableTimeseriesPipelineElement {
                element: Element::MapData { function }
            }
        )
    }
}


pub fn apply_to(mut series: TimeSeries<'_>, func: pg_sys::RegProcedure)
-> TimeSeries<'_> {
    let mut flinfo: pg_sys::FmgrInfo = unsafe {
        MaybeUninit::zeroed().assume_init()
    };



    let fn_addr: unsafe extern "C" fn(*mut pg_sys::FunctionCallInfoBaseData) -> usize;
    let mut fc_info = unsafe {
        pg_sys::fmgr_info(func, &mut flinfo);
        fn_addr = flinfo.fn_addr.expect("null function in timeseries map");
        union FcInfo1 {
            data: ManuallyDrop<pg_sys::FunctionCallInfoBaseData>,
            #[allow(dead_code)]
            bytes: [u8; mem::size_of::<pg_sys::FunctionCallInfoBaseData>()
                + mem::size_of::<pg_sys::NullableDatum>()]
        }
        FcInfo1 {
            data: ManuallyDrop::new(pg_sys::FunctionCallInfoBaseData {
                flinfo: &mut flinfo,
                context: std::ptr::null_mut(),
                resultinfo: std::ptr::null_mut(),
                fncollation: pg_sys::InvalidOid,
                isnull: false,
                nargs: 1,
                args: Default::default(),
            }),
        }
    };


    let invoke = |val: f64| unsafe {
        let fc_info = &mut *fc_info.data;
        let args = fc_info.args.as_mut_slice(1);
        args[0].value = val.into_datum().unwrap();
        args[0].isnull = false;
        let res = fn_addr(fc_info);
        f64::from_datum(res, false, pg_sys::InvalidOid)
            .expect("unexpected NULL in timeseries mapping function")
    };

    //FIXME add setjmp guard around loop
    map_series(&mut series, invoke);
    series
}

fn map_series(series: &mut TimeSeries<'_>, mut func: impl FnMut(f64) -> f64) {
    use SeriesType::*;
    //FIXME add setjmp guard around loops
    match &mut series.series {
        SortedSeries { points, .. } => {
            for point in points.as_owned() {
                *point = TSPoint {
                    ts: point.ts,
                    val: func(point.val),
                }
            }
        },
        NormalSeries { values, .. } => {
            for value in values.as_owned() {
                *value = func(*value)
            }
        },
        ExplicitSeries { points, .. } => {
            for point in points.as_owned() {
                *point = TSPoint {
                    ts: point.ts,
                    val: func(point.val),
                }
            }
        },
        GappyNormalSeries { values, .. } => {
                for value in values.as_owned() {
                    *value = func(*value)
                }
        },
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_pipeline_map_data() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)",
                None,
                None
            );

            let val = client.select(
                "SELECT (timeseries(time, value))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":25.0},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":10.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":20.0},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":15.0},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":30.0}\
            ]");

            client.select(
                "CREATE FUNCTION x2(double precision) RETURNS DOUBLE PRECISION AS 'SELECT $1 * 2;' LANGUAGE SQL",
                None,
                None,
            );


            let val = client.select(
                "SELECT (timeseries(time, value) |> map_data('x2'))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":50.0},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":20.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":40.0},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":30.0},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":60.0}\
            ]");
        });
    }
}