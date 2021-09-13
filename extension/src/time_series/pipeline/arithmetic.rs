
use pgx::*;

use super::*;

use super::Element::Arithmetic;
use Function::*;

#[derive(Debug, Copy, Clone, flat_serialize_macro::FlatSerializable, serde::Serialize, serde::Deserialize)]
#[repr(u64)]
//XXX note that the order here _is_ significant
pub enum Function {
    // binary functions
    Add = 1,
    Sub = 2,
    Mul = 3,
    Div = 4,
    Mod = 5,
    Power = 6,
    LogN = 7,
}

pub fn apply(
    mut series: TimeSeries<'_>,
    function: Function,
    rhs: f64,
) -> TimeSeries<'_> {
    let function: fn(f64, f64) -> f64 = match function {
        Add => |a, b| a + b,
        Sub => |a, b| a - b,
        Mul => |a, b| a * b,
        Div => |a, b| a / b,
        // TODO is this the right mod?
        Mod => |a, b| a % b,
        Power => |a, b| a.powf(b),
        LogN => |a, b| a.log(b),
    };
    map::map_series(&mut series, |lhs| function(lhs, rhs));
    series
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="add",
    schema="toolkit_experimental"
)]
pub fn pipeline_add<'e>(
    rhs: f64,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    build!(
        UnstableTimeseriesPipelineElement {
            element: Arithmetic { function: Add, rhs: rhs }
        }
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="sub",
    schema="toolkit_experimental"
)]
pub fn pipeline_sub<'e>(
    rhs: f64,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    build!(
        UnstableTimeseriesPipelineElement {
            element: Arithmetic { function: Sub, rhs: rhs }
        }
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="mul",
    schema="toolkit_experimental"
)]
pub fn pipeline_mul<'e>(
    rhs: f64,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    build!(
        UnstableTimeseriesPipelineElement {
            element: Arithmetic { function: Mul, rhs: rhs }
        }
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="div",
    schema="toolkit_experimental"
)]
pub fn pipeline_div<'e>(
    rhs: f64,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    build!(
        UnstableTimeseriesPipelineElement {
            element: Arithmetic { function: Div, rhs: rhs }
        }
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="mod",
    schema="toolkit_experimental"
)]
pub fn pipeline_mod<'e>(
    rhs: f64,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    build!(
        UnstableTimeseriesPipelineElement {
            element: Arithmetic { function: Mod, rhs: rhs }
        }
    )
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="power",
    schema="toolkit_experimental"
)]
pub fn pipeline_power<'e>(
    rhs: f64,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    build!(
        UnstableTimeseriesPipelineElement {
            element: Arithmetic { function: Power, rhs: rhs }
        }
    )
}

// log(double) already exists as the log base 10 so we need a new name
#[pg_extern(
    immutable,
    parallel_safe,
    name="logN",
    schema="toolkit_experimental"
)]
pub fn pipeline_log_n<'e>(
    rhs: f64,
) -> toolkit_experimental::UnstableTimeseriesPipelineElement<'e> {
    build!(
        UnstableTimeseriesPipelineElement {
            element: Arithmetic { function: LogN, rhs: rhs }
        }
    )
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_simple_arith_map() {
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
                &format!("SELECT (series |> add(1.0))::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":26.0},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":11.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":21.0},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":16.0},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":31.0}\
            ]");

            let val = client.select(
                &format!("SELECT (series |> sub(3.0))::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":22.0},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":7.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":17.0},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":12.0},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":27.0}\
            ]");

            let val = client.select(
                &format!("SELECT (series |> mul(2.0))::TEXT FROM ({}) s", create_series),
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

            let val = client.select(
                &format!("SELECT (series |> div(5.0))::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":5.0},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":2.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":4.0},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":3.0},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":6.0}\
            ]");

            let val = client.select(
                &format!("SELECT (series |> mod(5.0))::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":0.0},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":0.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":0.0},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":0.0},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":0.0}\
            ]");

            let val = client.select(
                &format!("SELECT (series |> power(2.0))::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":625.0},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":100.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":400.0},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":225.0},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":900.0}\
            ]");


            let val = client.select(
                &format!("SELECT (series |> logN(10.0))::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-04 00:00:00+00\",\"val\":1.3979400086720375},\
                {\"ts\":\"2020-01-01 00:00:00+00\",\"val\":1.0},\
                {\"ts\":\"2020-01-03 00:00:00+00\",\"val\":1.301029995663981},\
                {\"ts\":\"2020-01-02 00:00:00+00\",\"val\":1.1760912590556811},\
                {\"ts\":\"2020-01-05 00:00:00+00\",\"val\":1.4771212547196624}\
            ]");
        });
    }
}