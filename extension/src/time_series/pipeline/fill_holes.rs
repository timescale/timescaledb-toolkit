
use pgx::*;

use flat_serialize_macro::FlatSerializable;

use serde::{Deserialize, Serialize};

use super::*;

// TODO: there are one or two other gapfill objects in this extension, these should be unified
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, FlatSerializable)]
#[repr(u64)]
pub enum FillMethod {
    LOCF,
    Interpolate,
}

impl FillMethod {
    pub fn process<'s>(&self, series: TimeSeries<'s>) -> TimeSeries<'s> {
        match &series.series {
            SeriesType::GappyNormalSeries{start_ts, step_interval, count, present, values, ..} => {
                match self {
                    FillMethod::LOCF => {
                        let mut results = Vec::new();
                        let mut last_val = 0.0;
                        let mut vidx = 0;

                        for pidx in 0..*count {
                            if present.as_slice()[pidx as usize / 64] & 1 << (pidx % 64) != 0 {
                                last_val = values.as_slice()[vidx];
                                vidx += 1;
                            }
                            results.push(last_val);
                        }

                        build!(
                            TimeSeries {
                                series : SeriesType::NormalSeries {
                                    start_ts: *start_ts,
                                    step_interval: *step_interval,
                                    num_vals: *count,
                                    values: results.into(),
                                }
                            }
                        )
                    }
                    FillMethod::Interpolate => {
                        let mut iter = series.iter();
                        let mut prev = iter.next().unwrap();
                        let mut results = vec!(prev.val);

                        for point in iter {
                            let points = (point.ts - prev.ts) / step_interval;
                            for p in 1..=points {
                                results.push(prev.val + (point.val - prev.val) * (p as f64 / points as f64));
                            }
                            prev = point;
                        }

                        build!(
                            TimeSeries {
                                series : SeriesType::NormalSeries {
                                    start_ts: *start_ts,
                                    step_interval: *step_interval,
                                    num_vals: *count,
                                    values: results.into(),
                                }
                            }
                        )
                    }
                }
            }

            SeriesType::NormalSeries{..} => series.clone(),

            _ => panic!("Gapfill not currently implemented for explicit timeseries")
        }
    }
}

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="fill_holes",
    schema="toolkit_experimental"
)]
pub fn holefill_pipeline_element<'e> (
    fill_method: String,
) -> toolkit_experimental::UnstableTimeseriesPipeline<'e> {
    let fill_method = match fill_method.to_lowercase().as_str() {
        "locf" => FillMethod::LOCF,
        "interpolate" => FillMethod::Interpolate,
        "linear" => FillMethod::Interpolate,
        _ => panic!("Invalid downsample method")
    };

    Element::FillHoles {
        fill_method
    }.flatten()
}

pub fn fill_holes<'s>(
    series: toolkit_experimental::TimeSeries<'s>,
    element: &toolkit_experimental::Element
) -> toolkit_experimental::TimeSeries<'s> {
    let method = match element {
        Element::FillHoles{fill_method: gapfill_method} => gapfill_method,
        _ => panic!("Gapfill evaluator called on incorrect pipeline element")
    };

    method.process(series)
}


#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_pipeline_gapfill() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            client.select(
                "CREATE TABLE gappy_series(time timestamptz, value double precision)",
                None,
                None
            );
            client.select(
                "INSERT INTO gappy_series \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(1,5,0.1) foo",
                None,
                None
            );


            client.select(
                "INSERT INTO gappy_series \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(5.5,8,0.1) foo",
                None,
                None
            );

            client.select(
                "INSERT INTO gappy_series \
                    SELECT \
                        '2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int) as time, \
                        TRUNC((10 + 5 * cos(foo))::numeric, 4) as val \
                    FROM generate_series(11,13,0.1) foo",
                None,
                None
            );

            let val = client.select(
                "SELECT (timeseries(time, value) |> resample_to_rate('average', '240 hours', true))::TEXT FROM gappy_series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.5779},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":6.30572},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.430009999999999},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":8.75585},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.679616666666666},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":12.10525},\
                {\"ts\":\"2020-05-05 00:00:00+00\",\"val\":14.76376},\
                {\"ts\":\"2020-05-15 00:00:00+00\",\"val\":14.5372}\
            ]");


            let val = client.select(
                "SELECT (timeseries(time, value) |> resample_to_rate('average', '240 hours', true) |> fill_holes('LOCF'))::TEXT FROM gappy_series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.5779},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":6.30572},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.430009999999999},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":8.75585},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.679616666666666},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":12.10525},\
                {\"ts\":\"2020-05-05 00:00:00+00\",\"val\":14.76376},\
                {\"ts\":\"2020-05-15 00:00:00+00\",\"val\":14.5372}\
            ]");

            let val = client.select(
                "SELECT (timeseries(time, value) |> resample_to_rate('average', '240 hours', true) |> fill_holes('interpolate'))::TEXT FROM gappy_series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                {\"ts\":\"2020-01-16 00:00:00+00\",\"val\":10.5779},\
                {\"ts\":\"2020-01-26 00:00:00+00\",\"val\":6.30572},\
                {\"ts\":\"2020-02-05 00:00:00+00\",\"val\":5.430009999999999},\
                {\"ts\":\"2020-02-15 00:00:00+00\",\"val\":8.75585},\
                {\"ts\":\"2020-02-25 00:00:00+00\",\"val\":13.679616666666666},\
                {\"ts\":\"2020-03-06 00:00:00+00\",\"val\":14.729629999999997},\
                {\"ts\":\"2020-03-16 00:00:00+00\",\"val\":11.885259999999999},\
                {\"ts\":\"2020-03-26 00:00:00+00\",\"val\":9.2724},\
                {\"ts\":\"2020-04-05 00:00:00+00\",\"val\":10.216683333333332},\
                {\"ts\":\"2020-04-15 00:00:00+00\",\"val\":11.160966666666667},\
                {\"ts\":\"2020-04-25 00:00:00+00\",\"val\":12.10525},\
                {\"ts\":\"2020-05-05 00:00:00+00\",\"val\":14.76376},\
                {\"ts\":\"2020-05-15 00:00:00+00\",\"val\":14.5372}\
            ]");
        });
    }
}