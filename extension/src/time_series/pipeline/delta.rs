
use pgx::*;

use super::*;

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="delta",
    schema="toolkit_experimental"
)]
pub fn delta_pipeline_element<'p, 'e>(
) -> toolkit_experimental::UnstableTimeseriesPipeline<'e> {
    Element::Delta {}.flatten()
}

pub fn timeseries_delta<'s>(
    series: &toolkit_experimental::TimeSeries<'s>,
) -> toolkit_experimental::TimeSeries<'s> {
    if !series.is_sorted() {
        panic!("can only compute deltas for sorted timeseries");
    }

    let mut it = series.iter();
    let mut prev = it.next().unwrap().val;
    let mut delta_points = Vec::new();

    for pt in it {
        delta_points.push(TSPoint{ts: pt.ts, val: pt.val - prev});
        prev = pt.val;
    }

    build!(
        TimeSeries {
            series: SeriesType::SortedSeries {
                num_points: delta_points.len() as u64,
                points: delta_points.into(),
            }
        }
    )
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_pipeline_delta() {
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
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 92.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.8), \
                    ('2020-01-06 UTC'::TIMESTAMPTZ, 30.8), \
                    ('2020-01-07 UTC'::TIMESTAMPTZ, 30.8), \
                    ('2020-01-08 UTC'::TIMESTAMPTZ, 30.9), \
                    ('2020-01-09 UTC'::TIMESTAMPTZ, -427.2)",
                None,
                None
            );

            let val = client.select(
                "SELECT (timeseries(time, value) |> delta())::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-03 00:00:00+00\",val:-5),\
                (ts:\"2020-01-04 00:00:00+00\",val:72),\
                (ts:\"2020-01-05 00:00:00+00\",val:-61.2),\
                (ts:\"2020-01-06 00:00:00+00\",val:0),\
                (ts:\"2020-01-07 00:00:00+00\",val:0),\
                (ts:\"2020-01-08 00:00:00+00\",val:0.09999999999999787),\
                (ts:\"2020-01-09 00:00:00+00\",val:-458.09999999999997)\
            ]");
        });
    }
}