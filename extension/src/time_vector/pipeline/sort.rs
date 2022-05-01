
use pgx::*;

use super::*;

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="sort",
    schema="toolkit_experimental"
)]
pub fn sort_pipeline_element<'p, 'e>(
) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    Element::Sort {}.flatten()
}

pub fn sort_timevector(
    mut series: toolkit_experimental::Timevector<'_>,
) -> toolkit_experimental::Timevector<'_> {
    if series.is_sorted() {
        return series;
    }
    
    let mut points = std::mem::take(series.points.as_owned());
    points.sort_by(|a, b| a.ts.cmp(&b.ts));
    TimevectorData {
        header: 0,
        version: 1,
        padding: [0; 3],
        num_points: points.len() as u64,
        points: points.into(),
        is_sorted: true,
    }.into()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_pipeline_sort() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);

            client.select(
                "CREATE TABLE series(time timestamptz, value double precision)",
                None,
                None
            );
            client.select(
                "INSERT INTO series \
                    VALUES \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 25), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30)",
                None,
                None
            );

            let val = client.select(
                "SELECT (timevector(time, value))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ]");


            let val = client.select(
                "SELECT (timevector(time, value) -> sort())::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-04 00:00:00+00\",val:25),\
                (ts:\"2020-01-05 00:00:00+00\",val:30)\
            ]");
        });
    }
}