
use pgx::*;

use super::*;

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="delta_cast",
    schema="toolkit_experimental"
)]
pub fn delta_pipeline_element<'p, 'e>(
    accessor: toolkit_experimental::AccessorDelta<'p>,
) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    let _ = accessor;
    Element::Delta {}.flatten()
}

extension_sql!(r#"
    CREATE CAST (toolkit_experimental.AccessorDelta AS toolkit_experimental.UnstableTimevectorPipeline)
        WITH FUNCTION toolkit_experimental.delta_cast
        AS IMPLICIT;
"#,
name="accessor_delta_cast",
);

pub fn timevector_delta<'s>(
    series: &toolkit_experimental::Timevector<'s>,
) -> toolkit_experimental::Timevector<'s> {
    if !series.is_sorted() {
        panic!("can only compute deltas for sorted timevector");
    }
    if series.has_nulls() {
        panic!("Unable to compute deltas over timevector containing nulls");
    }

    let mut it = series.iter();
    let mut prev = it.next().unwrap().val;
    let mut delta_points = Vec::new();

    for pt in it {
        delta_points.push(TSPoint{ts: pt.ts, val: pt.val - prev});
        prev = pt.val;
    }
    
    let nulls_len = (delta_points.len() + 7) / 8;

    build!(
        Timevector {
            num_points: delta_points.len() as u32,
            is_sorted: true,
            flags: series.flags,
            internal_padding: [0; 2],
            points: delta_points.into(),
            null_val: std::vec::from_elem(0 as u8, nulls_len).into(),
        }
    )
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_pipeline_delta() {
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
                "SELECT (timevector(time, value) -> delta())::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "(version:1,num_points:8,is_sorted:true,flags:0,internal_padding:(0,0),points:[\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-03 00:00:00+00\",val:-5),\
                (ts:\"2020-01-04 00:00:00+00\",val:72),\
                (ts:\"2020-01-05 00:00:00+00\",val:-61.2),\
                (ts:\"2020-01-06 00:00:00+00\",val:0),\
                (ts:\"2020-01-07 00:00:00+00\",val:0),\
                (ts:\"2020-01-08 00:00:00+00\",val:0.09999999999999787),\
                (ts:\"2020-01-09 00:00:00+00\",val:-458.09999999999997)\
            ],null_val:[0])");
        });
    }
}