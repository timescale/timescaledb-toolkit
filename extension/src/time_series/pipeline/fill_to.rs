
use pgx::*;

use flat_serialize_macro::FlatSerializable;

use serde::{Deserialize, Serialize};

use super::*;

// TODO: there are one or two other gapfill objects in this extension, these should be unified
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, FlatSerializable)]
#[repr(u64)]
pub enum FillToMethod {
    Locf,
    Interpolate,
    Nearest,
}

impl FillToMethod {
    pub fn fill_point(&self, lhs: &TSPoint, rhs: &TSPoint, target_ts: i64) -> TSPoint {
        match *self {
            FillToMethod::Locf => TSPoint{ts: target_ts, val: lhs.val},
            FillToMethod::Interpolate => {
                let interval = rhs.ts as f64 - lhs.ts as f64;
                let left_wt = 1. - (target_ts - lhs.ts) as f64 / interval;
                let right_wt = 1. - (rhs.ts - target_ts) as f64 / interval;
                TSPoint{ts: target_ts, val: lhs.val * left_wt + rhs.val * right_wt}
            },
            FillToMethod::Nearest =>
                if rhs.ts - target_ts >= target_ts - lhs.ts {
                    TSPoint{ts: target_ts, val: lhs.val}
                } else {
                    TSPoint{ts: target_ts, val: rhs.val}
                },
        }
    }
}

// TODO is (immutable, parallel_safe) correct?
#[pg_extern(
    immutable,
    parallel_safe,
    name="fill_to",
    schema="toolkit_experimental"
)]
pub fn fillto_pipeline_element<'e> (
    interval: crate::raw::Interval,
    fill_method: String,
) -> toolkit_experimental::UnstableTimevectorPipeline<'e> {
    unsafe {
        let interval = interval.0 as *const pg_sys::Interval;
        // TODO: store the postgres interval object and use postgres timestamp/interval functions
        let interval = ((*interval).month as i64 * 30 + (*interval).day as i64) * 24 * 60 * 60 * 1000000 + (*interval).time;

        let fill_method = match fill_method.to_lowercase().as_str() {
            "locf" => FillToMethod::Locf,
            "interpolate" => FillToMethod::Interpolate,
            "linear" => FillToMethod::Interpolate,
            "nearest" => FillToMethod::Nearest,
            _ => panic!("Invalid fill method")
        };

        Element::FillTo {
            interval,
            fill_method
        }.flatten()
    }
}

pub fn fill_to<'s>(
    series: toolkit_experimental::Timevector<'s>,
    element: &toolkit_experimental::Element
) -> toolkit_experimental::Timevector<'s> {
    let (interval, method) = match element {
        Element::FillTo{interval, fill_method} => (*interval, fill_method),
        _ => unreachable!()
    };

    match series.series {
        SeriesType::Explicit{..} => panic!("Timeseries must be sorted prior to passing to fill_to"),
        SeriesType::Normal{step_interval, ..} => if step_interval <= interval { return series; },
        _ => ()
    }

    let mut result = vec![];
    let mut it = series.iter().peekable();
    let mut current = it.next();

    while let (Some(lhs), Some(rhs)) = (current, it.peek()) {
        if rhs.ts - lhs.ts > interval {
            let mut target = lhs.ts + interval;
            while target < rhs.ts {
                result.push(method.fill_point(&lhs, rhs, target));
                target += interval;
            }
        }

        current = it.next();
    }

    if result.is_empty() {
        return series;
    }

    let mut result: Vec<TSPoint> = series.iter().chain(result.into_iter()).collect();
    result.sort_by_key(|p| p.ts);
    build!{
        Timevector {
            series: SeriesType::Sorted{
                num_points: result.len() as _,
                points: result.into(),
            }
        }
    }
}


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_pipeline_fill_to() {
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
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-04 UTC'::TIMESTAMPTZ, 90.0), \
                    ('2020-01-06 UTC'::TIMESTAMPTZ, 30),   \
                    ('2020-01-09 UTC'::TIMESTAMPTZ, 40.0)",
                None,
                None
            );


            let val = client.select(
                "SELECT (timevector(time, value) -> fill_to('24 hours', 'locf'))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-02 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-04 00:00:00+00\",val:90),\
                (ts:\"2020-01-05 00:00:00+00\",val:90),\
                (ts:\"2020-01-06 00:00:00+00\",val:30),\
                (ts:\"2020-01-07 00:00:00+00\",val:30),\
                (ts:\"2020-01-08 00:00:00+00\",val:30),\
                (ts:\"2020-01-09 00:00:00+00\",val:40)]");

            let val = client.select(
                "SELECT (timevector(time, value) -> fill_to('24 hours', 'linear'))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-02 00:00:00+00\",val:15),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-04 00:00:00+00\",val:90),\
                (ts:\"2020-01-05 00:00:00+00\",val:60),\
                (ts:\"2020-01-06 00:00:00+00\",val:30),\
                (ts:\"2020-01-07 00:00:00+00\",val:33.33333333333334),\
                (ts:\"2020-01-08 00:00:00+00\",val:36.66666666666667),\
                (ts:\"2020-01-09 00:00:00+00\",val:40)]");

            let val = client.select(
                "SELECT (timevector(time, value) -> fill_to('24 hours', 'nearest'))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-02 00:00:00+00\",val:10),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-04 00:00:00+00\",val:90),\
                (ts:\"2020-01-05 00:00:00+00\",val:90),\
                (ts:\"2020-01-06 00:00:00+00\",val:30),\
                (ts:\"2020-01-07 00:00:00+00\",val:30),\
                (ts:\"2020-01-08 00:00:00+00\",val:40),\
                (ts:\"2020-01-09 00:00:00+00\",val:40)]");

            let val = client.select(
                "SELECT (timevector(time, value) -> fill_to('10 hours', 'nearest'))::TEXT FROM series",
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "[\
                (ts:\"2020-01-01 00:00:00+00\",val:10),\
                (ts:\"2020-01-01 10:00:00+00\",val:10),\
                (ts:\"2020-01-01 20:00:00+00\",val:10),\
                (ts:\"2020-01-02 06:00:00+00\",val:20),\
                (ts:\"2020-01-02 16:00:00+00\",val:20),\
                (ts:\"2020-01-03 00:00:00+00\",val:20),\
                (ts:\"2020-01-03 10:00:00+00\",val:20),\
                (ts:\"2020-01-03 20:00:00+00\",val:90),\
                (ts:\"2020-01-04 00:00:00+00\",val:90),\
                (ts:\"2020-01-04 10:00:00+00\",val:90),\
                (ts:\"2020-01-04 20:00:00+00\",val:90),\
                (ts:\"2020-01-05 06:00:00+00\",val:30),\
                (ts:\"2020-01-05 16:00:00+00\",val:30),\
                (ts:\"2020-01-06 00:00:00+00\",val:30),\
                (ts:\"2020-01-06 10:00:00+00\",val:30),\
                (ts:\"2020-01-06 20:00:00+00\",val:30),\
                (ts:\"2020-01-07 06:00:00+00\",val:30),\
                (ts:\"2020-01-07 16:00:00+00\",val:40),\
                (ts:\"2020-01-08 02:00:00+00\",val:40),\
                (ts:\"2020-01-08 12:00:00+00\",val:40),\
                (ts:\"2020-01-08 22:00:00+00\",val:40),\
                (ts:\"2020-01-09 00:00:00+00\",val:40)]");
        });
    }
}