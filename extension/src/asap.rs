use asap::*;
use pgrx::*;
use serde::{Deserialize, Serialize};

use crate::{
    aggregate_utils::in_aggregate_context,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    time_vector,
};

use tspoint::TSPoint;

use crate::time_vector::{Timevector_TSTZ_F64, Timevector_TSTZ_F64Data};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ASAPTransState {
    ts: Vec<TSPoint>,
    sorted: bool,
    resolution: i32,
}

#[pg_extern(immutable, parallel_safe)]
pub fn asap_trans(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    asap_trans_internal(unsafe { state.to_inner() }, ts, val, resolution, fcinfo).internal()
}
pub fn asap_trans_internal(
    state: Option<Inner<ASAPTransState>>,
    ts: Option<crate::raw::TimestampTz>,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<ASAPTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let p = match (ts, val) {
                (_, None) => return state,
                (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint { ts: ts.into(), val },
            };

            match state {
                None => Some(
                    ASAPTransState {
                        ts: vec![p],
                        sorted: true,
                        resolution,
                    }
                    .into(),
                ),
                Some(mut s) => {
                    s.add_point(p);
                    Some(s)
                }
            }
        })
    }
}

impl ASAPTransState {
    fn add_point(&mut self, point: TSPoint) {
        self.ts.push(point);
        if let Some(window) = self.ts.windows(2).last() {
            if window[0].ts > window[1].ts {
                self.sorted = false
            }
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
fn asap_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    asap_final_inner(unsafe { state.to_inner() }, fcinfo)
}
fn asap_final_inner(
    state: Option<Inner<ASAPTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state.clone(),
            };

            let mut points = state.ts;
            if !state.sorted {
                points.sort_by_key(|p| p.ts);
            }

            let start_ts = points.first().unwrap().ts;
            let end_ts = points.last().unwrap().ts;

            let mut values: Vec<f64> = points.iter().map(|p| p.val).collect();
            values = asap_smooth(&values, state.resolution as u32);

            let interval = if values.len() > 1 {
                (end_ts - start_ts) / (values.len() - 1) as i64
            } else {
                1
            };

            let points: Vec<_> = values
                .into_iter()
                .enumerate()
                .map(|(i, val)| TSPoint {
                    ts: start_ts + i as i64 * interval,
                    val,
                })
                .collect();

            let nulls_len = points.len().div_ceil(8);

            Some(crate::build! {
                Timevector_TSTZ_F64 {
                    num_points: points.len() as u32,
                    flags: time_vector::FLAG_IS_SORTED,
                    internal_padding: [0; 3],
                    points: points.into(),
                    null_val: std::vec::from_elem(0_u8, nulls_len).into(),
                }
            })
        })
    }
}

#[pg_extern(name = "asap_smooth", immutable, parallel_safe)]
pub fn asap_on_timevector(
    mut series: Timevector_TSTZ_F64<'static>,
    resolution: i32,
) -> Option<Timevector_TSTZ_F64<'static>> {
    // TODO: implement this using zero copy (requires sort, find_downsample_interval, and downsample_and_gapfill on Timevector)
    let needs_sort = series.is_sorted();

    if needs_sort {
        series.points.as_owned().sort_by_key(|p| p.ts);
    }
    let start_ts = series.points.as_slice().first().unwrap().ts;
    let end_ts = series.points.as_slice().last().unwrap().ts;

    let values: Vec<f64> = series.points.as_slice().iter().map(|p| p.val).collect();

    let result = asap_smooth(&values, resolution as u32);

    let interval = if result.len() > 1 {
        (end_ts - start_ts) / (result.len() - 1) as i64
    } else {
        1
    };

    let points: Vec<_> = result
        .into_iter()
        .enumerate()
        .map(|(i, val)| TSPoint {
            ts: start_ts + i as i64 * interval,
            val,
        })
        .collect();

    let nulls_len = points.len().div_ceil(8);

    Some(crate::build! {
        Timevector_TSTZ_F64 {
            num_points: points.len() as u32,
            flags: time_vector::FLAG_IS_SORTED,
            internal_padding: [0; 3],
            points: points.into(),
            null_val: std::vec::from_elem(0_u8, nulls_len).into(),
        }
    })
}

// Aggregate on only values (assumes aggregation over ordered normalized timestamp)
extension_sql!(
    "\n\
    CREATE AGGREGATE asap_smooth(ts TIMESTAMPTZ, value DOUBLE PRECISION, resolution INT)\n\
    (\n\
        sfunc = asap_trans,\n\
        stype = internal,\n\
        finalfunc = asap_final\n\
    );\n",
    name = "asap_agg",
    requires = [asap_trans, asap_final],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use approx::assert_relative_eq;
    use pgrx::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn test_against_reference() {
        // Test our ASAP implementation against the reference implementation at http://www.futuredata.io.s3-website-us-west-2.amazonaws.com/asap/
        // The sample data is the first 100 points of the second sample data set.  Note that the dates are not important for this test.
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            let mut result = client.update(
                "
                SELECT value
                FROM unnest(
                (SELECT asap_smooth('2020-1-1'::timestamptz + i * '1d'::interval, val, 10)
                    FROM (VALUES 
                        (1,1.1),(2,4.4),(3,7.5),(4,8.9),(5,11.7),(6,15),(7,15.3),(8,15.6),(9,13.3),(10,11.1),
                        (11,7.5),(12,5.8),(13,5.6),(14,4.2),(15,4.7),(16,7.2),(17,11.4),(18,15.3),(19,15),(20,16.2),
                        (21,14.4),(22,8.6),(23,5.3),(24,3.3),(25,4.4),(26,3.3),(27,5),(28,8.1),(29,10.8),(30,12.2),
                        (31,13.8),(32,13.3),(33,12.8),(34,9.4),(35,6.9),(36,3.9),(37,1.1),(38,4.2),(39,4.2),(40,8.4),
                        (41,13.4),(42,16.4),(43,16),(44,15.6),(45,14.7),(46,10.2),(47,6.1),(48,1.8),(49,4.2),(50,5),
                        (51,5.1),(52,9.2),(53,13.6),(54,14.9),(55,16.9),(56,16.9),(57,14.4),(58,10.8),(59,4.7),(60,3.6),
                        (61,3.9),(62,2.4),(63,7.1),(64,8.3),(65,12.5),(66,16.4),(67,16.9),(68,16),(69,12.8),(70,9.1),
                        (71,7.2),(72,1.6),(73,1.2),(74,2.3),(75,2.8),(76,7.1),(77,10.3),(78,15.1),(79,16.8),(80,15.7),
                        (81,16.6),(82,10.1),(83,8.1),(84,5),(85,4.1),(86,4.7),(87,6.2),(88,8.7),(89,12.4),(90,14),
                        (91,15.3),(92,16.3),(93,15.3),(94,10.9),(95,9.2),(96,3.4),(97,1.9),(98,2.2),(99,6),(100,6.8)
                    ) AS v(i, val)
                )) s",
                None,
            &[]).unwrap();

            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                10.39
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                9.29
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                7.54
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                7.8
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                10.34
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                11.01
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                10.54
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                8.01
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                8.99
            );
            assert_relative_eq!(
                result.next().unwrap()[1].value::<f64>().unwrap().unwrap() as f32,
                8.73
            );
            assert!(result.next().is_none());
        })
    }

    #[pg_test]
    fn test_asap_equivalence() {
        Spi::connect_mut(|client| {
            let mut value_result = client.update(
                "
                SELECT time::text, value
                FROM unnest(
                (SELECT asap_smooth('2020-1-1'::timestamptz + i * '1d'::interval, val, 10)
                    FROM (VALUES 
                        (1,1.1),(2,4.4),(3,7.5),(4,8.9),(5,11.7),(6,15),(7,15.3),(8,15.6),(9,13.3),(10,11.1),
                        (11,7.5),(12,5.8),(13,5.6),(14,4.2),(15,4.7),(16,7.2),(17,11.4),(18,15.3),(19,15),(20,16.2),
                        (21,14.4),(22,8.6),(23,5.3),(24,3.3),(25,4.4),(26,3.3),(27,5),(28,8.1),(29,10.8),(30,12.2),
                        (31,13.8),(32,13.3),(33,12.8),(34,9.4),(35,6.9),(36,3.9),(37,1.1),(38,4.2),(39,4.2),(40,8.4),
                        (41,13.4),(42,16.4),(43,16),(44,15.6),(45,14.7),(46,10.2),(47,6.1),(48,1.8),(49,4.2),(50,5),
                        (51,5.1),(52,9.2),(53,13.6),(54,14.9),(55,16.9),(56,16.9),(57,14.4),(58,10.8),(59,4.7),(60,3.6),
                        (61,3.9),(62,2.4),(63,7.1),(64,8.3),(65,12.5),(66,16.4),(67,16.9),(68,16),(69,12.8),(70,9.1),
                        (71,7.2),(72,1.6),(73,1.2),(74,2.3),(75,2.8),(76,7.1),(77,10.3),(78,15.1),(79,16.8),(80,15.7),
                        (81,16.6),(82,10.1),(83,8.1),(84,5),(85,4.1),(86,4.7),(87,6.2),(88,8.7),(89,12.4),(90,14),
                        (91,15.3),(92,16.3),(93,15.3),(94,10.9),(95,9.2),(96,3.4),(97,1.9),(98,2.2),(99,6),(100,6.8)
                    ) AS v(i, val)
                )) s",
                None,
            &[]).unwrap();

            let mut tvec_result = client.update(
                "
                SELECT time::text, value
                FROM unnest(
                (SELECT asap_smooth(
                    (SELECT timevector('2020-1-1'::timestamptz + i * '1d'::interval, val)
                        FROM (VALUES 
                            (1,1.1),(2,4.4),(3,7.5),(4,8.9),(5,11.7),(6,15),(7,15.3),(8,15.6),(9,13.3),(10,11.1),
                            (11,7.5),(12,5.8),(13,5.6),(14,4.2),(15,4.7),(16,7.2),(17,11.4),(18,15.3),(19,15),(20,16.2),
                            (21,14.4),(22,8.6),(23,5.3),(24,3.3),(25,4.4),(26,3.3),(27,5),(28,8.1),(29,10.8),(30,12.2),
                            (31,13.8),(32,13.3),(33,12.8),(34,9.4),(35,6.9),(36,3.9),(37,1.1),(38,4.2),(39,4.2),(40,8.4),
                            (41,13.4),(42,16.4),(43,16),(44,15.6),(45,14.7),(46,10.2),(47,6.1),(48,1.8),(49,4.2),(50,5),
                            (51,5.1),(52,9.2),(53,13.6),(54,14.9),(55,16.9),(56,16.9),(57,14.4),(58,10.8),(59,4.7),(60,3.6),
                            (61,3.9),(62,2.4),(63,7.1),(64,8.3),(65,12.5),(66,16.4),(67,16.9),(68,16),(69,12.8),(70,9.1),
                            (71,7.2),(72,1.6),(73,1.2),(74,2.3),(75,2.8),(76,7.1),(77,10.3),(78,15.1),(79,16.8),(80,15.7),
                            (81,16.6),(82,10.1),(83,8.1),(84,5),(85,4.1),(86,4.7),(87,6.2),(88,8.7),(89,12.4),(90,14),
                            (91,15.3),(92,16.3),(93,15.3),(94,10.9),(95,9.2),(96,3.4),(97,1.9),(98,2.2),(99,6),(100,6.8)
                        ) AS v(i, val)
                    ), 10)
                ))",
                None,
            &[]).unwrap();

            for _ in 0..10 {
                let v = value_result.next().unwrap();
                let t = tvec_result.next().unwrap();
                assert_eq!(v[1].value::<&str>(), t[1].value::<&str>());
                assert_eq!(v[2].value::<f64>().unwrap(), t[2].value::<f64>().unwrap());
            }
            assert!(value_result.next().is_none());
            assert!(tvec_result.next().is_none());
        })
    }
}
