use pgx::*;
use std::borrow::Cow;

use crate::{
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    time_vector,
};

use tspoint::TSPoint;

use crate::time_vector::{Timevector_TSTZ_F64, Timevector_TSTZ_F64Data};

pub struct LttbTrans {
    series: Vec<TSPoint>,
    resolution: usize,
    gap_interval: i64,
}

#[pg_extern(immutable, parallel_safe)]
pub fn lttb_trans(
    state: Internal,
    time: crate::raw::TimestampTz,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    lttb_trans_inner(unsafe { state.to_inner() }, time, val, resolution, fcinfo).internal()
}
pub fn lttb_trans_inner(
    state: Option<Inner<LttbTrans>>,
    time: crate::raw::TimestampTz,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<LttbTrans>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let val = match val {
                None => return state,
                Some(val) => val,
            };
            let mut state = match state {
                Some(state) => state,
                None => {
                    if resolution <= 2 {
                        error!("resolution must be greater than 2")
                    }
                    LttbTrans {
                        series: vec![],
                        resolution: resolution as usize,
                        gap_interval: 0,
                    }
                    .into()
                }
            };

            state.series.push(TSPoint {
                ts: time.into(),
                val,
            });
            Some(state)
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn lttb_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    lttb_final_inner(unsafe { state.to_inner() }, fcinfo)
}
pub fn lttb_final_inner(
    state: Option<Inner<LttbTrans>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.series.sort_by_key(|point| point.ts);
            let downsampled = lttb(&state.series[..], state.resolution);
            flatten!(Timevector_TSTZ_F64 {
                num_points: downsampled.len() as u32,
                flags: time_vector::FLAG_IS_SORTED,
                internal_padding: [0; 3],
                points: (&*downsampled).into(),
                null_val: std::vec::from_elem(0_u8, (downsampled.len() + 7) / 8).into()
            })
            .into()
        })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn gp_lttb_trans(
    state: Internal,
    time: crate::raw::TimestampTz,
    val: Option<f64>,
    gap: crate::raw::Interval,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    let state = unsafe { state.to_inner() };
    let needs_interval = state.is_none();

    // Don't love this code, but need to compute gap_val if needed before time is moved
    let gap_val = if needs_interval {
        crate::datum_utils::interval_to_ms(&time, &gap)
    } else {
        0
    };

    let mut trans = lttb_trans_inner(state, time, val, resolution, fcinfo);
    if needs_interval {
        trans.as_mut().map(|s| {
            s.gap_interval = gap_val;
            s
        });
    }
    trans.internal()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn gp_lttb_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    gap_preserving_lttb_final_inner(unsafe { state.to_inner() }, fcinfo)
}
pub fn gap_preserving_lttb_final_inner(
    state: Option<Inner<LttbTrans>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.series.sort_by_key(|point| point.ts);

            let count = state.series.len();
            let max_gap = if state.gap_interval > 0 {
                state.gap_interval
            } else {
                let range = state.series[count - 1].ts - state.series[0].ts;
                range / state.resolution as i64
            };

            // Tracking endpoints remaining will keep us from assigning too many points
            // to early LTTB computations when there are lots of gaps later in the timeseries
            let mut endpoints_remaining = 2;
            let mut start = 0;
            for i in 0..count - 1 {
                if state.series[i + 1].ts - state.series[i].ts > max_gap {
                    if i == start {
                        endpoints_remaining += 1;
                    } else {
                        endpoints_remaining += 2;
                    }
                    start = i + 1;
                }
            }

            let mut points_remaining = state.resolution as i64;
            let mut downsampled = vec![];
            start = 0;

            for i in 0..count - 1 {
                if state.series[i + 1].ts - state.series[i].ts > max_gap {
                    if i == start {
                        // 1 len subarray
                        downsampled.push(state.series[i]);
                        start = i + 1;
                        points_remaining -= 1;
                        endpoints_remaining -= 1;
                    } else {
                        let sgmt_pct_of_remaining_pts =
                            (i - start - 1) as f64 / (count - start - endpoints_remaining) as f64;
                        let pts_for_sgmt = std::cmp::max(
                            ((points_remaining - endpoints_remaining as i64) as f64
                                * sgmt_pct_of_remaining_pts) as usize,
                            0,
                        ) + 2;
                        downsampled
                            .append(&mut lttb(&state.series[start..=i], pts_for_sgmt).into_owned());
                        start = i + 1;
                        points_remaining -= pts_for_sgmt as i64;
                        endpoints_remaining -= 2;
                    }
                }
            }
            // remainder
            if start == count - 1 {
                downsampled.push(state.series[count - 1]);
            } else {
                downsampled.append(
                    &mut lttb(
                        &state.series[start..count],
                        std::cmp::max(points_remaining, 2) as usize,
                    )
                    .into_owned(),
                );
            }

            flatten!(Timevector_TSTZ_F64 {
                num_points: downsampled.len() as u32,
                flags: time_vector::FLAG_IS_SORTED,
                internal_padding: [0; 3],
                null_val: std::vec::from_elem(0_u8, (downsampled.len() + 7) / 8).into(),
                points: downsampled.into(),
            })
            .into()
        })
    }
}

extension_sql!(
    "\n\
CREATE AGGREGATE lttb(ts TIMESTAMPTZ, value DOUBLE PRECISION, resolution integer) (\n\
    sfunc = lttb_trans,\n\
    stype = internal,\n\
    finalfunc = lttb_final\n\
);\n\
",
    name = "lttb_agg",
    requires = [lttb_trans, lttb_final],
);

extension_sql!("\n\
CREATE AGGREGATE toolkit_experimental.gp_lttb(ts TIMESTAMPTZ, value DOUBLE PRECISION, resolution integer) (\n\
    sfunc = lttb_trans,\n\
    stype = internal,\n\
    finalfunc = toolkit_experimental.gp_lttb_final\n\
);\n\
",
name = "gp_lttb_agg",
requires = [lttb_trans, gp_lttb_final],
);

extension_sql!("\n\
CREATE AGGREGATE toolkit_experimental.gp_lttb(ts TIMESTAMPTZ, value DOUBLE PRECISION, gapsize INTERVAL, resolution integer) (\n\
    sfunc = toolkit_experimental.gp_lttb_trans,\n\
    stype = internal,\n\
    finalfunc = toolkit_experimental.gp_lttb_final\n\
);\n\
",
name = "gp_lttb_agg_with_size",
requires = [gp_lttb_trans, gp_lttb_final],
);

// based on https://github.com/jeromefroe/lttb-rs version 0.2.0
pub fn lttb(data: &[TSPoint], threshold: usize) -> Cow<'_, [TSPoint]> {
    if threshold >= data.len() || threshold == 0 {
        // Nothing to do.
        return Cow::Borrowed(data);
    }

    let mut sampled = Vec::with_capacity(threshold);

    // Bucket size. Leave room for start and end data points.
    let every = ((data.len() - 2) as f64) / ((threshold - 2) as f64);

    // Initially a is the first point in the triangle.
    let mut a = 0;

    // Always add the first point.
    sampled.push(data[a]);

    for i in 0..threshold - 2 {
        // Calculate point average for next bucket (containing c).
        let mut avg_x = 0i64;
        let mut avg_y = 0f64;

        let avg_range_start = (((i + 1) as f64) * every) as usize + 1;

        let mut end = (((i + 2) as f64) * every) as usize + 1;
        if end >= data.len() {
            end = data.len();
        }
        let avg_range_end = end;

        let avg_range_length = (avg_range_end - avg_range_start) as f64;

        for i in 0..(avg_range_end - avg_range_start) {
            let idx = (avg_range_start + i) as usize;
            avg_x += data[idx].ts;
            avg_y += data[idx].val;
        }
        avg_x /= avg_range_length as i64;
        avg_y /= avg_range_length;

        // Get the range for this bucket.
        let range_offs = ((i as f64) * every) as usize + 1;
        let range_to = (((i + 1) as f64) * every) as usize + 1;

        // Point a.
        let point_a_x = data[a].ts;
        let point_a_y = data[a].val;

        let mut max_area = -1f64;
        let mut next_a = range_offs;
        for i in 0..(range_to - range_offs) {
            let idx = (range_offs + i) as usize;

            // Calculate triangle area over three buckets.
            let area = ((point_a_x - avg_x) as f64 * (data[idx].val - point_a_y)
                - (point_a_x - data[idx].ts) as f64 * (avg_y - point_a_y))
                .abs()
                * 0.5;
            if area > max_area {
                max_area = area;
                next_a = idx; // Next a is this b.
            }
        }

        sampled.push(data[next_a]); // Pick this point from the bucket.
        a = next_a; // This a is the next a (chosen b).
    }

    // Always add the last point.
    sampled.push(data[data.len() - 1]);

    Cow::Owned(sampled)
}

#[pg_extern(name = "lttb", immutable, parallel_safe)]
pub fn lttb_on_timevector(
    series: Timevector_TSTZ_F64<'static>,
    threshold: i32,
) -> Option<Timevector_TSTZ_F64<'static>> {
    lttb_ts(series, threshold as usize).into()
}

// based on https://github.com/jeromefroe/lttb-rs version 0.2.0
pub fn lttb_ts(data: Timevector_TSTZ_F64, threshold: usize) -> Timevector_TSTZ_F64 {
    if !data.is_sorted() {
        panic!("lttb requires sorted timevector");
    }

    if threshold >= data.num_points() || threshold == 0 {
        // Nothing to do.
        return data.in_current_context(); // can we avoid this copy???
    }

    let mut sampled = Vec::with_capacity(threshold);

    // Bucket size. Leave room for start and end data points.
    let every = ((data.num_points() - 2) as f64) / ((threshold - 2) as f64);

    // Initially a is the first point in the triangle.
    let mut a = 0;

    // Always add the first point.
    sampled.push(data.get(a).unwrap());

    for i in 0..threshold - 2 {
        // Calculate point average for next bucket (containing c).
        let mut avg_x = 0i64;
        let mut avg_y = 0f64;

        let avg_range_start = (((i + 1) as f64) * every) as usize + 1;

        let mut end = (((i + 2) as f64) * every) as usize + 1;
        if end >= data.num_points() {
            end = data.num_points();
        }
        let avg_range_end = end;

        let avg_range_length = (avg_range_end - avg_range_start) as f64;

        for i in 0..(avg_range_end - avg_range_start) {
            let idx = (avg_range_start + i) as usize;
            let point = data.get(idx).unwrap();
            avg_x += point.ts;
            avg_y += point.val;
        }
        avg_x /= avg_range_length as i64;
        avg_y /= avg_range_length;

        // Get the range for this bucket.
        let range_offs = ((i as f64) * every) as usize + 1;
        let range_to = (((i + 1) as f64) * every) as usize + 1;

        // Point a.
        let point_a_x = data.get(a).unwrap().ts;
        let point_a_y = data.get(a).unwrap().val;

        let mut max_area = -1f64;
        let mut next_a = range_offs;
        for i in 0..(range_to - range_offs) {
            let idx = (range_offs + i) as usize;

            // Calculate triangle area over three buckets.
            let area = ((point_a_x - avg_x) as f64 * (data.get(idx).unwrap().val - point_a_y)
                - (point_a_x - data.get(idx).unwrap().ts) as f64 * (avg_y - point_a_y))
                .abs()
                * 0.5;
            if area > max_area {
                max_area = area;
                next_a = idx; // Next a is this b.
            }
        }

        sampled.push(data.get(next_a).unwrap()); // Pick this point from the bucket.
        a = next_a; // This a is the next a (chosen b).
    }

    // Always add the last point.
    sampled.push(data.get(data.num_points() - 1).unwrap());

    let nulls_len = (sampled.len() + 7) / 8;

    crate::build! {
        Timevector_TSTZ_F64 {
            num_points: sampled.len() as _,
            flags: time_vector::FLAG_IS_SORTED,
            internal_padding: [0; 3],
            points: sampled.into(),
            null_val: std::vec::from_elem(0_u8, nulls_len).into(),
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_lttb_equivalence() {
        Spi::connect(|client| {
            client.select(
                "CREATE TABLE test(time TIMESTAMPTZ, value DOUBLE PRECISION);",
                None,
                None,
            );
            client.select(
                "INSERT INTO test
                SELECT time, value
                FROM toolkit_experimental.generate_periodic_normal_series('2020-01-01 UTC'::timestamptz, NULL);", None, None);

            client.select(
                "CREATE TABLE results1(time TIMESTAMPTZ, value DOUBLE PRECISION);",
                None,
                None,
            );
            client.select(
                "INSERT INTO results1
                SELECT time, value
                FROM unnest(
                    (SELECT lttb(time, value, 100) FROM test)
                );",
                None,
                None,
            );

            client.select(
                "CREATE TABLE results2(time TIMESTAMPTZ, value DOUBLE PRECISION);",
                None,
                None,
            );
            client.select(
                "INSERT INTO results2
                SELECT time, value
                FROM unnest(
                    (SELECT lttb(
                        (SELECT timevector(time, value) FROM test), 100)
                    )
                );",
                None,
                None,
            );

            let delta = client
                .select("SELECT count(*)  FROM results1 r1 FULL OUTER JOIN results2 r2 ON r1 = r2 WHERE r1 IS NULL OR r2 IS NULL;" , None, None)
                .unwrap().first()
                .get_one::<i32>().unwrap();
            assert_eq!(delta.unwrap(), 0);
        })
    }

    #[pg_test]
    fn test_lttb_result() {
        Spi::connect(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            let mut result = client
                .select(
                    r#"SELECT unnest(lttb(ts, val, 5))::TEXT
                FROM (VALUES
                    ('2020-1-1'::timestamptz, 10),
                    ('2020-1-2'::timestamptz, 21),
                    ('2020-1-3'::timestamptz, 19),
                    ('2020-1-4'::timestamptz, 32),
                    ('2020-1-5'::timestamptz, 12),
                    ('2020-1-6'::timestamptz, 14),
                    ('2020-1-7'::timestamptz, 18),
                    ('2020-1-8'::timestamptz, 29),
                    ('2020-1-9'::timestamptz, 23),
                    ('2020-1-10'::timestamptz, 27),
                    ('2020-1-11'::timestamptz, 14)
                ) AS v(ts, val)"#,
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-01 00:00:00+00\",10)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-04 00:00:00+00\",32)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-05 00:00:00+00\",12)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-08 00:00:00+00\",29)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-11 00:00:00+00\",14)")
            );
            assert!(result.next().is_none());
        })
    }

    #[pg_test]
    fn test_gp_lttb() {
        Spi::connect(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            let mut result = client
                .select(
                    r#"SELECT unnest(toolkit_experimental.gp_lttb(ts, val, 7))::TEXT
                FROM (VALUES
                    ('2020-1-1'::timestamptz, 10),
                    ('2020-1-2'::timestamptz, 21),
                    ('2020-1-3'::timestamptz, 19),
                    ('2020-1-4'::timestamptz, 32),
                    ('2020-1-5'::timestamptz, 12),
                    ('2020-2-6'::timestamptz, 14),
                    ('2020-3-7'::timestamptz, 18),
                    ('2020-3-8'::timestamptz, 29),
                    ('2020-3-9'::timestamptz, 23),
                    ('2020-3-10'::timestamptz, 27),
                    ('2020-3-11'::timestamptz, 14)
                ) AS v(ts, val)"#,
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-01 00:00:00+00\",10)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-04 00:00:00+00\",32)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-05 00:00:00+00\",12)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-02-06 00:00:00+00\",14)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-03-07 00:00:00+00\",18)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-03-08 00:00:00+00\",29)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-03-11 00:00:00+00\",14)")
            );
            assert!(result.next().is_none());
        })
    }

    #[pg_test]
    fn test_gp_lttb_with_gap() {
        Spi::connect(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            let mut result = client
                .select(
                    r#"SELECT unnest(toolkit_experimental.gp_lttb(ts, val, '36hr', 5))::TEXT
                FROM (VALUES
                    ('2020-1-1'::timestamptz, 10),
                    ('2020-1-2'::timestamptz, 21),
                    ('2020-1-4'::timestamptz, 32),
                    ('2020-1-5'::timestamptz, 12),
                    ('2020-2-6'::timestamptz, 14),
                    ('2020-3-7'::timestamptz, 18),
                    ('2020-3-8'::timestamptz, 29),
                    ('2020-3-10'::timestamptz, 27),
                    ('2020-3-11'::timestamptz, 14)
                ) AS v(ts, val)"#,
                    None,
                    None,
                )
                .unwrap();

            // This should include everything, despite target resolution of 5
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-01 00:00:00+00\",10)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-02 00:00:00+00\",21)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-04 00:00:00+00\",32)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-01-05 00:00:00+00\",12)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-02-06 00:00:00+00\",14)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-03-07 00:00:00+00\",18)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-03-08 00:00:00+00\",29)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-03-10 00:00:00+00\",27)")
            );
            assert_eq!(
                result.next().unwrap()[1].value().unwrap(),
                Some("(\"2020-03-11 00:00:00+00\",14)")
            );
            assert!(result.next().is_none());
        })
    }
}
