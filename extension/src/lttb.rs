
use pgx::*;
use std::borrow::Cow;

use crate::{
    aggregate_utils::in_aggregate_context, flatten, palloc::{Internal, InternalAsValue, Inner, ToInternal}, time_vector,
};

use tspoint::TSPoint;

use crate::time_vector::{TimevectorData, Timevector};


pub struct LttbTrans {
    series: Vec<TSPoint>,
    resolution: usize,
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn lttb_trans(
    state: Internal,
    time: crate::raw::TimestampTz,
    val: Option<f64>,
    resolution: i32,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    lttb_trans_inner(unsafe{ state.to_inner() }, time, val, resolution, fcinfo).internal()
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
                    }.into()
                },
            };

            state.series.push(TSPoint {
                ts: time.into(),
                val,
            });
            Some(state)
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn lttb_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_vector::toolkit_experimental::Timevector<'static>> {
    lttb_final_inner(unsafe{ state.to_inner() }, fcinfo)
}
pub fn lttb_final_inner(
    state: Option<Inner<LttbTrans>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<crate::time_vector::toolkit_experimental::Timevector<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.series.sort_by_key(|point| point.ts);
            let series = Cow::from(&state.series);
            let downsampled = lttb(&*series, state.resolution);
            flatten!(
                Timevector {
                    num_points: downsampled.len() as u32,
                    flags: time_vector::FLAG_IS_SORTED,
                    internal_padding: [0; 3],
                    points: (&*downsampled).into(),
                    null_val: std::vec::from_elem(0_u8, (downsampled.len() + 7) / 8).into()
                }
            ).into()
        })
    }
}

extension_sql!("\n\
CREATE AGGREGATE toolkit_experimental.lttb(ts TIMESTAMPTZ, value DOUBLE PRECISION, resolution integer) (\n\
    sfunc = toolkit_experimental.lttb_trans,\n\
    stype = internal,\n\
    finalfunc = toolkit_experimental.lttb_final\n\
);\n\
",
name = "lttb_agg",
requires = [lttb_trans, lttb_final],
);


// based on https://github.com/jeromefroe/lttb-rs version 0.2.0
pub fn lttb(data: &[TSPoint], threshold: usize) -> Cow<'_, [TSPoint]> {
    if threshold >= data.len() || threshold == 0 {
        // Nothing to do.
        return Cow::Borrowed(data)
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

#[pg_extern(name="lttb", schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn lttb_on_timevector(
    series: crate::time_vector::toolkit_experimental::Timevector<'static>,
    threshold: i32,
) -> Option<crate::time_vector::toolkit_experimental::Timevector<'static>> {
    lttb_ts(series, threshold as usize).into()
}

// based on https://github.com/jeromefroe/lttb-rs version 0.2.0
pub fn lttb_ts(
    data: crate::time_vector::toolkit_experimental::Timevector,
    threshold: usize
)
-> crate::time_vector::toolkit_experimental::Timevector
{
    if !data.is_sorted() {
        panic!("lttb requires sorted timevector");
    }

    if threshold >= data.num_points() || threshold == 0 {
        // Nothing to do.
        return data.in_current_context();  // can we avoid this copy???
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
        Timevector {
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
        Spi::execute(|client| {
            client.select("CREATE TABLE test(time TIMESTAMPTZ, value DOUBLE PRECISION);", None, None);
            client.select(
                "INSERT INTO test
                SELECT time, value
                FROM toolkit_experimental.generate_periodic_normal_series('2020-01-01 UTC'::timestamptz, NULL);", None, None);

            client.select("CREATE TABLE results1(time TIMESTAMPTZ, value DOUBLE PRECISION);", None, None);
            client.select(
                "INSERT INTO results1
                SELECT time, value
                FROM toolkit_experimental.unnest(
                    (SELECT toolkit_experimental.lttb(time, value, 100) FROM test)
                );", None, None);

            client.select("CREATE TABLE results2(time TIMESTAMPTZ, value DOUBLE PRECISION);", None, None);
            client.select(
                "INSERT INTO results2
                SELECT time, value
                FROM toolkit_experimental.unnest(
                    (SELECT toolkit_experimental.lttb(
                        (SELECT toolkit_experimental.timevector(time, value) FROM test), 100)
                    )
                );", None, None);

            let delta = client
                .select("SELECT count(*)  FROM results1 r1 FULL OUTER JOIN results2 r2 ON r1 = r2 WHERE r1 IS NULL OR r2 IS NULL;" , None, None)
                .first()
                .get_one::<i32>();
            assert_eq!(delta.unwrap(), 0);
        })
    }
}
