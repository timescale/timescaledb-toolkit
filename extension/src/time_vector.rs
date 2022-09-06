#![allow(clippy::identity_op)] // clippy gets confused by pg_type! enums

use pgx::*;

use crate::{
    accessors::AccessorUnnest,
    aggregate_utils::in_aggregate_context,
    build,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type, ron_inout_funcs,
};

use tspoint::TSPoint;

pub use iter::Iter;

use flat_serialize::*;

mod iter;
mod pipeline;

use crate::raw::bytea;

// Bit flags stored in Timevector flags
pub const FLAG_IS_SORTED: u8 = 0x01;
pub const FLAG_HAS_NULLS: u8 = 0x01 << 1;

pg_type! {
    #[derive(Debug)]
    #[allow(non_camel_case_types)]
    struct Timevector_TSTZ_F64<'input> {
        num_points: u32,
        flags: u8,         // extra information about the stored data
        internal_padding: [u8; 3],  // required to be aligned
        points: [TSPoint; self.num_points],
        null_val: [u8; (self.num_points + 7)/ 8], // bit vector, must be last element for alignment purposes
    }
}

ron_inout_funcs!(Timevector_TSTZ_F64);

impl<'input> Timevector_TSTZ_F64<'input> {
    pub fn num_points(&self) -> usize {
        self.num_points as usize
    }

    // Gets the nth point of a timevector
    // Differs from normal vector get in that it returns a copy rather than a reference (as the point may have to be constructed)
    pub fn get(&self, index: usize) -> Option<TSPoint> {
        if index >= self.num_points() {
            return None;
        }

        Some(self.points.as_slice()[index])
    }

    #[inline]
    pub fn is_sorted(&self) -> bool {
        self.flags & FLAG_IS_SORTED != 0
    }

    #[inline]
    pub fn has_nulls(&self) -> bool {
        self.flags & FLAG_HAS_NULLS != 0
    }

    pub fn is_null_val(&self, index: usize) -> bool {
        assert!(index < self.num_points()); // should we handle this better

        let byte_id = index / 8;
        let byte_idx = index % 8;

        self.null_val.as_slice()[byte_id] & (1 << byte_idx) != 0
    }

    fn clone_owned(&self) -> Timevector_TSTZ_F64<'static> {
        Timevector_TSTZ_F64Data::clone(self).into_owned().into()
    }
}

impl<'a> Timevector_TSTZ_F64<'a> {
    pub fn iter(&self) -> Iter<'_> {
        Iter::Slice {
            iter: self.points.iter(),
        }
    }

    pub fn num_vals(&self) -> usize {
        self.num_points()
    }
}

impl<'a> IntoIterator for Timevector_TSTZ_F64<'a> {
    type Item = TSPoint;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        #[allow(clippy::unnecessary_to_owned)] // Pretty sure clippy's wrong about this
        Iter::Slice {
            iter: self.points.to_owned().into_iter(),
        }
    }
}

pub static TIMEVECTOR_OID: once_cell::sync::Lazy<pg_sys::Oid> =
    once_cell::sync::Lazy::new(Timevector_TSTZ_F64::type_oid);

#[pg_extern(immutable, parallel_safe)]
pub fn unnest(
    series: Timevector_TSTZ_F64<'_>,
) -> impl std::iter::Iterator<Item = (name!(time, crate::raw::TimestampTz), name!(value, f64))> + '_
{
    series
        .into_iter()
        .map(|points| (points.ts.into(), points.val))
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_timevector_unnest<'a>(
    series: Timevector_TSTZ_F64<'a>,
    _accessor: AccessorUnnest,
) -> impl std::iter::Iterator<Item = (name!(time, crate::raw::TimestampTz), name!(value, f64))> + 'a
{
    unnest(series)
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn timevector_serialize(state: Internal) -> bytea {
    // FIXME: This might duplicate the version and padding bits
    let state: &Timevector_TSTZ_F64Data = unsafe { state.get().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn timevector_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let data: Timevector_TSTZ_F64<'static> = crate::do_deserialize!(bytes, Timevector_TSTZ_F64Data);
    Inner::from(data).internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn timevector_tstz_f64_trans(
    state: Internal,
    time: Option<crate::raw::TimestampTz>,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { timevector_trans_inner(state.to_inner(), time, value, fcinfo).internal() }
}

pub fn timevector_trans_inner(
    state: Option<Inner<Timevector_TSTZ_F64<'_>>>,
    time: Option<crate::raw::TimestampTz>,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector_TSTZ_F64<'_>>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let time: pg_sys::TimestampTz = match time {
                None => return state,
                Some(time) => time.into(),
            };
            let mut state = match state {
                None => Inner::from(build! {
                    Timevector_TSTZ_F64 {
                        num_points: 0,
                        flags: FLAG_IS_SORTED,
                        internal_padding: [0; 3],
                        points: vec![].into(),
                        null_val: vec![].into(),
                    }
                }),
                Some(state) => state,
            };
            if let Some(last_point) = state.points.as_slice().last() {
                if state.is_sorted() && last_point.ts > time {
                    state.flags ^= FLAG_IS_SORTED;
                }
            }
            if state.num_points % 8 == 0 {
                state.null_val.as_owned().push(0);
            }
            match value {
                None => {
                    state.flags |= FLAG_HAS_NULLS;
                    state.points.as_owned().push(TSPoint {
                        ts: time,
                        val: f64::NAN,
                    });
                    let byte_idx = state.num_points % 8; // off by 1, but num_points isn't yet incremented
                    *state.null_val.as_owned().last_mut().unwrap() |= 1 << byte_idx;
                }
                Some(val) => state.points.as_owned().push(TSPoint { ts: time, val }),
            };
            state.num_points += 1;
            Some(state)
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn timevector_tstz_f64_compound_trans(
    state: Internal,
    series: Option<Timevector_TSTZ_F64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    inner_compound_trans(unsafe { state.to_inner() }, series, fcinfo).internal()
}

pub fn inner_compound_trans<'b>(
    state: Option<Inner<Timevector_TSTZ_F64<'static>>>,
    series: Option<Timevector_TSTZ_F64<'b>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector_TSTZ_F64<'static>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, series) {
            (None, None) => None,
            (Some(state), None) => Some(state),
            (None, Some(series)) => Some(series.clone_owned().into()),
            (Some(state), Some(series)) => {
                // TODO: this should be doable without cloning 'state'
                Some(combine(state.clone(), series.clone()).into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn timevector_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { inner_combine(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}

pub fn inner_combine<'a, 'b>(
    state1: Option<Inner<Timevector_TSTZ_F64<'a>>>,
    state2: Option<Inner<Timevector_TSTZ_F64<'b>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Timevector_TSTZ_F64<'static>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(state2)) => Some(state2.clone_owned().into()),
            (Some(state1), None) => Some(state1.clone_owned().into()),
            (Some(state1), Some(state2)) => Some(combine(state1.clone(), state2.clone()).into()),
        })
    }
}

pub fn combine(
    first: Timevector_TSTZ_F64<'_>,
    second: Timevector_TSTZ_F64<'_>,
) -> Timevector_TSTZ_F64<'static> {
    if first.num_vals() == 0 {
        return second.clone_owned();
    }
    if second.num_vals() == 0 {
        return first.clone_owned();
    }

    let is_sorted = first.is_sorted()
        && second.is_sorted()
        && first.points.as_slice().last().unwrap().ts
            <= second.points.as_slice().first().unwrap().ts;
    let points: Vec<_> = first.iter().chain(second.iter()).collect();

    let mut flags = (first.flags & FLAG_HAS_NULLS) | (second.flags & FLAG_HAS_NULLS);
    if is_sorted {
        flags |= FLAG_IS_SORTED;
    }

    let null_val = if flags & FLAG_HAS_NULLS == 0 {
        std::vec::from_elem(0_u8, (points.len() + 7) / 8)
    } else {
        let mut v = first.null_val.as_slice().to_vec();
        v.resize((points.len() + 7) / 8, 0);
        if second.has_nulls() {
            for i in 0..second.num_points {
                if second.is_null_val(i as usize) {
                    let idx = i + first.num_points;
                    let byte_id = idx / 8;
                    let byte_idx = idx % 8;
                    v[byte_id as usize] |= 1 << byte_idx;
                }
            }
        }
        v
    };

    build! {
        Timevector_TSTZ_F64 {
            num_points: points.len() as _,
            flags,
            internal_padding: [0; 3],
            points: points.into(),
            null_val: null_val.into(),
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn timevector_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    unsafe { timevector_final_inner(state.to_inner(), fcinfo) }
}

pub fn timevector_final_inner<'a>(
    state: Option<Inner<Timevector_TSTZ_F64<'a>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Timevector_TSTZ_F64<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => state,
            };
            Some(state.in_current_context())
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE timevector(ts TIMESTAMPTZ, value DOUBLE PRECISION) (\n\
        sfunc = timevector_tstz_f64_trans,\n\
        stype = internal,\n\
        finalfunc = timevector_final,\n\
        combinefunc = timevector_combine,\n\
        serialfunc = timevector_serialize,\n\
        deserialfunc = timevector_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "timevector_tstz_f64_agg",
    requires = [
        timevector_tstz_f64_trans,
        timevector_final,
        timevector_combine,
        timevector_serialize,
        timevector_deserialize
    ],
);

extension_sql!(
    "\n\
CREATE AGGREGATE rollup(\n\
    timevector_tstz_f64\n\
) (\n\
    sfunc = timevector_tstz_f64_compound_trans,\n\
    stype = internal,\n\
    finalfunc = timevector_final,\n\
    combinefunc = timevector_combine,\n\
    serialfunc = timevector_serialize,\n\
    deserialfunc = timevector_deserialize,\n\
    parallel = safe\n\
);\n\
",
    name = "timevector_tstz_f64_rollup",
    requires = [
        timevector_tstz_f64_compound_trans,
        timevector_final,
        timevector_combine,
        timevector_serialize,
        timevector_deserialize
    ],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    pub fn test_unnest() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select(
                "CREATE TABLE data(time TIMESTAMPTZ, value DOUBLE PRECISION)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO data VALUES
                    ('1-1-2020', 30.0),
                    ('1-2-2020', 45.0),
                    ('1-3-2020', NULL),
                    ('1-4-2020', 55.5),
                    ('1-5-2020', 10.0)"#,
                None,
                None,
            );

            let mut unnest = client.select(
                "SELECT unnest(timevector(time, value))::TEXT FROM data",
                None,
                None,
            );

            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-01 00:00:00+00\",30)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-02 00:00:00+00\",45)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-03 00:00:00+00\",NaN)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-04 00:00:00+00\",55.5)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-05 00:00:00+00\",10)")
            );
            assert!(unnest.next().is_none());
        })
    }

    #[pg_test]
    pub fn timevector_io() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select(
                "CREATE TABLE data(time TIMESTAMPTZ, value DOUBLE PRECISION)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO data VALUES
                    ('1-1-2020', 30.0),
                    ('1-2-2020', 45.0),
                    ('1-3-2020', NULL),
                    ('1-4-2020', 55.5),
                    ('1-5-2020', 10.0)"#,
                None,
                None,
            );

            let tvec = client
                .select("SELECT timevector(time,value)::TEXT FROM data", None, None)
                .first()
                .get_one::<String>()
                .unwrap();
            let expected = r#"(version:1,num_points:5,flags:3,internal_padding:(0,0,0),points:[(ts:"2020-01-01 00:00:00+00",val:30),(ts:"2020-01-02 00:00:00+00",val:45),(ts:"2020-01-03 00:00:00+00",val:NaN),(ts:"2020-01-04 00:00:00+00",val:55.5),(ts:"2020-01-05 00:00:00+00",val:10)],null_val:[4])"#;

            assert_eq!(tvec, expected);

            let mut unnest = client.select(
                &format!("SELECT unnest('{}'::timevector_tstz_f64)::TEXT", expected),
                None,
                None,
            );

            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-01 00:00:00+00\",30)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-02 00:00:00+00\",45)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-03 00:00:00+00\",NaN)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-04 00:00:00+00\",55.5)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-05 00:00:00+00\",10)")
            );
            assert!(unnest.next().is_none());
        })
    }

    #[pg_test]
    pub fn test_arrow_equivalence() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select(
                "CREATE TABLE data(time TIMESTAMPTZ, value DOUBLE PRECISION)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO data VALUES
                    ('1-1-2020', 30.0),
                    ('1-2-2020', 45.0),
                    ('1-3-2020', NULL),
                    ('1-4-2020', 55.5),
                    ('1-5-2020', 10.0)"#,
                None,
                None,
            );

            let mut func = client.select(
                "SELECT unnest(timevector(time, value))::TEXT FROM data",
                None,
                None,
            );
            let mut op = client.select(
                "SELECT (timevector(time, value) -> unnest())::TEXT FROM data",
                None,
                None,
            );

            let mut test = true;
            while test {
                match (func.next(), op.next()) {
                    (None, None) => test = false,
                    (Some(a), Some(b)) =>
                        assert_eq!(a[1].value::<&str>(), b[1].value::<&str>()),
                    _ => panic!("Arrow operator didn't contain the same number of elements as nested function"),
                };
            }
        })
    }

    #[pg_test]
    pub fn test_rollup() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select(
                "CREATE TABLE data(time TIMESTAMPTZ, value DOUBLE PRECISION, bucket INTEGER)",
                None,
                None,
            );
            client.select(
                r#"INSERT INTO data VALUES
                    ('1-1-2020', 30.0, 1),
                    ('1-2-2020', 45.0, 1),
                    ('1-3-2020', NULL, 2),
                    ('1-4-2020', 55.5, 2),
                    ('1-5-2020', 10.0, 3),
                    ('1-6-2020', 13.0, 3),
                    ('1-7-2020', 71.0, 4),
                    ('1-8-2020', 0.0, 4)"#,
                None,
                None,
            );

            let mut unnest = client.select(
                "SELECT unnest(rollup(tvec))::TEXT
                        FROM (
                            SELECT timevector(time, value) AS tvec
                            FROM data 
                            GROUP BY bucket 
                            ORDER BY bucket
                        ) s",
                None,
                None,
            );

            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-01 00:00:00+00\",30)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-02 00:00:00+00\",45)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-03 00:00:00+00\",NaN)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-04 00:00:00+00\",55.5)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-05 00:00:00+00\",10)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-06 00:00:00+00\",13)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-07 00:00:00+00\",71)")
            );
            assert_eq!(
                unnest.next().unwrap()[1].value(),
                Some("(\"2020-01-08 00:00:00+00\",0)")
            );
            assert!(unnest.next().is_none());
        })
    }

    #[pg_test]
    fn test_rollup_preserves_nulls_flag() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select(
                "CREATE TABLE tvecs (vector Timevector_TSTZ_F64)",
                None,
                None,
            );
            client.select(
                "INSERT INTO tvecs SELECT timevector('2020-1-1', 20)",
                None,
                None,
            );
            client.select(
                "INSERT INTO tvecs SELECT timevector('2020-1-2', 30)",
                None,
                None,
            );
            client.select(
                "INSERT INTO tvecs SELECT timevector('2020-1-3', 15)",
                None,
                None,
            );

            let tvec = client
                .select("SELECT rollup(vector)::TEXT FROM tvecs", None, None)
                .first()
                .get_one::<String>()
                .unwrap();
            let expected = r#"(version:1,num_points:3,flags:1,internal_padding:(0,0,0),points:[(ts:"2020-01-01 00:00:00+00",val:20),(ts:"2020-01-02 00:00:00+00",val:30),(ts:"2020-01-03 00:00:00+00",val:15)],null_val:[0])"#;
            assert_eq!(tvec, expected);

            client.select(
                "INSERT INTO tvecs SELECT timevector('2019-1-4', NULL)",
                None,
                None,
            );
            let tvec = client
                .select("SELECT rollup(vector)::TEXT FROM tvecs", None, None)
                .first()
                .get_one::<String>()
                .unwrap();
            let expected = r#"(version:1,num_points:4,flags:2,internal_padding:(0,0,0),points:[(ts:"2020-01-01 00:00:00+00",val:20),(ts:"2020-01-02 00:00:00+00",val:30),(ts:"2020-01-03 00:00:00+00",val:15),(ts:"2019-01-04 00:00:00+00",val:NaN)],null_val:[8])"#;
            assert_eq!(tvec, expected);
        })
    }
}
