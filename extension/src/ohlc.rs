use pgx::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};
use tspoint::TSPoint;

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Copy, Debug)]
        #[repr(C)]
        struct OpenHighLowClose {
            open: TSPoint,
            high: TSPoint,
            low: TSPoint,
            close: TSPoint,
        }
    }

    impl OpenHighLowClose<'_> {
        pub fn new() -> Self {
            unsafe {
                flatten!(OpenHighLowClose {
                    open: TSPoint {
                        ts: i64::MAX,
                        val: 0.0
                    },
                    high: TSPoint {
                        ts: 0,
                        val: f64::MIN
                    },
                    low: TSPoint {
                        ts: 0,
                        val: f64::MAX
                    },
                    close: TSPoint {
                        ts: i64::MIN,
                        val: 0.0
                    },
                })
            }
        }

        pub fn add_value(&mut self, point: TSPoint) {
            if point.ts < self.open.ts {
                self.open = point;
            }

            if point.val > self.high.val {
                self.high = point;
            }

            if point.val < self.low.val {
                self.low = point;
            }

            if point.ts > self.close.ts {
                self.close = point;
            }
        }

        pub fn combine(&mut self, other: &OpenHighLowClose) {
            if other.open.ts < self.open.ts {
                self.open = other.open;
            }

            if other.high.val > self.high.val {
                self.high = other.high;
            }

            if other.low.val < self.low.val {
                self.low = other.low;
            }

            if other.close.ts > self.close.ts {
                self.close = other.close;
            }
        }

        pub fn open(&self) -> f64 {
            self.open.val
        }

        pub fn high(&self) -> f64 {
            self.high.val
        }

        pub fn low(&self) -> f64 {
            self.low.val
        }

        pub fn close(&self) -> f64 {
            self.close.val
        }

        pub fn open_at(&self) -> i64 {
            self.open.ts
        }

        pub fn high_at(&self) -> i64 {
            self.high.ts
        }

        pub fn low_at(&self) -> i64 {
            self.low.ts
        }

        pub fn close_at(&self) -> i64 {
            self.close.ts
        }
    }

    impl Default for OpenHighLowClose<'_> {
        fn default() -> Self {
            Self::new()
        }
    }
    ron_inout_funcs!(OpenHighLowClose);
}

use toolkit_experimental::OpenHighLowClose;

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn ohlc_transition(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    price: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    ohlc_transition_inner(unsafe { state.to_inner() }, ts, price, fcinfo).internal()
}
pub fn ohlc_transition_inner(
    state: Option<Inner<OpenHighLowClose>>,
    ts: Option<crate::raw::TimestampTz>,
    price: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<OpenHighLowClose>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let point = match (ts, price) {
                (_, None) | (None, _) => return state,
                (Some(ts), Some(val)) => TSPoint { ts: ts.into(), val },
            };
            match state {
                None => {
                    let mut ohlc = OpenHighLowClose::new();
                    ohlc.add_value(point);
                    Some(ohlc.into())
                }
                Some(mut ohlc) => {
                    ohlc.add_value(point);
                    Some(ohlc)
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn ohlc_rollup_trans(
    state: Internal,
    value: Option<OpenHighLowClose>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    ohlc_rollup_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn ohlc_rollup_trans_inner<'input>(
    state: Option<Inner<OpenHighLowClose<'input>>>,
    value: Option<OpenHighLowClose<'input>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<OpenHighLowClose<'input>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (state, None) => state,
            (None, Some(value)) => {
                let mut state = OpenHighLowClose::new();
                state.combine(&value);
                Some(state.into())
            }
            (Some(state), Some(value)) => {
                let mut state = *state;
                state.combine(&value);
                Some(state.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn ohlc_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<OpenHighLowClose<'static>> {
    unsafe { ohlc_final_inner(state.to_inner(), fcinfo) }
}
pub fn ohlc_final_inner(
    state: Option<Inner<OpenHighLowClose<'static>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<OpenHighLowClose<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let state = match state {
                None => return None,
                Some(state) => *state,
            };
            Some(state)
        })
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn ohlc_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { ohlc_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}
pub fn ohlc_combine_inner<'input>(
    state1: Option<Inner<OpenHighLowClose<'input>>>,
    state2: Option<Inner<OpenHighLowClose<'input>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<OpenHighLowClose<'input>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(only)) | (Some(only), None) => Some(only),
            (Some(a), Some(b)) => {
                let (mut a, b) = (*a, *b);
                a.combine(&b);
                Some(a.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe, strict, schema = "toolkit_experimental")]
pub fn ohlc_serialize(state: Internal) -> bytea {
    let ohlc: &mut OpenHighLowClose = unsafe { state.get_mut().unwrap() };
    let ser = &**ohlc;
    crate::do_serialize!(ser)
}

#[pg_extern(immutable, parallel_safe, strict, schema = "toolkit_experimental")]
pub fn ohlc_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    ohlc_deserialize_inner(bytes).internal()
}
pub fn ohlc_deserialize_inner(bytes: bytea) -> Inner<OpenHighLowClose<'static>> {
    use crate::ohlc::toolkit_experimental::OpenHighLowCloseData;
    let de: OpenHighLowCloseData = crate::do_deserialize!(bytes, OpenHighLowCloseData);
    let ohlc: OpenHighLowClose = de.into();
    ohlc.into()
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.ohlc( ts timestamptz, price DOUBLE PRECISION )\n\
    (\n\
        sfunc = toolkit_experimental.ohlc_transition,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.ohlc_final,\n\
        combinefunc = toolkit_experimental.ohlc_combine,\n\
        serialfunc = toolkit_experimental.ohlc_serialize,\n\
        deserialfunc = toolkit_experimental.ohlc_deserialize,\n\
        parallel = safe\n\
    );\n",
    name = "ohlc",
    requires = [
        ohlc_transition,
        ohlc_final,
        ohlc_combine,
        ohlc_serialize,
        ohlc_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.rollup( ohlc toolkit_experimental.OpenHighLowClose)\n\
    (\n\
        sfunc = toolkit_experimental.ohlc_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.ohlc_final,\n\
        combinefunc = toolkit_experimental.ohlc_combine,\n\
        serialfunc = toolkit_experimental.ohlc_serialize,\n\
        deserialfunc = toolkit_experimental.ohlc_deserialize,\n\
        parallel = safe\n\
    );\n",
    name = "ohlc_rollup",
    requires = [
        ohlc_rollup_trans,
        ohlc_final,
        ohlc_combine,
        ohlc_serialize,
        ohlc_deserialize
    ],
);

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn open(aggregate: Option<OpenHighLowClose>) -> f64 {
    let ohlc = aggregate.unwrap();
    ohlc.open()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn high(aggregate: Option<OpenHighLowClose>) -> f64 {
    let ohlc = aggregate.unwrap();
    ohlc.high()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn low(aggregate: Option<OpenHighLowClose>) -> f64 {
    let ohlc = aggregate.unwrap();
    ohlc.low()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn close(aggregate: Option<OpenHighLowClose>) -> f64 {
    let ohlc = aggregate.unwrap();
    ohlc.close()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn open_at(aggregate: Option<OpenHighLowClose>) -> crate::raw::TimestampTz {
    let ohlc = aggregate.unwrap();
    ohlc.open_at().into()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn high_at(aggregate: Option<OpenHighLowClose>) -> crate::raw::TimestampTz {
    let ohlc = aggregate.unwrap();
    ohlc.high_at().into()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn low_at(aggregate: Option<OpenHighLowClose>) -> crate::raw::TimestampTz {
    let ohlc = aggregate.unwrap();
    ohlc.low_at().into()
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn close_at(aggregate: Option<OpenHighLowClose>) -> crate::raw::TimestampTz {
    let ohlc = aggregate.unwrap();
    ohlc.close_at().into()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    macro_rules! select_one {
        ($client:expr, $stmt:expr, $type:ty) => {
            $client.select($stmt, None, None).first().get_one::<$type>()
        };
    }
    macro_rules! select_two {
        ($client:expr, $stmt:expr, $type1:ty, $type2:ty) => {
            $client
                .select($stmt, None, None)
                .first()
                .get_two::<$type1, $type2>()
        };
    }

    #[pg_test]
    fn ohlc_single_point() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE test(ts TIMESTAMPTZ, price FLOAT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                   ('2022-08-01 00:00:00+00', 0.0)
               "#,
                None,
                None,
            );

            let output = select_one!(
                client,
                "SELECT toolkit_experimental.ohlc(ts, price)::text FROM test",
                &str
            );

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-01 00:00:00+00\",val:0)\
                            )";
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn ohlc_accessors() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE test(ts TIMESTAMPTZ, price FLOAT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                   ('2022-08-01 00:00:00+00', 0.0)
               "#,
                None,
                None,
            );

            client.select(
                "CREATE VIEW ohlc_view AS \
                SELECT toolkit_experimental.ohlc(ts, price) \
                FROM test",
                None,
                None,
            );

            let sanity = client
                .select("SELECT COUNT(*) FROM ohlc_view", None, None)
                .first()
                .get_one::<i32>();
            assert!(sanity.unwrap_or(0) > 0);

            for ohlc in &["open", "high", "low", "close"] {
                let (val, ts) = select_two!(
                    client,
                    format!("SELECT toolkit_experimental.{ohlc}(ohlc), toolkit_experimental.{ohlc}_at(ohlc)::text FROM ohlc_view").as_str(),
                    f64,
                    &str
                );
                assert_eq!(0.0, val.unwrap());
                assert_eq!("2022-08-01 00:00:00+00", ts.unwrap());
            }
        });
    }

    #[pg_test]
    fn ohlc_null_inputs() {
        Spi::execute(|client| {
            for (x, y) in &[("NULL", "NULL"), ("NULL", "1.0"), ("now()", "NULL")] {
                let output = select_one!(
                    client,
                    format!("SELECT toolkit_experimental.ohlc({x}, {y})").as_str(),
                    String
                );
                assert_eq!(output, None);
            }
        });
    }

    #[pg_test]
    fn ohlc_constant() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE test(ts TIMESTAMPTZ, price FLOAT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                   ('2022-08-01 00:00:00+00', 0.0),
                   ('2022-08-01 06:00:00+00', 0.0),
                   ('2022-08-01 12:00:00+00', 0.0),
                   ('2022-08-01 18:00:00+00', 0.0),
                   ('2022-08-01 23:59:59+00', 0.0)
               "#,
                None,
                None,
            );

            let stmt = "SELECT date_trunc('day', ts)::text \
                             , toolkit_experimental.ohlc(ts, price)::text \
                          FROM test \
                         GROUP BY 1";

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-01 23:59:59+00\",val:0)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn ohlc_strictly_increasing() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE test(ts TIMESTAMPTZ, price FLOAT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                   ('2022-08-01 00:00:00+00', 1.0),
                   ('2022-08-01 06:00:00+00', 2.0),
                   ('2022-08-01 12:00:00+00', 3.0),
                   ('2022-08-01 18:00:00+00', 4.0),
                   ('2022-08-01 23:59:59+00', 5.0)
               "#,
                None,
                None,
            );

            let stmt = "SELECT date_trunc('day', ts)::text \
                             , toolkit_experimental.ohlc(ts, price)::text \
                          FROM test \
                         GROUP BY 1";

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            high:(ts:\"2022-08-01 23:59:59+00\",val:5),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            close:(ts:\"2022-08-01 23:59:59+00\",val:5)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn ohlc_strictly_decreasing() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE test(ts TIMESTAMPTZ, price FLOAT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                   ('2022-08-01 00:00:00+00', 5.0),
                   ('2022-08-01 06:00:00+00', 4.0),
                   ('2022-08-01 12:00:00+00', 3.0),
                   ('2022-08-01 18:00:00+00', 2.0),
                   ('2022-08-01 23:59:59+00', 1.0)
               "#,
                None,
                None,
            );

            let stmt = "SELECT date_trunc('day', ts)::text \
                             , toolkit_experimental.ohlc(ts, price)::text \
                          FROM test \
                         GROUP BY 1";

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:5),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:5),\
                            low:(ts:\"2022-08-01 23:59:59+00\",val:1),\
                            close:(ts:\"2022-08-01 23:59:59+00\",val:1)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn ohlc_oscillating() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE test(ts TIMESTAMPTZ, price FLOAT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                   ('2022-08-01 00:00:00+00',  3.0),
                   ('2022-08-01 02:00:00+00',  4.0),
                   ('2022-08-01 04:00:00+00', 11.0),
                   ('2022-08-01 06:00:00+00',  5.0),
                   ('2022-08-01 08:00:00+00',  2.0),
                   ('2022-08-01 10:00:00+00',  1.0),
                   ('2022-08-01 12:00:00+00', 12.0),
                   ('2022-08-01 14:00:00+00',  9.0),
                   ('2022-08-01 16:00:00+00', 10.0),
                   ('2022-08-01 18:00:00+00',  7.0),
                   ('2022-08-01 20:00:00+00',  6.0),
                   ('2022-08-01 22:00:00+00',  8.0)
               "#,
                None,
                None,
            );
            let stmt = "SELECT date_trunc('day', ts)::text \
                             , toolkit_experimental.ohlc(ts, price)::text \
                          FROM test \
                         GROUP BY 1";

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:3),\
                            high:(ts:\"2022-08-01 12:00:00+00\",val:12),\
                            low:(ts:\"2022-08-01 10:00:00+00\",val:1),\
                            close:(ts:\"2022-08-01 22:00:00+00\",val:8)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn ohlc_rollup() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            client.select("CREATE TABLE test(ts TIMESTAMPTZ, price FLOAT)", None, None);
            client.select(
                r#"INSERT INTO test VALUES
                   ('2022-08-01 00:00:00+00', 0.0),
                   ('2022-08-01 06:00:00+00', 1.0),
                   ('2022-08-01 12:00:00+00', 2.0),
                   ('2022-08-01 18:00:00+00', 3.0),
                   ('2022-08-01 23:59:59+00', 4.0),
                   ('2022-08-02 06:00:00+00', 5.0),
                   ('2022-08-02 12:00:00+00', 6.0),
                   ('2022-08-02 18:00:00+00', 7.0),
                   ('2022-08-02 23:59:59+00', 8.0)
               "#,
                None,
                None,
            );

            let stmt = "WITH t AS (\
                        SELECT date_trunc('day', ts) as date\
                             , toolkit_experimental.ohlc(ts, price) \
                          FROM test \
                          GROUP BY 1\
                        ) \
                        SELECT date_trunc('month', date)::text \
                             , toolkit_experimental.rollup(ohlc)::text \
                          FROM t \
                         GROUP BY 1";

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-02 23:59:59+00\",val:8),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-02 23:59:59+00\",val:8)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }
}
