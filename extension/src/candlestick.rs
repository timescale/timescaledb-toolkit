use pgx::*;
use serde::{Deserialize, Serialize};

use crate::accessors::{
    AccessorClose, AccessorCloseTime, AccessorHigh, AccessorHighTime, AccessorLow, AccessorLowTime,
    AccessorOpen, AccessorOpenTime,
};
use crate::{
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};
use tspoint::TSPoint;

flat_serialize_macro::flat_serialize! {
    #[derive(Serialize, Deserialize, Debug, Copy)]
    enum VolKind {
        unused_but_required_by_flat_serialize: u64,
        Missing: 1 {},
        Transaction: 2 { vol: f64, vwap: f64 },
    }
}

pg_type! {
    #[derive(Debug, Copy)]
    struct Candlestick {
        open: TSPoint,
        high: TSPoint,
        low: TSPoint,
        close: TSPoint,
        #[flat_serialize::flatten]
        volume: VolKind,
    }
}

impl Candlestick<'_> {
    pub fn new(ts: i64, open: f64, high: f64, low: f64, close: f64, volume: Option<f64>) -> Self {
        let volume = match volume {
            None => VolKind::Missing {},
            Some(volume) => {
                let typical = (high + low + close) / 3.0;
                VolKind::Transaction {
                    vol: volume,
                    vwap: volume * typical,
                }
            }
        };

        unsafe {
            flatten!(Candlestick {
                open: TSPoint { ts, val: open },
                high: TSPoint { ts, val: high },
                low: TSPoint { ts, val: low },
                close: TSPoint { ts, val: close },
                volume,
            })
        }
    }

    pub fn from_tick(ts: i64, price: f64, volume: Option<f64>) -> Self {
        Candlestick::new(ts, price, price, price, price, volume)
    }

    pub fn add_tick_data(&mut self, ts: i64, price: f64, volume: Option<f64>) {
        if ts < self.open.ts {
            self.open = TSPoint { ts, val: price };
        }

        if price > self.high.val {
            self.high = TSPoint { ts, val: price };
        }

        if price < self.low.val {
            self.low = TSPoint { ts, val: price };
        }

        if ts > self.close.ts {
            self.close = TSPoint { ts, val: price };
        }

        if let (VolKind::Transaction { vol, vwap }, Some(volume)) = (self.volume, volume) {
            self.volume = VolKind::Transaction {
                vol: vol + volume,
                vwap: vwap + volume * price,
            };
        } else {
            self.volume = VolKind::Missing {};
        };
    }

    pub fn combine(&mut self, candlestick: &Candlestick) {
        if candlestick.open.ts < self.open.ts {
            self.open = candlestick.open;
        }

        if candlestick.high.val > self.high.val {
            self.high = candlestick.high;
        }

        if candlestick.low.val < self.low.val {
            self.low = candlestick.low;
        }

        if candlestick.close.ts > self.close.ts {
            self.close = candlestick.close;
        }

        if let (
            VolKind::Transaction {
                vol: vol1,
                vwap: vwap1,
            },
            VolKind::Transaction {
                vol: vol2,
                vwap: vwap2,
            },
        ) = (self.volume, candlestick.volume)
        {
            self.volume = VolKind::Transaction {
                vol: vol1 + vol2,
                vwap: vwap1 + vwap2,
            };
        } else {
            self.volume = VolKind::Missing {};
        };
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

    pub fn open_time(&self) -> i64 {
        self.open.ts
    }

    pub fn high_time(&self) -> i64 {
        self.high.ts
    }

    pub fn low_time(&self) -> i64 {
        self.low.ts
    }

    pub fn close_time(&self) -> i64 {
        self.close.ts
    }

    pub fn volume(&self) -> Option<f64> {
        match self.volume {
            VolKind::Transaction { vol, .. } => Some(vol),
            VolKind::Missing {} => None,
        }
    }

    pub fn vwap(&self) -> Option<f64> {
        match self.volume {
            VolKind::Transaction { vol, vwap } => {
                if vol > 0.0 && vwap.is_finite() {
                    Some(vwap / vol)
                } else {
                    None
                }
            }
            VolKind::Missing {} => None,
        }
    }
}

ron_inout_funcs!(Candlestick);

#[pg_extern(immutable, parallel_safe)]
pub fn candlestick(
    ts: Option<crate::raw::TimestampTz>,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
) -> Option<Candlestick<'static>> {
    match ts {
        Some(ts) => Some(Candlestick::new(
            ts.into(),
            open?,
            high?,
            low?,
            close?,
            volume,
        )),
        None => None,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn tick_data_no_vol_transition(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    price: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    tick_data_transition_inner(unsafe { state.to_inner() }, ts, price, None, fcinfo).internal()
}

#[pg_extern(immutable, parallel_safe)]
pub fn tick_data_transition(
    state: Internal,
    ts: Option<crate::raw::TimestampTz>,
    price: Option<f64>,
    volume: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    tick_data_transition_inner(unsafe { state.to_inner() }, ts, price, volume, fcinfo).internal()
}

pub fn tick_data_transition_inner(
    state: Option<Inner<Candlestick>>,
    ts: Option<crate::raw::TimestampTz>,
    price: Option<f64>,
    volume: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Candlestick>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            if let (Some(ts), Some(price)) = (ts, price) {
                match state {
                    None => {
                        let cs = Candlestick::from_tick(ts.into(), price, volume);
                        Some(cs.into())
                    }
                    Some(mut cs) => {
                        cs.add_tick_data(ts.into(), price, volume);
                        Some(cs)
                    }
                }
            } else {
                state
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn candlestick_rollup_trans<'a>(
    state: Internal,
    value: Option<Candlestick<'a>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    candlestick_rollup_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}

pub fn candlestick_rollup_trans_inner<'input>(
    state: Option<Inner<Candlestick<'input>>>,
    value: Option<Candlestick<'input>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Candlestick<'input>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (state, None) => state,
            (None, Some(value)) => Some(value.into()),
            (Some(state), Some(value)) => {
                let mut state = *state;
                state.combine(&value);
                Some(state.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn candlestick_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Candlestick<'static>> {
    unsafe { candlestick_final_inner(state.to_inner(), fcinfo) }
}

pub fn candlestick_final_inner(
    state: Option<Inner<Candlestick<'static>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Candlestick<'static>> {
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

#[pg_extern(immutable, parallel_safe)]
pub fn candlestick_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { candlestick_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}

pub fn candlestick_combine_inner<'input>(
    state1: Option<Inner<Candlestick<'input>>>,
    state2: Option<Inner<Candlestick<'input>>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<Candlestick<'input>>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(only)) | (Some(only), None) => Some((*only).into()),
            (Some(a), Some(b)) => {
                let (mut a, b) = (*a, *b);
                a.combine(&b);
                Some(a.into())
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn candlestick_serialize(state: Internal) -> bytea {
    let cs: &mut Candlestick = unsafe { state.get_mut().unwrap() };
    let ser = &**cs;
    crate::do_serialize!(ser)
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn candlestick_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    candlestick_deserialize_inner(bytes).internal()
}

pub fn candlestick_deserialize_inner(bytes: bytea) -> Inner<Candlestick<'static>> {
    let de: CandlestickData = crate::do_deserialize!(bytes, CandlestickData);
    let cs: Candlestick = de.into();
    cs.into()
}

extension_sql!(
    "\n\
    CREATE AGGREGATE candlestick_agg( \n\
        ts TIMESTAMPTZ,\n\
        price DOUBLE PRECISION,\n\
        volume DOUBLE PRECISION\n\
    )\n\
    (\n\
        sfunc = tick_data_transition,\n\
        stype = internal,\n\
        finalfunc = candlestick_final,\n\
        combinefunc = candlestick_combine,\n\
        serialfunc = candlestick_serialize,\n\
        deserialfunc = candlestick_deserialize,\n\
        parallel = safe\n\
    );\n",
    name = "candlestick_agg",
    requires = [
        tick_data_transition,
        candlestick_final,
        candlestick_combine,
        candlestick_serialize,
        candlestick_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup( candlestick Candlestick)\n\
    (\n\
        sfunc = candlestick_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = candlestick_final,\n\
        combinefunc = candlestick_combine,\n\
        serialfunc = candlestick_serialize,\n\
        deserialfunc = candlestick_deserialize,\n\
        parallel = safe\n\
    );\n",
    name = "candlestick_rollup",
    requires = [
        candlestick_rollup_trans,
        candlestick_final,
        candlestick_combine,
        candlestick_serialize,
        candlestick_deserialize
    ],
);

#[pg_extern(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_open(
    candlestick: Option<Candlestick<'_>>,
    _accessor: AccessorOpen<'_>,
) -> Option<f64> {
    candlestick.map(|cs| cs.open())
}

#[pg_extern(immutable, parallel_safe)]
pub fn open(candlestick: Option<Candlestick<'_>>) -> Option<f64> {
    candlestick.map(|cs| cs.open())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_high(
    candlestick: Option<Candlestick<'_>>,
    _accessor: AccessorHigh<'_>,
) -> Option<f64> {
    candlestick.map(|cs| cs.high())
}

#[pg_extern(immutable, parallel_safe)]
pub fn high(candlestick: Option<Candlestick<'_>>) -> Option<f64> {
    candlestick.map(|cs| cs.high())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_low(candlestick: Option<Candlestick<'_>>, _accessor: AccessorLow<'_>) -> Option<f64> {
    candlestick.map(|cs| cs.low())
}

#[pg_extern(immutable, parallel_safe)]
pub fn low(candlestick: Option<Candlestick<'_>>) -> Option<f64> {
    candlestick.map(|cs| cs.low())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_close(
    candlestick: Option<Candlestick<'_>>,
    _accessor: AccessorClose<'_>,
) -> Option<f64> {
    candlestick.map(|cs| cs.close())
}

#[pg_extern(immutable, parallel_safe)]
pub fn close(candlestick: Option<Candlestick<'_>>) -> Option<f64> {
    candlestick.map(|cs| cs.close())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_open_time(
    candlestick: Option<Candlestick<'_>>,
    _accessor: AccessorOpenTime<'_>,
) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.open_time().into())
}

#[pg_extern(immutable, parallel_safe)]
pub fn open_time(candlestick: Option<Candlestick<'_>>) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.open_time().into())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_high_time(
    candlestick: Option<Candlestick<'_>>,
    _accessor: AccessorHighTime<'_>,
) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.high_time().into())
}

#[pg_extern(immutable, parallel_safe)]
pub fn high_time(candlestick: Option<Candlestick<'_>>) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.high_time().into())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_low_time(
    candlestick: Option<Candlestick<'_>>,
    _accessor: AccessorLowTime<'_>,
) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.low_time().into())
}

#[pg_extern(immutable, parallel_safe)]
pub fn low_time(candlestick: Option<Candlestick<'_>>) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.low_time().into())
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_close_time(
    candlestick: Option<Candlestick<'_>>,
    _accessor: AccessorCloseTime<'_>,
) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.close_time().into())
}

#[pg_extern(immutable, parallel_safe)]
pub fn close_time(candlestick: Option<Candlestick<'_>>) -> Option<crate::raw::TimestampTz> {
    candlestick.map(|cs| cs.close_time().into())
}

#[pg_extern(immutable, parallel_safe)]
pub fn volume(candlestick: Option<Candlestick<'_>>) -> Option<f64> {
    match candlestick {
        None => None,
        Some(cs) => cs.volume(),
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn vwap(candlestick: Option<Candlestick<'_>>) -> Option<f64> {
    match candlestick {
        None => None,
        Some(cs) => cs.vwap(),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::ptr;

    use super::*;
    use pgx_macros::pg_test;

    macro_rules! select_one {
        ($client:expr, $stmt:expr, $type:ty) => {
            $client
                .update($stmt, None, None)
                .unwrap()
                .first()
                .get_one::<$type>()
                .unwrap()
        };
    }
    macro_rules! select_two {
        ($client:expr, $stmt:expr, $type1:ty, $type2:ty) => {
            $client
                .update($stmt, None, None)
                .unwrap()
                .first()
                .get_two::<$type1, $type2>()
                .unwrap()
        };
    }

    #[pg_test]
    fn candlestick_single_point() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"SELECT candlestick(ts, open, high, low, close, volume)::text
                          FROM (
                              VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 0.0, 0.0, 0.0, 1.0)
                          ) AS v(ts, open, high, low, close, volume)"#;

            let output = select_one!(client, stmt, &str);

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            volume:Transaction(vol:1,vwap:0)\
                            )";
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_agg_single_point() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"SELECT candlestick_agg(ts, price, volume)::text
                          FROM (
                              VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 1.0)
                          ) AS v(ts, price, volume)"#;

            let output = select_one!(client, stmt, &str);

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            volume:Transaction(vol:1,vwap:0)\
                            )";
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_accessors() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            for ohlc in ["open", "high", "low", "close"] {
                let stmt = format!(
                    r#"SELECT
                           {ohlc}(candlestick),
                           {ohlc}_time(candlestick)::text
                       FROM (
                           SELECT candlestick(ts, open, high, low, close, volume)
                           FROM (
                               VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 0.0, 0.0, 0.0, 1.0)
                           ) AS v(ts, open, high, low, close, volume)
                       ) AS v(candlestick)"#
                );
                let (val, ts) = select_two!(client, &stmt, f64, &str);
                assert_eq!(0.0, val.unwrap());
                assert_eq!("2022-08-01 00:00:00+00", ts.unwrap());
            }

            // testing arrow operators
            for ohlc in ["open", "high", "low", "close"] {
                let stmt = format!(
                    r#"SELECT
                           candlestick->{ohlc}(),
                           (candlestick->{ohlc}_time())::text
                       FROM (
                           SELECT candlestick(ts, open, high, low, close, volume)
                           FROM (
                               VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 0.0, 0.0, 0.0, 1.0)
                           ) AS v(ts, open, high, low, close, volume)
                       ) AS v(candlestick)"#
                );
                let (val, ts) = select_two!(client, &stmt, f64, &str);
                assert_eq!(0.0, val.unwrap());
                assert_eq!("2022-08-01 00:00:00+00", ts.unwrap());
            }

            let stmt = r#"SELECT
                              volume(candlestick),
                              vwap(candlestick)
                          FROM (
                              SELECT candlestick(ts, open, high, low, close, volume)
                              FROM (
                                  VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 0.0, 0.0, 0.0, 1.0)
                              ) AS v(ts, open, high, low, close, volume)
                          ) AS v(candlestick)"#;
            let (vol, vwap) = select_two!(client, stmt, f64, f64);
            assert_eq!(1.0, vol.unwrap());
            assert_eq!(0.0, vwap.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_agg_accessors() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            for ohlc in ["open", "high", "low", "close"] {
                let stmt = format!(
                    r#"SELECT
                           {ohlc}(candlestick),
                           {ohlc}_time(candlestick)::text
                       FROM (
                           SELECT candlestick_agg(ts, price, volume)
                           FROM (
                               VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 1.0)
                           ) AS v(ts, price, volume)
                       ) AS v(candlestick)"#
                );
                let (val, ts) = select_two!(client, &stmt, f64, &str);
                assert_eq!(0.0, val.unwrap());
                assert_eq!("2022-08-01 00:00:00+00", ts.unwrap());
            }

            let stmt = r#"SELECT
                               volume(candlestick),
                               vwap(candlestick)
                          FROM (
                              SELECT candlestick_agg(ts, price, volume)
                              FROM (
                                  VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 1.0)
                              ) AS v(ts, price, volume)
                          ) AS v(candlestick)"#;

            let (vol, vwap) = select_two!(client, stmt, f64, f64);
            assert_eq!(1.0, vol.unwrap());
            assert_eq!(0.0, vwap.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_agg_extreme_values() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            // timestamptz low and high val according to https://www.postgresql.org/docs/14/datatype-datetime.html
            for extreme_time in &["4713-01-01 00:00:00+00 BC", "294276-12-31 23:59:59+00"] {
                let stmt = format!(
                    r#"SELECT candlestick_agg(ts, price, volume)::text
                         FROM (VALUES ('{}'::timestamptz, 1.0, 1.0)) AS v(ts, price, volume)"#,
                    extreme_time
                );

                let output = select_one!(client, &stmt, &str);

                let expected = format!(
                    "(\
                            version:1,\
                            open:(ts:\"{}\",val:1),\
                            high:(ts:\"{}\",val:1),\
                            low:(ts:\"{}\",val:1),\
                            close:(ts:\"{}\",val:1),\
                            volume:Transaction(vol:1,vwap:1)\
                            )",
                    extreme_time, extreme_time, extreme_time, extreme_time
                );
                assert_eq!(expected, output.unwrap());
            }

            for extreme_price in &[f64::MAX, f64::MIN] {
                let stmt = format!(
                    r#"SELECT candlestick_agg(ts, price, volume)::text
                 FROM (VALUES ('2022-08-01 00:00:00+00'::timestamptz, {}, 1.0)) AS v(ts, price, volume)"#,
                    extreme_price
                );

                let output = select_one!(client, &stmt, &str);

                let expected = format!(
                    "(\
                 version:1,\
                 open:(ts:\"2022-08-01 00:00:00+00\",val:{}),\
                 high:(ts:\"2022-08-01 00:00:00+00\",val:{}),\
                 low:(ts:\"2022-08-01 00:00:00+00\",val:{}),\
                 close:(ts:\"2022-08-01 00:00:00+00\",val:{}),\
                 volume:Transaction(vol:1,vwap:{})\
                 )",
                    extreme_price,
                    extreme_price,
                    extreme_price,
                    extreme_price,
                    (extreme_price + extreme_price + extreme_price)
                );
                assert_eq!(expected, output.unwrap());
            }
        });
    }

    #[pg_test]
    fn candlestick_null_inputs() {
        Spi::connect(|mut client| {
            for (t, o, h, l, c, v) in &[
                ("NULL", "NULL", "NULL", "NULL", "NULL", "NULL"),
                ("NULL", "1.0", "1.0", "1.0", "1.0", "1.0"),
                ("now()", "NULL", "1.0", "1.0", "1.0", "1.0"),
                ("now()", "1.0", "NULL", "1.0", "1.0", "1.0"),
                ("now()", "1.0", "1.0", "NULL", "1.0", "1.0"),
                ("now()", "1.0", "1.0", "1.0", "NULL", "1.0"),
            ] {
                let stmt = format!("SELECT candlestick({t}, {o}, {h}, {l}, {c}, {v})");
                let output = select_one!(client, &stmt, String);
                assert_eq!(output, None);
            }
        });
    }

    #[pg_test]
    fn candlestick_agg_null_inputs() {
        Spi::connect(|mut client| {
            for (ts, price, vol) in &[
                ("NULL", "NULL", "NULL"),
                ("NULL", "1.0", "1.0"),
                ("now()", "NULL", "1.0"),
            ] {
                let stmt = format!("SELECT candlestick_agg({ts}, {price}, {vol})");
                let output = select_one!(client, &stmt, String);
                assert_eq!(output, None);
            }

            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            close:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            volume:Missing()\
                            )";

            let output = select_one!(
                client,
                "SELECT candlestick_agg(ts, price, vol)::TEXT
                   FROM (VALUES('2022-08-01 00:00:00+00'::timestamptz, 1.0, NULL::double precision)) AS v(ts, price, vol)",
                String
            ).unwrap();
            assert_eq!(expected, output);
        });
    }

    #[pg_test]
    fn candlestick_as_constructor() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"SELECT
                              candlestick(ts, open, high, low, close, volume)::text
                          FROM (
                              VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 0.0, 0.0, 0.0, 1.0),
                                     ('2022-08-02 00:00:00+00'::timestamptz, 9.0, 12.0, 3.0, 6.0, 1.0)
                          ) AS v(ts, open, high, low, close, volume)"#;

            let mut candlesticks = client.update(stmt, None, None).unwrap();

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            volume:Transaction(vol:1,vwap:0)\
                            )";

            assert_eq!(
                Some(expected),
                candlesticks.next().unwrap()[1].value().unwrap()
            );

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-02 00:00:00+00\",val:9),\
                            high:(ts:\"2022-08-02 00:00:00+00\",val:12),\
                            low:(ts:\"2022-08-02 00:00:00+00\",val:3),\
                            close:(ts:\"2022-08-02 00:00:00+00\",val:6),\
                            volume:Transaction(vol:1,vwap:7)\
                            )";

            assert_eq!(
                Some(expected),
                candlesticks.next().unwrap()[1].value().unwrap()
            );
        });
    }

    #[pg_test]
    fn candlestick_agg_constant() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"SELECT
                              date_trunc('day', ts)::text,
                              candlestick_agg(ts, price, volume)::text
                          FROM (
                              VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 1.0),
                                     ('2022-08-01 06:00:00+00'::timestamptz, 0.0, 1.0),
                                     ('2022-08-01 12:00:00+00'::timestamptz, 0.0, 1.0),
                                     ('2022-08-01 18:00:00+00'::timestamptz, 0.0, 1.0),
                                     ('2022-08-01 23:59:59+00'::timestamptz, 0.0, 1.0)
                          ) AS v(ts, price, volume)
                          GROUP BY 1"#;

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-01 23:59:59+00\",val:0),\
                            volume:Transaction(vol:5,vwap:0)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_agg_strictly_increasing() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"SELECT
                              date_trunc('day', ts)::text,
                              candlestick_agg(ts, price, volume)::text
                          FROM (
                              VALUES ('2022-08-01 00:00:00+00'::timestamptz, 1.0, 1.0),
                                     ('2022-08-01 06:00:00+00'::timestamptz, 2.0, 1.0),
                                     ('2022-08-01 12:00:00+00'::timestamptz, 3.0, 1.0),
                                     ('2022-08-01 18:00:00+00'::timestamptz, 4.0, 1.0),
                                     ('2022-08-01 23:59:59+00'::timestamptz, 5.0, 1.0)
                          ) AS v(ts, price, volume)
                          GROUP BY 1"#;

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            high:(ts:\"2022-08-01 23:59:59+00\",val:5),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:1),\
                            close:(ts:\"2022-08-01 23:59:59+00\",val:5),\
                            volume:Transaction(vol:5,vwap:15)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_agg_strictly_decreasing() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"SELECT
                              date_trunc('day', ts)::text,
                              candlestick_agg(ts, price, volume)::text
                          FROM (
                              VALUES ('2022-08-01 00:00:00+00'::timestamptz, 5.0, 1.0),
                                     ('2022-08-01 06:00:00+00'::timestamptz, 4.0, 1.0),
                                     ('2022-08-01 12:00:00+00'::timestamptz, 3.0, 1.0),
                                     ('2022-08-01 18:00:00+00'::timestamptz, 2.0, 1.0),
                                     ('2022-08-01 23:59:59+00'::timestamptz, 1.0, 1.0)
                          ) AS v(ts, price, volume)
                          GROUP BY 1"#;

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:5),\
                            high:(ts:\"2022-08-01 00:00:00+00\",val:5),\
                            low:(ts:\"2022-08-01 23:59:59+00\",val:1),\
                            close:(ts:\"2022-08-01 23:59:59+00\",val:1),\
                            volume:Transaction(vol:5,vwap:15)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_agg_oscillating() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"SELECT
                              date_trunc('day', ts)::text,
                              candlestick_agg(ts, price, volume)::text
                          FROM (
                              VALUES ('2022-08-01 00:00:00+00'::timestamptz,  3.0, 1.0),
                                     ('2022-08-01 02:00:00+00'::timestamptz,  4.0, 1.0),
                                     ('2022-08-01 04:00:00+00'::timestamptz, 11.0, 1.0),
                                     ('2022-08-01 06:00:00+00'::timestamptz,  5.0, 1.0),
                                     ('2022-08-01 08:00:00+00'::timestamptz,  2.0, 1.0),
                                     ('2022-08-01 10:00:00+00'::timestamptz,  1.0, 1.0),
                                     ('2022-08-01 12:00:00+00'::timestamptz, 12.0, 1.0),
                                     ('2022-08-01 14:00:00+00'::timestamptz,  9.0, 1.0),
                                     ('2022-08-01 16:00:00+00'::timestamptz, 10.0, 1.0),
                                     ('2022-08-01 18:00:00+00'::timestamptz,  7.0, 1.0),
                                     ('2022-08-01 20:00:00+00'::timestamptz,  6.0, 1.0),
                                     ('2022-08-01 22:00:00+00'::timestamptz,  8.0, 1.0)
                          ) AS v(ts, price, volume)
                          GROUP BY 1"#;

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:3),\
                            high:(ts:\"2022-08-01 12:00:00+00\",val:12),\
                            low:(ts:\"2022-08-01 10:00:00+00\",val:1),\
                            close:(ts:\"2022-08-01 22:00:00+00\",val:8),\
                            volume:Transaction(vol:12,vwap:78)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_rollup() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"WITH t AS (
                              SELECT
                                  candlestick(ts, open, high, low, close, volume) AS candlestick
                              FROM (
                                  VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 4.0, 0.0, 4.0, 5.0),
                                         ('2022-08-02 00:00:00+00'::timestamptz, 5.0, 8.0, 5.0, 8.0, 4.0)
                              ) AS v(ts, open, high, low, close, volume)
                          )
                          SELECT
                              rollup(candlestick)::text
                          FROM t"#;

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-02 00:00:00+00\",val:8),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-02 00:00:00+00\",val:8),\
                            volume:Transaction(vol:9,vwap:41.33333333333333)\
                            )";

            let output = select_one!(client, stmt, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_agg_rollup() {
        Spi::connect(|mut client| {
            client.update("SET timezone TO 'UTC'", None, None).unwrap();

            let stmt = r#"WITH t AS (
                              SELECT
                                  date_trunc('day', ts) AS date,
                                  candlestick_agg(ts, price, volume) AS candlestick
                              FROM (
                                  VALUES ('2022-08-01 00:00:00+00'::timestamptz, 0.0, 1.0),
                                         ('2022-08-01 06:00:00+00'::timestamptz, 1.0, 1.0),
                                         ('2022-08-01 12:00:00+00'::timestamptz, 2.0, 1.0),
                                         ('2022-08-01 18:00:00+00'::timestamptz, 3.0, 1.0),
                                         ('2022-08-01 23:59:59+00'::timestamptz, 4.0, 1.0),
                                         ('2022-08-02 06:00:00+00'::timestamptz, 5.0, 1.0),
                                         ('2022-08-02 12:00:00+00'::timestamptz, 6.0, 1.0),
                                         ('2022-08-02 18:00:00+00'::timestamptz, 7.0, 1.0),
                                         ('2022-08-02 23:59:59+00'::timestamptz, 8.0, 1.0)
                              ) AS v(ts, price, volume)
                              GROUP BY 1
                          )
                          SELECT
                              date_trunc('month', date)::text,
                              rollup(candlestick)::text
                          FROM t
                          GROUP BY 1"#;

            let expected = "(\
                            version:1,\
                            open:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            high:(ts:\"2022-08-02 23:59:59+00\",val:8),\
                            low:(ts:\"2022-08-01 00:00:00+00\",val:0),\
                            close:(ts:\"2022-08-02 23:59:59+00\",val:8),\
                            volume:Transaction(vol:9,vwap:36)\
                            )";
            let (_, output) = select_two!(client, stmt, &str, &str);
            assert_eq!(expected, output.unwrap());
        });
    }

    #[pg_test]
    fn candlestick_byte_io() {
        let state = tick_data_transition_inner(
            None,
            Some(100.into()),
            Some(10.0),
            Some(1.0),
            ptr::null_mut(),
        );
        let state = tick_data_transition_inner(
            state,
            Some(200.into()),
            Some(1.0),
            Some(2.0),
            ptr::null_mut(),
        );

        let output_buffer = state.unwrap().to_pg_bytes();
        let expected = [
            128, 1, 0, 0, 1, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 100, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 200, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            240, 63, 200, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 2, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 40, 64,
        ];
        assert_eq!(*output_buffer, expected);
    }
}
