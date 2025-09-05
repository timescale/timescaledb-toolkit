use std::borrow::Cow;

use pgrx::{
    iter::{SetOfIterator, TableIterator},
    *,
};

use super::*;

pub use executor::ExpressionExecutor;

mod executor;
mod parser;

pub use self::toolkit_experimental::{Lambda, LambdaData};

#[pg_schema]
pub mod toolkit_experimental {
    pub(crate) use super::*;

    //
    // lambda type
    //

    pg_type! {
        #[derive(Debug)]
        struct Lambda<'input> {
            len: u32,
            string: [u8; self.len],
        }
    }
}

impl<'input> InOutFuncs for Lambda<'input> {
    fn output(&self, buffer: &mut StringInfo) {
        use crate::serialization::{str_to_db_encoding, EncodedStr::*};

        let stringified = std::str::from_utf8(self.string.as_slice()).unwrap();
        match str_to_db_encoding(stringified) {
            Utf8(s) => buffer.push_str(s),
            Other(s) => buffer.push_bytes(s.to_bytes()),
        }
    }

    fn input(input: &std::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        use crate::serialization::str_from_db_encoding;

        let s = str_from_db_encoding(input);
        // validate the string
        let _ = parser::parse_expression(s);
        unsafe {
            flatten! {
                Lambda {
                    len: s.len() as _,
                    string: s.as_bytes().into(),
                }
            }
        }
    }
}

impl<'a> LambdaData<'a> {
    pub fn parse(&self) -> Expression {
        parser::parse_expression(std::str::from_utf8(self.string.as_slice()).unwrap())
    }
}

//
// Direct lambda execution functions for testing
//

#[pg_extern(stable, parallel_safe, schema = "toolkit_experimental")]
pub fn bool_lambda<'a>(
    lambda: toolkit_experimental::Lambda<'a>,
    time: crate::raw::TimestampTz,
    value: f64,
) -> bool {
    let expression = lambda.parse();
    if expression.expr.ty() != &Type::Bool {
        panic!("invalid return type, must return a BOOLEAN for {expression:?}")
    }
    let mut executor = ExpressionExecutor::new(&expression);
    executor.exec(value, time.into()).bool()
}

#[pg_extern(stable, parallel_safe, schema = "toolkit_experimental")]
pub fn f64_lambda<'a>(
    lambda: toolkit_experimental::Lambda<'a>,
    time: crate::raw::TimestampTz,
    value: f64,
) -> f64 {
    let expression = lambda.parse();
    if expression.expr.ty() != &Type::Double {
        panic!("invalid return type, must return a DOUBLE PRECISION")
    }
    let mut executor = ExpressionExecutor::new(&expression);
    executor.exec(value, time.into()).float()
}

#[pg_extern(stable, parallel_safe, schema = "toolkit_experimental")]
pub fn ttz_lambda<'a>(
    lambda: toolkit_experimental::Lambda<'a>,
    time: crate::raw::TimestampTz,
    value: f64,
) -> crate::raw::TimestampTz {
    let expression = lambda.parse();
    if expression.expr.ty() != &Type::Time {
        panic!("invalid return type, must return a TimestampTZ")
    }
    let mut executor = ExpressionExecutor::new(&expression);
    executor.exec(value, time.into()).time().into()
}

use crate::raw::Interval;
#[pg_extern(stable, parallel_safe, schema = "toolkit_experimental")]
pub fn interval_lambda<'a>(
    lambda: toolkit_experimental::Lambda<'a>,
    time: crate::raw::TimestampTz,
    value: f64,
) -> Interval {
    let expression = lambda.parse();
    if expression.expr.ty() != &Type::Interval {
        panic!("invalid return type, must return a INTERVAL")
    }
    let mut executor = ExpressionExecutor::new(&expression);
    pg_sys::Datum::from(executor.exec(value, time.into()).interval()).into()
}

#[pg_extern(stable, parallel_safe, schema = "toolkit_experimental")]
pub fn point_lambda<'a>(
    lambda: toolkit_experimental::Lambda<'a>,
    time: crate::raw::TimestampTz,
    value: f64,
) -> TableIterator<'static, (name!(time, crate::raw::TimestampTz), name!(value, f64))> {
    let expression = lambda.parse();
    if !expression.expr.ty_is_ts_point() {
        panic!("invalid return type, must return a (TimestampTZ, DOUBLE PRECISION)")
    }

    let mut executor = ExpressionExecutor::new(&expression);
    let columns = match executor.exec(value, time.into()) {
        Value::Tuple(columns) => columns,
        _ => unreachable!(),
    };
    TableIterator::new(Some((columns[0].time().into(), columns[1].float())).into_iter())
}

#[pg_extern(stable, parallel_safe, schema = "toolkit_experimental")]
pub fn trace_lambda<'a>(
    lambda: toolkit_experimental::Lambda<'a>,
    time: crate::raw::TimestampTz,
    value: f64,
) -> SetOfIterator<'static, String> {
    let expression = lambda.parse();

    let mut trace: Vec<_> = vec![];
    let mut executor = ExpressionExecutor::with_fn_tracer(&expression, |e, v| {
        trace.push((e.name(), format!("{v:?}")))
    });

    let _ = executor.exec(value, time.into());
    let col1_size = trace.iter().map(|(e, _)| e.len()).max().unwrap_or(0);

    SetOfIterator::new(
        trace
            .into_iter()
            .map(move |(e, v)| format!("{e:>col1_size$}: {v:?}")),
    )
}

//
// Common types across the parser and executor
//

// expressions
#[derive(Debug)]
pub struct Expression {
    variables: Vec<ExpressionSegment>,
    expr: ExpressionSegment,
}

#[derive(Clone, Debug)]
pub enum ExpressionSegment {
    ValueVar,
    TimeVar,
    DoubleConstant(f64),
    TimeConstant(i64),
    IntervalConstant(*mut pg_sys::Interval),
    UserVar(usize, Type),
    Unary(UnaryOp, Box<Self>, Type),
    Binary(BinOp, Box<Self>, Box<Self>, Type),
    FunctionCall(Function, Vec<Self>),
    BuildTuple(Vec<Self>, Type),
}

#[derive(Clone, Copy, Debug)]
pub enum UnaryOp {
    Not,
    Negative,
}

#[derive(Clone, Copy, Debug)]
pub enum BinOp {
    Plus,
    Minus,
    Mul,
    Div,
    Pow,
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
    Neq,
    And,
    Or,
}

#[derive(Clone, Copy, Debug)]
pub enum Function {
    Abs,
    Cbrt,
    Ceil,
    Floor,
    Ln,
    Log10,
    Log,
    Pi,
    Round,
    Sign,
    Sqrt,
    Trunc,
    Acos,
    Asin,
    Atan,
    Atan2,
    Cos,
    Sin,
    Tan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
}

// types
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Time,
    Double,
    Bool,
    Interval,
    Tuple(Vec<Self>),
}

// values
#[derive(Clone, Debug)]
pub enum Value {
    Bool(bool),
    Double(f64),
    Time(i64),
    Interval(*mut pg_sys::Interval),
    Tuple(Vec<Self>),
}

impl Expression {
    pub fn ty(&self) -> &Type {
        self.expr.ty()
    }

    pub fn ty_is_ts_point(&self) -> bool {
        self.expr.ty_is_ts_point()
    }
}

impl ExpressionSegment {
    pub fn ty(&self) -> &Type {
        use ExpressionSegment::*;
        use Type::*;
        match self {
            ValueVar => &Double,
            TimeVar => &Time,
            DoubleConstant(_) => &Double,
            TimeConstant(_) => &Time,
            IntervalConstant(_) => &Interval,
            UserVar(_, ty) => ty,
            FunctionCall(_, _) => &Double,
            Unary(_, _, ty) => ty,
            Binary(_, _, _, ty) => ty,
            BuildTuple(_, ty) => ty,
        }
    }

    pub fn ty_is_ts_point(&self) -> bool {
        let columns = match self {
            ExpressionSegment::BuildTuple(_, Type::Tuple(ty)) => ty,
            _ => return false,
        };

        matches!(&**columns, [Type::Time, Type::Double])
    }

    pub fn name(&self) -> Cow<'static, str> {
        use ExpressionSegment::*;
        match self {
            ValueVar => "$value".into(),
            TimeVar => "$time".into(),
            DoubleConstant(_) => "f64 const".into(),
            TimeConstant(_) => "time const".into(),
            IntervalConstant(_) => "interval const".into(),
            UserVar(i, t) => format!("user var {i}: {t:?}").into(),
            Unary(op, _, t) => format!("uop {op:?} {t:?}").into(),
            Binary(op, _, _, t) => format!("binop {op:?} {t:?}").into(),
            FunctionCall(f, _) => format!("function {f:?}").into(),
            BuildTuple(_, t) => format!("tuple {t:?}").into(),
        }
    }
}

impl Value {
    pub(crate) fn bool(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            _ => unreachable!(),
        }
    }

    pub(crate) fn float(&self) -> f64 {
        match self {
            Value::Double(f) => *f,
            _ => unreachable!(),
        }
    }

    pub(crate) fn time(&self) -> i64 {
        match self {
            Value::Time(t) => *t,
            _ => unreachable!(),
        }
    }

    pub(crate) fn interval(&self) -> *mut pg_sys::Interval {
        match self {
            Value::Interval(i) => *i,
            _ => unreachable!(),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::mem::discriminant;
        use Value::*;

        // XXX `NodeTag` somewhere inside `pg_sys::FunctionCallInfo` triggers
        // `improper_ctypes` lint. The `pgrx` author explains the issue in
        // details here:
        //
        // https://github.com/rust-lang/rust/issues/116831
        //
        // For now it seems OK to suppress these warnings here and below with
        // #[allow(improper_ctypes)]
        unsafe extern "C-unwind" {
            #[allow(improper_ctypes)]
            fn interval_cmp(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
        }

        if discriminant(self) != discriminant(other) {
            return None;
        }
        match (self, other) {
            (Bool(l0), Bool(r0)) => l0.partial_cmp(r0),
            (Double(l0), Double(r0)) => l0.partial_cmp(r0),
            (Time(l0), Time(r0)) => l0.partial_cmp(r0),
            (Tuple(l0), Tuple(r0)) => l0.partial_cmp(r0),
            (Interval(l0), Interval(r0)) => unsafe {
                let res = pg_sys::DirectFunctionCall2Coll(
                    Some(interval_cmp),
                    pg_sys::InvalidOid,
                    pg_sys::Datum::from(*l0),
                    pg_sys::Datum::from(*r0),
                )
                .value() as i32;
                res.cmp(&0).into()
            },
            (_, _) => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use std::mem::discriminant;
        use Value::*;
        unsafe extern "C-unwind" {
            #[allow(improper_ctypes)]
            fn interval_eq(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
        }

        if discriminant(self) != discriminant(other) {
            return false;
        }
        match (self, other) {
            (Bool(l0), Bool(r0)) => l0 == r0,
            (Double(l0), Double(r0)) => l0 == r0,
            (Time(l0), Time(r0)) => l0 == r0,
            (Tuple(l0), Tuple(r0)) => l0 == r0,
            (Interval(l0), Interval(r0)) => unsafe {
                let res = pg_sys::DirectFunctionCall2Coll(
                    Some(interval_eq),
                    pg_sys::InvalidOid,
                    pg_sys::Datum::from(*l0),
                    pg_sys::Datum::from(*r0),
                );
                res.value() != 0
            },
            (_, _) => false,
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Double(f)
    }
}

impl<'a> Lambda<'a> {
    pub fn into_data(self) -> LambdaData<'a> {
        self.0
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::*;
    use pgrx_macros::pg_test;

    macro_rules! trace_lambda {
        ($client: expr, $expr:literal) => {
            $client
                .update(
                    concat!("SELECT trace_lambda($$ ", $expr, " $$, '2021-01-01', 2.0)"),
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| r.get::<String>(1).unwrap().unwrap())
                .collect()
        };
    }

    macro_rules! point_lambda {
        ($client: expr, $expr:literal) => {
            $client
                .update(
                    concat!(
                        "SELECT point_lambda($$ ",
                        $expr,
                        " $$, '2021-01-01', 2.0)::text"
                    ),
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap()
        };
    }

    macro_rules! interval_lambda {
        ($client: expr, $expr:literal) => {
            $client
                .update(
                    concat!(
                        "SELECT interval_lambda($$ ",
                        $expr,
                        " $$, now(), 2.0)::text"
                    ),
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap()
        };
    }

    macro_rules! f64_lambda {
        ($client: expr, $expr:literal) => {
            $client
                .update(
                    concat!("SELECT f64_lambda($$ ", $expr, " $$, now(), 2.0)"),
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap()
        };
    }

    macro_rules! bool_lambda {
        ($client: expr, $expr:literal) => {
            $client
                .update(
                    concat!("SELECT bool_lambda($$ ", $expr, " $$, now(), 2.0)::text"),
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap()
        };
    }

    macro_rules! point_lambda_eq {
        ($client: expr, $expr:literal, $expects:literal) => {
            assert_eq!(point_lambda!($client, $expr), $expects,)
        };
    }

    macro_rules! interval_lambda_eq {
        ($client: expr, $expr:literal, $expects:literal) => {
            assert_eq!(interval_lambda!($client, $expr), $expects,)
        };
    }

    macro_rules! f64_lambda_eq {
        ($client: expr, $expr:literal, $expects:expr) => {
            assert!((f64_lambda!($client, $expr) - ($expects)).abs() < f64::EPSILON,)
        };
    }

    macro_rules! bool_lambda_eq {
        ($client: expr, $expr:literal, $expects:literal) => {
            assert_eq!(bool_lambda!($client, $expr), $expects,)
        };
    }

    #[pg_test]
    fn test_lambda_general() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {sp}"), None, &[])
                .unwrap();
            client
                .update(
                    "SELECT $$ let $1 = 1.0; 2.0, $1 $$::toolkit_experimental.lambda",
                    None,
                    &[],
                )
                .unwrap();
            // client.update("SELECT $$ '1 day'i $$::toolkit_experimental.lambda", None, &[]).unwrap();
            // client.update("SELECT $$ '2020-01-01't $$::toolkit_experimental.lambda", None, &[]).unwrap();

            let res = client
                .update("SELECT f64_lambda($$ 1.0 $$, now(), 0.0)::text", None, &[])
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "1");

            let res = client
                .update(
                    "SELECT f64_lambda($$ 1.0 + 1.0 $$, now(), 0.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "2");

            let res = client
                .update(
                    "SELECT f64_lambda($$ 1.0 - 1.0 $$, now(), 0.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "0");

            let res = client
                .update(
                    "SELECT f64_lambda($$ 2.0 * 3.0 $$, now(), 0.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "6");

            let res = client
                .update(
                    "SELECT f64_lambda($$ $value + 3.0 $$, now(), 2.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "5");

            let res = client
                .update(
                    "SELECT f64_lambda($$ 3.0 - 1.0 * 3.0 $$, now(), 2.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "0");

            bool_lambda_eq!(client, "3.0 = 3.0", "true");
            bool_lambda_eq!(client, "3.0 != 3.0", "false");
            bool_lambda_eq!(client, "2.0 != 3.0", "true");
            bool_lambda_eq!(client, "2.0 != 3.0 and 1 = 1", "true");
            bool_lambda_eq!(client, "2.0 != 3.0 and (1 = 1)", "true");

            let res = client
                .update(
                    "SELECT ttz_lambda($$ '2020-11-22 13:00:01't $$, now(), 2.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "2020-11-22 13:00:01+00");

            let res = client
                .update(
                    "SELECT ttz_lambda($$ $time $$, '1930-01-12 14:20:21', 2.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "1930-01-12 14:20:21+00");

            let res = client
                .update(
                    "SELECT ttz_lambda($$ '2020-11-22 13:00:01't - '1 day'i $$, now(), 2.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "2020-11-21 13:00:01+00");

            let res = client
                .update(
                    "SELECT ttz_lambda($$ '2020-11-22 13:00:01't + '1 day'i $$, now(), 2.0)::text",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(&*res.unwrap(), "2020-11-23 13:00:01+00");

            point_lambda_eq!(
                client,
                "'2020-11-22 13:00:01't + '1 day'i, 2.0 * 3.0",
                r#"("2020-11-23 13:00:01+00",6)"#
            );

            point_lambda_eq!(
                client,
                "($time, $value^2 + $value * 2.3 + 43.2)",
                r#"("2021-01-01 00:00:00+00",51.800000000000004)"#
            );
        });
    }

    #[pg_test]
    fn test_lambda_comparison() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {sp}"), None, &[])
                .unwrap();

            bool_lambda_eq!(client, "2.0 <  3.0", "true");
            bool_lambda_eq!(client, "2.0 <= 3.0", "true");
            bool_lambda_eq!(client, "2.0 >  3.0", "false");
            bool_lambda_eq!(client, "2.0 >= 3.0", "false");
            bool_lambda_eq!(client, "4.0 >  3.0", "true");
            bool_lambda_eq!(client, "4.0 >= 3.0", "true");
            bool_lambda_eq!(client, "4.0 >  4.0", "false");
            bool_lambda_eq!(client, "4.0 >= 4.0", "true");

            bool_lambda_eq!(client, "'2020-01-01't <  '2021-01-01't", "true");
            bool_lambda_eq!(client, "'2020-01-01't <= '2021-01-01't", "true");
            bool_lambda_eq!(client, "'2020-01-01't >  '2021-01-01't", "false");
            bool_lambda_eq!(client, "'2020-01-01't >= '2021-01-01't", "false");
            bool_lambda_eq!(client, "'2022-01-01't <  '2021-01-01't", "false");
            bool_lambda_eq!(client, "'2022-01-01't <= '2021-01-01't", "false");
            bool_lambda_eq!(client, "'2022-01-01't >  '2021-01-01't", "true");
            bool_lambda_eq!(client, "'2022-01-01't >= '2021-01-01't", "true");
            bool_lambda_eq!(client, "'2022-01-01't >  '2021-01-01't", "true");
            bool_lambda_eq!(client, "'2022-01-01't >= '2021-01-01't", "true");

            bool_lambda_eq!(client, "'1 day'i  <  '1 week'i", "true");
            bool_lambda_eq!(client, "'1 day'i  <= '1 week'i", "true");
            bool_lambda_eq!(client, "'1 day'i  >  '1 week'i", "false");
            bool_lambda_eq!(client, "'1 day'i  >= '1 week'i ", "false");
            bool_lambda_eq!(client, "'1 year'i >  '1 week'i", "true");
            bool_lambda_eq!(client, "'1 year'i >= '1 week'i", "true");
            bool_lambda_eq!(client, "'1 year'i >  '1 year'i", "false");
            bool_lambda_eq!(client, "'1 year'i >= '1 year'i", "true");
        });
    }

    #[pg_test]
    fn test_lambda_function() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {sp}"), None, &[])
                .unwrap();

            f64_lambda_eq!(client, "pi()", std::f64::consts::PI);

            f64_lambda_eq!(client, "abs(-2.0)", (-2.0f64).abs());
            f64_lambda_eq!(client, "cbrt(-2.0)", (-2.0f64).cbrt());
            f64_lambda_eq!(client, "ceil(-2.1)", (-2.1f64).ceil());
            f64_lambda_eq!(client, "floor(-2.1)", (-2.1f64).floor());
            f64_lambda_eq!(client, "ln(2.0)", (2.0f64).ln());
            f64_lambda_eq!(client, "log10(2.0)", (2.0f64).log10());
            f64_lambda_eq!(client, "round(-2.1)", (-2.1f64).round());
            f64_lambda_eq!(client, "sign(-2.0)", (-2.0f64).signum());
            f64_lambda_eq!(client, "sqrt(2.0)", (2.0f64).sqrt());
            f64_lambda_eq!(client, "trunc(-2.0)", (-2.0f64).trunc());
            f64_lambda_eq!(client, "acos(0.2)", (0.2f64).acos());
            f64_lambda_eq!(client, "asin(0.2)", (0.2f64).asin());
            f64_lambda_eq!(client, "atan(0.2)", (0.2f64).atan());
            f64_lambda_eq!(client, "cos(2.0)", (2.0f64).cos());
            f64_lambda_eq!(client, "sin(2.0)", (2.0f64).sin());
            f64_lambda_eq!(client, "tan(2.0)", (2.0f64).tan());
            f64_lambda_eq!(client, "sinh(2.0)", (2.0f64).sinh());
            f64_lambda_eq!(client, "cosh(2.0)", (2.0f64).cosh());
            f64_lambda_eq!(client, "tanh(2.0)", (2.0f64).tanh());
            f64_lambda_eq!(client, "asinh(1.0)", (1.0f64).asinh());
            f64_lambda_eq!(client, "acosh(1.0)", (1.0f64).acosh());
            f64_lambda_eq!(client, "atanh(0.9)", (0.9f64).atanh());

            f64_lambda_eq!(client, "log(2.0, 10)", 2.0f64.log(10.0));
            f64_lambda_eq!(client, "atan2(2.0, 10)", 2.0f64.atan2(10.0));
        });
    }

    #[pg_test]
    fn test_lambda_unary() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {sp}"), None, &[])
                .unwrap();

            f64_lambda_eq!(client, "-(2.0)", -2.0f64);
            f64_lambda_eq!(client, "-(-2.0)", 2.0f64);

            bool_lambda_eq!(client, "not (1 = 1)", "false");
            bool_lambda_eq!(client, "not (1 = 2)", "true");
            bool_lambda_eq!(client, "not not (1 = 1)", "true");
            bool_lambda_eq!(client, "not not (1 = 2)", "false");
            bool_lambda_eq!(client, "not (1 <> 1)", "true");
            bool_lambda_eq!(client, "not (1 <> 2)", "false");
        });
    }

    #[pg_test]
    fn test_lambda_interval_ops() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {sp}"), None, &[])
                .unwrap();

            interval_lambda_eq!(client, "'1 day'i + '1 day'i", "2 days");
            interval_lambda_eq!(client, "'1 day'i + '1 week'i", "8 days");
            interval_lambda_eq!(client, "'1 week'i - '1 day'i", "6 days");

            interval_lambda_eq!(client, "'1 day'i * 3", "3 days");
            interval_lambda_eq!(client, "4 * '1 day'i", "4 days");
            interval_lambda_eq!(client, "'4 day'i / 4", "1 day");
        });
    }

    #[pg_test]
    fn test_lambda_variable() {
        Spi::connect_mut(|client| {
            client.update("SET timezone TO 'UTC'", None, &[]).unwrap();
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client
                .update(
                    "SELECT format(' %s, toolkit_experimental',current_setting('search_path'))",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap()
                .unwrap();
            client
                .update(&format!("SET LOCAL search_path TO {sp}"), None, &[])
                .unwrap();

            f64_lambda_eq!(client, "let $foo = 2.0; $foo", 2.0);
            f64_lambda_eq!(client, "let $foo = -2.0; $foo", -2.0);
            f64_lambda_eq!(client, "let $foo = abs(-2.0); $foo", 2.0);
            f64_lambda_eq!(client, "let $foo = abs(-2.0); $foo * $foo", 4.0);

            bool_lambda_eq!(client, "let $foo = 1 = 1; $foo", "true");
            bool_lambda_eq!(client, "let $foo = 1 = 1; $foo and $foo", "true");
            bool_lambda_eq!(client, "let $foo = 1 = 1; $foo or $foo", "true");

            // verify that variables are only expanded once
            let rows: Vec<_> = trace_lambda!(client, "let $bar = 1 + 1; $bar + $bar + $bar");
            assert_eq!(
                &*rows,
                [
                    r#"         f64 const: "Double(1.0)""#,
                    r#"         f64 const: "Double(1.0)""#,
                    r#" binop Plus Double: "Double(2.0)""#,
                    r#"user var 0: Double: "Double(2.0)""#,
                    r#"user var 0: Double: "Double(2.0)""#,
                    r#" binop Plus Double: "Double(4.0)""#,
                    r#"user var 0: Double: "Double(2.0)""#,
                    r#" binop Plus Double: "Double(6.0)""#,
                ],
            );

            let rows: Vec<_> = trace_lambda!(
                client,
                "let $foo = -2;\nlet $bar = $foo * $foo;\n $bar * $bar"
            );
            assert_eq!(
                &*rows,
                [
                    // TODO try and fix parsing so than `-2` parses as a constant `-2`
                    r#"          f64 const: "Double(2.0)""#,
                    r#"uop Negative Double: "Double(-2.0)""#,
                    r#" user var 0: Double: "Double(-2.0)""#,
                    r#" user var 0: Double: "Double(-2.0)""#,
                    r#"   binop Mul Double: "Double(4.0)""#,
                    r#" user var 1: Double: "Double(4.0)""#,
                    r#" user var 1: Double: "Double(4.0)""#,
                    r#"   binop Mul Double: "Double(16.0)""#,
                ],
            );
        });
    }
}
