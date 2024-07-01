use pgrx::*;

use super::*;

pub struct ExpressionExecutor<'e, T> {
    exprs: &'e Expression,
    var_vals: Vec<Option<Value>>,
    tracer: T,
}

impl<'e> ExpressionExecutor<'e, ()> {
    pub fn new(exprs: &'e Expression) -> Self {
        Self::with_tracer(exprs, ())
    }
}

impl<'e, T> ExpressionExecutor<'e, T>
where
    T: Tracer,
{
    pub fn with_fn_tracer(exprs: &'e Expression, tracer: T) -> Self
    where
        T: FnMut(&ExpressionSegment, &Value),
    {
        Self::with_tracer(exprs, tracer)
    }

    pub fn with_tracer(exprs: &'e Expression, tracer: T) -> Self {
        Self {
            var_vals: vec![None; exprs.variables.len()],
            exprs,
            tracer,
        }
    }

    pub fn reset(&mut self) {
        for v in &mut self.var_vals {
            *v = None
        }
    }

    pub fn exec(&mut self, value: f64, time: i64) -> Value {
        self.exec_expression(&self.exprs.expr, value, time)
    }

    fn exec_expression(
        &mut self,
        expr: &ExpressionSegment,
        value: f64,
        time: i64,
        // trace_function: impl FnMut(&ExpressionSegment, &Value),
    ) -> Value {
        use ExpressionSegment::*;
        let res = match expr {
            ValueVar => Value::Double(value),
            TimeVar => Value::Time(time),
            DoubleConstant(f) => Value::Double(*f),
            TimeConstant(t) => Value::Time(*t),
            IntervalConstant(i) => Value::Interval(*i),

            UserVar(i, _) => self.force_var(*i, value, time),

            FunctionCall(function, args) => self.exec_function(function, args, value, time),

            Unary(op, expr, ty) => self.exec_unary_op(*op, ty, expr, value, time),

            Binary(op, left, right, ty) => self.exec_binary_op(*op, ty, left, right, value, time),

            BuildTuple(exprs, _) => Value::Tuple(
                exprs
                    .iter()
                    .map(|e| self.exec_expression(e, value, time))
                    .collect(),
            ),
        };
        self.tracer.trace(expr, &res);
        res
    }

    fn force_var(&mut self, i: usize, value: f64, time: i64) -> Value {
        if let Some(value) = &self.var_vals[i] {
            return value.clone();
        }

        let value = self.exec_expression(&self.exprs.variables[i], value, time);
        self.var_vals[i] = Some(value.clone());
        value
    }

    fn exec_function(
        &mut self,
        function: &Function,
        args: &[ExpressionSegment],
        value: f64,
        time: i64,
    ) -> Value {
        use Function::*;
        macro_rules! unary_function {
            ($func:ident ( )) => {{
                let then = self.exec_expression(&args[0], value, time).float();
                then.$func().into()
            }};
        }
        macro_rules! binary_function {
            ($func:ident ( )) => {{
                let args = &args[0..2];
                let a = self.exec_expression(&args[0], value, time).float();
                let b = self.exec_expression(&args[1], value, time).float();
                a.$func(b).into()
            }};
        }
        match function {
            Abs => unary_function!(abs()),
            Cbrt => unary_function!(cbrt()),
            Ceil => unary_function!(ceil()),
            Floor => unary_function!(floor()),
            Ln => unary_function!(ln()),
            Log10 => unary_function!(log10()),
            Log => {
                let base = self.exec_expression(&args[1], value, time).float();
                let a = self.exec_expression(&args[0], value, time).float();
                a.log(base).into()
            }
            Pi => std::f64::consts::PI.into(),
            Round => unary_function!(round()),
            Sign => unary_function!(signum()),
            Sqrt => unary_function!(sqrt()),
            Trunc => unary_function!(trunc()),
            Acos => unary_function!(acos()),
            Asin => unary_function!(asin()),
            Atan => unary_function!(atan()),
            Atan2 => binary_function!(atan2()),
            Cos => unary_function!(cos()),
            Sin => unary_function!(sin()),
            Tan => unary_function!(tan()),
            Sinh => unary_function!(sinh()),
            Cosh => unary_function!(cosh()),
            Tanh => unary_function!(tanh()),
            Asinh => unary_function!(asinh()),
            Acosh => unary_function!(acosh()),
            Atanh => unary_function!(atanh()),
        }
    }

    fn exec_unary_op(
        &mut self,
        op: UnaryOp,
        ty: &Type,
        expr: &ExpressionSegment,
        value: f64,
        time: i64,
    ) -> Value {
        use Type::*;
        use UnaryOp::*;
        match op {
            Not => {
                let val = self.exec_expression(expr, value, time).bool();
                (!val).into()
            }
            Negative => {
                match ty {
                    Double => {
                        let val = self.exec_expression(expr, value, time).float();
                        (-val).into()
                    }
                    // TODO interval?
                    _ => unreachable!(),
                }
            }
        }
    }

    fn exec_binary_op(
        &mut self,
        op: BinOp,
        ty: &Type,
        left: &ExpressionSegment,
        right: &ExpressionSegment,
        value: f64,
        time: i64,
    ) -> Value {
        use BinOp::*;
        use Type::*;

        // FIXME pgrx wraps all functions in rust wrappers, which makes them
        //       uncallable with DirectFunctionCall(). Is there a way to
        //       export both?
        // TODO This is fixed in a newer pgrx version, should remove after upgrade
        #[allow(improper_ctypes)]
        extern "C" {
            fn interval_pl(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
            fn interval_mi(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
            fn interval_mul(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
            fn interval_div(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;

            fn timestamptz_pl_interval(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
            fn timestamptz_mi_interval(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
        }

        macro_rules! float_op {
            (($left: ident, $right: ident) $calc: expr) => {{
                let $left = self.exec_expression(left, value, time).float();
                let $right = self.exec_expression(right, value, time).float();
                ($calc).into()
            }};
        }

        macro_rules! interval_op {
            (($left: ident, $right: ident) $calc: ident) => {{
                let left = self.exec_expression(left, value, time).interval();
                let right = self.exec_expression(right, value, time).interval();

                let res: *mut pg_sys::Interval = unsafe {
                    pg_sys::DirectFunctionCall2Coll(
                        Some($calc),
                        pg_sys::InvalidOid,
                        pg_sys::Datum::from(left),
                        pg_sys::Datum::from(right),
                    )
                    .cast_mut_ptr()
                };
                assert!(!res.is_null());
                Value::Interval(res)
            }};
        }

        macro_rules! interval_float_op {
            (($left: ident, $right: ident) $calc: ident) => {{
                let left = self.exec_expression(left, value, time).interval();
                let right = self.exec_expression(right, value, time).float();

                let res: *mut pg_sys::Interval = unsafe {
                    pg_sys::DirectFunctionCall2Coll(
                        Some($calc),
                        pg_sys::InvalidOid,
                        pg_sys::Datum::from(left),
                        right.into_datum().unwrap(),
                    )
                    .value() as _
                };
                assert!(!res.is_null());
                Value::Interval(res)
            }};
        }

        macro_rules! time_op {
            (($left: ident, $right: ident) $calc: ident) => {{
                let left = self.exec_expression(left, value, time).time();
                let right = self.exec_expression(right, value, time).interval();

                let res: i64 = unsafe {
                    pg_sys::DirectFunctionCall2Coll(
                        Some($calc),
                        pg_sys::InvalidOid,
                        pg_sys::Datum::from(left),
                        pg_sys::Datum::from(right),
                    )
                    .value() as _
                };

                Value::Time(res)
            }};
        }

        match op {
            // arithmetic operators
            Plus => match ty {
                Double => float_op!((left, right) left + right),
                Time => time_op!((left, right) timestamptz_pl_interval),
                Interval => interval_op!((left, right) interval_pl),
                _ => unreachable!(),
            },

            Minus => match ty {
                Double => float_op!((left, right) left - right),
                Time => time_op!((left, right) timestamptz_mi_interval),
                Interval => interval_op!((left, right) interval_mi),
                _ => unreachable!(),
            },

            Mul => match ty {
                Double => float_op!((left, right) left * right),
                Interval => interval_float_op!((left, right) interval_mul),
                _ => unreachable!(),
            },

            Div => match ty {
                Double => float_op!((left, right) left / right),
                Interval => interval_float_op!((left, right) interval_div),
                _ => unreachable!(),
            },

            Pow => float_op!((left, right) left.powf(right)),

            // comparison operators
            Eq => {
                let left = self.exec_expression(left, value, time);
                let right = self.exec_expression(right, value, time);
                (left == right).into()
            }

            Neq => {
                let left = self.exec_expression(left, value, time);
                let right = self.exec_expression(right, value, time);
                (left != right).into()
            }

            Lt => {
                let left = self.exec_expression(left, value, time);
                let right = self.exec_expression(right, value, time);
                (left < right).into()
            }

            Gt => {
                let left = self.exec_expression(left, value, time);
                let right = self.exec_expression(right, value, time);
                (left > right).into()
            }

            Le => {
                let left = self.exec_expression(left, value, time);
                let right = self.exec_expression(right, value, time);
                (left <= right).into()
            }

            Ge => {
                let left = self.exec_expression(left, value, time);
                let right = self.exec_expression(right, value, time);
                (left >= right).into()
            }

            // boolean operators
            And => {
                let left = self.exec_expression(left, value, time).bool();
                if !left {
                    return false.into();
                }
                self.exec_expression(right, value, time)
            }

            Or => {
                let left = self.exec_expression(left, value, time).bool();
                if left {
                    return true.into();
                }
                self.exec_expression(right, value, time)
            }
        }
    }
}

pub trait Tracer {
    fn trace(&mut self, expr: &ExpressionSegment, result: &Value);
}

impl Tracer for () {
    fn trace(&mut self, _: &ExpressionSegment, _: &Value) {}
}

impl<T> Tracer for T
where
    T: FnMut(&ExpressionSegment, &Value),
{
    fn trace(&mut self, expr: &ExpressionSegment, result: &Value) {
        self(expr, result)
    }
}
