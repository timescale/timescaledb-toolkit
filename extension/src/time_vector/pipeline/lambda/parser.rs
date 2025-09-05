use std::{collections::HashMap, ffi::CString};

use pgrx::*;

use super::*;

use pest::{
    iterators::{Pair, Pairs},
    prec_climber::{Assoc, Operator, PrecClimber},
    Parser,
};

use ExpressionSegment::*;
use Rule::*;
use Type::*;
use UnaryOp::*;

// Idealized expression grammar ignoring precedence
// ```
// Expression       :=  'let' Variable '=' Expression ';' Expression | BinaryExpression
// BinaryExpression := PrefixExpression ({',', '+', '-', '*', ...}  BinaryExpression)
// PrefixExpression := {'-', 'NOT'} ParenExpression
// ParenExpression  := '(' Expression ')' | Variable | Literal
// Variable         := $[a-bA-B_][a-bA-B0-9_]*
// Literal          := <number> | '<string>'
// ```
// Josh - I believe this is unambiguous and LL(1), but we should check before
//        stabilization
// FIXME check the grammar

#[derive(pest_derive::Parser)]
#[grammar = "time_vector/pipeline/lambda/lambda_expr.pest"] // relative to src
pub struct ExpressionParser;

pub fn parse_expression(input: &str) -> Expression {
    let parsed = ExpressionParser::parse(calculation, input).unwrap_or_else(|e| panic!("{}", e));

    let mut variables = Vec::new();
    let expr = build_expression(parsed, &mut variables, &mut HashMap::new());
    Expression { variables, expr }
}

// main parsing function.
fn build_expression<'a>(
    parsed: Pairs<'a, Rule>,
    var_expressions: &mut Vec<ExpressionSegment>,
    known_vars: &mut HashMap<&'a str, (Type, usize)>,
) -> ExpressionSegment {
    // Everything except binary operations are handled by `parse_primary()`
    // when we encounter a sequence of binary operations eg `<> + <> * <>`
    // the `(Expression, op, Expression)` triple is passed to `build_binary_op()`
    // in descending precedence order.
    PREC_CLIMBER.climb(
        parsed,
        |pair| parse_primary(pair, var_expressions, known_vars),
        |left: ExpressionSegment, op: Pair<Rule>, right: ExpressionSegment| {
            build_binary_op(op, left, right)
        },
    )
}

// handles everything except infix binary operators, which are handled by the
// precedence climber and `build_binary_op()`
fn parse_primary<'a>(
    pair: Pair<'a, Rule>,
    var_expressions: &mut Vec<ExpressionSegment>,
    known_vars: &mut HashMap<&'a str, (Type, usize)>,
) -> ExpressionSegment {
    // HOW TO READ:
    //   every rule (the left hand side of the `=` in the `.pest` file) has a
    //   variant in the following `match` statement. When seeing a rule like
    //   ```
    //   foo = { bar ~ "baz" ~ qux }
    //   ```
    //   1. `pair.as_str()` will be the entire string that matched the rule.
    //   2. `pair.into_iterator()` returns an iterator over the `Pair`s
    //       representing the sub rules. In this case it'll return two, one for
    //       `bar` and one for `qux`. These 'Pair's can be passed back to
    //       `parse_primary()` to parse them into `Expression`s for further
    //       handling.
    match pair.as_rule() {
        num => {
            let val: f64 = pair.as_str().parse().unwrap();
            DoubleConstant(val)
        }

        val_var => ValueVar,
        time_var => TimeVar,

        time => {
            let s = pair.as_str();
            let parsed_time = parse_timestamptz(&s[1..s.len() - 2]);
            TimeConstant(parsed_time)
        }

        interval => {
            let s = pair.as_str();
            let parsed_interval = parse_interval(&s[1..s.len() - 2]);
            IntervalConstant(parsed_interval)
        }

        var => {
            let (ty, v) = known_vars
                .get(pair.as_str())
                .unwrap_or_else(|| panic!("unknown variable: {}", pair.as_str()))
                .clone();
            UserVar(v, ty)
        }

        function => {
            let mut pairs = pair.into_inner();
            let func_name = pairs.next().unwrap();
            let (num_args, func_id) = *BUILTIN_FUNCTION
                .get(func_name.as_str())
                .unwrap_or_else(|| panic!("unknown function: {}", func_name.as_str()));

            let args: Vec<_> = pairs
                .map(|p| parse_primary(p, var_expressions, known_vars))
                .collect();
            if args.len() != num_args {
                panic!(
                    "function `{}` expects {} arguments and received {}",
                    func_name.as_str(),
                    num_args,
                    args.len(),
                )
            }

            FunctionCall(func_id, args)
        }

        neg => {
            let value = pair.into_inner().next().unwrap();
            let value = parse_primary(value, var_expressions, known_vars);
            if value.ty() != &Double {
                panic!("can only apply `-` to a DOUBLE PRECISION")
            }
            Unary(Negative, value.into(), Double)
        }

        not => {
            let value = pair.into_inner().next().unwrap();
            let value = parse_primary(value, var_expressions, known_vars);
            if value.ty() != &Bool {
                panic!("can only apply NOT to a BOOLEAN")
            }
            Unary(Not, value.into(), Bool)
        }

        // pass the sequence of binary operation to the precedence_climber to handle
        binops => build_expression(pair.into_inner(), var_expressions, known_vars),

        let_expr => {
            let mut pairs = pair.into_inner();
            loop {
                // let_expr has two forms
                // `let <variable> = <expression>; <expression>` and `<expression>`
                // if we have more than one sub-pair in our pairs then we know we're
                // in the first state, otherwise we must be in the second.
                let var_name_or_expr = pairs.next().unwrap();
                let var_value = match pairs.next() {
                    None => return parse_primary(var_name_or_expr, var_expressions, known_vars),
                    Some(val) => val,
                };

                let var_value = parse_primary(var_value, var_expressions, known_vars);

                let var_name = var_name_or_expr.as_str();
                known_vars
                    .entry(var_name)
                    .and_modify(|_| panic!("duplicate var {var_name}"))
                    .or_insert_with(|| (var_value.ty().clone(), var_expressions.len()));
                var_expressions.push(var_value);
            }
        }

        tuple => {
            // the tuple rule effectively has two forms
            // `<binops>` and `<binops> (, <binops>)+`
            // it's only in the second case that we'll actually build something
            // of a tuple type, in the former we'll just turn into the inner
            // expression.
            let mut pairs = pair.into_inner();
            let first = pairs.next().unwrap();
            let first_val = parse_primary(first, var_expressions, known_vars);
            match pairs.next() {
                None => first_val,
                Some(pair) => {
                    let mut vals = vec![first_val];
                    let val = parse_primary(pair, var_expressions, known_vars);
                    vals.push(val);
                    for p in pairs {
                        let val = parse_primary(p, var_expressions, known_vars);
                        vals.push(val);
                    }
                    let ty = Tuple(vals.iter().map(|v| v.ty().clone()).collect());
                    BuildTuple(vals, ty)
                }
            }
        }

        // operations marked with a `_` or that are below a `@` are never passed
        // to us, so we can ignore them.
        EOI | int | operation | string | unary | term | function_name | WHITESPACE
        | calculation => unreachable!("{} should be transparent", pair),

        // infix operations should be passed to `build_binary_op()` by the
        // precedence climber, so we should never see them here.
        add | subtract | multiply | divide | power | eq | neq | lt | le | gt | ge | and | or => {
            unreachable!("{} should be handled by precedence climbing", pair)
        }
    }
}

fn build_binary_op(
    op: Pair<Rule>,
    left: ExpressionSegment,
    right: ExpressionSegment,
) -> ExpressionSegment {
    use BinOp::*;
    use Type::Interval;
    macro_rules! return_ty {
        ($op:literal $(($l: pat, $r:pat) => $ty:expr),+ $(,)?) => {
            match (left.ty(), right.ty()) {
                $(($l, $r) => $ty,)+
                // TODO the error should report the location
                (l, r) => panic!(
                    concat!("no operator `{:?} {op} {:?}` only ",
                        $("`", stringify!($l), " {op} ", stringify!($r), "` ",)+
                    ),
                    l, r, op=$op),
            }
        };
    }
    match op.as_rule() {
        add => {
            let result_type = return_ty!("+"
                (Double, Double) => Double,
                (Type::Time, Interval) => Type::Time,
                (Interval, Interval) => Interval,
            );
            Binary(Plus, left.into(), right.into(), result_type)
        }

        subtract => {
            let result_type = return_ty!("-"
                (Double, Double) => Double,
                (Type::Time, Interval) => Type::Time,
                (Interval, Interval) => Interval,
            );
            Binary(Minus, left.into(), right.into(), result_type)
        }

        multiply => match (left.ty(), right.ty()) {
            (Double, Double) => Binary(Mul, left.into(), right.into(), Double),
            (Interval, Double) => Binary(Mul, left.into(), right.into(), Interval),
            // TODO right now BinOp(Mul, .., Interval) expects the interval on the left
            //      and the double on the left. We could check in the executor which one
            //      actually is, but it seems easier to just revers the value here if
            //      they're in an unexpected order.
            (Double, Interval) => Binary(Mul, right.into(), left.into(), Interval),
            (l, r) => {
                panic!("no operator `{l:?} * {r:?}` only `DOUBLE * DOUBLE` and `INTERVAL * FLOAT`")
            }
        },

        divide => {
            let result_type = return_ty!("/"
                (Double, Double) => Double,
                (Interval, Double) => Interval,
            );
            Binary(Div, left.into(), right.into(), result_type)
        }

        power => {
            let result_type = return_ty!("^"
                (Double, Double) => Double,
            );
            Binary(Pow, left.into(), right.into(), result_type)
        }

        eq => {
            if left.ty() != right.ty() {
                panic!(
                    "mismatched types for `=`: {:?}, {:?}",
                    left.ty(),
                    right.ty()
                )
            }
            Binary(Eq, left.into(), right.into(), Bool)
        }

        neq => {
            if left.ty() != right.ty() {
                panic!(
                    "mismatched types for `!=`: {:?}, {:?}",
                    left.ty(),
                    right.ty()
                )
            }
            Binary(Neq, left.into(), right.into(), Bool)
        }

        lt => {
            if left.ty() != right.ty() {
                panic!(
                    "mismatched types for `<`: {:?}, {:?}",
                    left.ty(),
                    right.ty()
                )
            }
            Binary(Lt, left.into(), right.into(), Bool)
        }

        le => {
            if left.ty() != right.ty() {
                panic!(
                    "mismatched types for `<=`: {:?}, {:?}",
                    left.ty(),
                    right.ty()
                )
            }
            Binary(Le, left.into(), right.into(), Bool)
        }

        gt => {
            if left.ty() != right.ty() {
                panic!(
                    "mismatched types for `>`: {:?}, {:?}",
                    left.ty(),
                    right.ty()
                )
            }
            Binary(Gt, left.into(), right.into(), Bool)
        }

        ge => {
            if left.ty() != right.ty() {
                panic!(
                    "mismatched types for `>=`: {:?}, {:?}",
                    left.ty(),
                    right.ty()
                )
            }
            Binary(Ge, left.into(), right.into(), Bool)
        }

        and => {
            let result_type = return_ty!("and"
                (Bool, Bool) => Bool,
            );
            Binary(And, left.into(), right.into(), result_type)
        }

        or => {
            let result_type = return_ty!("or"
                (Bool, Bool) => Bool,
            );
            Binary(Or, left.into(), right.into(), result_type)
        }

        _ => unreachable!(),
    }
}

fn parse_timestamptz(val: &str) -> i64 {
    // FIXME pgrx wraps all functions in rust wrappers, which makes them
    //       uncallable with DirectFunctionCall(). Is there a way to
    //       export both?
    unsafe extern "C-unwind" {
        #[allow(improper_ctypes)]
        fn timestamptz_in(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
    }

    let cstr = CString::new(val).unwrap();
    let parsed_time = unsafe {
        pg_sys::DirectFunctionCall3Coll(
            Some(timestamptz_in),
            pg_sys::InvalidOid as _,
            pg_sys::Datum::from(cstr.as_ptr()),
            pg_sys::Datum::from(pg_sys::InvalidOid),
            pg_sys::Datum::from(-1i32),
        )
    };
    parsed_time.value() as _
}

fn parse_interval(val: &str) -> *mut pg_sys::Interval {
    // FIXME pgrx wraps all functions in rust wrappers, which makes them
    //       uncallable with DirectFunctionCall(). Is there a way to
    //       export both?
    unsafe extern "C-unwind" {
        #[allow(improper_ctypes)]
        fn interval_in(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
    }

    let cstr = CString::new(val).unwrap();
    let parsed_interval = unsafe {
        pg_sys::DirectFunctionCall3Coll(
            Some(interval_in),
            pg_sys::InvalidOid as _,
            pg_sys::Datum::from(cstr.as_ptr()),
            pg_sys::Datum::from(pg_sys::InvalidOid),
            pg_sys::Datum::from(-1i32),
        )
    };
    parsed_interval.cast_mut_ptr()
}

// This static determines the precedence of infix operators
static PREC_CLIMBER: once_cell::sync::Lazy<PrecClimber<Rule>> = once_cell::sync::Lazy::new(|| {
    use Assoc::*;

    // operators according to their precedence, ordered in a vector
    // from lowest to highest. Multiple operators with the same precedence are
    // joined with `|`
    PrecClimber::new(vec![
        Operator::new(or, Left),
        Operator::new(and, Left),
        Operator::new(eq, Left)
            | Operator::new(neq, Left)
            | Operator::new(lt, Left)
            | Operator::new(le, Left)
            | Operator::new(gt, Left)
            | Operator::new(ge, Left),
        Operator::new(add, Left) | Operator::new(subtract, Left),
        Operator::new(multiply, Left) | Operator::new(divide, Left),
        Operator::new(power, Right),
    ])
});

// Table of builtin functions (all of them for now).
// Maps function name to a tuple (num arguments, function identifier)
static BUILTIN_FUNCTION: once_cell::sync::Lazy<HashMap<&str, (usize, Function)>> =
    once_cell::sync::Lazy::new(|| {
        use Function::*;
        [
            ("abs", (1, Abs)),
            ("cbrt", (1, Cbrt)),
            ("ceil", (1, Ceil)),
            ("floor", (1, Floor)),
            ("ln", (1, Ln)),
            ("log10", (1, Log10)),
            ("log", (2, Log)),
            ("pi", (0, Pi)),
            ("round", (1, Round)),
            ("sign", (1, Sign)),
            ("sqrt", (1, Sqrt)),
            ("trunc", (1, Trunc)),
            ("acos", (1, Acos)),
            ("asin", (1, Asin)),
            ("atan", (1, Atan)),
            ("atan2", (2, Atan2)),
            ("cos", (1, Cos)),
            ("sin", (1, Sin)),
            ("tan", (1, Tan)),
            ("sinh", (1, Sinh)),
            ("cosh", (1, Cosh)),
            ("tanh", (1, Tanh)),
            ("asinh", (1, Asinh)),
            ("acosh", (1, Acosh)),
            ("atanh", (1, Atanh)),
        ]
        .into_iter()
        .collect()
    });
