# Aggregate Builder #

Library for building Postgres [aggregate functions](https://www.postgresql.org/docs/current/xaggr.html)
that imitates
[`CREATE AGGREGATE`](https://www.postgresql.org/docs/current/sql-createaggregate.html).

## Syntax ##

Current syntax looks something like like

```rust
#[aggregate] impl aggregate_name {
    type State = InternalTransitionType;

    fn transition(
        state: Option<State>,
        #[sql_type("sql_type")] argument: RustType, // can have an arbitrary number of args
    ) -> Option<State> {
        // transition function function body goes here
    }

    fn finally(state: Option<&mut State>) -> Option<ResultType> {
        // final function function body goes here
    }

    // the remaining items are optional

    // parallel-safety marker if desirable
    const PARALLEL_SAFE: bool = true;

    fn serialize(state: &State) -> bytea {
        // serialize function body goes here
    }

    fn deserialize(bytes: bytea) -> State {
        // deserialize function body goes here
    }

    fn combine(state1: Option<&State>, state2: Option<&State>) -> Option<State> {
        // combine function body goes here
    }
}
```

All items except for `type State`, `fn transition()`, and `fn finally()` are
optional. The SQL for the aggregate and its functions will be created
automatically, and any necessary memory context switching is handled
automatically for most cases¹.

¹It will switch to the aggregate memory context before calling the transition
function body and the combine function body. Looking through `array_agg()`'s
code this seems to be the correct places to do so. Note that if you want to
allocate in the aggregate memory context in the final function other work may
be needed.

## Example ##

Below is a complete example of an `anything()` aggregate that returns one of
the aggregated values.

```rust
#[aggregate] impl anything {
    type State = String;

    fn transition(
        state: Option<State>,
        #[sql_type("text")] value: String,
    ) -> Option<State> {
        state.or(Some(value))
    }

    fn finally(state: Option<&State>) -> Option<String> {
        state.as_deref().cloned()
    }
}
```

## Expansion ##

Ignoring some supplementary type checking we add to improve error messages, the
macro expands aggregate definitions to rust code something like the following
(explanations as comments in-line)
```rust
// we nest things within a module to mimic the namespacing of an `impl` block
pub mod aggregate_name {
    // glob import to further act like an `impl`
    use super::*;

    pub type State = String;

    // PARALLEL_SAFE constant in case someone wants to use it
    // unlikely to be actually used in practice
    #[allow(dead_code)]
    pub const PARALLEL_SAFE: bool = true;

    #[pgx::pg_extern(immutable, parallel_safe)]
    pub fn aggregate_name_transition_fn_outer(
        __inner: pgx::Internal,
        value: RustType,
        __fcinfo: pg_sys::FunctionCallInfo,
    ) -> Option<Internal> {
        use crate::palloc::{Inner, InternalAsValue, ToInternal};
        unsafe {
            // Translate from the SQL type to the rust one
            // we actually store an `Option<State>` rather than a `State`.
            let mut __inner: Option<Inner<Option<State>>> = __inner.to_inner();
            // We steal the state out from under the pointer leaving `None` in
            // its place. This means that if the inner transition function
            // panics the inner transition function will free `State` while the
            // teardown hook in the aggregate memory context will only free inner
            let inner: Option<State> = match &mut __inner {
                None => None,
                Some(inner) => Option::take(&mut **inner),
            };
            let state: Option<State> = inner;
            // Switch to the aggregate memory context. This ensures that the
            // transition state lives for as long as the aggregate, and that if
            // we allocate from Postgres within the inner transition function
            // those too will stay around.
            crate::aggregate_utils::in_aggregate_context(__fcinfo, || {
                // call the inner transition function
                let result = transition(state, value);

                // return the state to postgres, if we have a pointer just store
                // in that, if not allocate one only if needed.
                let state: Option<State> = result;
                __inner = match (__inner, state) {
                    (None, None) => None,
                    (None, state @ Some(..)) => Some(state.into()),
                    (Some(mut inner), state) => {
                        *inner = state;
                        Some(inner)
                    }
                };
                __inner.internal()
            })
        }
    }
    pub fn transition(state: Option<State>, value: String) -> Option<State> {
        // elided
    }

    #[pgx::pg_extern(immutable, parallel_safe)]
    pub fn aggregate_name_finally_fn_outer(
        __internal: pgx::Internal,
        __fcinfo: pg_sys::FunctionCallInfo,
    ) -> Option<String> {
        use crate::palloc::InternalAsValue;
        unsafe {
            // Convert to the rust transition type, see the comment in the
            // transition function for why we store an `Option<State>`
            let mut input: Option<Inner<Option<State>>> = __internal.to_inner();
            let input: Option<&mut State> = input.as_deref_mut()
                .map(|i| i.as_mut())
                .flatten();
            // We pass in an `Option<&mut State>`; `Option<>` because the
            // transition state might not have been initialized yet;
            // `&mut State` since while the final function has unique access to
            // the transition function it must leave it a valid state when it's
            // finished
            let state: Option<&mut State> = input;
            finally(state)
        }
    }
    pub fn finally(state: Option<&mut State>) -> Option<String> {
        // elided
    }

    #[pgx::pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
    pub fn aggregate_name_serialize_fn_outer(__internal: pgx::Internal) -> bytea {
        use crate::palloc::{Inner, InternalAsValue};
        // Convert to the rust transition type, see the comment in the
        // transition function for why we store an `Option<State>`
        let input: Option<Inner<Option<State>>> = unsafe { __internal.to_inner() };
        let mut input: Inner<Option<State>> = input.unwrap();
        // We pass by-reference for the same reason as the final function.
        // Note that _technically_ you should not mutate in the serialize,
        // function though there are cases you can get away with it when using
        // an `internal` transition type.
        let input: &mut State = input.as_mut().unwrap();
        let state: &State = input;
        serialize(state)
    }
    pub fn serialize(state: &State) -> bytea {
        // elided
    }

    #[pgx::pg_extern(strict, immutable, parallel_safe, schema = "toolkit_experimental")]
    pub fn aggregate_name_deserialize_fn_outer(
        bytes: crate::raw::bytea,
        _internal: Internal,
    ) -> Option<Internal> {
        use crate::palloc::ToInternal;
        let result = deserialize(bytes);
        let state: State = result;
        // Convert to the rust transition type, see the comment in the
        // transition function for why we store an `Option<State>`.
        // We deliberately don't switch to the aggregate transition context
        // because the postgres aggregates do not do so.
        let state: Inner<Option<State>> = Some(state).into();
        unsafe { Some(state).internal() }
    }
    pub fn deserialize(bytes: crate::raw::bytea) -> State {
        // elided
    }

    #[pgx::pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
    pub fn aggregate_name_combine_fn_outer(
        a: Internal,
        b: Internal,
        __fcinfo: pg_sys::FunctionCallInfo,
    ) -> Option<Internal> {
        use crate::palloc::{Inner, InternalAsValue, ToInternal};
        unsafe {
            // Switch to the aggregate memory context. This ensures that the
            // transition state lives for as long as the aggregate, and that if
            // we allocate from Postgres within the inner transition function
            // those too will stay around.
            crate::aggregate_utils::in_aggregate_context(__fcinfo, || {
                let result = combine(a.to_inner().as_deref(), b.to_inner().as_deref());
                let state: Option<State> = result;
                let state = match state {
                    None => None,
                    state @ Some(..) => {
                        let state: Inner<Option<State>> = state.into();
                        Some(state)
                    }
                };
                state.internal()
            })
        }
    }
    pub fn combine(a: Option<&State>, b: Option<&State>) -> Option<State> {
        // elided
    }

    // SQL generated for the aggregate
    pgx::extension_sql!("\n\
        CREATE AGGREGATE toolkit_experimental.aggregate_name (value RustType) (\n\
            stype = internal,\n\
            sfunc = toolkit_experimental.aggregate_name_transition_fn_outer,\n\
            finalfunc = toolkit_experimental.aggregate_name_finally_fn_outer,\n\
            parallel = safe,\n
            serialfunc = toolkit_experimental.aggregate_name_serialize_fn_outer,\n\
            deserialfunc = toolkit_experimental.aggregate_name_deserialize_fn_outer,\n\
            combinefunc = toolkit_experimental.aggregate_name_combine_fn_outer\n\
        );\n",
        name = "aggregate_name_extension_sql",
        requires = [
            aggregate_name_transition_fn_outer,
            aggregate_name_finally_fn_outer,
            aggregate_name_serialize_fn_outer,
            aggregate_name_deserialize_fn_outer,
            aggregate_name_combine_fn_outer,
        ],
    );
}
```