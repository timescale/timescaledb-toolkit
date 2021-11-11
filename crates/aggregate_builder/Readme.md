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
        state: Option<Inner<State>>,
        #[sql_type("sql_type")] argument: RustType, // can have an arbitrary number of args
    ) -> Option<Inner<State>> {
        // transition function function body goes here
    }

    fn finally(state: Option<Inner<State>>) -> Option<ResultType> {
        // final function function body goes here
    }

    // the remaining items are optional

    // parallel-safety marker if desireable
    const PARALLEL_SAFE: bool = true;

    fn serialize(state: Inner<State>) -> bytea {
        // serialize function body goes here
    }

    fn deserialize(bytes: bytea) -> Inner<State> {
        // deserialize function body goes here
    }

    fn combine(state1: Option<Inner<State>>, state2: Option<Inner<State>>) -> Option<Inner<State>> {
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

Below is a complete example of an `arbitrary()` aggregate that returns one of
the aggregated values.

```rust
#[aggregate] impl arbitrary {
    type State = String;

    fn transition(
        state: Option<Inner<State>>,
        #[sql_type("text")] value: String,
    ) -> Option<Inner<State>> {
        match state {
            Some(value) => Some(value),
            None => Some(value.into()),
        }
    }

    fn finally(mut state: Option<Inner<State>>) -> Option<String> {
        match &mut state {
            None => None,
            Some(state) => Some(state.clone()),
        }
    }
}
```