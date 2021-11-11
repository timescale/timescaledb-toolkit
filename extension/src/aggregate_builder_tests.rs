// Tests for `aggregate_builder::aggregate`. This can't be in the
// aggregate_builder crate because it requires too much of postgres to actually
// function
use aggregate_builder::aggregate;

use pgx::*;

use crate::{
    palloc::Inner,
    raw::bytea,
};

// just about the simplest aggregate `arbitrary()` returns an arbitrary element
// from the input set. We have three versions
//  1. `anything()` tests that the minimal functionality works.
//  2. `cagg_anything()` tests that the config we use for caggs (serialization
//     but not parallel-safe) outputs the expected config.
//  3. `parallel_anything()` tests that the parallel version outputs the expected
//      config.
#[aggregate] impl toolkit_experimental::anything {
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

    fn finally(state: Option<Inner<State>>) -> Option<String> {
        state.as_deref().cloned()
    }
}

#[aggregate] impl toolkit_experimental::cagg_anything {
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

    fn finally(state: Option<Inner<State>>) -> Option<String> {
        state.as_deref().cloned()
    }

    fn serialize(state: Inner<State>) -> bytea {
        crate::do_serialize!(state)
    }

    fn deserialize(bytes: bytea) -> Inner<State> {
        crate::do_deserialize!(bytes, State)
    }
}

#[aggregate] impl toolkit_experimental::parallel_anything {
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

    fn finally(state: Option<Inner<State>>) -> Option<String> {
        state.as_deref().cloned()
    }

    const PARALLEL_SAFE: bool = true;

    fn serialize(state: Inner<State>) -> bytea {
        crate::do_serialize!(state)
    }

    fn deserialize(bytes: bytea) -> Inner<State> {
        crate::do_deserialize!(bytes, State)
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_anything_in_experimental_and_returns_first() {
        Spi::execute(|client| {
            let output = client.select(
                "SELECT toolkit_experimental.anything(val) \
                FROM (VALUES ('foo'), ('bar'), ('baz')) as v(val)",
                None,
                None,
            ).first()
                .get_one::<String>();
            assert_eq!(output.as_deref(), Some("foo"));
        })
    }

    #[pg_test]
    fn test_anything_has_correct_fn_names_and_def() {
        Spi::execute(|client| {
            let spec = get_aggregate_spec(&client, "anything");
            // output is
            //   fn kind (`a`), volatility, parallel-safety, num args, final fn modify (is this right?)
            //   transition type (`internal`)
            //   output type
            //   transition fn name,
            //   final fn name,
            //   serialize fn name or - if none,
            //   deserialize fn name or - if none,
            assert_eq!(
                spec,
                "(\
                    a,i,u,1,r,\
                    internal,\
                    text,\
                    toolkit_experimental.anything_transition_fn_outer,\
                    toolkit_experimental.anything_finally_fn_outer,\
                    -,\
                    -\
                )"
            );
        });
    }

    #[pg_test]
    fn test_cagg_anything_has_correct_fn_names_and_def() {
        Spi::execute(|client| {
            let spec = get_aggregate_spec(&client, "cagg_anything");
            // output is
            //   fn kind (`a`), volatility, parallel-safety, num args, final fn modify (is this right?)
            //   transition type (`internal`)
            //   output type
            //   transition fn name,
            //   final fn name,
            //   serialize fn name or - if none,
            //   deserialize fn name or - if none,
            assert_eq!(
                spec,
                "(\
                    a,i,u,1,r,\
                    internal,\
                    text,\
                    toolkit_experimental.cagg_anything_transition_fn_outer,\
                    toolkit_experimental.cagg_anything_finally_fn_outer,\
                    toolkit_experimental.cagg_anything_serialize_fn_outer,\
                    toolkit_experimental.cagg_anything_deserialize_fn_outer\
                )"
            );
        });
    }

    #[pg_test]
    fn test_parallel_anything_has_correct_fn_names_and_def() {
        Spi::execute(|client| {
            let spec = get_aggregate_spec(&client, "parallel_anything");
            // output is
            //   fn kind (`a`), volatility, parallel-safety, num args, final fn modify (is this right?)
            //   transition type (`internal`)
            //   output type
            //   transition fn name,
            //   final fn name,
            //   serialize fn name or - if none,
            //   deserialize fn name or - if none,
            assert_eq!(
                spec,
                "(\
                    a,i,s,1,r,\
                    internal,\
                    text,\
                    toolkit_experimental.parallel_anything_transition_fn_outer,\
                    toolkit_experimental.parallel_anything_finally_fn_outer,\
                    toolkit_experimental.parallel_anything_serialize_fn_outer,\
                    toolkit_experimental.parallel_anything_deserialize_fn_outer\
                )"
            );
        });
    }

    // It gets annoying, and segfaulty to handle many arguments from the Spi.
    // For simplicity, we just return a single string representing the tuple
    // and use string-comparison.
    fn get_aggregate_spec(client: &spi::SpiClient, aggregate_name: &str) -> String {
        client.select(
            &format!(r#"SELECT (
                prokind,
                provolatile,
                proparallel,
                pronargs,
                aggfinalmodify,
                aggtranstype::regtype,
                prorettype::regtype,
                aggtransfn,
                aggfinalfn,
                aggserialfn,
                aggdeserialfn)::TEXT
            FROM pg_proc, pg_aggregate
            WHERE proname = '{}'
              AND pg_proc.oid = aggfnoid;"#, aggregate_name),
            None,
            None,
        ).first()
            .get_one::<String>()
            .expect("no aggregate found")
    }
}