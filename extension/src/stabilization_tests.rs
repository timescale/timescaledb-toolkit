#[cfg(any(test, feature = "pg_test"))]
use pgrx::*;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::collections::HashSet;

    use pgrx::*;
    use pgrx_macros::pg_test;

    // Test that any new features are added to the the experimental schema
    #[pg_test]
    fn test_schema_qualification() {
        Spi::connect(|mut client| {
            let stable_functions: HashSet<String> = stable_functions();
            let stable_types: HashSet<String> = stable_types();
            let stable_operators: HashSet<String> = stable_operators();
            let unexpected_features: Vec<_> = client
                .update(
                    "SELECT pg_catalog.pg_describe_object(classid, objid, 0) \
                    FROM pg_catalog.pg_extension e, pg_catalog.pg_depend d \
                    WHERE e.extname='timescaledb_toolkit' \
                    AND refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass \
                    AND d.refobjid = e.oid \
                    AND deptype = 'e'
                    ORDER BY 1",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|row| {
                    let val: String = row
                        .get_datum_by_ordinal(1)
                        .unwrap()
                        .value()
                        .unwrap()
                        .unwrap();

                    if let Some(schema) = val.strip_prefix("schema ") {
                        // the only schemas we should define are
                        // `toolkit_experimental` our experimental schema, and
                        // `tests` which contains our pgrx-style regression tests
                        // (including the function currently running)
                        match schema {
                            "toolkit_experimental" => return None,
                            "tests" => return None,
                            _ => return Some(val),
                        }
                    }

                    if let Some(ty) = val.strip_prefix("type ") {
                        // types in the experimental schema are experimental
                        if ty.starts_with("toolkit_experimental.") {
                            return None;
                        }

                        // PG17 started automatically creating an array type for types, so we need
                        // to take those into account.
                        let ty_no_array = ty.replace("[]", "");

                        if stable_types.contains(ty) || stable_types.contains(&ty_no_array) {
                            return None;
                        }

                        return Some(val);
                    }

                    if let Some(function) = val.strip_prefix("function ") {
                        // functions in the experimental schema are experimental
                        if function.starts_with("toolkit_experimental.") {
                            return None;
                        }

                        // functions in test schema only exist for tests and
                        // won't be in release versions of the extension
                        if function.starts_with("tests.") {
                            return None;
                        }

                        // arrow functions outside the experimental schema are
                        // considered experimental as long as one of their argument
                        // types are experimental (`#[pg_operator]` doesn't allow
                        // us to declare these in a schema and the operator using
                        // them not in the schema). We use name-based resolution
                        // to tell if a function exists to implement an arrow
                        // operator because we didn't have a better method
                        let is_arrow =
                            function.starts_with("arrow_") || function.starts_with("finalize_with");
                        if is_arrow && function.contains("toolkit_experimental.") {
                            return None;
                        }

                        if stable_functions.contains(function) {
                            return None;
                        }

                        // Hack to fix the function macro's inability to handle [] in the type double precision[].
                        if function == "approx_percentile_array(double precision[],uddsketch)"
                            || function == "approx_percentiles(double precision[])"
                        {
                            return None;
                        }

                        return Some(val);
                    }

                    if let Some(operator) = val.strip_prefix("operator ") {
                        // we generally don't put operators in the experimental
                        // schema if we can avoid it because we consider the
                        // `OPERATOR(schema.<op>)` syntax to be to much a
                        // usability hazard. Instead we rely on one of the input
                        // types being experimental and the cascading nature of
                        // drop. This means that we consider an operator
                        // unstable if either of its arguments or the operator
                        // itself are in the experimental schema
                        if operator.contains("toolkit_experimental.") {
                            return None;
                        }

                        if stable_operators.contains(operator) {
                            return None;
                        }

                        return Some(val);
                    }

                    if let Some(cast) = val.strip_prefix("cast ") {
                        // casts cannot be schema-qualified, so we rely on one
                        // of the types involved being experimental and the
                        // cascading nature of drop. This means that we consider
                        // a cast unstable if and only if one of the types
                        // involved are in the experimental schema
                        if cast.contains("toolkit_experimental.") {
                            return None;
                        }

                        return Some(val);
                    }

                    Some(val)
                })
                .collect();

            if unexpected_features.is_empty() {
                return;
            }

            panic!("unexpectedly released features: {:#?}", unexpected_features)
        });
    }

    fn stable_functions() -> HashSet<String> {
        crate::stabilization_info::STABLE_FUNCTIONS()
    }

    fn stable_types() -> HashSet<String> {
        crate::stabilization_info::STABLE_TYPES()
    }

    fn stable_operators() -> HashSet<String> {
        crate::stabilization_info::STABLE_OPERATORS()
    }
}

#[macro_export]
macro_rules! functions_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($fn_name: ident ( $( $($fn_type: ident)+ ),* ) ),* $(,)?
            }
        )*
    ) => {
        #[cfg(any(test, feature = "pg_test"))]
        #[allow(non_snake_case)]
        // we do this instead of just stringifying everything b/c stringify adds
        // whitespace in places we don't want
        pub fn $export_symbol() -> std::collections::HashSet<String> {
            static FUNCTIONS: &[(&str, &[&str])] = &[
                $(
                    $(
                        (
                            stringify!($fn_name),
                            &[
                                $( stringify!($($fn_type)+) ),*
                            ]
                        ),
                    )*
                )*
            ];
            FUNCTIONS.iter().map(|(name, types)| {
                format!("{}({})", name, types.join(","))
            }).collect()
        }

    };
}

#[macro_export]
macro_rules! types_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($type_name: ident),* $(,)?
            }
        )*
    ) => {
        #[cfg(any(test, feature = "pg_test"))]
        #[allow(non_snake_case)]
        // we do this instead of just stringifying everything b/c stringify adds
        // whitespace in places we don't want
        pub fn $export_symbol() -> std::collections::HashSet<String> {
            pub static TYPES: &[&str] = &[
                $(
                    $(stringify!($type_name),)*
                )*
            ];
            TYPES.iter().map(|s| s.to_ascii_lowercase()).collect()
        }
    };
}

#[macro_export]
macro_rules! operators_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($operator_name: literal ( $( $($fn_type: ident)+ ),* ) ),* $(,)?
            }
        )*
    ) => {
        #[cfg(any(test, feature = "pg_test"))]
        #[allow(non_snake_case)]
        pub fn $export_symbol() -> std::collections::HashSet<String> {
            static OPERATORS: &[(&str, &[&str])] = &[
                $(
                    $(
                        (
                            $operator_name,
                            &[
                                $( stringify!($($fn_type)+) ),*
                            ]
                        ),
                    )*
                )*
            ];
            OPERATORS.iter().map(|(name, types)| {
                format!("{}({})", name, types.join(","))
            }).collect()
        }
    };
}
