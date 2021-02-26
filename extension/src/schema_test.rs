
#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use std::collections::HashSet;

    use pgx::*;

    #[pg_extern(schema="timescale_analytics_experimental")]
    fn expected_failure() -> i32 { 1 }

    #[pg_test(error = "features in timescale_analytics_experimental are unstable, and objects depending on them will be deleted on extension update (there will be a DROP SCHEMA timescale_analytics_experimental CASCADE), which on Forge can happen at any time.")]
    fn test_blocks_view() {
        Spi::execute(|client| {
            let _ = client.select(
                "CREATE VIEW failed AS SELECT timescale_analytics_experimental.expected_failure();",
               None,
                None);
        })
    }


    // Test that any new features are added to the the experimental schema
    #[pg_test]
    fn test_schema_qualification() {
        Spi::execute(|client| {
            let released_features: HashSet<_> = RELEASED_FEATURES.iter().cloned().collect();
            let unexpected_features: Vec<_> = client
                .select(
                    "SELECT pg_catalog.pg_describe_object(classid, objid, 0) \
                    FROM pg_catalog.pg_extension e, pg_catalog.pg_depend d \
                    WHERE e.extname='timescale_analytics' \
                    AND refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass \
                    AND d.refobjid = e.oid \
                    AND deptype = 'e'
                    ORDER BY 1",
                    None,
                    None,
                ).filter_map(|row| {
                    let val: String = row.by_ordinal(1).unwrap().value().unwrap();

                    if released_features.contains(&*val) {
                        return None
                    }

                    if val.starts_with("schema")
                        && val.strip_prefix("schema ") == Some("timescale_analytics_experimental") {
                        return None
                    }

                    if val.starts_with("schema")
                        && val.strip_prefix("schema ") == Some("tests") {
                        return None
                    }

                    let type_prefix = "type timescale_analytics_experimental.";
                    if val.starts_with(type_prefix)
                        && val.strip_prefix(type_prefix).is_some() {
                            return None
                    }

                    let function_prefix = "function timescale_analytics_experimental.";
                    if val.starts_with(function_prefix)
                        && val.strip_prefix(function_prefix).is_some() {
                            return None
                    }

                    // ignore the pgx test schema
                    let test_prefix = "function tests.";
                    if val.starts_with(test_prefix)
                        && val.strip_prefix(test_prefix).is_some() {
                            return None
                    }

                    return Some(val)
                }).collect();

            if unexpected_features.is_empty() {
                return
            }

            panic!("unexpectedly released features: {:#?}", unexpected_features)
        });
    }

    // list of features that are released and can be in places other than the
    // experimental schema
    // TODO it may pay to auto-discover this list based on the previous version of
    //      the extension, once we have a released extension
    static RELEASED_FEATURES: &[&'static str] = &[
        "event trigger disallow_experimental_deps",
        "event trigger disallow_experimental_dependencies_on_views",
        "function disallow_experimental_dependencies()",
        "function disallow_experimental_view_dependencies()",
        "function timescale_analytics_probe()",
    ];
}