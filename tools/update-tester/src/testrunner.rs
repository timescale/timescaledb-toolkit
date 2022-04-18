use colored::Colorize;

use postgres::{Client, NoTls, SimpleQueryMessage};

use postgres_connection_configuration::ConnectionConfig;

use crate::{defer, Deferred};

mod stabilization;

pub fn run_update_tests(
    root_config: &ConnectionConfig,
    current_version: String,
    old_versions: Vec<String>,
) -> Result<(), xshell::Error> {
    for old_version in old_versions {
        eprintln!(
            " {} {} -> {}",
            "Testing".bold().cyan(),
            old_version,
            current_version
        );

        let test_db_name = format!("tsdb_toolkit_test_{}--{}", old_version, current_version);
        let test_config = root_config.with_db(&test_db_name);
        with_temporary_db(&test_db_name, root_config, || {
            let mut test_client = connect_to(&test_config);

            test_client.install_toolkit_at_version(&old_version);
            let installed_version = test_client.get_installed_extension_version();
            assert_eq!(
                installed_version, old_version,
                "installed unexpected version"
            );

            let validation_values = test_client.create_test_objects_for(&old_version);

            test_client.update_to_current_version();
            let new_version = test_client.get_installed_extension_version();
            assert_eq!(
                new_version, current_version,
                "updated to unexpected version"
            );

            test_client.validate_test_objects(validation_values);

            test_client.check_no_references_to_the_old_binary_leaked(&current_version);

            test_client.validate_stable_objects_exist();
        });
        eprintln!(
            "{} {} -> {}",
            "Finished".bold().green(),
            old_version,
            current_version
        );
    }
    Ok(())
}

fn connect_to(config: &ConnectionConfig<'_>) -> TestClient {
    let client = Client::connect(&config.config_string(), NoTls).unwrap_or_else(|e| {
        panic!(
            "could not connect to postgres DB {} due to {}",
            config.database.unwrap_or(""),
            e,
        )
    });
    TestClient(client)
}

//---------------//
//- DB creation -//
//---------------//

fn with_temporary_db<T>(
    db_name: impl AsRef<str>,
    root_config: &ConnectionConfig<'_>,
    f: impl FnOnce() -> T,
) -> T {
    let _db_dropper = create_db(root_config, db_name.as_ref());
    let res = f();
    drop(_db_dropper);
    res
}

// create a database returning an guard that will DROP the db on `drop()`
#[must_use]
fn create_db<'a>(
    root_config: &'a ConnectionConfig<'_>,
    new_db_name: &'a str,
) -> Deferred<(), impl FnMut() + 'a> {
    let mut client = connect_to(root_config).0;
    let create = format!(r#"CREATE DATABASE "{}""#, new_db_name);
    client
        .simple_query(&create)
        .unwrap_or_else(|e| panic!("could not create db {} due to {}", new_db_name, e));

    defer(move || {
        let mut client = connect_to(root_config).0;
        let drop = format!(r#"DROP DATABASE "{}""#, new_db_name);
        client
            .simple_query(&drop)
            .unwrap_or_else(|e| panic!("could not drop db {} due to {}", new_db_name, e));
    })
}

//-------------------//
//- Test Components -//
//-------------------//

struct TestClient(Client);

#[must_use]
type QueryValues = Vec<Vec<Option<String>>>;

impl TestClient {
    fn install_toolkit_at_version(&mut self, old_version: &str) {
        let create = format!(
            r#"CREATE EXTENSION timescaledb_toolkit VERSION "{}""#,
            old_version
        );
        self.simple_query(&create).unwrap_or_else(|e| {
            panic!(
                "could not install extension at version {} due to {}",
                old_version, e,
            )
        });
    }

    #[must_use]
    fn create_test_objects_for(&mut self, _old_version: &str) -> QueryValues {
        let create_data_table = "\
            CREATE TABLE test_data(ts timestamptz, val DOUBLE PRECISION);\
            INSERT INTO test_data \
                SELECT '2020-01-01 00:00:00+00'::timestamptz + i * '1 hour'::interval, \
                100 + i % 100\
            FROM generate_series(0, 10000) i;\
        ";
        self.simple_query(create_data_table)
            .unwrap_or_else(|e| panic!("could create the data table due to {}", e));

        // TODO JOSH - I want to have additional stuff for newer versions,
        //             but it's not ready yet
        let create_test_view = "\
            CREATE VIEW regression_view AS \
                SELECT \
                    counter_agg(ts, val) AS countagg, \
                    hyperloglog(32, val) AS hll, \
                    time_weight('locf', ts, val) AS twa, \
                    uddsketch(100, 0.001, val) as udd, \
                    tdigest(100, val) as tdig, \
                    stats_agg(val) as stats \
                FROM test_data;\
        ";
        self.simple_query(create_test_view)
            .unwrap_or_else(|e| panic!("could create the regression view due to {}", e));

        let query_test_view = "\
            SET TIME ZONE 'UTC'; \
            SELECT \
                num_resets(countagg), \
                distinct_count(hll), \
                average(twa), \
                approx_percentile(0.1, udd), \
                approx_percentile(0.1, tdig), \
                kurtosis(stats) \
            FROM regression_view;\
        ";
        let view_output = self
            .simple_query(query_test_view)
            .unwrap_or_else(|e| panic!("could query the regression view due to {}", e));
        get_values(view_output)
    }

    fn update_to_current_version(&mut self) {
        let update = "ALTER EXTENSION timescaledb_toolkit UPDATE";
        self.simple_query(update)
            .unwrap_or_else(|e| panic!("could not update extension due to {}", e));
    }

    fn validate_test_objects(&mut self, validation_values: QueryValues) {
        let query_test_view = "\
            SET TIME ZONE 'UTC'; \
            SELECT \
                num_resets(countagg), \
                distinct_count(hll), \
                average(twa), \
                approx_percentile(0.1, udd), \
                approx_percentile(0.1, tdig), \
                kurtosis(stats) \
            FROM regression_view;\
        ";
        let view_output = self
            .simple_query(query_test_view)
            .unwrap_or_else(|e| panic!("could query the regression view due to {}", e));
        let new_values = get_values(view_output);
        assert_eq!(
            new_values, validation_values,
            "values returned by the view changed on update",
        );
    }

    fn check_no_references_to_the_old_binary_leaked(&mut self, current_version: &str) {
        let query_get_leaked_objects = format!(
            "SELECT pg_proc.proname \
            FROM pg_catalog.pg_proc \
            WHERE pg_proc.probin LIKE '$libdir/timescaledb_toolkit%' \
              AND pg_proc.probin <> '$libdir/timescaledb_toolkit-{}';",
            current_version,
        );
        let leaks = self
            .simple_query(&query_get_leaked_objects)
            .unwrap_or_else(|e| panic!("could query the leaked objects due to {}", e));
        let leaks = get_values(leaks);
        // flatten the list of returned objects for better output on errors
        // it shouldn't change the result since each row only has a single
        // non-null element anyway.
        let leaks: Vec<String> = leaks.into_iter()
            .flat_map(Vec::into_iter)
            .flatten()
            .collect();
        assert!(
            leaks.is_empty(),
            "objects reference the old binary: {:#?}",
            &*leaks,
        )
    }

    #[must_use]
    fn get_installed_extension_version(&mut self) -> String {
        let get_extension_version = "\
            SELECT extversion \
            FROM pg_extension \
            WHERE extname = 'timescaledb_toolkit'";
        let updated_version = self
            .simple_query(get_extension_version)
            .unwrap_or_else(|e| panic!("could get updated extension version due to {}", e));

        get_values(updated_version)
            .pop() // should have 1 row
            .expect("no timescaledb_toolkit version")
            .pop() // row should have one value
            .expect("no timescaledb_toolkit version")
            .expect("no timescaledb_toolkit version")
    }

    pub(crate) fn validate_stable_objects_exist(&mut self) {
        for function in stabilization::STABLE_FUNCTIONS {
            let check_existence = format!("SELECT '{}'::regprocedure;", function);
            self.simple_query(&check_existence)
                .unwrap_or_else(|e| panic!("error checking function existence: {}", e));
        }

        for ty in stabilization::STABLE_TYPES {
            let check_existence = format!("SELECT '{}'::regtype;", ty);
            self.simple_query(&check_existence)
                .unwrap_or_else(|e| panic!("error checking type existence: {}", e));
        }

        for operator in stabilization::STABLE_OPERATORS {
            let check_existence = format!("SELECT '{}'::regoperator;", operator);
            self.simple_query(&check_existence)
                .unwrap_or_else(|e| panic!("error checking operator existence: {}", e));
        }
    }
}

impl std::ops::Deref for TestClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for TestClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// translate a messages into their contained values
fn get_values(query_results: Vec<SimpleQueryMessage>) -> QueryValues {
    query_results
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::CommandComplete(_) => None,
            SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(row.get(i).map(|s| s.to_string()))
                }
                Some(values)
            }
            _ => unreachable!(),
        })
        .collect()
}
