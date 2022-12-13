use colored::Colorize;
use semver::{BuildMetadata, Prerelease, Version};

use crate::{defer, parser, Deferred};
use postgres::{Client, NoTls, SimpleQueryMessage};
use postgres_connection_configuration::ConnectionConfig;
mod stabilization;

use crate::parser::Test;
use postgres::error::DbError;

use std::{borrow::Cow, error::Error, fmt};
pub fn run_update_tests<OnErr: FnMut(Test, TestError)>(
    root_config: &ConnectionConfig,
    current_toolkit_version: String,
    old_toolkit_versions: Vec<String>,
    mut on_error: OnErr,
) -> Result<(), xshell::Error> {
    for old_toolkit_version in old_toolkit_versions {
        eprintln!(
            " {} {} -> {}",
            "Testing".bold().cyan(),
            old_toolkit_version,
            current_toolkit_version
        );

        let test_db_name = format!(
            "tsdb_toolkit_test_{}--{}",
            old_toolkit_version, current_toolkit_version
        );
        let test_config = root_config.with_db(&test_db_name);
        with_temporary_db(&test_db_name, root_config, || {
            let mut test_client = connect_to(&test_config);

            let errors = test_client
                .create_test_objects_from_files(test_config, old_toolkit_version.clone());

            for (test, error) in errors {
                match error {
                    Ok(..) => continue,
                    Err(error) => on_error(test, error),
                }
            }

            let errors = test_client
                .validate_test_objects_from_files(test_config, old_toolkit_version.clone());

            for (test, error) in errors {
                match error {
                    Ok(..) => continue,
                    Err(error) => on_error(test, error),
                }
            }
        });
        eprintln!(
            "{} {} -> {}",
            "Finished".bold().green(),
            old_toolkit_version,
            current_toolkit_version
        );
    }
    Ok(())
}

pub fn create_test_objects_for_package_testing<OnErr: FnMut(Test, TestError)>(
    root_config: &ConnectionConfig,
    mut on_error: OnErr,
) -> Result<(), xshell::Error> {
    eprintln!(" {}", "Creating test objects".bold().cyan());

    let test_db_name = "tsdb_toolkit_test";
    let test_config = root_config.with_db(test_db_name);

    let mut client = connect_to(root_config).0;

    let drop = format!(r#"DROP DATABASE IF EXISTS "{}""#, test_db_name);
    client
        .simple_query(&drop)
        .unwrap_or_else(|e| panic!("could not drop db {} due to {}", test_db_name, e));
    let create = format!(r#"create database "{}""#, test_db_name);
    client
        .simple_query(&create)
        .unwrap_or_else(|e| panic!("could not create db {} due to {}", test_db_name, e));

    let mut test_client = connect_to(&test_config);

    let create = "CREATE EXTENSION timescaledb_toolkit";
    test_client
        .simple_query(create)
        .unwrap_or_else(|e| panic!("could not install extension due to {}", e,));

    let current_toolkit_version = test_client.get_installed_extension_version();

    // create test objects
    let errors = test_client.create_test_objects_from_files(test_config, current_toolkit_version);

    for (test, error) in errors {
        match error {
            Ok(..) => continue,
            Err(error) => on_error(test, error),
        }
    }
    eprintln!("{}", "Finished Object Creation".bold().green());
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

pub fn update_to_and_validate_new_toolkit_version<OnErr: FnMut(Test, TestError)>(
    root_config: &ConnectionConfig,
    mut on_error: OnErr,
) -> Result<(), xshell::Error> {
    // update extension to new version
    let test_db_name = "tsdb_toolkit_test";
    let test_config = root_config.with_db(test_db_name);

    let mut test_client = connect_to(&test_config);

    // get the currently installed version before updating
    let old_toolkit_version = test_client.get_installed_extension_version();

    test_client.update_to_current_toolkit_version();
    // run validation tests
    let errors = test_client.validate_test_objects_from_files(test_config, old_toolkit_version);

    for (test, error) in errors {
        match error {
            Ok(..) => continue,
            Err(error) => on_error(test, error),
        }
    }

    // This close needs to happen before trying to drop the DB or else panics with `There is 1 other session using the database.`
    test_client
        .0
        .close()
        .unwrap_or_else(|e| panic!("Could not close connection to postgres DB due to {}", e));
    // if the validation passes, drop the db
    let mut client = connect_to(root_config).0;
    eprintln!("{}", "Dropping database.".bold().green());

    let drop = format!(r#"DROP DATABASE IF EXISTS "{}""#, test_db_name);
    client
        .simple_query(&drop)
        .unwrap_or_else(|e| panic!("could not drop db {} due to {}", test_db_name, e));
    Ok(())
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

type QueryValues = Vec<Vec<Option<String>>>;

impl TestClient {
    fn install_toolkit_at_version(&mut self, old_toolkit_version: &str) {
        let create = format!(
            r#"CREATE EXTENSION timescaledb_toolkit VERSION "{}""#,
            old_toolkit_version
        );
        self.simple_query(&create).unwrap_or_else(|e| {
            panic!(
                "could not install extension at version {} due to {}",
                old_toolkit_version, e,
            )
        });
    }

    fn create_test_objects_from_files(
        &mut self,
        root_config: ConnectionConfig<'_>,
        current_toolkit_version: String,
    ) -> Vec<(Test, Result<(), TestError>)> {
        let all_tests = parser::extract_tests("tests/update");
        // Hack to match previous versions of toolkit that don't conform to Semver.
        let current_toolkit_semver = match current_toolkit_version.as_str() {
            "1.4" => Version {
                major: 1,
                minor: 4,
                patch: 0,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            "1.5" => Version {
                major: 1,
                minor: 5,
                patch: 0,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            "1.10.0-dev" => Version {
                major: 1,
                minor: 10,
                patch: 0,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            x => Version::parse(x).unwrap(),
        };

        let errors: Vec<_> = all_tests
            .into_iter()
            .flat_map(|tests| {
                let mut db_creation_client = connect_to(&root_config);

                let test_db_name = format!("{}_{}", tests.name, current_toolkit_version);

                let drop = format!(r#"DROP DATABASE IF EXISTS "{}""#, test_db_name);
                db_creation_client
                    .simple_query(&drop)
                    .unwrap_or_else(|e| panic!("could not drop db {} due to {}", test_db_name, e));
                let create = format!(
                    r#"CREATE DATABASE "{}" LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'"#,
                    test_db_name
                );
                db_creation_client
                    .simple_query(&create)
                    .unwrap_or_else(|e| {
                        panic!("could not create db {} due to {}", test_db_name, e)
                    });

                let test_config = root_config.with_db(&test_db_name);

                let mut test_client = connect_to(&test_config);
                test_client
                    .simple_query("SET TIME ZONE 'UTC';")
                    .unwrap_or_else(|e| panic!("could not set time zone to UTC due to {}", e));

                // install new version and make sure it is correct
                test_client.install_toolkit_at_version(&current_toolkit_version);
                let installed_version = test_client.get_installed_extension_version();
                assert_eq!(
                    installed_version, current_toolkit_version,
                    "installed unexpected version"
                );

                tests
                    .tests
                    .into_iter()
                    .filter(|x| x.creation)
                    .filter(|x| match &x.min_toolkit_version {
                        Some(version) => version <= &current_toolkit_semver,
                        None => true,
                    })
                    .map(move |test| {
                        let output = run_test(&mut test_client, &test);
                        (test, output)
                    })
            })
            .collect();
        errors
    }

    fn update_to_current_toolkit_version(&mut self) {
        let update = "ALTER EXTENSION timescaledb_toolkit UPDATE";
        self.simple_query(update)
            .unwrap_or_else(|e| panic!("could not update extension due to {}", e));
    }

    fn validate_test_objects_from_files(
        &mut self,
        root_config: ConnectionConfig<'_>,
        old_toolkit_version: String,
    ) -> Vec<(Test, Result<(), TestError>)> {
        let all_tests = parser::extract_tests("tests/update");

        let old_toolkit_semver = match old_toolkit_version.as_str() {
            "1.4" => Version {
                major: 1,
                minor: 4,
                patch: 0,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            "1.5" => Version {
                major: 1,
                minor: 5,
                patch: 0,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            "1.10.0-dev" => Version {
                major: 1,
                minor: 10,
                patch: 0,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            x => Version::parse(x).unwrap(),
        };
        let errors: Vec<_> = all_tests
            .into_iter()
            .flat_map(|tests| {
                let test_db_name = format!("{}_{}", tests.name, old_toolkit_version);

                let test_config = root_config.with_db(&test_db_name);

                let mut test_client = connect_to(&test_config);

                test_client.update_to_current_toolkit_version();
                let new_toolkit_version = test_client.get_installed_extension_version();
                test_client.check_no_references_to_the_old_binary_leaked(&new_toolkit_version);

                test_client.validate_stable_objects_exist();

                test_client
                    .simple_query("SET TIME ZONE 'UTC';")
                    .unwrap_or_else(|e| panic!("could not set time zone to UTC due to {}", e));

                tests
                    .tests
                    .into_iter()
                    .filter(|x| x.validation)
                    .filter(|x| match &x.min_toolkit_version {
                        Some(min_version) => min_version <= &old_toolkit_semver,
                        None => true,
                    })
                    .map(move |test| {
                        let output = run_test(&mut test_client, &test);
                        // ensure that the DB is dropped after the client
                        (test, output)
                    })
            })
            .collect();
        errors
    }

    fn check_no_references_to_the_old_binary_leaked(&mut self, current_toolkit_version: &str) {
        let query_get_leaked_objects = format!(
            "SELECT pg_proc.proname \
            FROM pg_catalog.pg_proc \
            WHERE pg_proc.probin LIKE '$libdir/timescaledb_toolkit%' \
              AND pg_proc.probin <> '$libdir/timescaledb_toolkit-{}';",
            current_toolkit_version,
        );
        let leaks = self
            .simple_query(&query_get_leaked_objects)
            .unwrap_or_else(|e| panic!("could query the leaked objects due to {}", e));
        let leaks = get_values(leaks);
        // flatten the list of returned objects for better output on errors
        // it shouldn't change the result since each row only has a single
        // non-null element anyway.
        let leaks: Vec<String> = leaks
            .into_iter()
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

        for operator in stabilization::STABLE_OPERATORS() {
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

// Functions below this line are originally from sql-doctester/src/runner.rs

pub fn validate_output(output: Vec<SimpleQueryMessage>, test: &Test) -> Result<(), TestError> {
    use SimpleQueryMessage::*;

    let mut rows = Vec::with_capacity(test.output.len());
    for r in output {
        match r {
            Row(r) => {
                let mut row: Vec<String> = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    row.push(r.get(i).unwrap_or("").to_string())
                }
                rows.push(row);
            }
            CommandComplete(_) => {}
            _ => unreachable!(),
        }
    }
    let output_error = |header: &str| {
        format!(
            "{}\n{expected}\n{}{}\n\n{received}\n{}{}\n\n{delta}\n{}",
            header,
            stringify_table(&test.output),
            format!("({} rows)", test.output.len()).dimmed(),
            stringify_table(&rows),
            format!("({} rows)", rows.len()).dimmed(),
            stringify_delta(&test.output, &rows),
            expected = "Expected".bold().blue(),
            received = "Received".bold().blue(),
            delta = "Delta".bold().blue(),
        )
    };

    if test.output.len() != rows.len() {
        return Err(TestError::OutputError(output_error(
            "output has a different number of rows than expected.",
        )));
    }

    fn clamp_len<'s>(mut col: &'s str, idx: usize, test: &Test) -> &'s str {
        let max_len = test.precision_limits.get(&idx);
        if let Some(&max_len) = max_len {
            if col.len() > max_len {
                col = &col[..max_len]
            }
        }
        col
    }

    let all_eq = test.output.iter().zip(rows.iter()).all(|(out, row)| {
        out.len() == row.len()
            && out
                .iter()
                .zip(row.iter())
                .enumerate()
                .all(|(i, (o, r))| clamp_len(o, i, test) == clamp_len(r, i, test))
    });
    if !all_eq {
        return Err(TestError::OutputError(output_error(
            "output has a different values than expected.",
        )));
    }
    Ok(())
}
fn stringify_table(table: &[Vec<String>]) -> String {
    use std::{cmp::max, fmt::Write};
    if table.is_empty() {
        return "---".to_string();
    }
    let mut width = vec![0; table[0].len()];
    for row in table {
        // Ensure that we have width for every column
        // TODO this shouldn't be needed, but sometimes is?
        if width.len() < row.len() {
            width.extend((0..row.len() - width.len()).map(|_| 0));
        }
        for (i, value) in row.iter().enumerate() {
            width[i] = max(width[i], value.len())
        }
    }
    let mut output = String::with_capacity(width.iter().sum::<usize>() + width.len() * 3);
    for row in table {
        for (i, value) in row.iter().enumerate() {
            if i != 0 {
                output.push_str(" | ")
            }
            let _ = write!(&mut output, "{:>width$}", value, width = width[i]);
        }
        output.push('\n')
    }

    output
}

#[allow(clippy::needless_range_loop)]
fn stringify_delta(left: &[Vec<String>], right: &[Vec<String>]) -> String {
    use std::{cmp::max, fmt::Write};

    static EMPTY_ROW: Vec<String> = vec![];
    static EMPTY_VAL: String = String::new();

    let mut width = vec![
        0;
        max(
            left.get(0).map(Vec::len).unwrap_or(0),
            right.get(0).map(Vec::len).unwrap_or(0)
        )
    ];
    let num_rows = max(left.len(), right.len());
    for i in 0..num_rows {
        let left = left.get(i).unwrap_or(&EMPTY_ROW);
        let right = right.get(i).unwrap_or(&EMPTY_ROW);
        let cols = max(left.len(), right.len());
        for j in 0..cols {
            let left = left.get(j).unwrap_or(&EMPTY_VAL);
            let right = right.get(j).unwrap_or(&EMPTY_VAL);
            if left == right {
                width[j] = max(width[j], left.len())
            } else {
                width[j] = max(width[j], left.len() + right.len() + 2)
            }
        }
    }
    let mut output = String::with_capacity(width.iter().sum::<usize>() + width.len() * 3);
    for i in 0..num_rows {
        let left = left.get(i).unwrap_or(&EMPTY_ROW);
        let right = right.get(i).unwrap_or(&EMPTY_ROW);
        let cols = max(left.len(), right.len());
        for j in 0..cols {
            let left = left.get(j).unwrap_or(&EMPTY_VAL);
            let right = right.get(j).unwrap_or(&EMPTY_VAL);
            if j != 0 {
                let _ = write!(&mut output, " | ");
            }
            let (value, padding) = if left == right {
                (left.to_string(), width[j] - left.len())
            } else {
                let padding = width[j] - (left.len() + right.len() + 2);
                let value = format!(
                    "{}{}{}{}",
                    "-".magenta(),
                    left.magenta(),
                    "+".yellow(),
                    right.yellow()
                );
                (value, padding)
            };
            // trick to ensure correct padding, the color characters are counted
            // if done the normal way.
            let _ = write!(&mut output, "{:>padding$}{}", "", value, padding = padding);
        }
        let _ = writeln!(&mut output);
    }
    output
}

#[derive(Debug)]
pub enum TestError {
    PgError(postgres::Error),
    OutputError(String),
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestError::PgError(error) => {
                match error.source().and_then(|e| e.downcast_ref::<DbError>()) {
                    Some(e) => {
                        use postgres::error::ErrorPosition::*;
                        let pos = match e.position() {
                            Some(Original(pos)) => format!("At character {}", pos),
                            Some(Internal { position, query }) => {
                                format!("In internal query `{}` at {}", query, position)
                            }
                            None => String::new(),
                        };
                        write!(
                            f,
                            "{}\n{}\n{}\n{}",
                            "Postgres Error:".bold().red(),
                            e,
                            e.detail().unwrap_or(""),
                            pos,
                        )
                    }
                    None => write!(f, "{}", error),
                }
            }
            TestError::OutputError(err) => write!(f, "{} {}", "Error:".bold().red(), err),
        }
    }
}

impl From<postgres::Error> for TestError {
    fn from(error: postgres::Error) -> Self {
        TestError::PgError(error)
    }
}

impl TestError {
    pub fn annotate_position<'s>(&self, sql: &'s str) -> Cow<'s, str> {
        match self.location() {
            None => sql.into(),
            Some(pos) => format!(
                "{}{}{}",
                &sql[..pos as usize],
                "~>".bright_red(),
                &sql[pos as usize..],
            )
            .into(),
        }
    }

    fn location(&self) -> Option<u32> {
        use postgres::error::ErrorPosition::*;
        match self {
            TestError::OutputError(..) => None,
            TestError::PgError(e) => match e
                .source()
                .and_then(|e| e.downcast_ref::<DbError>().and_then(DbError::position))
            {
                None => None,
                Some(Internal { .. }) => None,
                Some(Original(pos)) => Some(pos.saturating_sub(1)),
            },
        }
    }
}

fn run_test(client: &mut Client, test: &Test) -> Result<(), TestError> {
    let output = client.simple_query(&test.text)?;
    validate_output(output, test)
}
