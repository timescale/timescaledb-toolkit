use rayon::{iter::ParallelIterator, prelude::*};

use std::{borrow::Cow, error::Error, fmt};

use colored::Colorize;

use postgres::{error::DbError, Client, NoTls, SimpleQueryMessage};
use uuid::Uuid;

use crate::{Test, TestFile};

#[derive(Copy, Clone)]
pub struct ConnectionConfig<'s> {
    pub host: Option<&'s str>,
    pub port: Option<&'s str>,
    pub user: Option<&'s str>,
    pub password: Option<&'s str>,
    pub database: Option<&'s str>,
}

impl<'s> ConnectionConfig<'s> {
    fn config_string(&self) -> Cow<'s, str> {
        use std::fmt::Write;

        let ConnectionConfig {
            host,
            port,
            user,
            password,
            database,
        } = self;
        let mut config = String::new();
        if let Some(host) = host {
            let _ = write!(&mut config, "host={} ", host);
        }
        if let Some(port) = port {
            let _ = write!(&mut config, "port={} ", port);
        }
        let _ = match user {
            Some(user) => write!(&mut config, "user={} ", user),
            None => write!(&mut config, "user=postgres "),
        };
        if let Some(password) = password {
            let _ = write!(&mut config, "password={} ", password);
        }
        if let Some(database) = database {
            let _ = write!(&mut config, "dbname={} ", database);
        }
        Cow::Owned(config)
    }
}

pub fn run_tests<OnErr: FnMut(Test, TestError)>(
    connection_config: ConnectionConfig<'_>,
    startup_script: Option<Cow<'_, str>>,
    all_tests: Vec<TestFile>,
    mut on_error: OnErr,
) {
    let startup_script = startup_script.as_deref();
    let root_connection_config = connection_config.config_string();
    let root_connection_config = &*root_connection_config;
    eprintln!("running {} test files", all_tests.len());

    let start_db = |tests_name: &str| {
        let db_name = format!("doctest_db__{}", Uuid::new_v4());
        let finish_name = tests_name.to_string();
        let drop_name = db_name.to_string();
        let deferred = Deferred(move || {
            eprintln!("{} {}", "Finished".bold().green(), finish_name);
            let _ = Client::connect(root_connection_config, NoTls).and_then(|mut client| {
                client.simple_query(&format!(r#"DROP DATABASE IF EXISTS "{}""#, drop_name))
            }).map_err(|e| eprintln!("error dropping DB {}", e));
        });
        {
            eprintln!("{} {}", "Starting".bold().green(), tests_name);
            let mut root_client = Client::connect(root_connection_config, NoTls)
                .expect("could not connect to postgres");
            root_client
                .simple_query(&format!(r#"CREATE DATABASE "{}""#, db_name))
                .expect("could not create test DB");
        }
        (db_name, deferred)
    };

    let (stateless_db, _dropper) = match all_tests.iter().any(|t| t.stateless) {
        false => (None, None),
        true => {
            let (name, dropper) = start_db("stateless tests");
            (Some(name), Some(dropper))
        }
    };

    if let (Some(db), Some(startup_script)) = (stateless_db.as_ref(), startup_script) {
        let stateless_connection_config = ConnectionConfig {
            database: Some(&*db),
            ..connection_config
        };
        let mut client = Client::connect(&stateless_connection_config.config_string(), NoTls)
            .expect("could not connect to test DB");
        let _ = client
            .simple_query(startup_script)
            .expect("could not run init script");
    }

    let stateless_db = stateless_db.as_ref();

    let errors: Vec<_> = all_tests
        .into_par_iter()
        .flat_map_iter(|tests| {
            let (db_name, deferred) = match tests.stateless {
                true => {
                    eprintln!("{} {}", "Running".bold().green(), tests.name);
                    (stateless_db.map(|s| Cow::Borrowed(&**s)), None)
                }
                false => {
                    let (db_name, deferred) = start_db(&*tests.name);
                    (Some(Cow::Owned(db_name)), Some(deferred))
                }
            };

            let test_connection_config = ConnectionConfig {
                database: db_name.as_deref(),
                ..connection_config
            };
            let mut client = Client::connect(&test_connection_config.config_string(), NoTls)
                .expect("could not connect to test DB");

            if let (false, Some(startup_script)) = (tests.stateless, startup_script) {
                let _ = client
                    .simple_query(startup_script)
                    .expect("could not run init script");
            }

            let deferred = deferred;

            tests.tests.into_iter().map(move |test| {
                let output = if test.transactional {
                    run_transactional_test(&mut client, &test)
                } else {
                    run_nontransactional_test(&mut client, &test)
                };
                // ensure that the DB is dropped after the client
                let _deferred = &deferred;
                (test, output)
            })
        })
        .collect();

    drop(_dropper);

    for (test, error) in errors {
        match error {
            Ok(..) => continue,
            Err(error) => on_error(test, error),
        }
    }
}

fn run_transactional_test(client: &mut Client, test: &Test) -> Result<(), TestError> {
    let mut txn = client.transaction()?;
    let output = txn.simple_query(&test.text)?;
    let res = validate_output(output, test);
    txn.rollback()?;
    res
}

fn run_nontransactional_test(client: &mut Client, test: &Test) -> Result<(), TestError> {
    let output = client.simple_query(&test.text)?;
    validate_output(output, test)
}

fn validate_output(output: Vec<SimpleQueryMessage>, test: &Test) -> Result<(), TestError> {
    use SimpleQueryMessage::*;
    if test.ignore_output {
        return Ok(());
    }

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
            CommandComplete(..) => break,
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
        && out.iter().zip(row.iter()).enumerate().all(|(i, (o, r))| {
            clamp_len(o, i, test) == clamp_len(r, i, test)
        })
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
        // TODO this shouldn't be needed, but somtimes is?
        if width.len() < row.len() {
            width.extend((0..row.len()-width.len()).map(|_| 0));
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

struct Deferred<T: FnMut()>(T);

impl<T: FnMut()> Drop for Deferred<T> {
    fn drop(&mut self) {
        self.0()
    }
}
