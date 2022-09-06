use std::{
    borrow::Cow,
    io::{self, Write},
    process::exit,
    time::Instant,
};

use clap::{Arg, Command};

use colored::Colorize;

use postgres::{Client, NoTls, SimpleQueryMessage::Row};
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};

fn main() {
    let matches = Command::new("testrunner")
        .about("Testrunner")
        .arg_required_else_help(true)
        .arg(Arg::new("HOST").short('h').long("host").takes_value(true))
        .arg(Arg::new("PORT").short('p').long("port").takes_value(true))
        .arg(Arg::new("USER").short('u').long("user").takes_value(true))
        .arg(
            Arg::new("PASSWORD")
                .short('a')
                .long("password")
                .takes_value(true),
        )
        .arg(Arg::new("DB").short('d').long("database").takes_value(true))
        .mut_arg("help", |_h| Arg::new("help").long("help"))
        .get_matches();
    let connection_config = ConnectionConfig {
        host: matches.value_of("HOST"),
        port: matches.value_of("PORT"),
        user: matches.value_of("USER"),
        password: matches.value_of("PASSWORD"),
        database: matches.value_of("DB"),
    };

    let root_connection_config = connection_config.config_string();
    let root_connection_config = &root_connection_config;

    let db_name = "_ta_temp_testrunner_db";
    let dropper = Deferred(move || {
        let mut dropper = Client::connect(&*root_connection_config, NoTls)
            .expect("cannot connect to drop test DBs");
        dropper
            .simple_query(&format!(r#"DROP DATABASE IF EXISTS "{}""#, db_name))
            .expect("could not drop test DB");
    });

    let mut root_client =
        Client::connect(&*root_connection_config, NoTls).expect("could not connect to postgres");
    root_client
        .simple_query(&format!(r#"CREATE DATABASE "{}""#, db_name))
        .expect("could not create test DB");

    let test_connection_config = ConnectionConfig {
        database: Some(db_name),
        ..connection_config
    };

    let start = Instant::now();

    println!("{}", "Connecting to DB".bold().green());
    let mut client = Client::connect(&test_connection_config.config_string(), NoTls)
        .expect("could not connect to test DB");

    println!("{}", "Creating Extension".bold().green());
    client
        .simple_query("CREATE EXTENSION timescaledb_toolkit;")
        .expect("cannot retrieve test names");

    println!("{}", "Retrieving Tests".bold().green());
    let tests_names = client
        .simple_query(
            "\
        SELECT proname \
        FROM pg_proc, pg_namespace \
        WHERE pronamespace=pg_namespace.oid \
            AND nspname='tests' \
        ORDER BY proname;",
        )
        .expect("cannot retrieve test names");

    type Passed = bool;
    let mut tests: Vec<(&str, Passed)> = tests_names
        .iter()
        .flat_map(|test| match test {
            Row(row) => Some((row.get(0).unwrap(), true)),
            _ => None,
        })
        .collect();

    println!("{} {} tests\n", "Running".bold().green(), tests.len());
    let connector = || {
        Client::connect(&test_connection_config.config_string(), NoTls)
            .expect("could not connect to test DB")
    };
    tests
        .par_iter_mut()
        .for_each_init(connector, |client, (test, passed)| {
            let mut txn = client.transaction().expect("cannot start transaction");
            let res = txn.simple_query(&format!("SELECT tests.{}()", test));
            txn.rollback().expect("cannot rollback transaction");

            match (test.starts_with("should_fail"), res) {
                (false, Err(error)) => {
                    println!("test `{}` failed with\n{}\n", test.blue(), error,);
                    *passed = false;
                }
                (true, Ok(..)) => {
                    println!(
                        "test `{}` completed successfully when it was expected to error\n",
                        test.blue(),
                    );
                    *passed = false;
                }
                _ => {}
            }
        });

    let elapsed = start.elapsed();

    let stdout = io::stdout();
    let mut out = stdout.lock();

    let mut num_errors = 0;

    for (test, passed) in &tests {
        if !passed {
            num_errors += 1;
        }
        writeln!(
            &mut out,
            "test {} ... {}",
            test,
            if *passed {
                "ok".green()
            } else {
                "FAILED".red()
            },
        )
        .expect("cannot output");
    }

    let failed = num_errors > 0;

    writeln!(
        &mut out,
        "\ntest result {}. {} passed; {} failed; finished in {:.2}s",
        if failed { "FAILED".red() } else { "ok".green() },
        tests.len() - num_errors,
        num_errors,
        elapsed.as_secs_f32(),
    )
    .expect("cannot output");

    drop(client);
    drop(dropper);
    if failed {
        exit(1)
    }
}

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

struct Deferred<T: FnMut()>(T);

impl<T: FnMut()> Drop for Deferred<T> {
    fn drop(&mut self) {
        self.0()
    }
}
