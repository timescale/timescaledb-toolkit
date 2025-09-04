use std::{
    collections::HashSet,
    io::{self, Write},
    path::Path,
    process,
};

use clap::Arg;
use clap::Command;

use colored::Colorize;

use xshell::{read_file, Cmd};

use control_file_reader::get_upgradeable_from;
use postgres_connection_configuration::ConnectionConfig;

// macro for literate path joins
macro_rules! path {
    ($start:ident $(/ $segment: literal)*) => {
        {
            let root: &Path = $start.as_ref();
            root $(.join($segment))*
        }
    };
    ($start:ident / $segment: expr) => {
        {
            let root: &Path = $start.as_ref();
            root.join($segment)
        }
    }
}

mod installer;
mod parser;
mod testrunner;

fn main() {
    let matches = Command::new("update-tester")
        .about("Update tester for toolkit releases")
        .subcommand_required(true)
        .arg_required_else_help(true)
	.subcommand(
            Command::new("full-update-test-source")
            .long_flag("full-update-test-source")
            .about("Run update-test, building toolkit from source unless a local cache is supplied")
            .arg(
                Arg::new("HOST")
                    .short('h')
                    .long("host")
                    .takes_value(true)
            )
            .arg(
                Arg::new("PORT")
                    .short('p')
                    .long("port")
                    .takes_value(true)
            )
            .arg(
                Arg::new("USER")
                    .short('u')
                    .long("user")
                    .takes_value(true)
            )
            .arg(
                Arg::new("PASSWORD")
                    .short('a')
                    .long("password")
                    .takes_value(true)
            )
            .arg(
                Arg::new("DB")
                    .short('d')
                    .long("database")
                    .takes_value(true)
            )
            .arg(Arg::new("CACHE").short('c').long("cache").takes_value(true))
            .arg(Arg::new("REINSTALL").long("reinstall").takes_value(true))
            .arg(Arg::new("PG_CONFIG").takes_value(true))
            .arg(Arg::new("CARGO_PGRX").takes_value(true))
            .arg(Arg::new("CARGO_PGRX_OLD").takes_value(true)),
    )
	.subcommand(
	    Command::new("create-test-objects")
            .long_flag("create-test-objects")
            .about("Creates test objects in a db using the currently installed version of Toolkit")
            .arg(
                Arg::new("HOST")
                    .short('h')
                    .long("host")
                    .takes_value(true)
            )
            .arg(
                Arg::new("PORT")
                    .short('p')
                    .long("port")
                    .takes_value(true)
            )
            .arg(
                Arg::new("USER")
                    .short('u')
                    .long("user")
                    .takes_value(true)
            )
            .arg(
                Arg::new("PASSWORD")
                    .short('a')
                    .long("password")
                    .takes_value(true)
            )
            .arg(
                Arg::new("DB")
                    .short('d')
                    .long("database")
                    .takes_value(true)
            )
	)
	.subcommand(
	    Command::new("validate-test-objects")
            .long_flag("validate-test-objects")
            .about("Runs a series of checks on the objects created by create-test-objects using the currently installed version of Toolkit")
            .arg(
                Arg::new("HOST")
                    .short('h')
                    .long("host")
                    .takes_value(true)
            )
            .arg(
                Arg::new("PORT")
                    .short('p')
                    .long("port")
                    .takes_value(true)
            )
            .arg(
                Arg::new("USER")
                    .short('u')
                    .long("user")
                    .takes_value(true)
            )
            .arg(
                Arg::new("PASSWORD")
                    .short('a')
                    .long("password")
                    .takes_value(true)
            )
            .arg(
                Arg::new("DB")
                    .short('d')
                    .long("database")
                    .takes_value(true)
            )
	)
// Mutates help, removing the short flag (-h) so that it can be used by HOST
	.mut_arg("help", |_h| {
      Arg::new("help")
          .long("help")
  })
	.get_matches();

    match matches.subcommand() {
        Some(("full-update-test-source", full_update_matches)) => {
            let connection_config = ConnectionConfig {
                host: full_update_matches.value_of("HOST"),
                port: full_update_matches.value_of("PORT"),
                user: full_update_matches.value_of("USER"),
                password: full_update_matches.value_of("PASSWORD"),
                database: full_update_matches.value_of("DB"),
            };

            let cache_dir = full_update_matches.value_of("CACHE");

            let root_dir = ".";

            let reinstall = full_update_matches
                .value_of("REINSTALL")
                .map(|r| r.split_terminator(',').collect())
                .unwrap_or_else(HashSet::new);

            let pg_config = full_update_matches
                .value_of("PG_CONFIG")
                .expect("missing pg_config");
            let cargo_pgrx = full_update_matches
                .value_of("CARGO_PGRX")
                .expect("missing cargo_pgrx");
            let cargo_pgrx_old = full_update_matches
                .value_of("CARGO_PGRX_OLD")
                .expect("missing cargo_pgrx_old");

            let mut num_errors = 0;
            let on_error = |test: parser::Test, error: testrunner::TestError| {
                num_errors += 1;
                eprintln!(
                    "{} {}\n",
                    test.location.bold().blue(),
                    test.header.bold().dimmed()
                );
                eprintln!("{}", error.annotate_position(&test.text));
                eprintln!("{error}\n");
            };

            let res = try_main(
                root_dir,
                cache_dir,
                &connection_config,
                pg_config,
                cargo_pgrx,
                cargo_pgrx_old,
                reinstall,
                on_error,
            );
            if let Err(err) = res {
                eprintln!("{err}");
                process::exit(1);
            }
            if num_errors > 0 {
                eprintln!(
                    "{} {}\n",
                    num_errors.to_string().bold().red(),
                    "Tests Failed".bold().red()
                );
                process::exit(1)
            }
            eprintln!("{}\n", "Tests Passed".bold().green());
        }
        Some(("create-test-objects", create_test_object_matches)) => {
            let connection_config = ConnectionConfig {
                host: create_test_object_matches.value_of("HOST"),
                port: create_test_object_matches.value_of("PORT"),
                user: create_test_object_matches.value_of("USER"),
                password: create_test_object_matches.value_of("PASSWORD"),
                database: create_test_object_matches.value_of("DB"),
            };

            let mut num_errors = 0;
            let on_error = |test: parser::Test, error: testrunner::TestError| {
                num_errors += 1;
                eprintln!(
                    "{} {}\n",
                    test.location.bold().blue(),
                    test.header.bold().dimmed()
                );
                eprintln!("{}", error.annotate_position(&test.text));
                eprintln!("{error}\n");
            };
            let res = try_create_objects(&connection_config, on_error);
            if let Err(err) = res {
                eprintln!("{err}");
                process::exit(1);
            }
            if num_errors > 0 {
                eprintln!(
                    "{} {} {}\n",
                    "Object Creation Failed With".bold().red(),
                    num_errors.to_string().bold().red(),
                    "Errors".bold().red()
                );
                process::exit(1)
            }
            eprintln!("{}\n", "Objects Created Successfully".bold().green());
        }
        Some(("validate-test-objects", validate_test_object_matches)) => {
            let connection_config = ConnectionConfig {
                host: validate_test_object_matches.value_of("HOST"),
                port: validate_test_object_matches.value_of("PORT"),
                user: validate_test_object_matches.value_of("USER"),
                password: validate_test_object_matches.value_of("PASSWORD"),
                database: validate_test_object_matches.value_of("DB"),
            };

            let mut num_errors = 0;
            let on_error = |test: parser::Test, error: testrunner::TestError| {
                num_errors += 1;
                eprintln!(
                    "{} {}\n",
                    test.location.bold().blue(),
                    test.header.bold().dimmed()
                );
                eprintln!("{}", error.annotate_position(&test.text));
                eprintln!("{error}\n");
            };

            let root_dir = ".";
            let res = try_validate_objects(&connection_config, root_dir, on_error);
            if let Err(err) = res {
                eprintln!("{err}");
                process::exit(1);
            }
            if num_errors > 0 {
                eprintln!("{num_errors} {}\n", "Tests Failed".bold().red());
                eprintln!("{}\n", "Validation Failed".bold().red());
                process::exit(1)
            }

            eprintln!("{}\n", "Validations Completed Successfully".bold().green());
        }
        _ => unreachable!(), // if all subcommands are defined, anything else is unreachable
    }
}

#[allow(clippy::too_many_arguments)]
fn try_main<OnErr: FnMut(parser::Test, testrunner::TestError)>(
    root_dir: &str,
    cache_dir: Option<&str>,
    db_conn: &ConnectionConfig<'_>,
    pg_config: &str,
    cargo_pgrx: &str,
    cargo_pgrx_old: &str,
    reinstall: HashSet<&str>,
    on_error: OnErr,
) -> xshell::Result<()> {
    let (current_version, old_versions) = get_version_info(root_dir)?;
    if old_versions.is_empty() {
        panic!("no old versions to upgrade from")
    }

    println!("{} [{}]", "Testing".green().bold(), old_versions.join(", "));

    installer::install_all_versions(
        root_dir,
        cache_dir,
        pg_config,
        cargo_pgrx,
        cargo_pgrx_old,
        &current_version,
        &old_versions,
        &reinstall,
    )?;

    testrunner::run_update_tests(db_conn, current_version, old_versions, on_error)
}
fn try_create_objects<OnErr: FnMut(parser::Test, testrunner::TestError)>(
    db_conn: &ConnectionConfig<'_>,
    on_error: OnErr,
) -> xshell::Result<()> {
    testrunner::create_test_objects_for_package_testing(db_conn, on_error)
}

fn try_validate_objects<OnErr: FnMut(parser::Test, testrunner::TestError)>(
    _conn: &ConnectionConfig<'_>,
    root_dir: &str,
    on_error: OnErr,
) -> xshell::Result<()> {
    let (_current_version, old_versions) = get_version_info(root_dir)?;
    if old_versions.is_empty() {
        panic!("no old versions to upgrade from")
    }
    testrunner::update_to_and_validate_new_toolkit_version(_conn, on_error)
}

fn get_version_info(root_dir: &str) -> xshell::Result<(String, Vec<String>)> {
    let extension_dir = path!(root_dir / "extension");
    let control_file = path!(extension_dir / "timescaledb_toolkit.control");
    let manifest_file = path!(extension_dir / "Cargo.toml");

    let manifest_contents = read_file(manifest_file)?;
    let control_contents = read_file(control_file)?;

    let current_version = manifest_contents
        .parse::<toml_edit::Document>()
        .expect("failed to parse extension/Cargo.toml")
        .get("package")
        .expect("failed to find [package] in extension/Cargo.toml")
        .get("version")
        .expect("failed to find package.version in extension/Cargo.toml")
        .as_str()
        .expect("package.version not a string in extension/Cargo.toml")
        .to_owned();

    let upgradable_from = get_upgradeable_from(&control_contents)
        .unwrap_or_else(|e| panic!("{e} in control file {control_contents}"));

    Ok((current_version, upgradable_from))
}

//-------------//
//- Utilities -//
//-------------//

// run a command, only printing the output on failure
fn quietly_run(cmd: Cmd) -> xshell::Result<()> {
    let display = format!("{cmd}");
    let output = cmd.ignore_status().output()?;
    if !output.status.success() {
        io::stdout()
            .write_all(&output.stdout)
            .expect("cannot write to stdout");
        io::stdout()
            .write_all(&output.stderr)
            .expect("cannot write to stdout");
        panic!(
            "{} `{display}` exited with a non-zero error code {}",
            "ERROR".bold().red(),
            output.status
        )
    }
    Ok(())
}

// run a command on `drop()`
fn defer<T>(f: impl FnMut() -> T) -> Deferred<T, impl FnMut() -> T> {
    Deferred(f)
}

struct Deferred<T, F: FnMut() -> T>(F);

impl<F, T> Drop for Deferred<T, F>
where
    F: FnMut() -> T,
{
    fn drop(&mut self) {
        self.0();
    }
}
