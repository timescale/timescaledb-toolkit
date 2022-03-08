use std::{
    io::{self, Write},
    path::Path,
    process,
};

use clap::clap_app;

use colored::Colorize;

use xshell::{read_file, Cmd};

use control_file_reader::{get_current_version, get_upgradeable_from};
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
mod testrunner;

fn main() {
    let matches = clap_app!(("update-tester") =>
        (@arg HOST: -h --host [hostname] conflicts_with[URL] "postgres host")
        (@arg PORT: -p --port [portnumber] conflicts_with[URL] "postgres port")
        (@arg USER: -u --user [username] conflicts_with[URL] "postgres user")
        (@arg PASSWORD: -a --password [password] conflicts_with[URL] "postgres password")
        (@arg DB: -d --database [database] conflicts_with[URL] "postgres database the root \
            connection should use. By default this DB will only be used \
            to spawn the individual test databases; no tests will run against \
            it.")
        (@arg CACHE: -c --cache [cache_dir] "Directory in which to look-for/store \
            old versions of the Toolkit.")
        (@arg ROOT_DIR: +required <dir> "Path in which to find the timescaledb-toolkit repo")
        (@arg PG_CONFIG: +required <pg_config> "Path to pg_config for the DB we are using")
    )
    .get_matches();

    let connection_config = ConnectionConfig {
        host: matches.value_of("HOST"),
        port: matches.value_of("PORT"),
        user: matches.value_of("USER"),
        password: matches.value_of("PASSWORD"),
        database: matches.value_of("DB"),
    };

    let cache_dir = matches.value_of("CACHE");

    let root_dir = matches
        .value_of("ROOT_DIR")
        .expect("missing path to root of the toolkit repo");

    let pg_config = matches.value_of("PG_CONFIG").expect("missing pg_config");

    if let Err(err) = try_main(root_dir, cache_dir, &connection_config, pg_config) {
        eprintln!("{}", err);
        process::exit(1);
    }
}

fn try_main(
    root_dir: &str,
    cache_dir: Option<&str>,
    db_conn: &ConnectionConfig<'_>,
    pg_config: &str,
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
        &current_version,
        &old_versions
    )?;

    testrunner::run_update_tests(db_conn, current_version, old_versions)
}

fn get_version_info(root_dir: &str) -> xshell::Result<(String, Vec<String>)> {
    let extension_dir = path!(root_dir / "extension");
    let control_file = path!(extension_dir / "timescaledb_toolkit.control");

    let control_contents = read_file(control_file)?;

    let current_version = get_current_version(&control_contents)
        .unwrap_or_else(|e| panic!("{} in control file {}", e, control_contents));

    let upgradable_from = get_upgradeable_from(&control_contents)
        .unwrap_or_else(|e| panic!("{} in control file {}", e, control_contents));

    Ok((current_version, upgradable_from))
}

//-------------//
//- Utilities -//
//-------------//

// run a command, only printing the output on failure
fn quietly_run(cmd: Cmd) -> xshell::Result<()> {
    let display = format!("{}", cmd);
    let output = cmd.ignore_status().output()?;
    if !output.status.success() {
        io::stdout()
            .write_all(&output.stdout)
            .expect("cannot write to stdout");
        io::stdout()
            .write_all(&output.stderr)
            .expect("cannot write to stdout");
        panic!(
            "{} `{}` exited with a non-zero error code {}",
            "ERROR".bold().red(),
            display,
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
