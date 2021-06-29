use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::OsStr,
    fs,
    io::{self, Write},
    process::exit,
};

use clap::clap_app;

use colored::Colorize;

use runner::ConnectionConfig;

mod parser;
mod runner;

fn main() {
    let matches = clap_app!(("sql-doctester") =>
        (@arg HOST: -h --host [hostname] conflicts_with[URL] "postgres host")
        (@arg PORT: -p --port [portnumber] conflicts_with[URL] "postgres port")
        (@arg USER: -u --user [username] conflicts_with[URL] "postgres user")
        (@arg PASSWORD: -a --password [password] conflicts_with[URL] "postgres password")
        (@arg DB: -d --database [database] conflicts_with[URL] "postgres database the root \
            connection should use. By default this DB will only be used \
            to spawn the individual test databases; no tests will run against \
            it.")
        (@arg START_SCRIPT: -s --("startup-script") [startup_script] conflicts_with[START_FILE] "SQL command that should be run when each test database is created.")
        (@arg START_FILE: -f --("startup-file") [startup_file] "File containing SQL commands that should be run when each test database is created.")
        (@arg INPUT: <tests>  "Path in which to search for tests")
    ).get_matches();
    let dirname = matches.value_of("INPUT").expect("need input");

    let connection_config = ConnectionConfig {
        host: matches.value_of("HOST"),
        port: matches.value_of("PORT"),
        user: matches.value_of("USER"),
        password: matches.value_of("PASSWORD"),
        database: matches.value_of("DB"),
    };

    let startup_script = match matches.value_of("START_SCRIPT") {
        Some(script) => Some(Cow::Borrowed(script)),
        None => matches.value_of("START_FILE").map(|file| {
            let contents = fs::read_to_string(file).expect("cannot read script file");
            Cow::Owned(contents)
        }),
    };

    let all_tests = extract_tests(dirname);

    let mut num_errors = 0;
    let stdout = io::stdout();
    let mut out = stdout.lock();

    let on_error = |test: Test, error: runner::TestError| {
        if num_errors == 0 {
            let _ = writeln!(&mut out, "{}\n", "Tests Failed".bold().red());
        }
        num_errors += 1;
        let _ = writeln!(
            &mut out,
            "{} {}\n",
            test.location.bold().blue(),
            test.header.bold().dimmed()
        );
        let _ = writeln!(&mut out, "{}", error.annotate_position(&test.text));
        let _ = writeln!(&mut out, "{}\n", error);
    };

    runner::run_tests(connection_config, startup_script, all_tests, on_error);
    if num_errors > 0 {
        exit(1)
    }
    let _ = writeln!(&mut out, "{}\n", "Tests Passed".bold().green());
}

#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub struct TestFile {
    name: String,
    stateless: bool,
    tests: Vec<Test>,
}

#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub struct Test {
    location: String,
    header: String,
    text: String,
    output: Vec<Vec<String>>,
    transactional: bool,
    ignore_output: bool,
    precision_limits: HashMap<usize, usize>,
}

fn extract_tests(root: &str) -> Vec<TestFile> {
    // TODO handle when root is a file
    let mut all_tests = vec![];
    let walker = walkdir::WalkDir::new(root)
        .follow_links(true)
        .sort_by(|a, b| a.path().cmp(b.path()));
    for entry in walker {
        let entry = entry.unwrap();
        if !entry.file_type().is_file() {
            continue;
        }

        if entry.path().extension() != Some(OsStr::new("md")) {
            continue;
        }

        let realpath;
        let path = if entry.file_type().is_symlink() {
            realpath = fs::read_link(entry.path()).unwrap();
            &*realpath
        } else {
            entry.path()
        };
        let contents = fs::read_to_string(path).unwrap();

        let tests = parser::extract_tests_from_string(&*contents, &*entry.path().to_string_lossy());
        if !tests.tests.is_empty() {
            all_tests.push(tests)
        }
    }
    all_tests
}
