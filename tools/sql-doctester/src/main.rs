use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::OsStr,
    fs,
    io::{self, Write},
    process::exit,
};

use colored::Colorize;

use clap::{Arg, Command};
use runner::ConnectionConfig;
mod parser;
mod runner;

fn main() {
    let matches = Command::new("sql-doctester")
        .about("Runs sql commands from docs/ dir to test out toolkit")
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
        .arg(
            Arg::new("START_SCRIPT")
                .short('s')
                .long("startup-script")
                .takes_value(true)
                .conflicts_with("START_FILE"),
        )
        .arg(
            Arg::new("START_FILE")
                .short('f')
                .long("startup-file")
                .takes_value(true)
                .conflicts_with("START_SCRIPT"),
        )
        .arg(Arg::new("INPUT").takes_value(true))
        .mut_arg("help", |_h| Arg::new("help").long("help"))
        .get_matches();

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

        let tests = parser::extract_tests_from_string(&contents, &entry.path().to_string_lossy());
        if !tests.tests.is_empty() {
            all_tests.push(tests)
        }
    }
    all_tests
}
