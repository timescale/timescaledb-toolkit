use std::{
    collections::HashMap,
    ffi::OsStr,
    fs,
    io::{self, Write},
    process::exit,
};

use colored::Colorize;

use clap::{Arg, ArgAction, Command};
use runner::ConnectionConfig;
mod parser;
mod runner;

fn main() {
    let mut matches = Command::new("sql-doctester")
        .about("Runs sql commands from docs/ dir to test out toolkit")
        .arg_required_else_help(true)
        .arg(Arg::new("HOST").short('h').long("host").num_args(1))
        .arg(Arg::new("PORT").short('p').long("port").num_args(1))
        .arg(Arg::new("USER").short('u').long("user").num_args(1))
        .arg(Arg::new("PASSWORD").short('a').long("password").num_args(1))
        .arg(Arg::new("DB").short('d').long("database").num_args(1))
        .arg(Arg::new("INPUT").num_args(1))
        .disable_help_flag(true)
        .arg(Arg::new("help").long("help").action(ArgAction::Help))
        .get_matches();

    let dirname = matches.remove_one::<String>("INPUT").expect("need input");

    let connection_config = ConnectionConfig {
        host: matches.remove_one::<String>("HOST"),
        port: matches.remove_one::<String>("PORT"),
        user: matches.remove_one::<String>("USER"),
        password: matches.remove_one::<String>("PASSWORD"),
        database: matches.remove_one::<String>("DB"),
    };

    let startup_script = include_str!("startup.sql");

    let all_tests = extract_tests(&dirname);

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
        let _ = writeln!(&mut out, "{error}\n");
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
