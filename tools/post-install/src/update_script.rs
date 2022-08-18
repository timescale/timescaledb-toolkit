use std::{
    collections::HashSet,
    io::{BufRead, Write},
    iter::Peekable,
};

use crate::PushLine;

static ALTERABLE_PROPERTIES: [&str; 6] = [
    "RECEIVE",
    "SEND",
    "TYPMOD_IN",
    "TYPMOD_OUT",
    "ANALYZE",
    "STORAGE",
];

#[path = "../../../extension/src/stabilization_info.rs"]
mod stabilization_info;

// our update script is a copy of the install script with the following changes
// 1. we drop the experimental schema so everything inside it is dropped.
// 2. drop the event triggers in case we're coming from a version that had them
// 3. for all CREATEs we check if the object is new in `current_version`
//     a. if it is, we output the CREATE as-is
//     b. if it's not, we output the equivalent REPLACE, if one is needed
pub(crate) fn generate_from_install(
    from_version: &str,
    current_version: &str,
    extension_file: impl BufRead,
    mut upgrade_file: impl Write,
) {
    let new_stabilizations = new_stabilizations(from_version, current_version);
    writeln!(
        &mut upgrade_file,
        "DROP SCHEMA IF EXISTS toolkit_experimental CASCADE;\n\
        -- drop the EVENT TRIGGERs; there's no CREATE OR REPLACE for those
        DROP EVENT TRIGGER IF EXISTS disallow_experimental_deps CASCADE;\n\
        DROP EVENT TRIGGER IF EXISTS disallow_experimental_dependencies_on_views CASCADE;\n\
        DROP FUNCTION IF EXISTS disallow_experimental_dependencies();\n\
        DROP FUNCTION IF EXISTS disallow_experimental_view_dependencies();\n\
        DROP FUNCTION IF EXISTS timescaledb_toolkit_probe;"
    )
    .unwrap();

    let lines = extension_file
        .lines()
        .map(|line| line.expect("cannot read install script"))
        .peekable();

    let mut script_creator = UpdateScriptCreator {
        lines,
        upgrade_file,
        new_stabilizations,
    };

    while script_creator.has_pending_input() {
        let create = script_creator.find_create();
        match create {
            Some(Create::Function(create)) => {
                script_creator.handle_create_functionlike(FunctionLike::Fn, create)
            }
            Some(Create::Aggregate(create)) => {
                script_creator.handle_create_functionlike(FunctionLike::Agg, create)
            }
            Some(Create::Type(create)) => script_creator.handle_create_type(create),
            Some(Create::Schema(create)) => {
                // TODO is there something more principled to do here?
                writeln!(script_creator.upgrade_file, "CREATE SCHEMA {}", create).unwrap();
            }
            Some(Create::Operator(create)) => {
                // TODO we have no stable operators
                // JOSH - operators are slightly different than other objects since
                //        they're not necessarily in the toolkit_experimental when unstable.
                //        Instead, to check if it's stable we need to check if
                //        one of the inputs types, or the function is experimental.
                //        in other words, we need to check if one of
                //          FUNCTION=toolkit_experimental.*
                //          LEFTARG=toolkit_experimental.*
                //          RIGHTARG=toolkit_experimental.*
                //        see `parse_operator_info()` for an attempt at this
                writeln!(script_creator.upgrade_file, "CREATE OPERATOR {}", create).unwrap();
            }
            Some(Create::Cast(create)) => {
                // TODO we don't have a stable one of these yet
                // JOSH - we should probably check if the FUNCTION is experimental also
                if create.contains("toolkit_experimental.") || create.starts_with("(tests.") {
                    writeln!(script_creator.upgrade_file, "CREATE CAST {}", create).unwrap();
                    continue;
                }
                unimplemented!("unprepared for stable CAST: {}", create)
            }
            None => continue,
        }
    }
}

struct UpdateScriptCreator<Lines, Dst>
where
    Lines: Iterator<Item = String>,
    Dst: Write,
{
    lines: Peekable<Lines>,
    upgrade_file: Dst,
    new_stabilizations: StabilizationInfo,
}

enum Create {
    Function(String),
    Aggregate(String),
    Type(String),
    Operator(String),
    Schema(String),
    Cast(String),
}

impl<Lines, Dst> UpdateScriptCreator<Lines, Dst>
where
    Lines: Iterator<Item = String>,
    Dst: Write,
{
    fn has_pending_input(&mut self) -> bool {
        self.lines.peek().is_some()
    }

    // find a `CREATE <OBJECT KIND> <something>` and return the `<something>`
    fn find_create(&mut self) -> Option<Create> {
        for line in &mut self.lines {
            // search for `CREATE FUNCTION/TYPE/OPERATOR <name>;`
            let trimmed = line.trim_start();
            if let Some(created) = trimmed.strip_prefix("CREATE ") {
                let l = created.trim_start();
                let create = match_start(
                    l,
                    [
                        ("FUNCTION", &mut |l| Create::Function(l.to_string())),
                        ("AGGREGATE", &mut |l| Create::Aggregate(l.to_string())),
                        ("TYPE", &mut |l| Create::Type(l.to_string())),
                        ("OPERATOR", &mut |l| Create::Operator(l.to_string())),
                        ("SCHEMA", &mut |l| Create::Schema(l.to_string())),
                        ("CAST", &mut |l| Create::Cast(l.to_string())),
                    ],
                );
                if create.is_some() {
                    return create;
                }
                unreachable!("unexpected CREATE `{}`", trimmed)
            }

            writeln!(self.upgrade_file, "{}", line).unwrap();
        }
        return None;

        // find which of a number of matchers a str starts with, and return the
        // rest. In other words, if find the first matcher matcher such that the
        // str is `<matcher> <remaining>` and return the `<remaining>`
        fn match_start<T, const N: usize>(
            line: &str,
            matchers: [(&str, &mut dyn FnMut(&str) -> T); N],
        ) -> Option<T> {
            for (matcher, constructor) in matchers {
                if let Some(line) = line.strip_prefix(matcher) {
                    let line = line.trim_start();
                    return Some(constructor(line));
                }
            }
            None
        }
    }

    // handle a function-like create: if the function or aggregate is new in this
    // version use `CREATE FUNCTION/AGGREGATE` to create the function, otherwise use
    // `CREATE OR REPLACE` to update it to the newest version
    fn handle_create_functionlike(&mut self, is_function: FunctionLike, mut create: String) {
        if create.starts_with("toolkit_experimental") || create.starts_with("tests") {
            writeln!(self.upgrade_file, "{} {}", is_function.create(), create).unwrap();
            return;
        }

        if !create.contains(')') {
            // look for the end of the argument list
            create.push('\n');
            for line in &mut self.lines {
                create.push_line(&line);
                if line.contains(')') {
                    break;
                }
            }
        }

        self.write_create_functionlike(is_function, &create);
    }

    fn write_create_functionlike(&mut self, is_function: FunctionLike, create_stmt: &str) {
        // parse a function or aggregate
        // it should look something like
        // ```
        // "<function name>"("<arg name>" <arg type>,*) ...
        // ```
        let (name, rem) = parse_ident(create_stmt);
        let types = parse_arg_types(rem);
        let function = Function { name, types };

        // write
        if self.new_stabilizations.new_functions.contains(&function) {
            writeln!(
                self.upgrade_file,
                "{} {}",
                is_function.create(),
                create_stmt
            )
            .expect("cannot write create function")
        } else {
            writeln!(
                self.upgrade_file,
                "{} {}",
                is_function.create_or_replace(),
                create_stmt
            )
            .expect("cannot write create or replace function")
        }
    }

    fn handle_create_type(&mut self, create: String) {
        let type_name = extract_name(&create);

        if type_name.starts_with("toolkit_experimental") || type_name.starts_with("tests") {
            writeln!(self.upgrade_file, "CREATE TYPE {}", create).unwrap();
            return;
        }

        if self.new_stabilizations.new_types.contains(&type_name) {
            writeln!(self.upgrade_file, "CREATE TYPE {}", create).unwrap();
            return;
        }

        if create.trim_end().ends_with(';') {
            // found `CREATE TYPE <name>;` we skip this in update scripts
        } else if create.trim_end().ends_with('(') {
            // found
            // ```
            // CREATE TYPE <name> (
            //     ...
            // );
            // ```
            // alter the type to match the new properties
            let alters = self.get_alterable_properties();
            self.write_alter_type(&type_name, &alters);
        } else {
            unreachable!()
        }
    }

    fn get_alterable_properties(&mut self) -> Vec<Option<String>> {
        let mut alters = vec![None; ALTERABLE_PROPERTIES.len()];
        for line in &mut self.lines {
            let mut split = line.split_ascii_whitespace();
            let first = match split.next() {
                None => continue,
                Some(first) => first,
            };

            // found `)` means we're done with
            // ```
            // CREATE TYPE <name> (
            //     ...
            // );
            // ```
            if first.starts_with(')') {
                break;
            }

            for (i, property) in ALTERABLE_PROPERTIES.iter().enumerate() {
                if first.eq_ignore_ascii_case(property) {
                    assert_eq!(split.next(), Some("="));
                    alters[i] = Some(split.next().expect("no value").to_string());
                }
            }
        }
        // Should return alters here, except PG12 doesn't allow alterations to type properties.
        // Once we no longer support PG12 change this back to returning alters
        vec![]
    }

    fn write_alter_type(&mut self, type_name: &str, alters: &[Option<String>]) {
        let mut alter_statement = String::new();
        for (i, alter) in alters.iter().enumerate() {
            use std::fmt::Write;
            let value = match alter {
                None => continue,
                Some(value) => value,
            };
            if alter_statement.is_empty() {
                write!(
                    &mut alter_statement,
                    "ALTER TYPE {name} SET (",
                    name = type_name
                )
                .expect("cannot write ALTER");
            } else {
                alter_statement.push_str(", ");
            }
            write!(
                &mut alter_statement,
                "{} = {}",
                ALTERABLE_PROPERTIES[i], value
            )
            .expect("cannot write ALTER");
        }
        if !alter_statement.is_empty() {
            alter_statement.push_str(");");
        }

        writeln!(self.upgrade_file, "{}", alter_statement).expect("cannot write ALTER TYPE");
    }
}

enum FunctionLike {
    Fn,
    Agg,
}

impl FunctionLike {
    fn create(&self) -> &'static str {
        match self {
            FunctionLike::Fn => "CREATE FUNCTION",
            FunctionLike::Agg => "CREATE AGGREGATE",
        }
    }

    fn create_or_replace(&self) -> &'static str {
        match self {
            FunctionLike::Fn => "CREATE OR REPLACE FUNCTION",
            FunctionLike::Agg => "CREATE OR REPLACE AGGREGATE",
        }
    }
}

fn parse_arg_types(stmt: &str) -> Vec<Vec<String>> {
    // extract the types from a
    // `( <ident> <type segment>,* )`
    // with arbitrary interior whitespace and comments into a
    // `Vec<Vec<type segment>>`
    let stmt = stmt.trim_start();
    assert!(stmt.starts_with('('), "stmt.starts_with('(') {}", stmt);
    let end = stmt.find(')').expect("cannot find ')' for arg list");
    let args = &stmt[1..end];
    let mut types = vec![];
    // TODO strip out comments
    for arg in args.split_terminator(',') {
        let ty = arg
            .split_whitespace()
            .filter(remove_block_comments()) // skip any block comments
            .skip(1) // skip the identifier at the start
            .take_while(|s| !s.starts_with("--")) // skip any line comments
            .map(|s| s.to_ascii_lowercase())
            .collect();

        types.push(ty)
    }
    return types;

    fn remove_block_comments() -> impl FnMut(&&str) -> bool {
        let mut keep = true;
        move |s| match *s {
            "*/" => {
                let ret = keep;
                keep = true;
                ret
            }
            "/*" => {
                keep = false;
                false
            }
            _ => keep,
        }
    }
}

fn parse_ident(mut stmt: &str) -> (String, &str) {
    // parse `<ident>` or `"<ident>"`
    let quoted = stmt.starts_with('"');
    if quoted {
        stmt = &stmt[1..];
        let end = stmt.find('"').expect("cannot find closing quote");
        let ident = stmt[..end].to_string();
        (ident, &stmt[end + 1..])
    } else {
        let end = stmt
            .find(|c| !(char::is_alphanumeric(c) || c == '_'))
            .expect("cannot find end of ident");
        let ident = stmt[..end].to_string();
        (ident, &stmt[end..])
    }
}

// TODO JOSH - this may not be done yet, but I'm leaving it in to help future devs
// fn parse_operator_info(operator: &str) -> Function {
//     let name = extract_name(operator);
//     let args_start = operator.find('(').unwrap();
//     let args = operator[args_start..].lines();
//     let (mut left, mut right) = (None, None);
//     for arg in args {
//         let arg = arg.trim_start();
//         let (sink, skip) = if arg.starts_with("LEFTARG") {
//             (&mut left, "LEFTARG".len())
//         } else if arg.starts_with("RIGHTARG") {
//             (&mut right, "RIGHTARG".len())
//         } else {
//             continue;
//         };
//         // skip LEFT/RIGHTARG=
//         let arg = &arg[skip + 1..];
//         *sink = match arg.rfind(',') {
//             Some(end) => arg[..end].to_ascii_lowercase().into(),
//             None => arg
//                 .split_ascii_whitespace()
//                 .map(|s| s.to_ascii_lowercase())
//                 .next(),
//         }
//     }
//     Function {
//         name,
//         types: vec![vec![left.unwrap()], vec![right.unwrap()]],
//     }
// }

fn extract_name(line: &str) -> String {
    let mut name: &str = line.split_ascii_whitespace().next().expect("no type name");
    if name.ends_with(';') {
        name = &name[..name.len() - 1];
    }
    name.to_ascii_lowercase()
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct StabilizationInfo {
    pub new_functions: HashSet<Function>,
    pub new_types: HashSet<String>,
    pub new_operators: HashSet<Function>,
}

pub(crate) fn new_stabilizations(from_version: &str, to_version: &str) -> StabilizationInfo {
    StabilizationInfo {
        new_functions: stabilization_info::STABLE_FUNCTIONS(from_version, to_version),
        new_types: stabilization_info::STABLE_TYPES(from_version, to_version),
        new_operators: stabilization_info::STABLE_OPERATORS(from_version, to_version),
    }
}

#[derive(Hash, Clone, PartialEq, Eq, Debug)]
pub(crate) struct Function {
    name: String,
    types: Vec<Vec<String>>,
}

#[derive(Debug)]
pub(crate) struct StaticFunction {
    name: &'static str,
    types: &'static [&'static [&'static str]],
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
struct Version {
    major: u64,
    minor: u64,
    patch: u64,
}

fn version(s: &str) -> Version {
    let mut nums = s.split('.');
    let version = Version {
        major: nums
            .next()
            .unwrap_or_else(|| panic!("no major version in `{}`", s))
            .parse()
            .unwrap_or_else(|e| panic!("error {} for major version in `{}`", e, s)),
        minor: nums
            .next()
            .unwrap_or_else(|| panic!("no minor version in `{}`", s))
            .parse()
            .unwrap_or_else(|e| panic!("error {} for minor version in `{}`", e, s)),
        patch: nums
            .next()
            .unwrap_or("0")
            .trim_end_matches("-dev")
            .parse()
            .unwrap_or_else(|e| panic!("error {} for major version in `{}`", e, s)),
    };
    if nums.next().is_some() {
        panic!("extra `.`s in `{}`", s)
    }
    version
}

fn new_objects<'a, T: std::fmt::Debug>(
    stabilizations: &'a [(&'a str, T)],
    from_version: &'a str,
    to_version: &'a str,
) -> impl Iterator<Item = &'a (&'a str, T)> + 'a {
    let to_version = to_version.trim_end_matches("-dev");
    println!("{}", from_version);
    let from_version = version(from_version);
    let to_version = version(to_version);
    stabilizations
        .iter()
        .skip_while(move |(version_str, _)| {
            let version = version(version_str);
            version > to_version
        })
        .take_while(move |(at, _)| at != &"prehistory" && version(at) > from_version)
}

#[macro_export]
macro_rules! functions_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($fn_name: ident ( $( $($fn_type: ident)+ ),* ) ),* $(,)?
            }
        )*
    ) => {
        #[allow(non_snake_case)]
        pub(crate) fn $export_symbol(from_version: &str, to_version: &str) -> super::HashSet<super::Function> {
            use super::*;
            static STABILIZATIONS: &[(&str, &[StaticFunction])] = &[
                $(
                    (
                        $version,
                        &[
                            $(StaticFunction {
                                name: stringify!($fn_name),
                                types: &[$(
                                    &[$(
                                        stringify!($fn_type),
                                    )*],
                                )*],
                            },)*
                        ],
                    ),
                )*
            ];

            new_objects(STABILIZATIONS, from_version, to_version)
                .flat_map(|(_, creates)| creates.into_iter().map(|StaticFunction { name, types }|
                    Function {
                        name: name.to_ascii_lowercase(),
                        types: types.into_iter().map(|v|
                                v.into_iter().map(|s| s.to_ascii_lowercase()).collect()
                            ).collect(),
                    })
                )
                .collect()
        }
    };
}

#[macro_export]
macro_rules! types_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($type_name: ident),* $(,)?
            }
        )*
    ) => {
        #[allow(non_snake_case)]
        pub(crate) fn $export_symbol(from_version: &str, to_version: &str) -> super::HashSet<String> {
            use super::*;
            static STABILIZATIONS: &[(&str, &[&str])] = &[
                $(
                    (
                        $version,
                        &[
                            $(stringify!($type_name),)*
                        ],
                    ),
                )*
            ];

            new_objects(STABILIZATIONS, from_version, to_version)
                .flat_map(|(_, creates)| creates.into_iter().map(|t| t.to_ascii_lowercase()) )
                .collect()
        }
    };
}

#[macro_export]
macro_rules! operators_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($operator_name: literal ( $( $($fn_type: ident)+ ),* ) ),* $(,)?
            }
        )*
    ) => {
        #[allow(non_snake_case)]
        pub(crate) fn $export_symbol(from_version: &str, to_version: &str) -> super::HashSet<super::Function> {
            use super::*;
            static STABILIZATIONS: &[(&str, &[StaticFunction])] = &[
                $(
                    (
                        $version,
                        &[
                            $(StaticFunction {
                                name: stringify!($fn_name),
                                types: &[$(
                                    &[$(
                                        stringify!($fn_type),
                                    )*],
                                )*],
                            },)*
                        ],
                    ),
                )*
            ];

            new_objects(STABILIZATIONS, from_version, to_version)
                .flat_map(|(_, creates)| creates.into_iter().map(|StaticFunction { name, types }|
                    Function {
                        name: name.to_ascii_lowercase(),
                        types: types.into_iter().map(|v|
                                v.into_iter().map(|s| s.to_ascii_lowercase()).collect()
                            ).collect(),
                    })
                )
                .collect()
        }
    };
}
