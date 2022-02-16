
use std::io::{BufRead, Write};

use crate::PushLine;

static ALTERABLE_PROPERTIES: [&str; 6] = [
    "RECEIVE",
    "SEND",
    "TYPMOD_IN",
    "TYPMOD_OUT",
    "ANALYZE",
    "STORAGE",
];

// our update script is a copy of the install script with the following changes
// 1. we drop the experimental schema so everything inside it is dopped.
// 2. drop the event triggers so we can recreate them
// 3. we replace `^CREATE AGGREGATE` with `^CREATE OR REPLACE AGGREGATE`
// 4. we add a guard around `CREATE TYPE` to make it work as if there was
//    `CREATE OR REPLACE TYPE`
// 5. we add a guard around `CREATE OPERATOR` to make it work as if there was
//    `CREATE OR REPLACE OPERATOR`
pub (crate) fn generate_from_install(
    extension_file: impl BufRead,
    mut upgrade_file: impl Write,
) {
    writeln!(
        &mut upgrade_file,
        "DROP SCHEMA toolkit_experimental CASCADE;\n\
        -- drop the EVENT TRIGGERs; there's no CREATE OR REPLACE for those
        DROP EVENT TRIGGER disallow_experimental_deps CASCADE;\n\
        DROP EVENT TRIGGER disallow_experimental_dependencies_on_views CASCADE;"
    ).unwrap();

    let mut lines = extension_file.lines()
        .map(|line| {
            // TODO move to our code gen
            let line = line
                .expect("cannot read install script");
            if line.trim_start().starts_with("CREATE AGGREGATE") {
                return line.replace("CREATE AGGREGATE", "CREATE OR REPLACE AGGREGATE")
            }
            line
        })
        .peekable();

    while lines.peek().is_some() {
        // search for `CREATE TYPE <name> ...` or `CREATE OPERATOR <name> ...`
        let create_line = find_create_line(&mut lines, &mut upgrade_file);
        let mut create_line = match create_line {
            Some(line) => line,
            None => continue,
        };

        if create_line.trim_start().starts_with("CREATE TYPE") {
            let type_name = extract_type_name(&create_line);

            if create_line.trim_end().ends_with(';') {
                // found `CREATE TYPE <name>;`
                write_guarded_create_start(&mut upgrade_file, &create_line);
            } else if create_line.trim_end().ends_with('(') {
                // found
                // ```
                // CREATE TYPE <name> (
                //     ...
                // );
                // ```
                create_line.push('\n');
                let alters = get_alterable_properties(
                    &mut lines,
                    &mut create_line
                );
                write_guarded_create_end(
                    &mut upgrade_file,
                    &type_name,
                    &create_line,
                    &alters
                );
            }
        } else if create_line.trim_start().starts_with("CREATE OPERATOR") {

            // Operators should all follow the form
            // ```
            // CREATE OPERATOR <name> (
            //     ...
            // );
            // ```
            create_line.push('\n');
            for line in &mut lines {
                create_line.push_line(&line);
                if line.trim_start().starts_with(')') {
                    break
                }
            }

            write_guarded_create_op(&mut upgrade_file, &create_line);
        } else {
            panic!("Unhandled CREATE statement");
        }
    }
}

fn find_create_line(
    lines: impl Iterator<Item=String>, mut upgrade_file: impl Write
) -> Option<String> {
    for line in lines {
        // search for `CREATE TYPE <name>;`
        if line.trim_start().starts_with("CREATE TYPE") || line.trim_start().starts_with("CREATE OPERATOR") {
            return Some(line)
        }

        writeln!(upgrade_file, "{}", line).unwrap();
    }
    None
}

fn extract_type_name(line: &str) -> String {
    let mut name: &str = line.split_ascii_whitespace()
        .nth(2)
        .expect("no type name");
    if name.ends_with(';') {
        name = &name[..name.len()-1];
    }
    name.to_string()
}

fn write_guarded_create_start(mut upgrade_file: impl Write, create_start: &str) {
    writeln!(upgrade_file, "\
DO $$
    BEGIN
        {}
    EXCEPTION WHEN duplicate_object THEN
        -- TODO validate that the object belongs to us
        RETURN;
    END
$$;",
        create_start,
    ).expect("cannot write type header");
}

fn write_guarded_create_op(mut upgrade_file: impl Write, create_start: &str) {
    writeln!(upgrade_file, "\
DO $$
    BEGIN
        {}
    EXCEPTION WHEN duplicate_function THEN
        -- TODO validate that the object belongs to us
        RETURN;
    END
$$;",
        create_start,
    ).expect("cannot write type header");
}

fn get_alterable_properties(
    lines: impl Iterator<Item=String>,
    create_stmt: &mut String,
) -> Vec<Option<String>> {
    let mut alters = vec![None; ALTERABLE_PROPERTIES.len()];
    for line in lines {
        create_stmt.push_line(&line);

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
            break
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

fn write_guarded_create_end(
    mut upgrade_file: impl Write,
    type_name: &str,
    create_stmt: &str,
    alters: &[Option<String>],
) {
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
                "ALTER TYPE {name} SET (", name=type_name
            ).expect("cannot write ALTER");
        } else {
            alter_statement.push_str(", ");
        }
        write!(
            &mut alter_statement,
            "{} = {}", ALTERABLE_PROPERTIES[i], value
        ).expect("cannot write ALTER");
    }
    if !alter_statement.is_empty() {
        alter_statement.push_str(");");
    }

    writeln!(&mut upgrade_file, "\
DO $$
    BEGIN
        {create_stmt}
    EXCEPTION WHEN duplicate_object THEN
        {alter_statement}
        RETURN;
    END
$$;",
        create_stmt=create_stmt,
        alter_statement=alter_statement,
    ).expect("cannot write type header");
}
