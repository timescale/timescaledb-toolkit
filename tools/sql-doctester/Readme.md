## SQL Doctester ##

Test SQL code in Markdown files.

This tool looks through a directory for markdown files containing SQL, runs any
examples it finds, and validates its output. For instance, when run against this
file it will validate that the following SQL runs correctly:

```SQL
SELECT count(v), sum(v), avg(v) FROM generate_series(1, 10) v;
```
```output
 count | sum |                avg
 ------+-----+--------------------
    10 |  55 | 5.5000000000000000
```

If we were to have errors in the example, for instance, we `count` and `sum`
swapped, the tool will report the errors, and where the output differs from the
expected output, like so:

```
Tests Failed

Readme.md:9 `SQL Doctester`

SELECT count(v), sum(v), avg(v) FROM generate_series(1, 10) v;

Error: output has a different values than expected.
Expected
55 | 10 | 5.5000000000000000
(1 rows)

Received
10 | 55 | 5.5000000000000000
(1 rows)

Delta
-55+10 | -10+55 | 5.5000000000000000
```

## Installation ##

```bash
cargo install --git https://github.com/timescale/timescale-analytics.git --branch main sql-doctester
```

## Usage ##

```
sql-doctester

USAGE:
    sql-doctester [OPTIONS] <tests>

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --database <database>
            postgres database the root connection should use. By default this DB will only be used
            to spawn the individual test databases; no tests will run against it.
    -h, --host <hostname>                    postgres host
    -a, --password <password>                postgres password
    -p, --port <portnumber>                  postgres port
    -f, --startup-file <startup_file>
            File containing SQL commands that should be run when each test database is created.

    -s, --startup-script <startup_script>
            SQL command that should be run when each test database is created.

    -u, --user <username>                    postgres user

ARGS:
    <tests>    Path in which to search for tests
```

## Formatting ##

The tool looks through every markdown file in the provided path for SQL code
blocks like

    ```SQL
    SELECT column_1, column_2, etc FROM foo
    ```

and will try to run them. The SQL is assumed to be followed with an `output`
block like which contains the expected output for the command

    ```output
     column 1 | column 2 | etc
    ----------+----------+-----
      value 1 |  value 1 | etc
    ```

Only the actual values are checked; the header, along with leading and trailing
whitespace are ignored. If no `output` is provided the tester will validate that
the output should be empty.

Output validation can be suppressed by adding `ignore-output` after
the `SQL` tag, like so

    ```SQL,ignore-output
    SELECT non_validated FROM foo
    ```
in which case the SQL will be run, and its output ignored.

SQL code blocks can be skipped entirely be adding `ignore` after the tag as in

    ```SQL,ignore
    This never runs, so it doesn't matter if it's valid SQL
    ```

By default, each code block is run in its own transaction, which is rolled back
after the command completes. If you want to run outside a transaction, because
you're running commands that cannot be run within a transaction, or because you
want to change global state, you can mark a block as non-transactional like so

    ```SQL,non-transactional
    CREATE TABLE bar();
    ```

Every file is run in its own database, so such commands can only affect the
remainder of the current file. This tag can be combined with any of the others.

The tool supports adding startup scripts that are run first on every new
database. This can be useful for repetitive initialization tasks, like CREATEing
extensions that must be done for every file. For file-specific initialization,
you can you `non-transactional` blocks. These blocks can be hidden `<div>` like
so

    <div hidden>

    ```SQL,non-transactional
    CREATE TABLE data()
    ```

    </div>

if you want them to be invisible to readers.

## Acknowledgements ##

Inspired by [rustdoc](https://doc.rust-lang.org/rustdoc/what-is-rustdoc.html)
and [rust-skeptic](https://github.com/budziq/rust-skeptic).
