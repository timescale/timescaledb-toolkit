# Update Tester #

Runs update tests. It'll install every version of the extension marked as
`upgradeable_from` in `timescaledb_toolkit.control` and test that updates to
the current version work correctly. At a high level:

1. For each version in `upgradeable_from`
    1. Checkout the corresponding tag in git,
    2. Build and install the extension at that tag,
    3. Set git back to the original state.
2. Build and install the extension at the original git state.
3. For each version in `upgradeable_from`
    1. create a database,
    2. install the old version of the extension,
    3. install some `timescaledb_toolkit` objects,
    4. update the extension,
    5. validate the extension is in the expected state.

**NOTE:** Running this _will_ move git's `HEAD`. Though git will warn on
          conflicts, and we do our best to reset the tree state before the
          script exits, we recommend only using it on a clean tree.




```
USAGE:
    update-tester [OPTIONS] <dir> <pg_config> <cargo_pgrx> <cargo_pgrx_old>

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --database <database>    postgres database the root connection should use. By
                                 default this DB will only be used to spawn the individual
                                 test databases; no tests will run against it.
    -h, --host <hostname>        postgres host
    -a, --password <password>    postgres password
    -p, --port <portnumber>      postgres port
    -u, --user <username>        postgres user

ARGS:
    <dir>              Path in which to find the timescaledb-toolkit repo
    <pg_config>        Path to pg_config for the DB we are using
    <cargo_pgrx>        Path to cargo-pgrx (must be 0.4 series or newer)
    <cargo_pgrx_old>    Path to cargo-pgrx 0.2-0.3 series
```
