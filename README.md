[![CI](https://github.com/timescale/timescaledb-toolkit/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/timescale/timescaledb-toolkit/actions/workflows/ci.yml)
[![Releases](https://img.shields.io/github/v/release/timescale/timescaledb-toolkit?include_prereleases)](https://github.com/timescale/timescaledb-toolkit/releases)

<div align=center>
<picture align=center>
    <source  srcset="https://assets.timescale.com/timescale-web/brand/show/horizontal-black.svg">
    <img alt="Tiger Data logo" >
</picture>
</div>

<div align=center>

<h3>TimescaleDB Toolkit is a PostgreSQL extension that adds analytical hyperfunctions
for time-series workloads</h3>

[![Docs](https://img.shields.io/badge/Read_the_docs-black?style=for-the-badge&logo=readthedocs&logoColor=white)](https://www.tigerdata.com/docs/reference/toolkit)
[![SLACK](https://img.shields.io/badge/Ask_the_community-black?style=for-the-badge&logo=slack&logoColor=white)](https://timescaledb.slack.com/archives/C4GT3N90X)
[![Try TimescaleDB for free](https://img.shields.io/badge/Try_Tiger_Cloud_for_free-black?style=for-the-badge&logo=timescale&logoColor=white)](https://console.cloud.timescale.com/signup)

</div>

It is built in Rust with [pgrx](https://github.com/pgcentralfoundation/pgrx)
and is designed to work with TimescaleDB and plain PostgreSQL data.

Toolkit focuses on the database-engine pieces that are awkward or expensive to
rebuild in every application: approximate aggregates, two-step aggregation,
counter and gauge analysis, time-weighted averages, state tracking, downsampling,
and time-series pipeline utilities. These functions run inside PostgreSQL, so
they can be used in SQL queries, materialized views, continuous aggregates, and
application-facing database APIs.

## What is included

- Approximate analytics: tdigest, uddsketch, HyperLogLog, and Count-Min Sketch.
- Time-series aggregates: counter aggregates, gauge aggregates, time-weighted averages,
  statistical aggregates, heartbeat aggregates, and state aggregates.
- Visualization and reduction helpers: ASAP smoothing and LTTB downsampling.
- Time-vector and pipeline utilities for composing time-series transformations in SQL.
- Upgrade tooling, SQL doctests, and packaging support for PostgreSQL extension releases.

## Quick start

Toolkit is pre-installed on [Tiger Cloud](https://www.tigerdata.com/cloud) and
is included in the [`timescale/timescaledb-ha`](https://hub.docker.com/r/timescale/timescaledb-ha)
Docker image.

On a database where the extension is installed, enable it with:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;
```

Then call Toolkit hyperfunctions from SQL:

```sql
SELECT
    average(stats_agg(value)) AS avg_value,
    approx_percentile(0.95, percentile_agg(value)) AS p95_value
FROM metrics;
```

For the full function reference, see the
[Toolkit documentation](https://www.tigerdata.com/docs/reference/toolkit). For
self-hosted package installation, see the Tiger Data guide to
[install Toolkit on self-hosted TimescaleDB](https://www.tigerdata.com/docs/deploy/self-hosted/tooling/install-toolkit).

## Stability model

Toolkit exposes APIs at different maturity levels:

- Stable APIs are intended to remain compatible across releases.
- Deprecated APIs remain available for now but are expected to be removed in a
  future release.
- Experimental APIs live in the `toolkit_experimental` schema. They may change,
  and extension upgrades may drop database objects that depend on them.

See the [tag notes](docs/README.md#tag-notes) for the precise rules. Treat the
experimental schema like an extension-development namespace, not a long-term
application contract.

## Build from source

### Supported build targets

The project is regularly tested on:

- `x86_64-unknown-linux-gnu` on Ubuntu 24.04 before every merge.
- `aarch64-unknown-linux-gnu` on Ubuntu 24.04 at release time.
- `x86_64-apple-darwin` on macOS engineering workstations.
- `aarch64-apple-darwin` on macOS engineering workstations.

Other platforms may work. Patches are welcome.

### Prerequisites

You need:

- PostgreSQL 15, 16, 17, or 18, including server development headers.
- Rust 1.96.0 with `rustfmt` and `clippy` components. The repository includes a
  `rust-toolchain.toml` file for rustup.
- `clang`, `make`, `gcc`, `pkg-config`, and OpenSSL development libraries.
- `cargo-pgrx` 0.18.1.

On Ubuntu, after configuring the PostgreSQL apt repository, install the common
build dependencies with:

```bash
sudo apt-get install make gcc pkg-config clang postgresql-server-dev-18 libssl-dev
```

Install the matching pgrx CLI:

```bash
cargo install cargo-pgrx --version 0.18.1 --locked --force
```

Initialize pgrx for the PostgreSQL version you want to target:

```bash
cargo pgrx init --pg18 pg_config
```

If you update the Rust compiler, reinstall `cargo-pgrx` with the same compiler.

### Compile and install

Clone the repository and install the extension into the PostgreSQL installation
reported by `pg_config`:

```bash
git clone https://github.com/timescale/timescaledb-toolkit.git
cd timescaledb-toolkit/extension

cargo pgrx install --release
cargo run --manifest-path ../tools/post-install/Cargo.toml -- pg_config
```

To build for a non-default PostgreSQL feature, disable default features and
select the target version:

```bash
cargo pgrx install --release --no-default-features --features pg17
```

After installation, connect with `psql` and create the extension:

```sql
CREATE EXTENSION timescaledb_toolkit;
```

On macOS, `tools/test-mac` can install Toolkit into a pgrx-managed PostgreSQL,
ensure TimescaleDB is available, start the server, and open `psql`:

```bash
tools/test-mac -pg18
```

## Development

The Rust workspace contains the PostgreSQL extension in `extension/`, reusable
aggregate and sketch crates in `crates/`, and release/test utilities in `tools/`.

Common commands:

```bash
# Run Rust crate tests that do not require PostgreSQL.
cargo test --workspace --exclude timescaledb_toolkit

# Run extension tests against a pgrx PostgreSQL target.
cargo pgrx test pg18

# Run clippy with the same shape used by project tooling.
tools/build clippy

# Run SQL documentation tests against a started pgrx server.
tools/build -pg18 test-doc
```

The helper script `tools/build` wraps several CI-style tasks:

```bash
tools/build -pg18 test-crates
tools/build -pg18 test-extension
tools/build -pg18 install
tools/build -pg18 test-doc
```

## Contributing

Issues and pull requests are welcome. Good contributions usually include:

- A clear description of the SQL behavior being added or changed.
- Tests for Rust-level logic and PostgreSQL extension behavior where applicable.
- Documentation updates in `docs/` for user-visible functions.
- Upgrade-safety consideration for stable SQL objects, serialized aggregate
  states, and extension update scripts.

For new functionality, check the
[proposed-feature](https://github.com/timescale/timescaledb-toolkit/labels/proposed-feature)
and [feature-request](https://github.com/timescale/timescaledb-toolkit/labels/feature-request)
labels before opening a new issue.

## Releases and compatibility

Release notes are published on the
[GitHub releases page](https://github.com/timescale/timescaledb-toolkit/releases),
and the project changelog is maintained in [Changelog.md](Changelog.md).

Toolkit is a PostgreSQL extension, so compatibility is affected by PostgreSQL
major version, extension SQL upgrade paths, serialized aggregate state layout,
and TimescaleDB integration points such as continuous aggregates.

## License

Unless otherwise stated, source code and binaries built from this repository are
licensed under the Timescale License. See [LICENSE](LICENSE) and [NOTICE](NOTICE)
for details.

## About Tiger Data

Tiger Data builds TimescaleDB and Tiger Cloud for PostgreSQL workloads that need
high-ingest time-series storage, operational analytics, and real-time querying.
Learn more at [tigerdata.com](https://www.tigerdata.com).
