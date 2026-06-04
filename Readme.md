[![CI](https://github.com/timescale/timescaledb-toolkit/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/timescale/timescaledb-toolkit/actions/workflows/ci.yml)

# TimescaleDB Toolkit

This repository is the home of the TimescaleDB Toolkit team. Our mission is to
ease all things analytics when using TimescaleDB, with a particular focus on
developer ergonomics and performance. Our issue tracker contains more
on [the features we're planning to work on](https://github.com/timescale/timescaledb-toolkit/labels/proposed-feature)
and [the problems we're trying to solve](https://github.com/timescale/timescaledb-toolkit/labels/feature-request).

Documentation for this version of the TimescaleDB Toolkit extension can be found
in this repository at [`docs`](https://github.com/timescale/timescaledb-toolkit/tree/main/docs).
The release history can be found on this repo's [GitHub releases](https://github.com/timescale/timescaledb-toolkit/releases).

## 🖥 Try It Out

The extension comes pre-installed on all [Tiger Cloud](https://www.tigerdata.com/cloud) instances and also on our full-featured [`timescale/timescaledb-ha` docker image](https://hub.docker.com/r/timescale/timescaledb-ha).

If DEB and RPM packages are a better fit for your situation, refer to the [Install Toolkit on self-hosted TimescaleDB](https://www.tigerdata.com/docs/deploy/self-hosted/tooling/install-toolkit) how-to guide for further instructions on installing the extension via your package manager.

All versions of the extension contain experimental features in the `toolkit_experimental` schema. See [our docs section on experimental features](/docs/README.md#tag-notes) for more details.

## 💿 Installing From Source

### Supported platforms

The engineering team regularly tests the extension on the following platforms:

- x86_64-unknown-linux-gnu (Ubuntu Linux 24.04) (tested prior to every merge)
- aarch64-unknown-linux-gnu (Ubuntu Linux 24.04) (tested at release time)
- x86_64-apple-darwin (MacOS 12) (tested frequently on eng workstation)
- aarch64-apple-darwin (MacOS 12) (tested frequently on eng workstation)

As for other platforms: patches welcome!

### 🔧 Tools Setup

Building the extension requires valid [rust](https://www.rust-lang.org/) (we build and test on 1.89.0), [rustfmt](https://github.com/rust-lang/rustfmt), and clang installs, along with the postgres headers for whichever version of postgres you are running, and pgrx.
We recommend installing rust using the [official instructions](https://www.rust-lang.org/tools/install):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

and build tools, the postgres headers, in the preferred manner for your system. You may also need to install OpenSSl.
For Ubuntu you can follow the [postgres install instructions](https://www.postgresql.org/download/linux/ubuntu/) then run

```bash
sudo apt-get install make gcc pkg-config clang postgresql-server-dev-18 libssl-dev
```

Next you need [cargo-pgrx](https://github.com/tcdi/pgrx), which can be installed with

```bash
cargo install cargo-pgrx --version 0.18.0 --locked --force
```

You must reinstall cargo-pgrx whenever you update your Rust compiler, since cargo-pgrx needs to be built with the same compiler as Toolkit.

Finally, setup the pgrx development environment with

```bash
cargo pgrx init --pg18 pg_config
```

Installing from source is also available on macOS and requires the same set of prerequisites and set up commands listed above.

### 💾 Building and Installing the extension

Download or clone this repository, and switch to the `extension` subdirectory, e.g.

```bash
git clone https://github.com/timescale/timescaledb-toolkit && \
cd timescaledb-toolkit/extension
```

Then run

```
cargo pgrx install --release && \
cargo run --manifest-path ../tools/post-install/Cargo.toml -- pg_config
```

To install the extension in a postgres version different that the default:
```shell
cargo pgrx install --release --no-default-features --features pg17
```

To initialize the extension after installation, enter the following into `psql`:

```
CREATE EXTENSION timescaledb_toolkit;
```

## ✏️ Get Involved

We appreciate your help in shaping the project's direction! Have a look at the
[list of features we're thinking of working on](https://github.com/timescale/timescaledb-toolkit/labels/proposed-feature)
and feel free to comment on the features or expand the list.

### 🔨 Testing

See above for prerequisites and installation instructions.

You can run tests against a postgres version `pg15`, `pg16`, `pg17`, or `pg18` using

```
cargo pgrx test ${postgres_version}
```

## Learn about Tiger Data

Tiger Data is the fastest PostgreSQL for transactional, analytical, and agentic workloads. To learn more about the company and its products, visit [tigerdata.com](https://www.tigerdata.com).
