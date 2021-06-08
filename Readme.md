# Timescale Analytics #

This repository is the home of the Timescale Analytics team. Our mission is to
ease all things analytics when using TimescaleDB, with a particular focus on
developer ergonomics and performance. Our issue tracker contains more
on [the features we're planning to work on](https://github.com/timescale/timescale-analytics/labels/proposed-feature)
and [the problems we're trying to solve](https://github.com/timescale/timescale-analytics/labels/feature-request),
and our [Discussions forum](https://github.com/timescale/timescale-analytics/discussions) contains ongoing conversation.

Documentation for this version of the Timescale Analytics extension can be found
in this repository at [`docs`](https://github.com/timescale/timescale-analytics/tree/main/docs).


## 🖥 Try It Out ##

The extension comes pre-installed on all [Timescale Forge](https://console.forge.timescale.com/) instances, and also on our full-featured [`timescale/timescaledb-ha` docker image](https://hub.docker.com/r/timescale/timescaledb-ha).

We also provide nightly builds as a docker images in `timescaledev/timescale-analytics:nightly`.

All versions of the extension contain experimental features in the `timecale_analytics_experimental`, schema see [our docs section on experimental features](/docs/README.md#tag-notes) for
more details.

## 💿 Installing From Source ##

### 🔧 Tools Setup ###

Building the extension requires valid rust and clang installs, along with the postgres headers for whichever version of postgres you are running, and pgx.
We recommend installing rust using the [official instructions](https://www.rust-lang.org/tools/install):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
and clang and the postgres header in the preferred manner for your system. For Ubuntu this could be
```bash
sudo apt-get clang postgresql-server-dev-13
```
and finally, [fork](https://github.com/JLockerman/pgx/tree/timescale)
of [pgx](https://github.com/zombodb/pgx) can be installed with
```bash
cargo install --git https://github.com/JLockerman/pgx.git --branch timescale cargo-pgx && cargo pgx init
```

### 💾 Building and Installing the extension ###

Download or clone this repository, and switch to the `extension` subdirectory, e.g.
```bash
git clone https://github.com/timescale/timescale-analytics &&
```
Then run
```
cargo pgx install --release && \
cargo run --manifest-path ./tools/post-install/Cargo.toml --  /path/to/your/pg_config
```

## ✏️ Get Involved ##

The Timescale Analytics project is still in the initial planning stage as we
decide our priorities and what to implement first. As such, now is a great time
to help shape the project's direction! Have a look at the
[list of features we're thinking of working on](https://github.com/timescale/timescale-analytics/labels/proposed-feature)
and feel free to comment on the features, expand the list, or
hop on the [Discussions forum](https://github.com/timescale/timescale-analytics/discussions) for more in-depth discussions.

### 🔨 Building ###

Building the extension requires a valid rust install see [the website](https://www.rust-lang.org/tools/install) for instructions.

The extension is built using a [fork](https://github.com/JLockerman/pgx/tree/timescale)
of [pgx](https://github.com/zombodb/pgx). To install pgx use

```bash
cargo install --git https://github.com/JLockerman/pgx.git --branch timescale cargo-pgx && cargo pgx init
```

Once you have `pgx` installed, clone this repo and swich into the extension directory, e.g
```bash
cd timescale_analytics/extension
```
you can run tests against a postgres version
`pg12`, or `pg13` using

```
cargo pgx test ${postgres_version}
```

to install the extension to a postgres install locatable using `pg_config` use

```bash
cargo pgx install --release
```


## 🐯 About TimescaleDB

**[TimescaleDB](https://github.com/timescale/timescaledb)** is a
**distributed time-series database built on PostgreSQL** that scales to
over 10 million of metrics per second, supports native compression,
handles high cardinality, and offers native time-series capabilities,
such as data-retention policies, continuous aggregate views,
downsampling, data gap-filling and interpolation.

TimescaleDB also supports full SQL, a variety of data types (numerics,
text, arrays, JSON, booleans), and ACID semantics. Operationally mature
capabilities include high availability, streaming backups, upgrades over
time, roles and permissions, and security.

TimescaleDB has a **large and active user community** (tens of millions
of downloads, hundreds of thousands of active deployments, Slack channel
with thousands of members).
