FROM rust:1.60 AS pgx_builder

RUN apt-get update \
    && apt-get install -y clang libclang1 sudo bash cmake

# We install PostgreSQL from PGDG rather than building it locally.
RUN apt-get -y install postgresql-common gnupg
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get -y install libclang-dev postgresql-12 postgresql-server-dev-12 postgresql-13 postgresql-server-dev-13 postgresql-14 postgresql-server-dev-14
RUN rm -rf /var/lib/apt/lists/*

# add clippy and rustfmt
RUN rustup component add clippy rustfmt

# install doctester
RUN cargo install --git https://github.com/timescale/timescaledb-toolkit.git --branch main sql-doctester

# Build the TimescaleDB extension for all the servers that we
# installed above.

RUN git clone --single-branch --branch 2.5.x https://github.com/timescale/timescaledb.git

WORKDIR timescaledb

RUN cmake -S . -B build-12 -DPG_CONFIG=/usr/lib/postgresql/12/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false
RUN cmake -S . -B build-13 -DPG_CONFIG=/usr/lib/postgresql/13/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false
RUN cmake -S . -B build-14 -DPG_CONFIG=/usr/lib/postgresql/14/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false

RUN cmake --build build-12 --parallel
RUN cmake --build build-13 --parallel
RUN cmake --build build-14 --parallel

RUN cmake --install build-12
RUN cmake --install build-13
RUN cmake --install build-14

RUN rm -rf build-12 build-13 build-14

WORKDIR pgx

# Install cargo pgx
# Keep synchronized with `cargo install --version N.N.N cargo-pgx` in Readme.md and Cargo.toml
RUN cargo install cargo-pgx --version '=0.2.4' --root /pgx/0.2
RUN cargo install cargo-pgx --version '=0.4.5' --root /pgx/0.4

ENV PATH "/pgx/0.4/bin:${PATH}"

USER postgres

# Initialize new PostgreSQL instances and update the configuration
# files so that they can use TimescaleDB that we installed above.
RUN cargo pgx init --pg12 /usr/lib/postgresql/12/bin/pg_config --pg13 /usr/lib/postgresql/13/bin/pg_config --pg14 /usr/lib/postgresql/14/bin/pg_config
RUN cargo pgx start pg12 && cargo pgx stop pg12
RUN cargo pgx start pg13 && cargo pgx stop pg13
RUN cargo pgx start pg14 && cargo pgx stop pg14

RUN echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-12/postgresql.conf
RUN echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-13/postgresql.conf
RUN echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-14/postgresql.conf

FROM pgx_builder AS rust-pgx

USER root
