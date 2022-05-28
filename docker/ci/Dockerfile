FROM rust:1.60

RUN apt-get update
RUN apt-get install -y clang libclang1 cmake

RUN useradd -ms /bin/bash postgres

USER postgres

# install cargo pgx
# Keep synchronized with `cargo install --version N.N.N cargo-pgx` in Readme.md and Cargo.toml
RUN cargo install cargo-pgx --version '=0.2.4' --root /home/postgres/pgx/0.2
RUN cargo install cargo-pgx --version '=0.4.5' --root /home/postgres/pgx/0.4

ENV PATH "/home/postgres/pgx/0.4/bin:${PATH}"

RUN set -ex \
    && cargo pgx init --pg12 download --pg13 download --pg14 download \
    && cargo pgx start pg12 \
    && cargo pgx stop  pg12 \
    && cargo pgx start pg13 \
    && cargo pgx stop  pg13 \
    && cargo pgx start pg14 \
    && cargo pgx stop  pg14

# install timescaledb
# TODO make seperate image from ^
# TODO move complex script to stand-alone shell script we can ADD and then RUN
#  ADD tools/install-timescaledb /
#  RUN /install-timescaledb
#  RUN rm /install-timescaledb
RUN jobs="-j$(nproc)" \
    && cd \
    && git clone -q -b 2.5.x --depth 1 https://github.com/timescale/timescaledb.git \
    && cd timescaledb \
        && ./bootstrap -DPG_CONFIG=~/.pgx/12.11/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make $jobs install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-12/postgresql.conf \
    && cd .. \
    && rm -rf ./build \
        && ./bootstrap -DPG_CONFIG=~/.pgx/13.7/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make $jobs install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-13/postgresql.conf \
    && cd .. \
    && rm -rf ./build \
        && ./bootstrap -DPG_CONFIG=~/.pgx/14.3/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make $jobs install
RUN rm -rf ~/timescaledb

RUN echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-14/postgresql.conf

# add clippy
RUN rustup component add clippy

USER root
