FROM rust:1.60

RUN echo SOMETHING HAPPENED
RUN id
RUN uname -a

RUN apt-get update
RUN apt-get install -y clang libclang1

RUN useradd -ms /bin/bash postgres

USER postgres

# install cargo pgx
# Keep synchronized with `cargo install --version N.N.N cargo-pgx` in Readme.md and Cargo.toml
RUN cargo install cargo-pgx --version '=0.2.4' --root /home/postgres/pgx/0.2
RUN cargo install cargo-pgx --version '=0.4.5' --root /home/postgres/pgx/0.4

# fail on purpose
RUN false

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
RUN set -ex \
    && cd ~ \
    && git clone https://github.com/timescale/timescaledb.git \
    && cd timescaledb \
    && git checkout 2.5.x \
    && cd ~/timescaledb \
        && ./bootstrap -DPG_CONFIG=~/.pgx/12.11/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make -j4 \
        && make -j4 install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-12/postgresql.conf \
    && cd .. \
    && rm -rf ./build \
        && ./bootstrap -DPG_CONFIG=~/.pgx/13.7/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make -j4 \
        && make -j4 install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-13/postgresql.conf \
    && cd .. \
    && rm -rf ./build \
        && ./bootstrap -DPG_CONFIG=~/.pgx/14.3/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make -j4 \
        && make -j4 install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-14/postgresql.conf \
    && cd ~ \
    && rm -rf ~/timescaledb

# add clippy
RUN rustup component add clippy

USER root
