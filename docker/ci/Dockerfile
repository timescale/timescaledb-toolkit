FROM rust:1.60 AS pgx_builder

RUN apt-get update \
    && apt-get install -y clang libclang1 sudo bash cmake \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash postgres
USER postgres

# install cargo pgx
RUN cargo install cargo-pgx --version '^0.2'

# only use pg12 for now timescaledb doesn't support 13
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
        && ./bootstrap -DPG_CONFIG=~/.pgx/12.10/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make -j4 \
        && make -j4 install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-12/postgresql.conf \
    && cd .. \
    && rm -rf ./build \
        && ./bootstrap -DPG_CONFIG=~/.pgx/13.6/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make -j4 \
        && make -j4 install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-13/postgresql.conf \
    && cd .. \
    && rm -rf ./build \
        && ./bootstrap -DPG_CONFIG=~/.pgx/14.2/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
        && cd build \
        && make -j4 \
        && make -j4 install \
        && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-14/postgresql.conf \
    && cd ~ \
    && rm -rf ~/timescaledb

# install doctester
RUN cargo install --git https://github.com/timescale/timescaledb-toolkit.git --branch main sql-doctester

# add clippy
RUN rustup component add clippy

FROM pgx_builder AS rust-pgx

USER root
