FROM rust:1.51.0 AS pgx_builder

RUN apt-get update \
    && apt-get install -y clang libclang1 sudo bash cmake \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash postgres
USER postgres

# custom pgx until upstream workspace support is added
RUN cargo install --git https://github.com/JLockerman/pgx.git --branch timescale cargo-pgx

# only use pg12 for now timescaledb doesn't support 13
RUN set -ex \
    && cargo pgx init --pg12 download --pg13 download \
    && cargo pgx start pg12 \
    && cargo pgx stop pg12

# install timescaledb
# TODO make seperate image from ^
RUN set -ex \
    && cd ~ \
    && git clone https://github.com/timescale/timescaledb.git \
    && cd timescaledb \
    && git checkout 2.0.0

RUN set -ex \
    && cd ~/timescaledb \
    && ./bootstrap -DPG_CONFIG=~/.pgx/12.6/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
    && cd build \
    && make -j4 \
    && make -j4 install \
    && echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-12/postgresql.conf \
    && cd ~ \
    && rm -rf ~/timescaledb

# install doctester
RUN cargo install --git https://github.com/timescale/timescale-analytics.git --branch main sql-doctester

USER root
