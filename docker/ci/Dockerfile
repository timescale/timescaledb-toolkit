FROM rust:1.48 AS pgx_builder

RUN apt-get update \
    && apt-get install -y clang libclang1 sudo bash cmake \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash postgres
USER postgres

# custom pgx until upstream workspace support is added
RUN cd ~ \
    && git clone https://github.com/JLockerman/pgx.git \
    && cd pgx \
    && git checkout fixed-syn-version

RUN cd ~/pgx/cargo-pgx \
    && cargo install --path . \
    && cd ~ \
    && rm -rf ~/pgx

# only use pg12 for now timescaledb doesn't support 13
RUN cargo pgx init --pg12 download

# install timescaledb
# TODO make seperate image from ^
RUN cd ~ \
    && git clone https://github.com/timescale/timescaledb.git \
    && cd timescaledb \
    && git checkout 2.0.0

RUN cd ~/timescaledb \
    && ./bootstrap -DPG_CONFIG=~/.pgx/12.4/pgx-install/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false \
    && cd build \
    && make -j4 \
    && make -j4 install \
    && cd ~ \
    && rm -rf ~/timescaledb

USER root
