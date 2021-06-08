FROM timescale/timescaledb-ha:pg13-latest AS analytics-tools

USER root

RUN mkdir rust

RUN set -ex \
    && apt-get update \
    && apt-get install -y \
        clang \
        gcc \
        git \
        libssl-dev \
        pkg-config \
        postgresql-server-dev-13 \
        make

ENV CARGO_HOME=/build/.cargo
ENV RUSTUP_HOME=/build/.rustup
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y --profile=minimal -c rustfmt
ENV PATH="/build/.cargo/bin:${PATH}"

#install pgx
RUN set -ex \
    && rm -rf "${CARGO_HOME}/registry" "${CARGO_HOME}/git" \
    && chown postgres:postgres -R "${CARGO_HOME}" \
    && cargo install --git https://github.com/JLockerman/pgx.git --branch timescale cargo-pgx \
    && cargo pgx init --pg13 /usr/lib/postgresql/13/bin/pg_config

COPY . /rust/timescale-analytics

RUN set -ex \
    && chown -R postgres:postgres /rust \
    && chown postgres:postgres -R "${CARGO_HOME}" \
    && chown postgres:postgres -R /usr/share/postgresql \
    && chown postgres:postgres -R /usr/lib/postgresql \
    && cd /rust/timescale-analytics \
        && cd extension \
        && cargo pgx install --release \
        && cargo run --manifest-path ../tools/post-install/Cargo.toml -- pg_config

# COPY over the new files to the image. Done as a seperate stage so we don't
# ship the build tools.
FROM timescale/timescaledb-ha:pg13-latest AS nightly

COPY --from=analytics-tools /usr/share/postgresql /usr/share/postgresql
COPY --from=analytics-tools /usr/lib/postgresql /usr/lib/postgresql
