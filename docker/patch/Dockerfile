FROM timescale/timescaledb-ha:pg13-latest AS toolkit-tools

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
    && cargo install cargo-pgx --version '^0.2' \
    # initdb fails as root so generate the correct config ourselves
    # && cargo pgx init --pg13 /usr/lib/postgresql/13/bin/pg_config
    && mkdir -p /root/.pgx \
    && printf '[configs]\npg13="/usr/lib/postgresql/13/bin/pg_config"\n' > /root/.pgx/config.toml


COPY . /rust/timescaledb-toolkit

RUN set -ex \
    && chown -R postgres:postgres /rust \
    && chown postgres:postgres -R "${CARGO_HOME}" \
    && chown postgres:postgres -R /usr/share/postgresql \
    && chown postgres:postgres -R /usr/lib/postgresql \
    && cd /rust/timescaledb-toolkit \
        && cd extension \
        && cargo pgx install --release \
        && cargo run --manifest-path ../tools/post-install/Cargo.toml -- pg_config

# COPY over the new files to the image. Done as a seperate stage so we don't
# ship the build tools.
FROM timescale/timescaledb-ha:pg13-latest AS nightly

COPY --from=toolkit-tools /usr/share/postgresql /usr/share/postgresql
COPY --from=toolkit-tools /usr/lib/postgresql /usr/lib/postgresql
