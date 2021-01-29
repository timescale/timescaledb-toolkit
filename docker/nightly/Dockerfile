FROM timescale/timescaledb:latest-pg12 AS analytics-tools

RUN mkdir rust

RUN set -ex \
    && apk add --no-cache --virtual .rust-build \
        cargo \
        clang-libs \
        git \
        gcc \
        make \
        openssl \
        openssl-dev \
        # we add rustup to get rustfmt (needed for pgx), but the version of rust
        # it installs does not support dynamic linking like the apk version
        # does.
        rust \
        rustup \
    && rustup-init -y --profile minimal -c rustfmt \
    # Add rustup components to the path for rustfmt to work. We need to use the
    # apk version of rust, to get dynamic linking, so add the rustup components
    # to the end of the path.
    && PATH="${PATH}:/root/.cargo/bin" \
    # install pgx.
    && git clone --branch work-in-workspaces https://github.com/JLockerman/pgx.git /rust/pgx \
        && cargo install --path /rust/pgx/cargo-pgx \
        && cargo pgx init --pg12 /usr/local/bin/pg_config

# we seperate out the build components so we we can just get them from cache
# and don't need to fetch/build them each time
FROM analytics-tools AS build-analytics

COPY . /rust/timescale-analytics

RUN set -ex \
    # Add rustup components to the path for rustfmt to work. We need to use the
    # apk version of rust, to get dynamic linking, so add the rustup components
    # to the end of the path.
    && PATH="${PATH}:/root/.cargo/bin" \
    && cd /rust/timescale-analytics && git reset HEAD --hard \
        && git clean -f -x \
        && cd extension \
        && cargo pgx install --release

# COPY over the new files to the image. Done as a seperate stage so we don't
# ship the build tools.
FROM timescale/timescaledb:latest-pg12 AS nightly

COPY --from=build-analytics /usr/local/lib/postgresql/ /usr/local/lib/postgresql/
