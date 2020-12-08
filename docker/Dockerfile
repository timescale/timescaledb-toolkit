FROM rust:1.48 AS pgx_builder

RUN apt-get update \
    && apt-get install -y clang libclang1 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash postgres
USER postgres

RUN cargo install cargo-pgx
RUN cargo pgx init
