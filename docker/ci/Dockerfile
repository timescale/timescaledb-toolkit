FROM rust:1.60 AS pgx_builder

COPY docker/ci/setup.sh /
RUN OS_NAME=debian OS_CODE_NAME=bullseye /setup.sh

FROM pgx_builder AS rust-pgx

USER root
