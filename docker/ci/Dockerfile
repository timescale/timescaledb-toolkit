FROM rust:1.60 AS pgx_builder

COPY docker/ci/setup.sh /
RUN /setup.sh

ENV PATH "/home/postgres/pgx/0.4/bin:${PATH}"

FROM pgx_builder AS rust-pgx

USER root
