#!/bin/sh

set -ex

case "$1" in
    '')
        # The postgresql packages create this user, but with /var/lib/postgresql as the home directory.
        # We have some old scripts that expect that to be /home/postgresql so let's stick with that.
        useradd -m postgres

        apt-get -qq update
        apt-get -qq install \
                bash \
                clang \
                cmake \
                gnupg \
                libclang1 \
                libclang-dev \
                postgresql-common \
                sudo
        # Install postgresql from postgresql.org repositories.
        /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
        # TODO install timescaledb here too
        apt-get -qq update

        for pg in 12 13 14; do
            apt-get -qq install \
                    postgresql-$pg \
                    postgresql-server-dev-$pg
        done

        # We install as user postgres, so that needs write access to these.
        chown postgres /usr/lib/postgresql/14/lib /usr/share/postgresql/14/extension

        cd ~postgres
        exec su postgres -c "$0 unprivileged"
        ;;

    unprivileged)
        rustup component add clippy rustfmt

        # TODO Install binaries above.
        # Build the TimescaleDB extension for all the servers that we
        # installed above.

        git clone --single-branch --branch 2.5.x https://github.com/timescale/timescaledb.git

        cd timescaledb

        set +e
        cmake -S . -B build-12 -DPG_CONFIG=/usr/lib/postgresql/12/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false
        cmake -S . -B build-13 -DPG_CONFIG=/usr/lib/postgresql/13/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false
        cmake -S . -B build-14 -DPG_CONFIG=/usr/lib/postgresql/14/bin/pg_config -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DUSE_OPENSSL=false -DSEND_TELEMETRY_DEFAULT=false -DREGRESS_CHECKS=false

        cmake --build build-12 --parallel
        cmake --build build-13 --parallel
        cmake --build build-14 --parallel

        cmake --install build-12
        cmake --install build-13
        cmake --install build-14

        set -e
        cd ..
        rm -rf timescaledb

        # Install cargo pgx
        # Keep synchronized with `cargo install --version N.N.N cargo-pgx` in Readme.md and Cargo.toml
        cargo install cargo-pgx --version =0.2.4 --root ~/pgx/0.2
        cargo install cargo-pgx --version =0.4.5 --root ~/pgx/0.4

        PATH=$HOME/pgx/0.4/bin:$PATH

        # Initialize new PostgreSQL instances and update the configuration
        # files so that they can use TimescaleDB that we installed above.
        # TODO Not sure all this is necessary...
        cargo pgx init --pg12 /usr/lib/postgresql/12/bin/pg_config --pg13 /usr/lib/postgresql/13/bin/pg_config --pg14 /usr/lib/postgresql/14/bin/pg_config
        cargo pgx start pg12 && cargo pgx stop pg12
        cargo pgx start pg13 && cargo pgx stop pg13
        cargo pgx start pg14 && cargo pgx stop pg14

        echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-12/postgresql.conf
        echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-13/postgresql.conf
        echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-14/postgresql.conf

        ;;

    *)
        echo >&2 'run as root without arguments'
        ;;
esac
