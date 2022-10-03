#!/bin/sh

set -ex

if [ -z "$OS_NAME" ] || [ -z "$OS_CODE_NAME" ]; then
    echo >&2 'OS_NAME and OS_CODE_NAME environment variables must be set'
    exit 2
fi

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

        # We'll run tools/testbin as this user, and it needs to (un)install packages.
        echo 'postgres ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

        # Setup the postgresql.org package repository.
        # Don't use the -y flag as it is not supported on old versions of the script.
        yes | sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh

        # Setup the timescaledb package repository.
        wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add -
        mkdir -p /etc/apt/sources.list.d
        # TODO Don't duplicate os name and version here.  Deduplicate with packaging scripts.
        cat > /etc/apt/sources.list.d/timescaledb.list <<EOF
deb https://packagecloud.io/timescale/timescaledb/$OS_NAME/ $OS_CODE_NAME main
EOF

        apt-get -qq update

        for pg in 12 13 14; do
            apt-get -qq install \
                    postgresql-$pg \
                    postgresql-server-dev-$pg
            # timescaledb packages Recommend toolkit, which we don't want here.
            apt-get -qq install --no-install-recommends timescaledb-2-postgresql-$pg
        done

        # We install as user postgres, so that needs write access to these.
        chown postgres /usr/lib/postgresql/14/lib /usr/share/postgresql/14/extension

        cd ~postgres
        exec su postgres -c "$0 unprivileged"
        ;;

    unprivileged)
        rustup component add clippy rustfmt

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
