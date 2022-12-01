#!/bin/sh

# TODO rename to tools/setup - this is useful even for developer setup (add Mac/brew support)

set -ex

if [ -z "$CARGO_HOME" ] || [ -z "$RUSTUP_HOME" ]; then
    echo >&2 'CARGO_HOME and RUSTUP_HOME environment variables must be set'
    exit 3
fi

if [ "$1" = -unprivileged ]; then
    privileged=false
    shift
else
    privileged=true
fi

if [ $# -ne 6 ]; then
    echo >&2 'usage: setup.sh ARCH OS_NAME OS_VERSION OS_CODE_NAME BUILDER_USERNAME BUILDER_HOME'
    exit 2
fi

ARCH=$1
OS_NAME=$2
OS_VERSION=$3
OS_CODE_NAME=$4
BUILDER_USERNAME=$5
BUILDER_HOME=$6

. /dependencies.sh

# Phase 0 - set platform-specific parameters
case $OS_NAME in
    centos | rockylinux)
        PG_BASE=/usr/pgsql-
        ;;
    debian | ubuntu)
        PG_BASE=/usr/lib/postgresql/
        ;;
    *)
        echo >&2 "unsupported $OS_NAME"
        exit 4
        ;;
esac

if $privileged; then
    # Phase 1 - cross-platform prerequisites
    useradd -u 1001 -md "$BUILDER_HOME" $BUILDER_USERNAME

    # Phase 2 - platform-specific package installation
    case $OS_NAME in
        # Red Hat Enterprise derivatives
        centos | rockylinux)
            case $OS_VERSION in
                7)
                    # Postgresql packages require both
                    # - llvm-toolset-7-clang from centos-release-scl-rh
                    # - llvm5.0-devel from epel-release
                    # ¯\_(ツ)_/¯
                    yum -q -y install centos-release-scl-rh epel-release
                    yum -q -y install devtoolset-7-gcc llvm-toolset-7-clang-devel llvm-toolset-7-clang-libs
                    # devtoolset-7 includes a BROKEN sudo!  It even leads with a TODO about its brokenness!
                    rm -f /opt/rh/devtoolset-7/root/usr/bin/sudo
                    # for fpm
                    yum -q -y install rh-ruby26-ruby-devel

                    # TODO Would be nice to be able to resolve all system
                    #  differences here rather than also in package-rpm.sh -
                    #  maybe install wrappers in /usr/local/bin or setup ~/.profile .
                    #  For now, most the knowledge is split.
                    # Here, we only need `cc` for installing cargo-pgx below.
                    set +e      # scl_source has unchecked yet harmless errors?!  ¯\_(ツ)_/¯
                    . scl_source enable devtoolset-7
                    # And rh-ruby26 for gem install fpm below.
                    . scl_source enable rh-ruby26
                    set -e
                    ;;

                8)
                    dnf -qy module disable postgresql
                    # fpm suddenly requires newer public_suffix that requires newer ruby
                    # https://github.com/jordansissel/fpm/issues/1923 ¯\_(ツ)_/¯
                    dnf -qy module enable ruby:2.6
                    dnf -qy install ruby-devel rubygems
                    ;;

                9)
                    dnf -qy install ruby-devel rubygems
                    ;;

                *)
                    echo >&2 'only 7 - 9 supported'
                    exit 5
                    ;;
            esac

            # pgx needs:
            # - gcc (specifically; clang won't do!)
            # - openssl-devel
            # - make
            # - pkg-config
            yum -q -y install \
                curl \
                gcc \
                git \
                make \
                openssl-devel \
                pkg-config \
                rpm-build \
                sudo

            # Setup the postgresql.org package repository.
            yum -q -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-${OS_VERSION}-${ARCH}/pgdg-redhat-repo-latest.noarch.rpm

            # Setup the timescaledb package repository.
            cat > /etc/yum.repos.d/timescale_timescaledb.repo <<EOF
[timescale_timescaledb]
name=timescale_timescaledb
baseurl=https://packagecloud.io/timescale/timescaledb/el/$OS_VERSION/$ARCH
repo_gpgcheck=1
gpgcheck=0
enabled=1
gpgkey=https://packagecloud.io/timescale/timescaledb/gpgkey
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOF

            for pg in $PG_VERSIONS; do
                yum -q -y install \
                    postgresql$pg-devel \
                    postgresql$pg-server \
                # We install as user postgres, so that needs write access to these.
                chown $BUILDER_USERNAME $PG_BASE$pg/lib $PG_BASE$pg/share/extension
            done
            for pg in $TSDB_PG_VERSIONS; do
                yum -q -y install \
                    timescaledb-2-postgresql-$pg
            done

            gem install fpm -v $FPM_VERSION -N
            ;;

        # Debian family
        debian | ubuntu)
            # Image comes in with no package lists so we have to start with this.
            apt-get -qq update

            # Stop most debconf prompts.  Some have no default and we'd need
            # to provide actual values via DEBCONF_OVERRIDE but we don't have
            # any of those right now.
            export DEBIAN_FRONTEND=noninteractive

            # pgx needs:
            # - gcc (specifically; clang won't do!)
            # - libssl-dev
            # - make
            # - pkg-config
            apt-get -qq install \
                    build-essential \
                    curl \
                    debhelper \
                    ed \
                    fakeroot \
                    gcc \
                    git \
                    gnupg \
                    libssl-dev \
                    make \
                    pkg-config \
                    postgresql-common \
                    sudo

            # Setup the postgresql.org package repository.
            # Don't use the -y flag as it is not supported on old versions of the script.
            yes | /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh

            # Setup the timescaledb package repository.
            # TODO Blindly trusting a key that may change every time we run
            #   defeats the purpose of package-signing but without a key rotation
            #   story, the only security we have here is by trusting the system
            #   certificate store, which we trust docker to provide a good copy
            #   of.  May as well just put [trusted=yes] into sources.list instead
            #   of bothering with apt-key...
            curl -Ls https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add -
            mkdir -p /etc/apt/sources.list.d
            cat > /etc/apt/sources.list.d/timescaledb.list <<EOF
deb https://packagecloud.io/timescale/timescaledb/$OS_NAME/ $OS_CODE_NAME main
EOF

            apt-get -qq update

        for pg in $PG_VERSIONS; do
            apt-get -qq install \
                    postgresql-$pg \
                    postgresql-server-dev-$pg
            # We install as user postgres, so that needs write access to these.
            chown $BUILDER_USERNAME $PG_BASE$pg/lib /usr/share/postgresql/$pg/extension
        done

        for pg in $TSDB_PG_VERSIONS; do
            # timescaledb packages Recommend toolkit, which we don't want here.
            apt-get -qq install --no-install-recommends timescaledb-2-postgresql-$pg
        done

        # Ubuntu is the only system we want an image for that sticks an extra
        # copy of the default PATH into PAM's /etc/environment and we su or sudo
        # to $BUILDER_USERNAME thereby picking up that PATH and clobbering the
        # one we set in Dockerfile.  There's nothing else in here, so at first I
        # thought to remove it.  That works on 20.04 and 22.04, but still leaves
        # a busted PATH on 18.04!  On 18.04, we get clobbered by ENV_PATH in
        # /etc/login.defs .  We fix all three by setting our PATH here:
        echo > /etc/environment "PATH=$PATH"
        ;;
    esac

    # Phase 3 - cross-platform privileged tasks after package installation

    # We've benefitted from being able to test expansions to our cargo
    # installation without having to rebuild the CI image before, so
    # donate the cargo installation to the builder user.
    install -d -o $BUILDER_USERNAME "$CARGO_HOME" "$RUSTUP_HOME"

    # We'll run tools/testbin as this user, and it needs to (un)install packages.
    echo "$BUILDER_USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

    cd "$BUILDER_HOME"
    exec sudo -H --preserve-env=PATH,CARGO_HOME,RUSTUP_HOME -u $BUILDER_USERNAME "$0" -unprivileged "$@"
fi

# Phase 4 - unprivileged cross-platform tasks

curl -s https://sh.rustup.rs |
    sh -s -- -q -y --no-modify-path --default-toolchain $RUST_TOOLCHAIN --profile $RUST_PROFILE -c $RUST_COMPONENTS

# Install pgx
cargo install cargo-pgx --version =$PGX_VERSION

# Configure pgx
## `cargo pgx init` is not additive; must specify all versions in one command.
for pg in $PG_VERSIONS; do
    init_flags="$init_flags --pg$pg $PG_BASE$pg/bin/pg_config"
done
cargo pgx init $init_flags
## Initialize pgx-managed databases so we can add the timescaledb load.
for pg in $PG_VERSIONS; do
    echo "shared_preload_libraries = 'timescaledb'" >> ~/.pgx/data-$pg/postgresql.conf
done

# Clone and fetch dependencies so we builds have less work to do.
git clone https://github.com/timescale/timescaledb-toolkit
cd timescaledb-toolkit
cargo fetch
