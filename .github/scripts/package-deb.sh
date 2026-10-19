#!/bin/bash

# TODO This can move to timescaledb-toolkit now.

# The oldest we support is Ubuntu 20.04 which has debhelper 12, but we have no
# reason to update this yet.
DEBHELPER_COMPAT=11

set -ex

if [ $# -lt 3 ]; then
    echo "usage: $0 OS_NAME DEBDIR TOOLKIT_VERSION [TOOLKIT_COMMIT]" >&2
    exit 3
fi

OS_NAME=$1
DEBDIR=$2
TOOLKIT_VERSION=$3
TOOLKIT_COMMIT=$4

git fetch

if [ $TOOLKIT_VERSION == LATEST-PR-ONLY ]; then
    if [ -n "$TOOLKIT_COMMIT" ]; then
        echo "LATEST-PR-ONLY is only for pull requests, which do not set TOOLKIT_COMMIT=$TOOLKIT_COMMIT" >&2
        exit 4
    fi
    # Picks the wrong version if we go to 2.x but that's a problem for another year.
    TOOLKIT_VERSION=1.`git tag | sed 's/^1\.//' | sort -n | tail -1`
    echo "TOOLKIT_VERSION was LATEST-PR-ONLY so picked $TOOLKIT_VERSION"
fi

# TOOLKIT_VERSION comes in like 'forge-stable-1.6.0' and that is correct for
# the other places we use it, e.g. the branch name.  But not only is it a bit
# silly in a package version, it's even illegal:
#   'Version' field value 'forge-stable-1.6.0~ubuntu20.04': version number does not start with digit
DEB_VERSION=${TOOLKIT_VERSION#forge-stable-}~${OS_NAME}${OS_VERSION}
if [[ ${DEB_VERSION} != [0-9]* ]]; then
    echo "TOOLKIT_VERSION '${TOOLKIT_VERSION}' starts with unknown illegal prefix" >&2
    exit 2
fi

git checkout ${TOOLKIT_COMMIT:-$TOOLKIT_VERSION}

# Show what we got to aid debugging.
git log -1

. tools/dependencies.sh

# Ubuntu 26.04 skips postgres 15 as there are no tsdb packages.
if [ "$OS_NAME" = ubuntu ] && [ "$OS_VERSION" = 26.04 ]; then
    filtered_pg_versions=
    for pg in $PG_VERSIONS; do
        [ "$pg" = 15 ] && continue
        filtered_pg_versions="${filtered_pg_versions:+$filtered_pg_versions }$pg"
    done
    PG_VERSIONS=$filtered_pg_versions
    echo "Skipping PostgreSQL 15 packages for Ubuntu 26.04; PG_VERSIONS=$PG_VERSIONS"
fi

mkdir debian
ln -s /usr/bin/dh debian/rules

date=$(TZ=Etc/UTC date -R)
maintainer='Timescale <hello@timescale.com>'

# deb-changelog(5)
cat > debian/changelog <<EOF
timescaledb-toolkit (1:$DEB_VERSION) unused; urgency=medium

  * See https://github.com/timescale/timescaledb-toolkit/releases/tag/$TOOLKIT_VERSION

 -- $maintainer  $date
EOF
# deb-src-control(5)
cat > debian/control <<EOF
Source: timescaledb-toolkit
Maintainer: $maintainer
Homepage: https://github.com/timescale/timescaledb-toolkit
Rules-Requires-Root: no
Section: database
Priority: extra
Build-Depends: debhelper-compat (= $DEBHELPER_COMPAT)
EOF

base_PATH=$PATH

for pg in $PG_VERSIONS; do
    PATH=/usr/lib/postgresql/$pg/bin:$base_PATH
    (cd extension && cargo pgrx package)
    cargo run --manifest-path tools/post-install/Cargo.toml -- --dir target/release/timescaledb_toolkit-pg$pg
    cat >> debian/control <<EOF

Package: timescaledb-toolkit-postgresql-$pg
Architecture: any
Depends: postgresql-$pg
Description: Library of analytical hyperfunctions, time-series pipelining, and other SQL utilities, compatible with TimescaleDB and PostgreSQL
EOF
    echo "target/release/timescaledb_toolkit-pg$pg/usr/* usr/" > debian/timescaledb-toolkit-postgresql-$pg.install
done

dpkg-buildpackage --build=binary --no-sign --post-clean

# Test the binaries before accepting them.
# TODO We have to filter versions anyway!  We didn't release every version for
#   every platform.  So move something like this into testbin.
CONTROL=extension/timescaledb_toolkit.control
case `dpkg --print-architecture`-$OS_NAME-$OS_VERSION in
    # 1.11.0 was the first release we built packages for
    # - arm64
    # - Debian 10
    arm64-* | *-debian-10)
        sed --in-place -e 's/1.5.1, 1.5.2, 1.6.0, 1.7.0, 1.8.0, 1.10.0-dev, 1.10.1//' $CONTROL
        ;;

    amd64-ubuntu-22.04)
        # 1.6.0 was the first release we built for Ubuntu 22.04.
        sed --in-place -e 's/1.5.1, 1.5.2, //' $CONTROL
        ;;
esac
# TODO Continuing to ignore the exit code for a little while longer, as
#   testbin currently fails when there are no upgrades to test, as for arm64
#   and Debian 10 (see above).
tools/testbin -version $TOOLKIT_VERSION -pgversions "$PG_VERSIONS" -bindir .. deb || echo testbin failed $?

# Pass:  Ship ship ship!

cd ..

# packagecloud.io doesn't support `.ddeb` files?  Like `.udeb`, they're just
# deb packages by another name, so:
for i in timescaledb-toolkit*.ddeb; do
    # But it's only on Ubuntu that dpkg-buildpackage creates dbgsym packages
    # with the suffix `.ddeb`.  On Debian, 'timescaledb-toolkit*.ddeb'
    # evaluates to 'timescaledb-toolkit*.ddeb' so there's nothing to do.
    [ "$i" = 'timescaledb-toolkit*.ddeb' ] || mv $i ${i%.ddeb}.deb
done

cp timescaledb-toolkit*.deb $DEBDIR
