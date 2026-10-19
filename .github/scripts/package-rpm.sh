#!/bin/bash

# TODO This can move to timescaledb-toolkit now.

# TODO Like our setup and testbin scripts, this is substantially the same as
#   the DEB build, so we ought to unify.

set -ex

if [ $# -lt 3 ]; then
    echo "usage: $0 OS_NAME RPMDIR TOOLKIT_VERSION [TOOLKIT_COMMIT]" >&2
    exit 3
fi

OS_NAME=$1
RPMDIR=$2
TOOLKIT_VERSION=$3
TOOLKIT_COMMIT=$4

# TODO Would be nice to be able to resolve all system differences in the image
#  build rather than also here.  See TODO over there.
OS_VERSION=`rpm -E %{rhel}`
if [ "$OS_VERSION" = 7 ]; then
    set +e        # scl_source has unchecked yet harmless errors?!  ¯\_(ツ)_/¯
    # Here, we need:
    ## rh-ruby26 for fpm
    . scl_source enable rh-ruby26
    ## both cc and gcc out of devtoolset-7 for building pgrx-pg-sys
    . scl_source enable devtoolset-7
    ## and clang on PATH and LIBCLANG_PATH=/opt/rh/llvm-toolset-7/root/usr/lib64 also for pgrx-pg-sys
    . scl_source enable llvm-toolset-7
    set -e
fi

# TODO This is all duplicated from ../deb/docker-build-toolkit.sh
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
git checkout ${TOOLKIT_COMMIT:-$TOOLKIT_VERSION}
# Show what we got to aid debugging.
git log -1

. tools/dependencies.sh


# El8 skips postgres18
# El10 skips postgres15
case $OS_VERSION in
    8)
        skip_pg_versions=18
        ;;
    10)
        skip_pg_versions=15
        ;;
    *)
        skip_pg_versions=
        ;;
esac

if [ -n "$skip_pg_versions" ]; then
    filtered_pg_versions=
    for pg in $PG_VERSIONS; do
        skip_pg=
        for skip_pg_version in $skip_pg_versions; do
            [ "$pg" = "$skip_pg_version" ] && skip_pg=1
        done
        [ -n "$skip_pg" ] && continue
        filtered_pg_versions="${filtered_pg_versions:+$filtered_pg_versions }$pg"
    done
    PG_VERSIONS=$filtered_pg_versions
    echo "Skipping PostgreSQL $skip_pg_versions packages for EL$OS_VERSION; PG_VERSIONS=$PG_VERSIONS"
fi

base_PATH=$PATH

for pg in $PG_VERSIONS; do
    PATH=/usr/pgsql-$pg/bin:$base_PATH
    (cd extension && cargo pgrx package)
    cargo run --manifest-path tools/post-install/Cargo.toml -- --dir target/release/timescaledb_toolkit-pg$pg

  fpm --log info -s dir -t rpm \
    --maintainer 'Timescale <hello@timescale.com>' \
    --license 'Timescale License' \
    --description "Library of analytical hyperfunctions, time-series pipelining, and other SQL utilities, compatible with TimescaleDB and PostgreSQL" \
    --vendor Timescale \
    --url https://github.com/timescale/timescaledb-toolkit \
    --category database \
    --depends postgresql$pg-server \
    --name timescaledb-toolkit-postgresql-$pg \
    --version $TOOLKIT_VERSION \
    --iteration 0 \
    -C target/release/timescaledb_toolkit-pg$pg
done

# rename packages to have centos version in filename
# TODO Support aarch64?
find . -name '*.rpm'
find . -name '*.rpm' -exec sh -c 'x="{}";mv "$x" $(echo $x|sed -e "s!.x86_64.!.el${OS_VERSION}.x86_64.!")' \;
for oldname in *.rpm; do
    # TODO When testing with unreleased toolkit, we have `-dev` in the version.  RPM changes this to `_dev`.  Options:
    # - keep this here
    # - move this into testbin
    # - if testing against main is infrequent, just don't bother with this at all (is it though?)
    newname=`echo $oldname | sed s/_dev-0/-dev-0/`
    [ $oldname = $newname ] || mv $oldname $newname
done

# Test the binaries before accepting them.
# TODO We have to filter versions anyway!  We didn't release every version for
#   every platform.  So move something like this into testbin.
#   Although, the concerns here and in package-deb.sh are particular to these
#   systems, so maybe it makes more sense here than there.  Hmm.
CONTROL=extension/timescaledb_toolkit.control
# TODO Never released for Rocky 9 before, so no upgrade to test.
if [ $OS_VERSION -lt 9 ]; then
    if [ $OS_VERSION = 7 ]; then
        # We couldn't support these versions on centos7 due to pgrx 0.4 segfault.
        sed --in-place -e 's/1.8.0, 1.10.0-dev, 1.10.1, 1.11.0//' $CONTROL
    fi
    tools/testbin -version $TOOLKIT_VERSION -pgversions "$PG_VERSIONS" -bindir . rpm
fi

# Pass:  Ship ship ship!

cp *.rpm ${RPMDIR}
