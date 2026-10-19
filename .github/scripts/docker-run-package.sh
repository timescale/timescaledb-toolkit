#!/bin/bash

# TODO Move to timescaledb-toolkit if we can access the hosted arm64 runner there.

set -ex

if [ $# -lt 6 ]; then
    echo "usage: $0 PACKAGE_DIR IMAGE ARCH OS_NAME OS_VERSION TOOLKIT_VERSION [TOOLKIT_COMMIT]" >&2
    exit 1
fi

PACKAGE_DIR=$1
IMAGE=$2
ARCH=$3
OS_NAME=$4
OS_VERSION=$5
TOOLKIT_VERSION=$6
TOOLKIT_COMMIT=$7

case $OS_NAME in
    debian | ubuntu)
        pkg=deb
        ;;
    *)
        pkg=rpm
        ;;
esac

mkdir $PACKAGE_DIR
chmod a+rwx $PACKAGE_DIR

DOCKER_PACKAGE_DIR=/hostdump
# Redirect stderr to stdout as docker buffers them independently thereby
# breaking the proper interleaving (i.e. output appears out of order).
# TODO Duplicating builder username from the container build until ready to
#   change the toolkit ci build not to run as root.
docker run \
        -e OS_NAME=${OS_NAME} \
        -e OS_VERSION=${OS_VERSION} \
        -e TOOLKIT_COMMIT=${TOOLKIT_COMMIT} \
        -e TOOLKIT_VERSION=${TOOLKIT_VERSION} \
        -i \
        --platform $ARCH \
        -v $PACKAGE_DIR:$DOCKER_PACKAGE_DIR \
        -v $PWD:/release-build-scripts \
        $IMAGE \
        /bin/sh -exs <<EOF
exec 2>&1
cd timescaledb-toolkit
su postgres -c "/release-build-scripts/toolkit/package-$pkg.sh $OS_NAME $DOCKER_PACKAGE_DIR $TOOLKIT_VERSION $TOOLKIT_COMMIT"
EOF
