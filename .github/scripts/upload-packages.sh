#!/bin/sh

set -ex

if [ $# -lt 2 ]; then
    echo >&2 "usage: $0 OS_NAME OS_VERSION [OS_CODE_NAME]"
    exit 2
fi

OS_NAME=$1
OS_VERSION=$2
OS_CODE_NAME=$3

case $OS_NAME in
    debian | ubuntu)
        if [ -z "$OS_CODE_NAME" ]; then
            echo >&2 'OS_CODE_NAME required for Debian and derivatives'
            exit 2
        fi
        package_cloud push --skip-errors timescale/timescaledb/$OS_NAME/$OS_CODE_NAME pkgdump/*.deb
        ;;
    *)
        package_cloud push --skip-errors timescale/timescaledb/el/$OS_VERSION pkgdump/*.rpm
        ;;
esac
