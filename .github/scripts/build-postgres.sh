#!/usr/bin/env bash
set -euo pipefail

cd "$RUNNER_TEMP"
archive="postgresql-$PG_RELEASE.tar.bz2"
url="https://ftp.postgresql.org/pub/source/v$PG_RELEASE/$archive"
curl -fsSLO --retry 3 "$url"
curl -fsSLO --retry 3 "$url.sha256"
sha256sum --check "$archive.sha256"
tar -xjf "$archive"
cd "postgresql-$PG_RELEASE"
./configure --prefix="$PG_PREFIX" --with-openssl
make -j"$(nproc)"
make install
