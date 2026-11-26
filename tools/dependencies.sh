# There is also configuration

# PG_RELEASES with minor versions.
PG_RELEASES='16.16 17.12 18.7 19.0'

# PG major versions.
PG_VERSIONS=$(echo "$PG_RELEASES" | sed 's/\.[0-9][0-9]*//g')

# PG supported by TSDB
TSDB_PG_VERSIONS='16 17 18' # PG19 not yet released by TSDB.

CARGO_EDIT=0.11.2

# Keep synchronized with extension/Cargo.toml and `cargo install --version N.N.N cargo-pgrx` in README.md .
PGRX_VERSION=0.19.1

RUST_TOOLCHAIN=1.96.0
RUST_PROFILE=minimal
RUST_COMPONENTS=clippy,rustfmt

# We use fpm to build RPMs.
# TODO Use rpmbuild directly.
FPM_VERSION=1.17.0

GH_DEB_URL=https://github.com/cli/cli/releases/download/v2.16.1/gh_2.16.1_linux_amd64.deb
GH_DEB_SHA256=d0ba8693b6e4c1bde6683ccfa971a15c00b9fe92865074d48609959d04399dc7
