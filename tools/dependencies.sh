# Dependency configuration
# Ideally, all dependencies would be specified in just one place.
# Exceptions:
# - crate dependencies are specified in Cargo.toml files
# - postgres versions are duplicated in the Github Actions matrix
# - Readme.md lists some, too.  TODO is it acceptable to just point to this file?
# All our automation scripts read this, so at least we're not duplicating this
# information across all those.

PG_VERSIONS='15 16 17'
# TODO: extend this with 18 this once TimescaleDB supports PostgreSQL 18
TSDB_PG_VERSIONS='15 16 17'

CARGO_EDIT=0.11.2

# Keep synchronized with extension/Cargo.toml and `cargo install --version N.N.N cargo-pgrx` in Readme.md .
PGRX_VERSION=0.12.9

RUST_TOOLCHAIN=1.89.0
RUST_PROFILE=minimal
RUST_COMPONENTS=clippy,rustfmt

# We use fpm 1.14.2 to build RPMs.
# TODO Use rpmbuild directly.
FPM_VERSION=1.14.2

GH_DEB_URL=https://github.com/cli/cli/releases/download/v2.16.1/gh_2.16.1_linux_amd64.deb
GH_DEB_SHA256=d0ba8693b6e4c1bde6683ccfa971a15c00b9fe92865074d48609959d04399dc7
