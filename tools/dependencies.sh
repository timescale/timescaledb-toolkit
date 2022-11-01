# Dependency configuration
# Ideally, all dependencies would be specified in just one place.
# Exceptions:
# - crate dependencies are specified in Cargo.toml files
# - postgres versions are duplicated in the Github Actions matrix
# - Readme.md lists some, too.  TODO is it acceptable to just point to this file?
# All our automation scripts read this, so at least we're not duplicating this
# information across all those.

PG_VERSIONS='12 13 14'

# Keep synchronized with extension/Cargo.toml and `cargo install --version N.N.N cargo-pgx` in Readme.md .
PGX_VERSION=0.5.4

RUST_TOOLCHAIN=1.60.0
RUST_PROFILE=minimal
RUST_COMPONENTS=clippy,rustfmt

# We use fpm 1.14.2 to build RPMs.
# TODO Use rpmbuild directly.
FPM_VERSION=1.14.2

# Builder username and home directory, for cooperation between our image builder
# and our Github Actions configurations which must ALSO know these things.
BUILDER_USERNAME=postgres
BUILDER_HOME=/home/$BUILDER_USERNAME
