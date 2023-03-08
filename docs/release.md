# Release and build procedures

We build the timescaledb_toolkit extension using Cargo, but we have many
higher-level tasks in need of automation:

- Build, lint, and test with particular flags in multiple environments
- Extract SQL examples from documentation and test
- Test upgrades
- Installation
- Publish a release
- Make a container image to run all the above on

The rest of this document elaborates on each of those.  But first..

## Dependency management

Ideally, all dependencies would be specified in just one place.  But that's
not quite feasible.  Cargo.toml files capture the crate dependencies.

The rest are needed by the six shell scripts used to solve the above list
of problems.  We configure those in `tools/dependencies.sh`.

## Build, lint, and test

`tools/build` is the relatively simple shell script that owns the cargo flags
for running clippy, running tests, installing, and testing upgrades.
The latter two are arguably out of place here.

Testing upgrades is now handled by `testbin` (below), but the version here was
useful for Mac.  That has now degraded as it would need to support a third
pgx...

Installing is only relevant for local development.

## Extract SQL examples from documentation and test

`tools/sql-doctester` is a Rust program which extracts example SQL programs
and expected output from documentation, runs the programs, and asserts their
output matches what was expected.

The intent here is merely to prevent sample code from bitrotting, but some
functionality is currently only tested here.

## Test upgrades

We include in each release a set of scripts to upgrade an installation from a
set of previous versions.  We test these upgrades by installing a supported
old version, materializing some data, running the upgrade script, and
asserting the extension can still load the old data.

`tools/update-tester` is a Rust program which loads tests from `tests/update`
to implement the materialize and verify steps.  It needs to know which version
each function was stabilized in, and we store that information in
`extension/src/stabilization_info.rs` (also used by post-install, see below).

`tools/testbin` is a shell script that uses `update-tester` to test upgrades
between released binaries (deb and rpm).

## Installation

Installation is a two-step process currently duplicated in three places.
The two steps are:

1. `cargo pgx install --release` OR `cargo pgx package`
2. `tools/post-install`

These steps are repeated in:

1. `Readme.md`
2. `tools/build`
3. `toolkit/package-deb.sh` and `toolkit/package-rpm.sh`

`Readme.md` could simply recommend running `tools/build install`.

`package-deb.sh` and `package-rpm.sh` could run `tools/build package` (which
doesn't yet exist).

`cargo pgx install` installs the extension into the directory specified by
`pg_config`.  `cargo pgx package` installs into a directory under
`$CARGO_TARGET_DIR` where we pick it up and pack it into deb and rpm packages.

`tools/post-install` performs miscellaneous install-time procedures:
- finalize control file
- rename `timescaledb_toolkit.so` to include the version number
- generate update scripts

`tools/post-install` needs to know which version each function was stabilized
in, and we store that information in `extension/src/stabilization_info.rs`.

## Publish a release

`tools/release` automates all the steps of our release process.  We run it via
github action (`.github/workflows/release.yml`).

`tools/release` creates and pushes a release branch and tag, runs tests,
starts a package build, prepares the `main` branch for the next release, and
creates an issue so we don't forget some tasks not yet automated.

The package build happens in a different repository for reasons described in
comments at the top of `tools/release`.

Over in that repository, we have `.github/workflows/toolkit-package.yml` which
runs `toolkit/docker-run-package.sh` to build packages and
`toolkit/upload-packages.sh` to upload them to PackageCloud.

`toolkit/docker-run-package.sh` runs `toolkit/package-deb.sh` and
`toolkit/package-rpm.sh` in various container images to build packags for
those platforms.  Which platforms we build for is controlled in the `yml`
action file.

### Usage:

1. https://github.com/timescale/timescaledb-toolkit/actions/workflows/release.yml
2. Click "Run workflow"
3. Fill out the form
4. Be sure to replace `-n` with `-push`!

We can replace this last one with a checkbox:
- unchecked: run with neither `-n` nor `-push`
- checked: run with `-push`

The script has three modes:

- `-n`:  print what would be done without doing anything
- `-push`:  do everything including pushing to Github and PackageCloud
- neither:  do all the work (branch, edit, test, package, upgrade-test), but don't push anywhere

The third mode is the most useful but it is not available from the Github
action.  Very sad.  We need to fix that.

### Resume after failure

Up until the packaging step, just rerun the release action after the problem
is resolved.

If packages have been published, the choices are:
- do the rest of what the script does manually
- increment the patch revision (1.3.X) and start another release

An obvious improvement would be to teach `tools/release` to resume at a
specific step, something like `tools/release --start-at-step 7`.  It would
need to verify that the previous steps were actually done and bail out if not.

Once packaging is no longer asynchronous in the other repository,
`tools/release` can simply be taught to figure out which steps are done all on
its own, without an operator having to tell it where to resume.

### Debugging

We run `tools/release` with the shell's `-x` option so it prints each command
it runs.  We redirect the standard error stream to the standard output because
Docker will otherwise separate them such that error messages may appear far
from related output.

So, when something goes wrong, it is easy to pinpoint exactly which part of
the script failed and how.

Things that can go wrong:

#### Transient network hiccough

This can happen at almost any stage.  A simple retry might be the easiest way
to see if the issue is transient.  If it's not, options are limited:
- wait
- complain, then wait

#### cargo install cargo-edit

- Is crates.io down?
- Has cargo-edit vanished from crates.io?

#### Install gh

- The version we use is gone.  Find the latest and figure out whether all our
  usage has been invalidated by incompatible changes.  Be careful!
- Or, just squirrel away a copy of the old binary and keep rolling, until the
  underlying APIs it uses break.
- The checksum doesn't match.  Did they break it?  Why would they do such a
  thing?  Were they hacked?  Probably should go ahead and update at this point.

#### `extension/timescaledb_toolkit.control` problems

`tools/release` edits this file, so it is very careful that the file looks the
way it expects.  It is and should remain very picky.

If we've made some unexpected edits, it will complain.  If the edits were
erroneous, fix them; else, you have to teach `tools/release` what you've done.

One of the things it checks is the `upgradeable_from` line.  Most importantly,
it expects that patch releases are upgradeable from the previous version in
the same minor version (e.g. 1.3.1 is upgradeable from 1.3.0).

#### `Changelog.md` problems

`tools/release` ensures the version being released has Changelog.md entries.

It also requires some particular boiler-plate text at the top to know where to
make its edits.  The boiler-plate is arbitrary text for intended for
consumption by the development team.  If we change that text, `tools/release`
needs to know about it.

#### Tests fail

Oh boy!

Test output is logged.

`tools/build test-extension` shouldn't fail since it already passed when the
release commit was merged to master.

You're not trying to release a commit that didn't pass CI, are you?

But, the upgrade tests are being run for the first time!  So those might
break.  We should run `tools/release --no-push` nightly.  In the mean time...
to the debugger!

#### git push fails

We've had branch permission problems before...

Is the authentication token working?

#### `gh` fails

Is GitHub API struggling?

Is the authentication token working?

Has the packaging action in the `release-build-scripts` repository
gone missing?

## Make a container image to run all the above on

`.github/workflows/toolkit-image.yml` configures the GitHub action which
builds all our supported container images.

One image is special:  debian-11-amd64.  This is the one we run all our GitHub
actions on.

`docker/ci/Dockerfile` is the entry-point and it runs `docker/ci/setup.sh` to
do the work:

- Create the build user
- Install necessary build tools and libraries
- Install postgresql and timescaledb
- Install `gh` github command-line tool used by `tools/release`
- Install Rust and PGX
- Pre-fetch toolkit's crate dependencies to minimize work done at CI time

## Maintenance tasks

So, we've automated build and release!  ONCE AND FOR ALL.  Right?

As the great Balki Bartokomous often said:
of course not; don't be ridiculous.

These are the sorts of things we have to do from time to time:

- Update Rust.  It moves pretty fast.
- Update PGX.  It moves even faster.
- Update other crates.  `cargo audit` and `cargo update` are our friends.
- Update OS versions.  Labels such as `rockylinux:9` eventually point to
  something different or disappear entirely.  The former actually surprised us
  once already.

### Things we update blindly

We install the latest version of these every time, so they may change in
surprising ways at inopportune times.

- fpm:  It's a Ruby script with lots of dependencies and we install the latest
  version and it bit us on the ass once already.  We use it because someone
  set it up for us a long time ago and no one has had the chance to sit down
  and figure out how to write an RPM spec file.  Shouldn't take more than a
  few hours, just haven't done it...

- postgresql:  We install the latest version of a fixed set of major versions,
  so this should be very unlikely to break on us.  Listed for completeness.

- timescaledb:  We test with their master branch nightly, so we should be
  ahead of this one.

### Unknown Unknowns

lol

They're inevitable.  You just need a good nose for debugging.
