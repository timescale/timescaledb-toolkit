# Toolkit Changelog

## Process for updating this change log (Proposal)
This change log should be updated as part of a PR if the work is worth noting (most of them should be). If unsure, always add an entery here for any PR targeted for the next release. It's easier to remove than add an entry at final review time for the next release.

## Next Release (Date TBD)

#### New experimental features

#### Features stabilized

#### Bug fixes
- [#568](https://github.com/timescale/timescaledb-toolkit/pull/568): Allow `approx_count` accessor function to take NULL inputs.
- [#574](https://github.com/timescale/timescaledb-toolkit/pull/574): Add default unit to interpolated_integral.

#### Other notable changes
- [#547](https://github.com/timescale/timescaledb-toolkit/pull/547): Update pgx to 0.5.0. This is necessary for adding Postgres 15 support coming soon.
- [#571](https://github.com/timescale/timescaledb-toolkit/pull/571): Update CI docker image for pgx 0.5.0.

#### Shout-outs
- @zyro for reporting null handling issue on count_min_sketch.

**Full Changelog**: 

## [1.11.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.11.0) (2022-09-21)

#### New experimental features

- arm64/aarch64 DEB packages are now available for Ubuntu 20.04 (focal) & 22.04 (jammy), and Debian 10 (buster) & 11 (bulleye).
- [#526](https://github.com/timescale/timescaledb-toolkit/pull/526): Add `integral` and `interpolated_integral` functions for the time_weight aggregate. Makes `trapezoidal` an alias for `linear` in `time_weight` as it might be a more familiar numeric integral method for some.
- [#517](https://github.com/timescale/timescaledb-toolkit/pull/517): Add a gap preserving `lttb` named `gp_lttb` to handle downsampling of data with large gaps.
- [#513](https://github.com/timescale/timescaledb-toolkit/pull/513): Add `first_val`, `last_val`, `first_time` and `last_time` to `time_weight` and `counter_agg` to access the first and the last data points within the aggregate data structures.
- [#527](https://github.com/timescale/timescaledb-toolkit/pull/527): Rename `{open, high, low, close}_at` to `{open, high, low, close}_time` to be consistent with newly added `first_time` and `last_time` accessor functions.

#### Features stabilized
- [#498](https://github.com/timescale/timescaledb-toolkit/pull/498): Stabilize `asap_smooth` aggregate.

#### Bug fixes

- [#509](https://github.com/timescale/timescaledb-toolkit/pull/509), [#531](https://github.com/timescale/timescaledb-toolkit/pull/531): Fix bugs in`hyperloglog`. Error rates are now significantly more consistent when the number of buckets are close to the actual cardinality.
- [#514](https://github.com/timescale/timescaledb-toolkit/pull/514): Fix a bug in `toolkit_experimental.interpolated_delta`.
- [#503](https://github.com/timescale/timescaledb-toolkit/pull/503): Fix bitwise logic in timevector combine.
- [#507](https://github.com/timescale/timescaledb-toolkit/pull/507): Fix a typo in `approx_count_distinct`.

#### Other notable changes
- DEB packages for Ubuntu 18.04 (Bionic) on amd64 are now available.
- [#536](https://github.com/timescale/timescaledb-toolkit/pull/536): Document equirement to use same compiler for cargo-pgx and Toolkit.
- [#535](https://github.com/timescale/timescaledb-toolkit/pull/535): Make tests pass in Canadian locales. 
- [#537](https://github.com/timescale/timescaledb-toolkit/pull/537): Enforce `cargo fmt` in CI.
- [#524](https://github.com/timescale/timescaledb-toolkit/pull/524): Updating Toolkit To Start Using Cargo Fmt.
- [#522](https://github.com/timescale/timescaledb-toolkit/pull/522): Move update-tester tests to markdown files.

#### Shout-outs
- @BenSandeen for fixing typos and errors in the hyperloglog++ implementation.
- @jaskij for reporting security advisories and suggestion on documenting support for PG 14.
- @jeremyhaberman for fixing a typo in `APPROX_COUNT_DISTINCT_DEFAULT_SIZE`.
- @jledentu for reporting an error on `interpolated_delta`.
- @stevedrip for a very detailed bug report on hyperloglog++ and suggestions for fixing it.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.10.1...1.11.0