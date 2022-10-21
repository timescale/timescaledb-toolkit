# Toolkit Changelog

## Process for updating this changelog
This changelog should be updated as part of a PR if the work is worth noting (most of them should be). If unsure, always add an entry here for any PR targeted for the next release. It's easier to remove than add an entry at final review time for the next release.

## Next Release (Date TBD)

#### New experimental features

#### Stabilized features

#### Bug fixes
- [#568](https://github.com/timescale/timescaledb-toolkit/pull/568): Allow `approx_count` accessor function to take NULL inputs.
- [#574](https://github.com/timescale/timescaledb-toolkit/pull/574): Add default unit to interpolated_integral.

#### Other notable changes
- [#547](https://github.com/timescale/timescaledb-toolkit/pull/547): Update pgx to 0.5.0. This is necessary for adding Postgres 15 support coming soon.
- [#571](https://github.com/timescale/timescaledb-toolkit/pull/571): Update CI docker image for pgx 0.5.0.

#### Shout-outs
- @zyro for reporting null handling issue on count_min_sketch.

**Full Changelog**: <TODO>

## [1.11.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.11.0) (2022-09-21)

#### New experimental features

- arm64/aarch64 DEB packages are now available for Ubuntu 20.04 (focal) & 22.04 (jammy), and Debian 10 (buster) & 11 (bulleye).
- [#526](https://github.com/timescale/timescaledb-toolkit/pull/526): Add `integral` and `interpolated_integral` functions for the time_weight aggregate. Makes `trapezoidal` an alias for `linear` in `time_weight` as it might be a more familiar numeric integral method for some.
- [#517](https://github.com/timescale/timescaledb-toolkit/pull/517): Add a gap preserving `lttb` named `gp_lttb` to handle downsampling of data with large gaps.
- [#513](https://github.com/timescale/timescaledb-toolkit/pull/513): Add `first_val`, `last_val`, `first_time` and `last_time` to `time_weight` and `counter_agg` to access the first and the last data points within the aggregate data structures.
- [#527](https://github.com/timescale/timescaledb-toolkit/pull/527): Rename `{open, high, low, close}_at` to `{open, high, low, close}_time` to be consistent with newly added `first_time` and `last_time` accessor functions.

#### Stabilized features
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

## [1.10.1](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.10.1) (2022-08-18)

#### New experimental features
- [#490](https://github.com/timescale/timescaledb-toolkit/pull/490): Month normalization function `month_normalize` and the helper function `days_in_month`, useful for normalizing data to a fixed month length for more meaningful month-to-month comparison.
- [#496](https://github.com/timescale/timescaledb-toolkit/pull/496): `OHLC` aggregate, and the associated `rollup` and accessor functions `open`, `high`, `low`, `close`, `{open, high, low, close}_at` mainly for trading data.

#### Stabilized features
- [#495](https://github.com/timescale/timescaledb-toolkit/pull/495): `LTTB` downsampling function.
- [#491](https://github.com/timescale/timescaledb-toolkit/pull/491), [#488](https://github.com/timescale/timescaledb-toolkit/pull/488): The arrow operators (->) of the accessor functions for `stats_agg`, `percentile_agg`, `counter_agg`, `gauge_agg` and `hyperloglog`. As an example, `average` accessor can now be used with `stats_agg` like this,
    ```SQL
    select location, 
        stats_agg(temperature) -> average() AS avg_temperature
    from conditions 
    group by location
    ```
#### Bug fixes
- [#465](https://github.com/timescale/timescaledb-toolkit/pull/465): Off by one error in state_agg interpolate.

#### Other notable changes
- Fix an issue where the 1.9.0 release unintentionally identified the toolkit extension version as 1.10.0-dev in the postgresql control file.
- [#467](https://github.com/timescale/timescaledb-toolkit/pull/467): Document supported platforms in Readme.
- [#463](https://github.com/timescale/timescaledb-toolkit/pull/463): Use pg14 as an example for instructions in  instead of pg13. Add reference to deb and rpm packages.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.8.0...1.10.1

## [1.9.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.9.0) (2022-08-16)
An incorrect version (1.10.0-dev) was used. Should not be used.

## [1.8.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.8.0) (2022-07-05)

#### New experimental features
- [#454](https://github.com/timescale/timescaledb-toolkit/pull/454): Saturating Math for i32/integers:
    - `saturating_add`
    - `saturating_add_pos`
    - `saturating_sub`
    - `saturating_sub_pos`
    - `saturating_mul`
- [#456](https://github.com/timescale/timescaledb-toolkit/pull/456): Adding interpolating accessors:
    - `interpolated_duration_in` to `state_agg`, 
    - `interpolated_average` to `time_weight`, `interpolated_delta`
    - `interpolated_rate` to `counter_agg` and `gauge_agg`.
- [#388](https://github.com/timescale/timescaledb-toolkit/pull/388): Create Count-Min Sketch crate.
- [#459](https://github.com/timescale/timescaledb-toolkit/pull/459): Add a convenient `approx_count_distinct` function which internally uses hyperloglog with a default bucket size of 2^15.
- [#458](https://github.com/timescale/timescaledb-toolkit/pull/458): Add `count_min_sketch` aggregate and `approx_count` accessor.
- [#434](https://github.com/timescale/timescaledb-toolkit/pull/434): Initial changes to support aarch64-unknown-linux-gnu.

#### Bug fixes
- [#429](https://github.com/timescale/timescaledb-toolkit/pull/429): Support explicit NULL values in timevectors.
- [#441](https://github.com/timescale/timescaledb-toolkit/pull/441): Relax tolerance in UDDSketch merge assertions.
- [#444](https://github.com/timescale/timescaledb-toolkit/pull/444): Fix default collation deserialization.

#### Other notable changes
- [#451](https://github.com/timescale/timescaledb-toolkit/pull/451): Improve error message for HyperLogLog.

#### Shout-outs
- @tyhoff for reporting UDDSketch assertion error [#396](https://github.com/timescale/timescaledb-toolkit/issues/396).
- @hardikm10 for reporting hyperloglog deserialization issue [#443](https://github.com/timescale/timescaledb-toolkit/issues/443).

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.7.0...1.8.0

