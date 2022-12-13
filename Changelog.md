# Toolkit Changelog

## Process for updating this changelog

This changelog should be updated as part of a PR if the work is worth noting (most of them should be). If unsure, always add an entry here for any PR targeted for the next release. It's easier to remove than add an entry at final review time for the next release.

## Next Release (Date TBD)

#### New experimental features
- [#615](https://github.com/timescale/timescaledb-toolkit/pull/615): Heatbeat aggregate
  Users can use the new `heartbeat_agg(timestamp, start_time, agg_interval, heartbeat_interval)` to track the liveness of a system in the range (`start_time`, `start_time` + `agg_interval`). Each timestamp seen in that range is assumed to indicate system liveness for the following `heartbeat_interval`.

  Once constructed, users can query heartbeat aggregates for `uptime` and `downtime`, as well as query for `live_ranges` or `dead_ranges`. Users can also check for `live_at(timestamp)`.

  Heartbeat aggregates can also interpolated to better see behavior around the boundaries of the individual aggregates.

- [#620](https://github.com/timescale/timescaledb-toolkit/pull/620): Expose TDigest type

  This is a prototype for building `TDigest` objects client-side, for `INSERT` into tables.

  This is a lightly tested prototype; try it out at your own risk!

  [Examples](docs/examples/)

- [#635](https://github.com/timescale/timescaledb-toolkit/pull/635): AsOf joins for timevectors

  This allows users to join two timevectors with the following semantics `timevectorA -> asof(timevectorB)`. This will return records with the LOCF value from timevectorA at the timestamps from timevectorB. Specifically the returned records contain, for each value in timevectorB, {the LOCF value from timevectorA, the value from timevectorB, the timestamp from timevectorB}.

- [#609](https://github.com/timescale/timescaledb-toolkit/pull/609): New `approx_percentile_array()` function

  Users can use the new `toolkit_experimental.approx_percentile_array(percentiles)` to generate an array of percentile results instead of having to call and rebuild the aggregate multiple times.

- [#636](https://github.com/timescale/timescaledb-toolkit/pull/636): New `timeline_agg` aggregate, which is similar to `state_agg` but tracks the entire state timeline instead of just the duration in each state.

- [#640](https://github.com/timescale/timescaledb-toolkit/pull/640): Support `rollup` for `state_agg` and `timeline_agg`.
- [#640](https://github.com/timescale/timescaledb-toolkit/pull/640): Support integer states for `state_agg` and `timeline_agg`.

- [#638](https://github.com/timescale/timescaledb-toolkit/pull/638): Introducing Time Vector Templates.

Users can use the new experimental function `toolkit_experimental.to_text(timevector(time, value),format_string)` to render a formatted text representation of their time vector series. These changes also include `toolkit_experimental.to_plotly(timevector(time, value))`, which will render your time vector series in a format suitable for use with plotly.

#### Bug fixes
- [#644](https://github.com/timescale/timescaledb-toolkit/pull/644): Fix bug in Candlestick aggregate and reenable partial aggregation.

#### Other notable changes
- [#646](https://github.com/timescale/timescaledb-toolkit/pull/646): Added experimental support for PostgreSQL 15.
- [#621](https://github.com/timescale/timescaledb-toolkit/pull/621): Rocky Linux 9 support

#### Shout-outs

**Full Changelog**: [TODO]

## [1.12.1](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.12.1) (2022-11-17)

#### Bug fixes
- [#624](https://github.com/timescale/timescaledb-toolkit/pull/624): Remove partial aggregation for Candlestick aggregates.
  We've determined that the cause for the bad results lives somewhere in the functions that are used to support partial aggregation.
  We can at least prevent folks from running the candlestick aggregates in parallel mode and hitting this bug by dropping support for partial aggregation until we've resolved the issue.

## [1.12.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.12.0) (2022-11-08)

#### New experimental features
- [#596](https://github.com/timescale/timescaledb-toolkit/pull/596): Introduce Candlestick Aggregate.
  Users can use either the `toolkit_experimental.candlestick_agg(timestamp, price, volume)` aggregate or the `toolkit_experimental.candlestick(timestamp, open, high, low, close, volume)` function, depending on whether they are starting from tick data or already aggregated data.
  Both the aggregate form and the function form of `Candlestick` support the following (experimental) accessors (in addition to being re-aggregated via `rollup`):
  `open`, `high`, `low`, `close`, `open_time`, `high_time`, `low_time`, `close_time`, `volume`, `vwap` (Volume Weighted Average Price)
  *NOTE*: This functionality improves upon and replaces the need for `toolkit_experimental.ohlc` which will be removed in the next release.

- [#590](https://github.com/timescale/timescaledb-toolkit/pull/590): New `min_n`/`max_n` functions and related `min_n_by`/`max_n_by`.
  The former is used to get the top N values from a column while the later will also track some additional data, such as another column or even the entire row.
  These should give the same results as a `SELECT ... ORDER BY ... LIMIT n`, except they can be composed and combined like other toolkit aggregates.

#### Bug fixes

- [#568](https://github.com/timescale/timescaledb-toolkit/pull/568): Allow `approx_count` accessor function to take NULL inputs.
- [#574](https://github.com/timescale/timescaledb-toolkit/pull/574): Add default unit to interpolated_integral.

#### Other notable changes

- RPM packages for CentOS 7 have returned.
- New Homebrew formula available for macOS installation: `brew install timescale/tap/timescaledb-toolkit`.
- [#547](https://github.com/timescale/timescaledb-toolkit/pull/547): Update pgx to 0.5.0. This is necessary for adding Postgres 15 support coming soon.
- [#571](https://github.com/timescale/timescaledb-toolkit/pull/571): Update CI docker image for pgx 0.5.0.
- [#599](https://github.com/timescale/timescaledb-toolkit/pull/599): Reduce floating point error when using `stats_agg` in moving aggregate mode.
- [#589](https://github.com/timescale/timescaledb-toolkit/pull/589): Update pgx to 0.5.4.
- [#594](https://github.com/timescale/timescaledb-toolkit/pull/594): Verify that pgx doesn't generate CREATE OR REPLACE FUNCTION.
- [#592](https://github.com/timescale/timescaledb-toolkit/pull/592): Add build script option to install in release mode.

#### Shout-outs

- @zyro for reporting null handling issue on `count_min_sketch`.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.11.0...1.12.0

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

**An incorrect version (1.10.0-dev) was used which can cause upgrade failures. Not made GA.**

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
- [#417](https://github.com/timescale/timescaledb-toolkit/pull/417): Include both pgx 0.2.x and pgx 0.4.x in CI image.
- [#416](https://github.com/timescale/timescaledb-toolkit/pull/416): Prepare for the 1.8.0 cycle.
- [#418](https://github.com/timescale/timescaledb-toolkit/pull/418): Made update-tester require two versions of cargo-pgx.
- [#421](https://github.com/timescale/timescaledb-toolkit/pull/421): Don't install pgx as root or under "/".
- [#427](https://github.com/timescale/timescaledb-toolkit/pull/427): Fix failing update-tester in CI.
- [#428](https://github.com/timescale/timescaledb-toolkit/pull/428): Update github cache keys.
- [#430](https://github.com/timescale/timescaledb-toolkit/pull/430): Lock pgx versions all the way.
- [#408](https://github.com/timescale/timescaledb-toolkit/pull/408): Upgrade to pgx 0.4.5.
- [#436](https://github.com/timescale/timescaledb-toolkit/pull/436): Change which cargo-pgx subcommand is added to PATH in CI image.
- [#432](https://github.com/timescale/timescaledb-toolkit/pull/432): Remove PATH hack in tools/build script.
- [#437](https://github.com/timescale/timescaledb-toolkit/pull/437): GitHub Actions improvements.
- [#448](https://github.com/timescale/timescaledb-toolkit/pull/448): Run clippy GitHub Actions job without qualification.
- [#446](https://github.com/timescale/timescaledb-toolkit/pull/446): Update README.md.
- [#414](https://github.com/timescale/timescaledb-toolkit/pull/414): Specify Ubuntu 20.04 instead of 'latest' in github configuration.

#### Shout-outs

- @tyhoff for reporting UDDSketch assertion error [#396](https://github.com/timescale/timescaledb-toolkit/issues/396).
- @hardikm10 for reporting hyperloglog deserialization issue [#443](https://github.com/timescale/timescaledb-toolkit/issues/443).

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.7.0...1.8.0

## [1.7.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.7.0) (2022-05-10)

#### New experimental features

- [#389](https://github.com/timescale/timescaledb-toolkit/pull/389): Create typed specialization for `freq_agg` and `topn_agg`.

#### Bug fixes

- [#378](https://github.com/timescale/timescaledb-toolkit/pull/378): Return INTERVAL from `duration_in(TEXT, StateAgg)` instead of `i64`.
- [#379](https://github.com/timescale/timescaledb-toolkit/pull/379): Handle NULL output from our aggregates: `asap`, `counter_agg`, `freq_agg`, `gauge_agg`, `hyperloglog`, `lttb`, `stats_agg`, `tdigest`, `uddsketch`.

#### Other notable changes
- [#367](https://github.com/timescale/timescaledb-toolkit/pull/367): Switch stabilization tests to new info, meaning that there's one central location for stabilization info.
- [#372](https://github.com/timescale/timescaledb-toolkit/pull/372): Improve tools/build flexibility for local builds.
- [#394](https://github.com/timescale/timescaledb-toolkit/pull/394): Copy almost all the counter_agg functions for gauge_agg.
- [#395](https://github.com/timescale/timescaledb-toolkit/pull/395): Remove GUC as they are no longer needed.
- [#399](https://github.com/timescale/timescaledb-toolkit/pull/399): Allow manual packaging.
- [#405](https://github.com/timescale/timescaledb-toolkit/pull/405): Update CI to rust 1.60.
- [#407](https://github.com/timescale/timescaledb-toolkit/pull/407): Update postgres versions in ci Dockerfile.
- [#409](https://github.com/timescale/timescaledb-toolkit/pull/409): Make depencencies version explicit in our CI image.
- [#404](https://github.com/timescale/timescaledb-toolkit/pull/404): Refactor TimeVector to greatly simplify structure.
- [#412](https://github.com/timescale/timescaledb-toolkit/pull/412): Allow building CI image in Actions.
- [#411](https://github.com/timescale/timescaledb-toolkit/pull/411), [#413](https://github.com/timescale/timescaledb-toolkit/pull/413): Create reportpackagingfailures.yml for reporting packaging failures not from CI builds.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.6.0...1.7.0

## [1.6.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.6.0) (2022-03-29)

#### New experimental features

- [#330](https://github.com/timescale/timescaledb-toolkit/pull/330): Add serialization for FrequencyTransState.
- [#368](https://github.com/timescale/timescaledb-toolkit/pull/368): Add `into_values` function for `state_agg`.
- [#370](https://github.com/timescale/timescaledb-toolkit/pull/370): Add a `topn (topn_agg)` variant of `freq_agg`, which is more convenient to use.
- [#375](https://github.com/timescale/timescaledb-toolkit/pull/375): Add `gauge_agg` and associated accessor functions `delta`, `idelta_left`, `idelta_right`, and the `rollup` function.

#### Other notable changes

- [#332](https://github.com/timescale/timescaledb-toolkit/pull/332): Speed up builds by fixing github action cache and cargo build cache.
- [#377](https://github.com/timescale/timescaledb-toolkit/pull/377): Stop auto building _nightly_ image.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.5.2...1.6.0

## [1.5.2](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.5.2) (2022-03-07)

**HIGH PRIORITY SECURITY UPDATE**.

#### Bug fixes

- There's a vulnerability in Toolkit 1.5 and earlier due to the fact that it creates a PLPGSQL function using CREATE OR REPLACE and without properly locking down the search path. This means that a user could pre-create the trigger function to run arbitrary code. To fix this we remove the trigger entirely; it no longer pulls its weight. This fix locks down our update scripts to only use CREATE OR REPLACE when actually necessary; while we don't yet have an exploit for the other functions, it would be unsurprising if one exists.
- [#351](https://github.com/timescale/timescaledb-toolkit/pull/351): Make serialize functions strict to handle NULL values in partitioned aggregates.

#### Shout-outs

- @svenklemm for reporting the vulnerability.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.5.0...1.5.2

## [1.5.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.5.0) (2022-01-31)

**The first version that unifies the community build with Timescale Cloud build.**

#### New experimental features

- `freq_agg` for estimating the most common elements in a column.
- `state_agg` for measuring the total time spent in different states.

#### Other notable changes

- Enforce clippy linting.
- Update rust to 1.57.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.4.0...1.5.0

## [1.4.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.4.0), [1.4.0-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.4.0-cloud) (2021-11-17)

#### Stabilized features

- Postgres 14 support.

#### Other notable changes

- Upgrade pgx to 0.2.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.3.1...1.4.0-cloud

## [1.3.1](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.3.1), [1.3.1-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.3.1-cloud) (2021-10-27)

#### Stabilized features

- Postgres 14 support.

#### Other notable changes

- Upgrade pgx to 0.2.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.3.0...1.3.1-cloud

## [1.3.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.3.0), [1.3.0-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.3.0-cloud) (2021-10-18)

#### New experimental features

- `timevector` function pipelines: a compact and more readable way to perform a sequence of analytic operations such as the following one,
    ```
    timevector(ts, val) -> sort() -> delta() -> abs() -> sum()
    ```
- `->` accessor for Toolkit types enables syntax like `stats_agg(data) -> average()`.
- `to_epoch()` wrapper for `extract ('EPOCH' FROM timestamp)` that makes it work more like an inverse of `to_timestamp(DOUBLE PRECISION)`.
#### Stabilized features

- `counter_agg` helper functions for Prometheus-style resetting monotonic counters.
- `hyperloglog` efficient approximate COUNT DISTINCT.
- `stats_agg` two-step aggregate for common statistics.

#### Other notable changes

- This release changes the textual I/O format for Toolkit types. We are uncertain if we will need to do so again in the future. Due to this we currently only support dump/restore within a single version of the extension.

#### Shout-outs

- @jonatas for the contribution [#237](https://github.com/timescale/timescaledb-toolkit/pull/237).
- @burmecia for the contribution [#251](https://github.com/timescale/timescaledb-toolkit/pull/251).

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.2.0...1.3.0-cloud

## [1.2.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.2.0), [1.2.0-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.2.0-cloud) (2021-09-14)

#### New experimental features

- Refinements to `hyperloglog` including a function to report relative error and fixing the functionality of `rollup`.
- Introduction of a `topn` approximation API. Presently this will only work for integer data, but expect to see further refinements that greatly expand this behavior.
- New `map_series` and `map_data` pipeline elements for the time series API that allow uses to provide custom transforms of their time series data. Additionally introduced a `|>>` pipeline operator for an even more streamlined interface into the new mapping functionality.

#### Bug fixes

- Make a pass through all toolkit functions to correctly label behavior as immutable and parallel safe. This should improve the optimizations Postgres can apply to toolkit plans, particularly when run in a Timescale multinode cluster.
- Improve handling of internal data structures to reduce extraneous copies of data.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.1.0...1.2.0-cloud

## [1.1.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.1.0), [1.1.0-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.1.0-cloud) (2021-08-04)

#### New experimental features

- `hyperloglog` has been updated to use Hyperloglog++ under the hood. This does not change the user-facing API but should improve the accuracy of hyperloglog() estimates. This is the last major change expected for hyperloglog() and is now a candidate for stabilization pending user feedback.
- We've started experimenting with the pipeline API. While it's still very much a work in progress, it's at a point where the high-level concepts should be understandable. For example, a pipeline that outputs the daily change of a set of data, interpolating away any gaps in daily data, could look like
    ```
    SELECT timeseries(time, val)
        |> sort()
        |> resample_to_rate('trailing_average', '24 hours', true)
        |> fill_holes('interpolate')
        |> delta()
    FROM ...
    ```
    It's still early days for this API and it is not yet polished, but we would love feedback about its direction.

#### Bug fixes

- Fix a small memory leak in aggregation functions. This could have leaked ≈8 bytes per aggregate call.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/1.0.0...1.1.0-cloud

## [1.0.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.0.0), [1.0.0-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/1.0.0-cloud) (2021-07-12)

**This release renames the extension to `TimescaleDB Toolkit` from Timescale Analytics and starts stabilizing functionality.**

#### New experimental features

- `stats_agg()` eases the analysis of more sophisticated bucketed statistics, such as rolling averages. (Docs are forthcoming, until then fell free to peruse the design discussion doc.
- `timeseries` which will serve as a building block for many pipelines, and unifies the output of lttb and ASAP.

#### Stabilized features

- Percentile-approximation algorithms including `percentile_agg()`, `uddsketch()` and `tdigest()` along with their associated functions. These are especially useful for computing percentiles in continuous aggregates.
- [Time-weighted average](https://github.com/timescale/timescaledb-toolkit/blob/main/docs/time_weighted_average.md) along with its associated functions. This eases taking the average over an irregularly spaced dataset that only includes changepoints.

#### Other notable changes

- The on-disk layout `uddsketch` has be reworked to store buckets compressed. This can result in an orders-of-magnitude reduction in it's storage requirements.
- The textual format `uddsketch` has been reworked to be more readable.
- Functions that take in a `uddsketch` or `tdigest` have been reworked to be 0-copy when applicable, improving the performance of such functions by 10-100x.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/0.3.0...1.0.0-cloud

## [0.3.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/0.3.0), [0.3.0-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/0.3.0-cloud) (2021-06-17)

#### Other notable changes

- Internal improvements.
- Largely prep work for the upcoming 1.0 release.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/0.2.0...0.3.0-cloud

## [0.2.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/0.2.0) (2021-04-08), [0.2.0-cloud](https://github.com/timescale/timescaledb-toolkit/releases/tag/0.2.0-cloud) (2021-04-29)

#### New experimental features

- ASAP Smoothing (`asap_smooth`) – A graph smoothing algorithm that highlights changes.
- Counter Aggregates (`counter_agg`) – Tools to ease working with reset-able counters.
- Largest Triangle Three Buckets (`lttb`) – A downsampling algorithm that tries to preserve visual similarity.
- Time Bucket Range – A version of `time_bucket()` that outputs the [start, end) times of the bucket.
- Update `UddSketch` with an aggregate that merges multiple `UddSketchs` and various internal improvements.

**Full Changelog**: https://github.com/timescale/timescaledb-toolkit/compare/0.1.0...0.2.0-cloud

## [0.1.0](https://github.com/timescale/timescaledb-toolkit/releases/tag/0.1.0) (2021-03-03)

#### New experimental features

- `hyperloglog` – An approximate COUNT DISTINCT based on hashing that provides reasonable accuracy in constant space.
- `tdigest` – A quantile estimate sketch optimized to provide more accurate estimates near the tails (i.e. 0.001 or 0.995) than conventional approaches.
- `uddsketch` – A quantile estimate sketch which provides a guaranteed maximum relative error.
- Time-weighted average (`time_weight`) – A time-weighted averaging function to determine the value of things proportionate to the time they are set.

#### Stabilized features

- None. All features are experimental.
