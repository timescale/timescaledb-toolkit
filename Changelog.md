# Toolkit Changelog

## Process for updating this change log (Proposal)
This change log should be updated as part of a PR if the work is something that's worth noting. If unsure, always add an entery here for any PR. It's easy to remove an entry at final review time before the release.


## 1.12.0 (WIP)

### New experimental features



### Features stabilized



### Bug fixes
[#568](https://github.com/timescale/timescaledb-toolkit/pull/568): Allow `approx_count` accessor function to take NULL inputs.


### Other notable changes
- [#547](https://github.com/timescale/timescaledb-toolkit/pull/547): Update pgx to 0.5.0. This is necessary for adding Postgres 15 support coming soon.
- [#571](https://github.com/timescale/timescaledb-toolkit/pull/571): Update CI docker image for pgx 0.5.0


### Shout-outs:
- @zyro for reporting null handling issue on count_min_sketch
