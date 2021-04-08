# Changelog #

## 0.2.0 ##

This release contains more [experimental](/extension/docs/README.md#tag-notes)
features, along with additional functionality for prior features.

New [Experimental](/extension/docs/README.md#tag-notes) features:

- [ASAP Smoothing](/extension/docs/asap.md) – A graph smoothing algorithm that
highlights changes.
- [Counter Aggregates](/extension/docs/counter_agg.md) – Tools to ease working
with reset-able counters.
- [Largest Triangle Three Buckets](/extension/docs/lttb.md) – A downsampling
algorithm that tries to preserve visual similarity.
- Time Bucket Range – A version of [`time_bucket()`](https://docs.timescale.com/latest/api#time_bucket) that outputs the `[start, end)` times of the bucket.

In addition, we've updated [UddSketch](/extension/docs/uddsketch.md) with an
aggregate that merges multiple UddSketchs and various internal improvements.


## 0.1.0 ##

First release. Currently all features are
[experimental](/extension/docs/README.md#tag-notes), and may change by future
versions; do not use them if you will unable to handle that. In particular,
the schema containing the experimental features will be dropped and recreated
in the upgrade script, so do not create any objects (views,
continuous aggregates, tables, functions, etc.) that rely on features in the
experimental schema unless you are ok with said objects being dropped on
upgrade.

There are no stable features added in this release.

Notable [Experimental](/extension/docs/README.md#tag-notes)
features in this release include:

- [Hyperloglog](/extension/docs/hyperloglog.md) – An approximate `COUNT DISTINCT`
based on hashing that provides reaonable accuracy in constant space.
- [T-Digest](/extension/docs/tdigest.md) A quantile estimate sketch optimized to provide more accurate estimates near the tails (i.e. 0.001 or 0.995) than
conventional approaches.
- [UddSketch](/extension/docs/uddsketch.md)  – A quantile estimate sketch which
provides a guaranteed maximum relative error.