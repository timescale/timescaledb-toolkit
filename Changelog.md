# Changelog #

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