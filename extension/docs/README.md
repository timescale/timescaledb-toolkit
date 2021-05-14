# Timescale Analytics Documentation
---
The Timescale Analytics project contains a number of utilities for working with time-series data.  This documentation is further broken down by utility or feature in the list [below](#analytics-features).

## A note on tags <a id="tag-notes"></a>
Functionality within the Timescale Analytics repository is intended to be introduced in varying stages of completeness.  To clarify which releases a given feature or function can be found in, the following tags are used:
 - **Experimental** - Denotes functionality that is still under very active development and may have poor performance, not handle corner cases or errors, etc.  Experimental APIs will change across releases, and extension-update will drop database objects that depend on experimental features. Do not use them unless you're willing ot deal with the object you've created (the view, table, continuous aggregates, function, etc.) being dropped on update. This is particularly important for managed cloud services (like Timescale Forge) that automate upgrades. Experimental features and functions can be found exclusively in the `timescale_analytics_experimental` schema.
 - **Stable** ***release id*** - Functionality in this state should be correct and performant.  Stable APIs will be found in our releases and should not be broken in future releases.  Note that this tag will also be accompanied with the version in which the feature was originally released, such as: Feature Foo<sup><mark>stable-1.2</mark></sup>.
 - **Deprecated** - It may be necessary to remove stable functionality at some point, for instance if it is being supplanted by newer functionality or if it has deprecated dependencies.  Functionality with this tag is expected to be removed in future releases and current users of it should move to alternatives.

Note that tags can be applied at either a feature or function scope.  The function tag takes precedence, but defaults to the feature scope if not present.  For example, if we have a feature `Foo` which is tagged `stable`, we would assume that an untagged function `FooCount` within that feature would be present in the current beta release.  However, if function `FooSum` were explicitly tagged `experimental` then we would only expect to find it in the nightly build.

## Analytics features <a id="analytics-features"></a>

The following links lead to pages for the different features in the Timescale Analytics repository.

- [ASAP Smoothing](asap.md) [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes) - A data smoothing algorithm designed to generate human readable graphs which maintain any erratic data behavior while smoothing away the cyclic noise.
- [Hyperloglog](hyperloglog.md) [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes) – An approximate `COUNT DISTINCT` based on hashing that provides reaonable accuracy in constant space. ([Methods](hyperloglog.md#hyperloglog_api))
- [LTTB](lttb.md) [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes) – A downsample method that preserves visual similarity. ([Methods](lttb.md#api))

- [Percentile Approximation](percentile_approximation.md) - A simple percentile approximation interface [([Methods](percentile_approximation.md#api))], wraps and simplifies the lower level algorithms:
    - [T-Digest](tdigest.md) – A quantile estimate sketch optimized to provide more accurate estimates near the tails (i.e. 0.001 or 0.995) than conventional approaches. ([Methods](tdigest#tdigest_api))
    - [UddSketch](uddsketch.md) – A quantile estimate sketch which provides a guaranteed maximum relative error. ([Methods](uddsketch.md#uddsketch_api))