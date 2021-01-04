# Timescale Analytics Documentation
---
The Timescale Analytics project contains a number of utilities for working with time-series data.  This documentation is further broken down by utility or feature in the list [below](#analytics-features).

## A note on tags
Functionality within the Timescale Analytics repository is intended to be introduced in varying stages of completeness.  To clarify which releases a given feature or function can be found in, the following tags are used:
 - Experimental - Denotes functionality that is still under very active development and may have poor performance, not handle corner cases or errors, etc.  Experimental APIs can be expected to change, or even completely dropped.  Experimental features and functions can be found in the Analytics nightly build, but will not be included in releases.
 - Stable ***release id*** - Functionality in this state should be correct and performant.  Stable APIs will be found in our releases and should not be broken in future releases.  Note that this tag will also be accompanied with the version in which the feature was originally released, such as: Feature Foo<sup><mark>stable-1.2</mark></sup>.
 - Deprecated - It may be necessary to remove stable functionality at some point, for instance if it is being supplanted by newer functionality or if it has deprecated dependencies.  Functionality with this tag is expected to be removed in future releases and current users of it should move to alternatives.

Note that tags can be applied at either a feature or function scope.  The function tag takes precedence, but defaults to the feature scope if not present.  For example, if we have a feature `Foo` which is tagged `stable`, we would assume that an untagged function `FooCount` within that feature would be present in the current beta release.  However, if function `FooSum` were explicitly tagged `experimental` then we would only expect to find it in the nightly build.

## Analytics features [](analytics-features)

The following links lead to pages for the different features in the Timescale Analytics repository.

- [Hyperloglog](tdigest) <sup><mark>experimental</mark></sup> – An approximate `COUNT DISTINCT` based on hashing that provides reaonable accuracy in constant space. ([Methods](hyperloglog#hyperloglog_api))
 - [T-Digest](tdigest) <sup><mark>experimental</mark></sup> – A quantile estimate sketch optimized to provide more accurate estimates near the tails (i.e. 0.001 or 0.995) than conventional approaches. ([Methods](tdigest#tdigest_api))

[tdigest]: /extension/docs/tdigest.md
[hyperloglog]: /extension/docs/hyperloglog.md