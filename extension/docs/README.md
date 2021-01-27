# Timescale Analytics Documentation
---
The Timescale Analytics project contains a number of utilities for working with time-series data.  This documentation is further broken down by utility or feature in the list [below](#analytics-features).

## A note on tags
Functionality within the Timescale Analytics repository is intended to be introduced in varying stages of completeness.  To clarify which releases a given feature or function can be found in, the following tags are used:
 - Experimental - Denotes functionality that is still under very active development and may have poor performance, not handle corner cases or errors, etc.  Experimental APIs can be expected to change, or even completely dropped.  Experimental features and functions can be found in the Analytics nightly build, but will not be included in releases.
 - Beta - Denotes functionality that has matured enough to be included in a release, but may not be suitable for production environments.  Such functionality is expected to work in  anticipated use cases but users may encounter issues.  Beta APIs should be stable, though rarely modifications may become necessary.  ***TODO: (included in relase? separate namespace?)***
 - Stable - Functionality in this state should be correct and performant.  Stable APIs should not be broken in future releases.
 - Deprecated - It may be necessary to remove stable functionality at some point, for instance if it is being supplanted by newer functionality or if it has deprecated dependencies.  Functionality with this tag is expected to be removed in future releases and current users of it should move to alternatives.

Note that tags can be applied at either a feature or function scope.  The function tag takes precedence, but defaults to the feature scope if not present.  For example, if we have a feature `Foo` which is tagged `beta`, we would assume that an untagged function `FooCount` within that feature would be present in the current beta release.  However, if function `FooSum` were explicitly tagged `experimental` then we would only expect to find it in the nightly build.

## Analytics features [](analytics-features)

The following links lead to pages for the different features in the Timescale Analytics repository.

 - [T-Digest](tdigest) <sup><mark>experimental</mark></sup> ([API](tdigest_api))- A quantile estimate sketch optimized to provide more accurate estimates near the tails (i.e. 0.001 or 0.995) than conventional approaches.

[tdigest]: /extension/docs/tdigest.md
[tdigest_api]: /extension/docs/tdigest_api.md