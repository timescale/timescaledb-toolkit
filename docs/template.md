# FEATURE-NAME [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

- Current status: ( prototype | experimental | stabilizing | stable )
- Effort remaining:  ( little | some | lots )

This is a living document.

## Purpose

- How will this be used?
- What problem is the user trying to solve?
- What kind of SQL are they going to write?
- Is there pure SQL query we are simplifying?

## Use cases

- e.g. single groupings and multiple groupings (not just on `"time"`)

### Test Data

Examples below are tested against the following data:

```SQL ,non-transactional
SET TIME ZONE 'UTC';
CREATE TABLE example(time TIMESTAMPTZ, value DOUBLE PRECISION);
```

TODO It would be nice not to have to front-load this.  It shouldn't be too
hard to mark prereq blocks as such so update-tester can find it and run those
blocks first.

### simple use case

```SQL
```
```output
```

### complex use cases

### edge cases

## Common functionality

For aggregates, list our common function overloads here and how this aggregate
implements them, or why it doesn't.

### rollup

### into_values / unnest

Is there a need to return a set from the aggregate?

## Implementation plan

### Current status

### Next steps

First step is a simple use case in `toolkit_experimental`.

Other steps may include:
- expanded functionality
- adjusting based on user feedback
- optimization

And finally:  stabilization or removal.

## Performance (aspirational)

notes on expectations, current status, future goals

TODO we'll need to document our approach to benchmarking first
talk to other groups (who?  query experience?)

For example if there's a pure SQL way to accomplish a goal and we're just
offering an improvement, we ought to measure both and show the results.

## Alternatives

Be sure to list alternatives considered and how we chose this approach.

```SQL ,ignore
[SQL that doesn't work because we didn't implement it]
```
