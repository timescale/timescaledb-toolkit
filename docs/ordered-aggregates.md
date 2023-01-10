# Implementing aggregates that require ordered inputs

Some aggregates require that each aggregate to be aggregated cover a non-overlapping period of time, since the way these aggregate items require the items to be sequential. The technique this document describes is specific to aggregates that require sequential ordering. This is useful for some of our `rollup` aggregates, and some other aggregates, such as `counter_agg`.

## Implementation example

Here is the rollup aggregate for `TimeWeightSummary`:
```SQL , ignore
CREATE AGGREGATE rollup(tws TimeWeightSummary)
(
    sfunc = time_weight_summary_trans,
    stype = internal,
    finalfunc = time_weight_final,
    combinefunc = time_weight_combine,
    serialfunc = time_weight_trans_serialize,
    deserialfunc = time_weight_trans_deserialize,
    parallel = restricted
);
```

### Parallel safety
The aggregate above is marked as `parallel = restricted`, which specifies that ["the function can be executed in parallel mode, but the execution is restricted to parallel group leader"](https://www.postgresql.org/docs/current/sql-createfunction.html). Note that only the value of the `parallel` parameter of the `CREATE AGGREGATE` call is used for determining the parallel safety of the aggregate; the parallel safetyness of the support functions that make up the aggregate are ignored when the aggregate is called.

### Merging on serialization

In many cases the implementation of aggregate merging requires that the aggregates to be merged cover non-overlapping periods of time. To handle this while allowing the inputs to be potentially unordered, in the aggregate:
- the transition function appends the input to a `Vec`
- the final function sorts the transition state and merges all of the elements

Storing all of the inputs ever seen in the transition state takes up a lot of memory, and makes the final function use a lot of compute. We can partially alleviate those issues by:

- Adding a `combinefunc` that appends the second transition state `Vec` to the first one
- Adding a `serialfunc` that:
  1. Sorts and merges the transition state
  2. Serializes the transition state
 - Adding a `deserialfunc` that deserializes the transition state

These extra functions improve performance when the inputs are partitioned since each partition is combined, and then the partition combinations are combined again.

`serialfunc` is called right before sending the current transition state from the parallel worker to the parent process, so it's the only place where we can do the sorting/merging of the transition state before it gets sent to the parent process. We do the merging in the parallel worker to reduce the amount of data sent from the parallel worker to the parent process.

![Each group of days is sorted and merged, then each group is sorted and merged](images/pgmerging.svg)

This method doesn't work when two partitions contain overlapping time ranges. That shouldn't happen when the partitions are chunks of a TimescaleDB hypertable, but it could happen when the partitions cover overlapping segments of time (e.g. a table that uses declarative partitioning to partition a table using the hash of an ID). When two partitions contain overlapping time ranges, the implementation should catch that and give an error.

Note that this approach means that `deserialfunc(serialfunc(x)) != x`, which is weird but doesn't seem to cause any problems.

## Ordered-set aggregates

PostgreSQL supports [ordered-set aggregates](https://www.postgresql.org/docs/15/xaggr.html#XAGGR-ORDERED-SET-AGGREGATES), which can be used to implement a very similar thing. We don't use it because it would result in a more confusing interface for most aggregates: `SELECT rollup(agg) FROM aggs` versus `SELECT rollup() WITHIN GROUP (ORDER BY agg) FROM aggs`. 

PostgreSQL doesn't sort the values of an ordered-set aggregate for it: the transition function may recieve the inputs in any order, and the final function is responsible for sorting them. For our use cases, ordered-set aggregates would provide a more confusing interface with minimal benefits.
