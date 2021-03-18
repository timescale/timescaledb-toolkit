# Hyperloglog [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes)

> [Description](#hyperloglog-description)<br>
> [Details](#hyperloglog-details)<br>
> [API](#hyperloglog-api)

## Description [](hyperloglog-description)

Timescale analytics provides an implementation of the [Hyperloglog estimator](https://en.wikipedia.org/wiki/HyperLogLog) for `COUNT DISTINCT` approximations of any type that has a hash function.

## Details [](hyperloglog-details)

Timescale's t-digest is implemented as an aggregate function in PostgreSQL.  They do not support moving-aggregate mode, and are not ordered-set aggregates.  It is restricted to values that have an extended hash function.  They are partializable and are good candidates for [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).


## Command List (A-Z) [](hyperloglog-api)
> - [hyperloglog](#hyperloglog)
> - [hyperloglog_count](#hyperloglog_count)

---
## **hyperloglog** [](hyperloglog)
```SQL,ignore
timescale_analytics_experimental.hyperloglog(
    size INTEGER,
    value AnyElement¹
) RETURNS TDigest
```
¹The type must have an extended (64bit) hash function.

This will construct and return a Hyperloglog with at least the specified number of buckets over the given values.

### Required Arguments [](hyperloglog-required-arguments)
|Name| Type |Description|
|---|---|---|
| `buckets` | `INTEGER` | Number of buckets in the digest. Will be rounded up to the next power of 2, must be between 16 and 2^18. Increasing this will usually provide more accurate at the expense of more storage. |
| `value` | `AnyElement` |  Column to count the distinct elements of. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `hyperloglog` | `Hyperloglog` | A hyperloglog object which may be passed to other hyperloglog APIs. |
<br>

### Sample Usages [](hyperloglog-examples)
For this examples assume we have a table 'samples' with a column 'weights' holding `DOUBLE PRECISION` values.  The following will simply return a digest over that column

```SQL ,ignore
SELECT timescale_analytics_experimental.hyperloglog(64, data) FROM samples;
```

It may be more useful to build a view from the aggregate that we can later pass to other tdigest functions.

```SQL ,ignore
CREATE VIEW digest AS SELECT timescale_analytics_experimental.hyperloglog(64, data) FROM samples;
```

---

## **hyperloglog_count** [](hyperloglog_count)

```SQL ,ignore
timescale_analytics_experimental.hyperloglog_count(hyperloglog Hyperloglog) RETURNS BIGINT
```

Get the number of distinct values from a hyperloglog.

### Required Arguments [](hyperloglog_count-required-arguments)
|Name|Type|Description|
|---|---|---|
| `hyperloglog` | `Hyperloglog` | The hyperloglog to extract the count from. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `hyperloglog_count` | `BIGINT` | The number of distinct elements counted by the hyperloglog. |
<br>

### Sample Usages [](hyperloglog_count-examples)

```SQL
SELECT timescale_analytics_experimental.hyperloglog_count(timescale_analytics_experimental.hyperloglog(64, data))
FROM generate_series(1, 100) data
```
```output
 hyperloglog_count
-------------------
               103
```