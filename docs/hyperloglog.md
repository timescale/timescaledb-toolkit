# Hyperloglog [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

> [Description](#hyperloglog-description)<br>
> [Details](#hyperloglog-details)<br>
> [API](#hyperloglog-api)

## Description <a id="hyperloglog-description"></a>

TimescaleDB Toolkit provides an implementation of the [Hyperloglog estimator](https://en.wikipedia.org/wiki/HyperLogLog) for `COUNT DISTINCT` approximations of any type that has a hash function.

## Details <a id="hyperloglog-details"></a>

Timescale's HyperLogLog is implemented as an aggregate function in PostgreSQL.  They do not support moving-aggregate mode, and are not ordered-set aggregates.  It is restricted to values that have an extended hash function.  They are partializable and are good candidates for [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).


## Command List (A-Z) <a id="hyperloglog-api"></a>
> - [hyperloglog](#hyperloglog)
> - [distinct_count](#distinct_count)

---
## **hyperloglog** <a id="hyperloglog"></a>
```SQL,ignore
toolkit_experimental.hyperloglog(
    size INTEGER,
    value AnyElement¹
) RETURNS Hyperloglog
```
¹The type must have an extended (64bit) hash function.

This will construct and return a Hyperloglog with at least the specified number of buckets over the given values.

### Required Arguments <a id="hyperloglog-required-arguments"></a>
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

### Sample Usages <a id="hyperloglog-examples"></a>
For this examples assume we have a table 'samples' with a column 'weights' holding `DOUBLE PRECISION` values.  The following will simply return a digest over that column

```SQL ,ignore
SELECT toolkit_experimental.hyperloglog(64, data) FROM samples;
```

It may be more useful to build a view from the aggregate that we can later pass to other tdigest functions.

```SQL ,ignore
CREATE VIEW digest AS SELECT toolkit_experimental.hyperloglog(64, data) FROM samples;
```

---

## **rollup** <a id="rollup"></a>

```SQL ,ignore
rollup(
    log hyperloglog
) RETURNS Hyperloglog
```

Returns a Hyperloglog by aggregating over the union of the input elements.

### Required Arguments <a id="hyperloglog-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `log` | `Hyperloglog` |  Column of Hyperloglogs to be unioned. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `hyperloglog` | `Hyperloglog` | A hyperloglog containing the count of the union of the input Hyperloglogs. |
<br>

### Sample Usages <a id="summary-form-examples"></a>

```SQL
SELECT toolkit_experimental.distinct_count(toolkit_experimental.rollup(logs))
FROM (
    (SELECT toolkit_experimental.hyperloglog(32, v::text) logs FROM generate_series(1, 100) v)
    UNION ALL
    (SELECT toolkit_experimental.hyperloglog(32, v::text) FROM generate_series(50, 150) v)
) hll;
```
```output
 count
-------
   152
```

---

## **distinct_count** <a id="distinct_count
```SQL ,ignore
toolkit_experimental.distinct_count(hyperloglog Hyperloglog) RETURNS BIGINT
```

Get the number of distinct values from a hyperloglog.

### Required Arguments <a id="distinct_count-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `hyperloglog` | `Hyperloglog` | The hyperloglog to extract the count from. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `distinct_count` | `BIGINT` | The number of distinct elements counted by the hyperloglog. |
<br>

### Sample Usages <a id="distinct_count-examples"></a>

```SQL
SELECT toolkit_experimental.distinct_count(toolkit_experimental.hyperloglog(64, data))
FROM generate_series(1, 100) data
```
```output
 distinct_count
----------------
            114
```

## **stderror** <a id="hyperloglog_stderror"></a>

```SQL ,ignore
toolkit_experimental.stderror(hyperloglog Hyperloglog) RETURNS DOUBLE PRECISION
```

Returns an estimate of the relative stderror of the hyperloglog based on the
hyperloglog error formula. Approximate result are:
```
 precision ┃ registers ┃  error ┃  bytes
━━━━━━━━━━━╋━━━━━━━━━━━╋━━━━━━━━╋━━━━━━━━
         4 ┃        16 ┃ 0.2600 ┃     12
         5 ┃        32 ┃ 0.1838 ┃     24
         6 ┃        64 ┃ 0.1300 ┃     48
         7 ┃       128 ┃ 0.0919 ┃     96
         8 ┃       256 ┃ 0.0650 ┃    192
         9 ┃       512 ┃ 0.0460 ┃    384
        10 ┃      1024 ┃ 0.0325 ┃    768
        11 ┃      2048 ┃ 0.0230 ┃   1536
        12 ┃      4096 ┃ 0.0163 ┃   3072
        13 ┃      8192 ┃ 0.0115 ┃   6144
        14 ┃     16384 ┃ 0.0081 ┃  12288
        15 ┃     32768 ┃ 0.0057 ┃  24576
        16 ┃     65536 ┃ 0.0041 ┃  49152
        17 ┃    131072 ┃ 0.0029 ┃  98304
        18 ┃    262144 ┃ 0.0020 ┃ 196608
```

### Required Arguments <a id="hyperloglog_stderror-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `hyperloglog` | `Hyperloglog` | The hyperloglog to extract the count from. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `stderror` | `BIGINT` | The number of distinct elements counted by the hyperloglog. |
<br>

### Sample Usages <a id="hyperloglog_stderror-examples"></a>

```SQL
SELECT toolkit_experimental.stderror(toolkit_experimental.hyperloglog(64, data))
FROM generate_series(1, 100) data
```
```output
 stderror
----------
     0.13
```