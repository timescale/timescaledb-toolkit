# TopN [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

> [Description](#topn-description)<br>
> [Details](#topn-details)<br>
> [API](#topn-api)

## Description <a id="topn-description"></a>

TimescaleDB Toolkit provides an experimental API for use in approximating the TopN most common elements in a data set, and their frequencies.  Currently this functionality is limited to just integer data, but this limitation is expected to be short lived.

In order to get a good estimate, it's important to size the TopN structure appropriately.  While finding a good sizing function will be a focus of ours during the experimental phase of this functionality (ultimately we'd like to be able to dynamically grow with the size of the data set), for the time being we recommend sizing to a factor of 100x the number of elements ultimately desired (so size to 10000 to be able to generate a top 100 estimate).

## Details <a id="topn-details"></a>

Currently Timescale's TopN is implemented using the [SpaceSaving algorithm](https://cs.ucsb.edu/sites/default/files/documents/2005-23.pdf).  Further work before stabilization will be to evaluate this algorithm against other TopN algorithms.


## Command List (A-Z) <a id="topn-api"></a>
Aggregate Functions
> - [topn_agg (point form)](#topn-agg)
> - [rollup (summary form)](#topn-summary)

Accessor Functions
> - [topn](#topn_topn)
> - [num_vals](#topn_num_vals)
> - [guaranteed_topn](#topn_guaranteed_topn)
> - [max_ordered_n](#topn_max_ordered_n)

---
## **topn** <a id="topn-agg"></a>
```SQL,ignore
toolkit_experimental.topn(
    size INTEGER,
    value INTEGER
) RETURNS topn
```

This will construct and return a topn object with the specified size over the given values.  The size here corresponds to the maximum number of elements tracked and should be much larger than the N elements ultimately queried for.  While the size needed will vary based upon the population distribution, a good starting point is 100x the desired query size.  So the top 100 values are needed, 10000 should be a good size for the aggregate.

### Required Arguments <a id="topn-agg-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `size` | `INTEGER` | Number of elements to track, this should be much larger then the number of elements ultimately queried for using [topn](#topn_topn). |
| `value` | `INTEGER` |  Column to count the distinct elements of.  Currently restricted to integer only, but we hope to relax that constraint. |
<br>

### Returns <a id="topn-agg-returns"></a>

|Column|Type|Description|
|---|---|---|
| `topn` | `topn` | A TopN object which may be passed to other TopN APIs. |
<br>

### Sample Usages <a id="topn-agg-examples"></a>
For this examples assume we have a table 'samples' with a column 'weights' holding `INTEGER` values.  The following will simply return a TopN aggregate over that column

```SQL ,ignore
SELECT toolkit_experimental.topn(1000, weights) FROM samples;
```

It may be more useful to build a view from the aggregate that we can later pass to other tdigest functions.

```SQL ,ignore
CREATE VIEW topn_samples AS SELECT toolkit_experimental.topn(1000, data) FROM samples;
```

---

## **rollup** <a id="topn-summary"></a>

```SQL ,ignore
rollup(
    agg topn
) RETURNS topn
```

Combines multiple TopN objects into a single TopN structure.

### Required Arguments <a id="topn-summary-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `agg` | `topn` |  Column of TopNs to be joined. |
<br>

### Returns <a id="topn-summary-returns"></a>

|Column|Type|Description|
|---|---|---|
| `topn` | `topn` | A topn containing the combination of the input TopNs. |
<br>

### Sample Usages <a id="topn-summary-examples"></a>

```SQL ,ignore
SELECT toolkit_experimental.rollup(daily) FROM daily_topn;
```

---

## **topn** <a id="topn_topn"></a>
```SQL ,ignore
toolkit_experimental.topn(count INTEGER, topn topn) RETURNS TABLE ("value" bigint, "min_freq" double precision, "max_freq" double precision) 
```

Estimate the `count` most frequent values of a set, along with the known bounds on their frequencies.  `count` is not limited to any factor of the size that the TopN structure was built with, but trying to get too many values out of an undersized TopN will result in low quality estimates.

### Required Arguments <a id="topn_topn-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `count` | `INTEGER` | The number of top values to estimate. |
| `topn` | `topn` | The TopN structure summarizing the data set. |
<br>

### Returns <a id="topn_topn-returns"></a>
|Column|Type|Description|
|---|---|---|
| `value` | `BIGINT` | One of the estimated most frequent elements. |
| `min_freq` | `DOUBLE PRECISION` | A floor on the true frequency of `value` within the set. |
| `max_freq` | `DOUBLE PRECISION` | A ceiling on the true frequency of `value` within the set. |
<br>

### Sample Usages <a id="topn_topn-examples"></a>

```SQL 
SELECT value, min_freq, max_freq
FROM toolkit_experimental.topn(5, 
    (SELECT toolkit_experimental.topn_agg(20, floor(sqrt(data))::int)
     FROM generate_series(1, 1000) data)
    );
```
```output
 value | min_freq | max_freq 
-------+----------+----------
    30 |    0.061 |    0.082
    29 |    0.059 |    0.078
    28 |    0.057 |    0.074
    27 |    0.055 |     0.07
    26 |    0.053 |    0.066
```

---

## **num_vals** <a id="topn_num_vals"></a>

```SQL ,ignore
toolkit_experimental.num_vals(topn topn) RETURNS INTEGER
```

Returns the number of elements that have been processed by the TopN structure.

### Required Arguments <a id="topn_num_vals-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `topn` | `topn` | The TopN object to query. |
<br>

### Returns <a id="topn_num_vals-returns"></a>

|Column|Type|Description|
|---|---|---|
| `num_vals` | `INTEGER` | The number of elements in the data set from which the TopN is constructed. |
<br>

### Sample Usages <a id="topn_num_vals-examples"></a>

```SQL
SELECT toolkit_experimental.num_vals(toolkit_experimental.topn_agg(10, data))
FROM generate_series(1, 100) data
```
```output
 num_vals 
----------
      100
```

---

## **guaranteed_topn** <a id="topn_guaranteed_topn"></a>

```SQL ,ignore
toolkit_experimental.num_vals(count INTEGER, topn topn) RETURNS BOOLEAN
```

This will query a TopN object to determine if calling [topn](#topn_topn) with `count` elements is guaranteed to return the top `count` elements.  Note that a false result doesn't imply all values higher than `count` will also return false (i.e. if the three most common elements of a set are too close to distinguish, we might not be able to guarantee which are the top two, but can still guarantee the top three).  Also, note that a true result does not guarantee that the ordered returned will be correct (see [max_ordered_n](#topn_max_ordered_n) if wanting this guarantee).

### Required Arguments <a id="topn_guaranteed_topn-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `count` | `INTEGER` | How many objects we'd like to guarantee. |
| `topn` | `topn` | The TopN object to query. |
<br>

### Returns <a id="topn_guaranteed_topn-returns"></a>

|Column|Type|Description|
|---|---|---|
| `guaranteed_topn` | `BOOLEAN` | If true, TopN can guarantee it's estimate for the top 'count' elements are the true top 'count' elements. |
<br>

### Sample Usages <a id="topn_guaranteed_topn-examples"></a>

```SQL
SELECT toolkit_experimental.guaranteed_topn(10, toolkit_experimental.topn_agg(50, data))
FROM generate_series(1, 100) data
```
```output
 guaranteed_topn 
-----------------
 f
```

---

## **max_ordered_n** <a id="topn_max_ordered_n"></a>

```SQL ,ignore
toolkit_experimental.max_ordered_n(topn topn) RETURNS INTEGER
```

This will return the maximum `count` for which the [topn](#topn_topn) results are guaranteed to be the true results, and in the right order.

### Required Arguments <a id="topn_max_ordered_n-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `topn` | `topn` | The TopN object to query. |
<br>

### Returns <a id="topn_max_ordered_n-returns"></a>

|Column|Type|Description|
|---|---|---|
| `max_ordered_n` | `INTEGER` | The number of elements we can query the TopN for and get the correct answer. |
<br>

### Sample Usages <a id="topn_max_ordered_n-examples"></a>

```SQL
SELECT toolkit_experimental.max_ordered_n(
    (SELECT toolkit_experimental.topn_agg(30, floor(sqrt(data))::int)
     FROM generate_series(1, 1000) data)
    )
```
```output
 max_ordered_n 
---------------
            10
```