# Counter Aggregates [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes)

> [Description](#counter-agg-description)<br>
> [Example Usage](counter-agg-examples)<br>
> [API](#counter-agg-api) <br>
> [Notes on Parallelism and Ordering](#counter-agg-ordering)<br>
> [Extrapolation Methods and Considerations](#counter-agg-methods)<br>


## Description 
[](counter-agg-description)

Metrics generally come in a few different varieties, which many systems have come to call *gauges* and *counters*. A gauge is a typical metric that can vary up or down, something like temperature or percent utilization. A counter is meant to be monotonically increasing. So it keeps track of, say, the total number of visitors to a website.

The main difference in processing counters and gauges is that a decrease in the value of a counter (compared to its previous value in the timeseries) is interpreted as a *reset*. This means that the "true value" of the counter after a decrease is the previous value + the current value. A reset could occur due to a server restart or any number of other reasons. Because of the feature of the reset a counter is often analyzed by taking its change over a time period, accounting for resets. (Our `delta` function offers a way to do this).

Accounting for resets is hard in pure SQL, so we've developed aggregate and accessor functions that do the proper calculations for counters. While the aggregate is not parallelizable, it is supported with [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).

Additionally, [see the notes on parallelism and ordering](#counter-agg-ordering) for a deeper dive into considerations for use with parallelism and some discussion of the internal data structures.

---
## Example Usage 
[](counter-agg-examples)
For these examples we'll assume a table `foo` defined as follows:
```SQL ,ignore
CREATE TABLE foo (
    measure_id      BIGINT,
    ts              TIMESTAMPTZ ,
    val             DOUBLE PRECISION,
    PRIMARY KEY (measure_id, ts)
);
```

We'll start by showing a typical usage of a counter aggregate as well as the `delta` accessor function which gives you the change in the counter's value over the time period in question, accounting for any resets. 

```SQL ,ignore
SELECT measure_id,
    timescale_analytics_experimental.delta(
        timescale_analytics_experimental.counter_agg(ts, val)
    )
FROM foo
GROUP BY measure_id;
```

We can also use the [`time_bucket` function](https://docs.timescale.com/latest/api#time_bucket) to produce a series of deltas over 15 minute increments. 
```SQL ,ignore
SELECT measure_id,
    time_bucket('15 min'::interval, ts) as bucket,
    timescale_analytics_experimental.delta(
        timescale_analytics_experimental.counter_agg(ts, val)
    )
FROM foo
GROUP BY measure_id, time_bucket('15 min'::interval, ts);
```

This will allow us to search for 15 minute periods where the counter increased by a larger or smaller amount.

If series are less regular and so the deltas are affected by the number of samples in the 15 minute period, you can use the `extrapolated_delta` function. For this we'll need to provide bounds so we know where to extrapolate to, for this we'll use the `time_bucket_range` function, which works just like `time_bucket` but produces the open ended range `[start, end)` of all the times in the bucket. We'll also use a CTE to do the `counter_agg` just so it's a little easier to understand what's going on in each part:

```SQL ,ignore
with t as (
    SELECT measure_id,
        time_bucket('15 min'::interval, ts) as bucket,
        timescale_analytics_experimental.counter_agg(ts, val, bounds => timescale_analytics_experimental.time_bucket_range('15 min'::interval, ts))
    FROM foo
    GROUP BY measure_id, time_bucket('15 min'::interval, ts))
SELECT time_bucket, 
    timescale_analytics_experimental.extrapolated_delta(counter_agg, method => 'prometheus') 
FROM t ;
```

Note that we're also using the `'prometheus'` method for doing our extrapolation. Our current extrapolation function is built to mimic the Prometheus project's [`increase` function](https://prometheus.io/docs/prometheus/latest/querying/functions/#increase), which measures the change of a counter extrapolated to the edges of the queried region. 

Of course this might be more useful if we make a continuous aggregate out of it. We'll first have to make it a hypertable partitioned on the ts column:

```SQL ,ignore
SELECT create_hypertable('foo', 'ts', chunk_time_interval=> '15 days'::interval, migrate_data => true);
```

Now we can make our continuous aggregate:

```SQL ,ignore
CREATE MATERIALIZED VIEW foo_15
WITH (timescaledb.continuous)
AS SELECT measure_id,
    time_bucket('15 min'::interval, ts) as bucket,
    timescale_analytics_experimental.counter_agg(ts, val, bounds => time_bucket_range('15 min'::interval, ts))
FROM foo
GROUP BY measure_id, time_bucket('15 min'::interval, ts);
```

Note that here, we just use the `counter_agg` function. It's often better to do that and simply run the accessor functions on the result, it's much more flexible that way, as there are many accessor functions, and the data is there so you can run multiple of them over the same aggregate. 
```SQL ,ignore
SELECT
    measure_id,
    bucket,
    timescale_analytics_experimental.delta(counter_agg),
    timescale_analytics_experimental.rate(counter_agg), 
    timescale_analytics_experimental.extrapolated_rate(counter_agg, method => 'prometheus'),
    timescale_analytics_experimental.slope(counter_agg)
FROM foo_15
```

Here we've used multiple other accessor functions, the `rate` function is a simple `Δval / Δtime` (both observed) calculation, whereas the `extrapolated_rate` with the `'prometheus'` method follows the [Prometheus `rate` function's](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate) behavior of extrapolating to the edges of the boundary and using the bounds provided rather than the observed values. The `slope` function calculates the slope of the least-squares fit line of the values over time. The counter resets are accounted for and "true" values are fed into the linear regression algorithm before this slope is computed.

We can also re-aggregate from the continuous aggregate into a larger bucket size quite simply:

```SQL ,ignore
SELECT
    measure_id,
    time_bucket('1 day'::interval, bucket),
    timescale_analytics_experimental.delta(
        timescale_analytics_experimental.counter_agg(counter_agg)
    )
FROM foo_15
GROUP BY measure_id, time_bucket('1 day'::interval, bucket);
```

There are several other accessor functions which we haven't described in the examples here, but are listed in the API section under the [accessors](#counter-agg-api-accessors).

---

# Command List  
[](counter-agg-api)

### Aggregate Functions
[](counter-agg-api-aggregates)
> - [counter_agg() (point form)](#counter-agg-point)
> - [counter_agg() (summary form)](#counter-agg-summary)
### Accessor Functions (A-Z)
[](counter-agg-api-accessors)
> - [corr()](#counter-agg-corr)
> - [counter_zero_time()](#counter-agg-counter-zero-time)
> - [delta()](#counter-agg-delta)
> - [extrapolated_delta()](#counter-agg-extrapolated-delta)
> - [extrapolated_rate()](#counter-agg-extrapolated-rate)
> - [idelta_left()](#counter-agg-idelta-left)
> - [idelta_right()](#counter-agg-idelta-right)
> - [intercept()](#counter-agg-intercept)
> - [irate_left()](#counter-agg-irate-left)
> - [irate_right()](#counter-agg-irate-right)
> - [num_changes()](#counter-agg-num-changes)
> - [num_elements()](#counter-agg-num-elements)
> - [num_resets()](#counter-agg-num-resets)
> - [rate()](#counter-agg-rate)
> - [slope()](#counter-agg-slope)
> - [time_delta()](#counter-agg-time-delta)
### Utility Functions
[](counter-agg-api-utilities)
> - [with_bounds()](#counter-agg-with-bounds)
---


# Aggregate Functions 
[](counter-agg-api-aggregates)
Aggregating a counter to produce a `CounterSummary` is the first step in performing any calculations on it. There are two basic forms, one which takes in timestamps and values (the point form) and one which can combine multiple `CounterSummaries` together to form a larger summary spanning a larger amount of time. (See [Notes on Parallelism and Ordering](#counter-agg-ordering) for more information on how that works). 

---
## **counter_agg() (point form)** 
[](counter-agg-point)
```SQL ,ignore
timescale_analytics_experimental.counter_agg(
    ts TIMESTAMPTZ,
    value DOUBLE PRECISION¹,
    bounds TSTZRANGE DEFAULT NULL
) RETURNS CounterSummary
```

An aggregate that produces a `CounterSummary` from timestamps and associated values. 

##### ¹ Note that the `value` is currently only accepted as a `DOUBLE PRECISION` number as most people use that for counters, even though other numeric types (ie `BIGINT`) might sometimes be more intuitive. If you store a value as a different numeric type you can cast to `DOUBLE PRECISION` on input to the function. 

### Required Arguments²
|Name| Type |Description|
|---|---|---|
| `ts` | `TIMESTAMPTZ` |  The time at each point |
| `value` | `DOUBLE PRECISION` | The value at each point to use for the counter aggregate|
<br>

##### ² Note that `ts` and `value` can be `null`, however the aggregate is not evaluated on `null` values and will return `null`, but it will not error on `null` inputs.

### Optional Arguments 
|Name| Type |Description|
|---|---|---|
| `bounds` | `TSTZRANGE` |  A range of `timestamptz` representing the largest and smallest possible times that could be input to this aggregate. Calling with `NULL` or leaving out the argument results in an unbounded `CounterSummary`. Bounds are required for extrapolation, but not for other [accessor functions](#counter-agg-api-accessors). |

<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `counter_agg` | `CounterSummary` | A CounterSummary object that can be passed to [accessor functions](#counter-agg-api-accessors) or other objects in the counter aggregate API |
<br>

### Sample Usage
```SQL ,ignore
WITH t as (
    SELECT
        time_bucket('1 day'::interval, ts) as dt,
        timescale_analytics_experimental.counter_agg(ts, val) AS cs -- get a CounterSummary
    FROM foo
    WHERE id = 'bar'
    GROUP BY time_bucket('1 day'::interval, ts)
)
SELECT
    dt,
    timescale_analytics_experimental.irate_right(cs) -- extract instantaneous rate from the CounterSummary
FROM t;
```

---
## **counter_agg() (summary form)**
[](counter-agg-summary)
```SQL ,ignore
timescale_analytics_experimental.counter_agg(
    cs CounterSummary
) RETURNS CounterSummary
```

An aggregate to compute a combined `CounterSummary` from a series of non-overlapping `CounterSummaries`. Non-disjoint `CounterSummaries` will cause errors. See [Notes on Parallelism and Ordering](#counter-agg-ordering) for more information. 

### Required Arguments² 
|Name| Type |Description|
|---|---|---|
| `cs` | `CounterSummary` | The input CounterSummary from a previous `counter_agg` (point form) call, often from a [continuous aggregate](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates)|

##### ² Note that `summary` can be `null`, however the aggregate is not evaluated on `null` values and will return `null`, but it will not error on `null` inputs.
### Returns

|Column|Type|Description|
|---|---|---|
| `counter_agg` | `CounterSummary` |  A CounterSummary object that can be passed to [accessor functions](counter-agg-api-accessors) or other objects in the counter aggregate API|
<br>

### Sample Usage
```SQL ,ignore
WITH t as (
    SELECT
        date_trunc('day', ts) as dt,
        timescale_analytics_experimental.counter_agg(ts, val) AS counter_summary -- get a time weight summary
    FROM foo
    WHERE id = 'bar'
    GROUP BY date_trunc('day')
), q as (
    SELECT timescale_analytics_experimental.counter_agg(counter_summary) AS full_cs -- do a second level of aggregation to get the full CounterSummary
    FROM t
)
SELECT
    dt,
    timescale_analytics_experimental.delta(counter_summary),  -- extract the delta from the  CounterSummary
    timescale_analytics_experimental.delta(counter_summary) / (SELECT timescale_analytics_experimental.delta(full_cs) FROM q LIMIT 1)  as normalized -- get the fraction of the delta that happened each day compared to the full change of the counter
FROM t;
```
# Accessor Functions 
[](counter-agg-api-accessors)

## Accessor Function List (by family)
### [Change over time (delta) functions](#counter-agg-delta-fam)
> - [delta()](#counter-agg-delta)
> - [extrapolated_delta()](#counter-agg-extrapolated-delta)
> - [idelta_left()](#counter-agg-idelta-left)
> - [idelta_right()](#counter-agg-idelta-right)
> - [time_delta()](#counter-agg-time-delta)

### [Rate of change over time (rate) functions](#counter-agg-rate-fam)
> - [rate()](#counter-agg-rate)
> - [extrapolated_rate()](#counter-agg-extrapolated-rate)
> - [irate_left()](#counter-agg-irate-left)
> - [irate_right()](#counter-agg-irate-right)

### Counting functions
> - [num_changes()](#counter-agg-num-changes)
> - [num_elements()](#counter-agg-num-elements)
> - [num_resets()](#counter-agg-num-resets)

### Statistical regression / least squares fit functions
> - [slope()](#counter-agg-slope)
> - [intercept()](#counter-agg-intercept)
> - [counter_zero_time()](#counter-agg-counter-zero-time)
> - [corr()](#counter-agg-corr)



---
## **Change over time (delta) functions** 
[](counter-agg-delta-fam)
Functions in the delta family are dedicated to finding the change in a value (or observed time, in the case of `time_delta`) of a counter during a time period, taking into account any counter resets that may have occurred. 

---
## **delta()** 
[](counter-agg-delta)
```SQL ,ignore
timescale_analytics_experimental.delta(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```
The change in the counter over the time period. This is the raw or simple delta computed by accounting for resets then subtracting the last seen value from the first. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `delta` | `DOUBLE PRECISION` | The delta computed from the `CounterSummary`|
<br>

### Sample Usage 
[](counter-agg-delta-sample)

```SQL ,ignore
SELECT
    id,
    timescale_analytics_experimental.delta(summary)
FROM (
    SELECT
        id,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id
) t
```

---
## **extrapolated_delta()** 
[](counter-agg-extrapolated-delta)
```SQL ,ignore
timescale_analytics_experimental.extrapolated_delta(
    summary CounterSummary, 
    method TEXT¹
) RETURNS DOUBLE PRECISION
```
The change in the counter during the time period specified by the `bounds` in the `CounterSummary`. To calculate the extrapolated delta, any counter resets are accounted for and the observed values are extrapolated to the bounds using the `method` specified (see [Extrapolation Methods and Considerations](#counter-agg-methods)) then the values are subtracted to compute the delta. 

The `bounds` must be specified for the `extrapolated_delta` function to work, the bounds can be provided in the [`counter_agg`](#counter-agg-point) call, or by using the [`with_bounds`](#counter-agg-with-bounds) utility function to set the bounds

##### ¹ Currently, the only allowed value of `method` is `'prometheus'`, as we have only implemented extrapolation following the Prometheus extrapolation protocol, see [Extrapolation Methods and Considerations](#counter-agg-methods) for more information.

### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|
| `method` | `TEXT` | The extrapolation method to use, the only option currently is 'prometheus', not case sensitive.|

### Returns

|Column|Type|Description|
|---|---|---|
| `extrapolated_delta` | `DOUBLE PRECISION` | The delta computed from the `CounterSummary`|
<br>

### Sample Usage 
[](counter-agg-extrapolated-delta-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.extrapolated_delta(
        timescale_analytics_experimental.with_bounds(
            summary,
            timescale_analytics_experimental.time_bucket_range('15 min'::interval, bucket)
        )
    )
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts)
) t
```

---
## **idelta_left()** 
[](counter-agg-idelta-left)
```SQL ,ignore
timescale_analytics_experimental.idelta_left(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The instantaneous change in the counter at the left (earlier) side of the time range. Essentially, the first value subtracted from the second value seen in the time range (handling resets appropriately). This can be especially useful for fast moving counters. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `idelta_left` | `DOUBLE PRECISION` | The instantaneous delta computed from left (earlier) side of the `CounterSummary`|
<br>

### Sample Usage 
[](counter-agg-idelta_left-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.idelta_left(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

---
## **idelta_right()** 
[](counter-agg-idelta-right)
```SQL ,ignore
timescale_analytics_experimental.idelta_right(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The instantaneous change in the counter at the right (later) side of the time range. Essentially, the penultimate value subtracted from the last value seen in the time range (handling resets appropriately). This can be especially useful for fast moving counters. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `idelta_right` | `DOUBLE PRECISION` | The instantaneous delta computed from right (later) side of the `CounterSummary`|
<br>

### Sample Usage 
[](counter-agg-idelta_left-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.idelta_right(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

---
## **time_delta()** 
[](counter-agg-time-delta)
```SQL ,ignore
timescale_analytics_experimental.time_delta(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The observed change in time (`last time - first time`) over the period aggregated. Measured in seconds.


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `time_delta` | `DOUBLE PRECISION` | The total duration in seconds between the first and last observed times in the `CounterSummary`|
<br>

### Sample Usage 
[](counter-agg-time-delta-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.time_delta(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

---
## **Rate of change over time (rate) functions** 
[](counter-agg-rate-fam)
The rate family of functions find the reset-adjusted rate of change (`delta(value)/delta(time)`) of a counter on a per-second basis.

---
## **rate()** [](counter-agg-rate)
```SQL ,ignore
timescale_analytics_experimental.rate(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```
The rate of change of the counter over the observed time period.  This is the raw or simple rate, equivalent to `delta(summary) / time_delta(summary)`. After accounting for resets, we subtract the last value from the first and divide by the duration between the last observed time and the first observed time. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `rate` | `DOUBLE PRECISION` | The per second observed rate computed from the `CounterSummary`|
<br>

### Sample Usage [](counter-agg-rate-sample)

```SQL ,ignore
SELECT
    id,
    timescale_analytics_experimental.rate(summary)
FROM (
    SELECT
        id,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id
) t
```

---
## **extrapolated_rate()** [](counter-agg-extrapolated-rate)
```SQL ,ignore
timescale_analytics_experimental.extrapolated_rate(
    summary CounterSummary, 
    method TEXT¹
) RETURNS DOUBLE PRECISION
```
The rate of change in the counter computed over the time period specified by the `bounds` in the `CounterSummary`, extrapolating to the edges. Essentially, it is an [`extrapolated_delta`](#counter-agg-extrapolated-delta) divided by the duration in seconds. 

The `bounds` must be specified for the `extrapolated_rate` function to work, the bounds can be provided in the [`counter_agg`](#counter-agg-point) call, or by using the [`with_bounds`](#counter-agg-with-bounds) utility function to set the bounds

##### ¹ Currently, the only allowed value of `method` is `'prometheus'`, as we have only implemented extrapolation following the Prometheus extrapolation protocol, see [Extrapolation Methods and Considerations](#counter-agg-methods) for more information.

### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|
| `method` | `TEXT` | The extrapolation method to use, the only option currently is 'prometheus', not case sensitive.|

### Returns

|Column|Type|Description|
|---|---|---|
| `extrapolated_rate` | `DOUBLE PRECISION` | The per-second rate of change of the counter computed from the `CounterSummary` extrapolated to the `bounds` specified there. |
<br>

### Sample Usage [](counter-agg-extrapolated-rate-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.extrapolated_rate(
        timescale_analytics_experimental.with_bounds(
            summary,
            timescale_analytics_experimental.time_bucket_range('15 min'::interval, bucket)
        )
    )
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts)
) t
```

---
## **irate_left()** [](counter-agg-irate-left)
```SQL ,ignore
timescale_analytics_experimental.irate_left(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The instantaneous rate of change of the counter at the left (earlier) side of the time range. Essentially, the [`idelta_left`](counter-agg-idelta-left) divided by the duration between the first and second observed points in the `CounterSummary`. This can be especially useful for fast moving counters. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `irate_left` | `DOUBLE PRECISION` | The instantaneous rate computed from left (earlier) side of the `CounterSummary`|
<br>

### Sample Usage [](counter-agg-irate-left-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.irate_left(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

---
## **irate_right()** [](counter-agg-irate-right)

```SQL ,ignore
timescale_analytics_experimental.irate_right(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The instantaneous rate of change of the counter at the right (later) side of the time range. Essentially, the [`idelta_right`](counter-agg-idelta-right) divided by the duration between the first and second observed points in the `CounterSummary`. This can be especially useful for fast moving counters. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `irate_right` | `DOUBLE PRECISION` | The instantaneous rate computed from right (later) side of the `CounterSummary`|
<br>

### Sample Usage [](counter-agg-irate-right-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.irate_right(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```
--- 
# **Counting functions** [](counter-agg-api-counting)
The counting functions comprise several accessor functions that calculate the number of times a certain thing occured while calculating the `counter_agg`. 

---
## **num_changes()** [](counter-agg-num-changes)

```SQL ,ignore
timescale_analytics_experimental.num_changes(
    summary CounterSummary
) RETURNS BIGINT
```

The number of times the value changed within the period over which the `CounterSummary` is calculated. This is determined by evaluating consecutive points, any change counts, including counter resets where the counter is reset to zero, while this would result in the same _adjusted_ counter value for consecutive points, we still treat it as a change. 

### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `num_changes` | `BIGINT` | The number of times the value changed|
<br>

### Sample Usage [](counter-agg-num-changes-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.num_changes(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

---
## **num_elements()** [](counter-agg-num-elements)

```SQL ,ignore
timescale_analytics_experimental.num_elements(
    summary CounterSummary
) RETURNS BIGINT
```

The total number of points we saw in calculating the `CounterSummary`. Only points with distinct times are counted, as duplicate times are thrown out in general in these calculations. 

### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input `CounterSummary` from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `num_elements` | `BIGINT` | The number of points seen during the `counter_agg` call|
<br>

### Sample Usage [](counter-agg-num-elements-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.num_elements(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

---
## **num_elements()** [](counter-agg-num-resets)

```SQL ,ignore
timescale_analytics_experimental.num_resets(
    summary CounterSummary
) RETURNS BIGINT
```

The total number of times we detected a counter reset while calculating the `CounterSummary`.

### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input `CounterSummary` from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `num_elements` | `BIGINT` | The number of resets detected during the `counter_agg` call|
<br>

### Sample Usage [](counter-agg-num-resets-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.num_resets(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```
--- 
# **Statistical regression functions** [](counter-agg-api-regression-fam)
The statistical regression family of functions contains several functions derived from a least squares fit of the adjusted value of the counter. All counter values have resets accounted for before being fed into the linear regression algorithm (and any combined `CounterSummaries` have the proper adjustments performed for resets to enable the proper regression analysis to be performed). 

###### NB: Note that the timestamps input are converted from their their internal representation (microseconds since the Postgres Epoch (which is 2000-01-01 00:00:00+00, for some reason), to double precision numbers representing seconds from the Postgres Epoch, with decimal places as fractional seconds, before running the linear regression. Because the internal representation of the timestamp is actually 64-bit integer representing microseconds from the Postgres Epoch, it provides more precision for very large timestamps (the representable range goes out to 294276-12-31). If you want to have accurate, microsecond level precision on your regression analysis dealing with dates at the edge of this range (first off, who are you and *what the heck are you working on???*) we recommend subtracting a large static date from your timestamps and then adding it back after the analysis has concluded. Very small timestamps should be fine as the range does not extend beyond 4714-11-01 BCE, beyond which Julian dates [are not considered reliable by Postgres](https://github.com/postgres/postgres/blob/c30f54ad732ca5c8762bb68bbe0f51de9137dd72/src/include/datatype/timestamp.h#L131). This means that the negative integers are not fully utilized in the timestamp representation and you don't have to worry about imprecision in your computed slopes if you have traveled back in time and are timing chariot races to the microsecond. However, if you travel much further back in time, you're still SOL, as we can't represent the timestamp in the Julian calendar. 

---
## **slope()** [](counter-agg-slope)

```SQL ,ignore
timescale_analytics_experimental.slope(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The slope of the least squares fit line computed from the adjusted counter values and times input in the `CounterSummary`. Because the times are input as seconds, the slope will provide a per-second rate of change estimate based on the least squares fit, which will often be similar to the result of the `rate` calculation, but may more accurately reflect the "usual" behavior if there are infrequent, large changes in a counter. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `slope` | `DOUBLE PRECISION` | The per second rate of change computed by taking the slope of the least squares fit of the points input in the `CounterSummary`|
<br>

### Sample Usage [](counter-agg-slope-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.slope(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```
---
## **intercept()** [](counter-agg-intercept)

```SQL ,ignore
timescale_analytics_experimental.intercept(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The intercept of the least squares fit line computed from the adjusted counter values and times input in the `CounterSummary`. This will correspond to the projected value at the Postgres Epoch (2000-01-01 00:00:00+00) - which is not all that useful for much of anything except potentially drawing the best fit line on a graph, using the slope and the intercept. 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `intercept` | `DOUBLE PRECISION` | The intercept of the least squares fit line computed from the points input to the `CounterSummary`|
<br>

### Sample Usage [](counter-agg-intercept-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.intercept(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```
--- 
## **counter_zero_time()** [](counter-agg-counter-zero-time)

```SQL ,ignore
timescale_analytics_experimental.counter_zero_time(
    summary CounterSummary
) RETURNS TIMESTAMPTZ
```

The time at which the counter value is predicted to have been zero based on the least squares fit line computed from the points in the `CounterSummary`. The 


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `counter_zero_time` | `TIMESTAMPTZ` | The time at which the counter value is predicted to have been zero based onthe least squares fit of the points input to the `CounterSummary`|
<br>

### Sample Usage [](counter-agg-counter-zero-time-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.counter_zero_time(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

---
## **corr())** [](counter-agg-corr)

```SQL ,ignore
timescale_analytics_experimental.corr(
    summary CounterSummary
) RETURNS DOUBLE PRECISION
```

The correlation coefficient of the least squares fit line of the adjusted counter value. Given that the slope a line for any counter value must be non-negative, this will also always be non-negative and in the range from [0.0, 1.0] It measures how well the least squares fit fit the available data, where a value of 1.0 represents the strongest correlation between time the counter increasing.


### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input CounterSummary from a `counter_agg` call.|

### Returns

|Column|Type|Description|
|---|---|---|
| `corr` | `DOUBLE PRECISION` | The correlation coefficient computed from the least squares fit of the adjusted counter values input to the `CounterSummary`|
<br>

### Sample Usage [](counter-agg-corr-sample)

```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.corr(summary)
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts) 
) t
```

# **Utility Functions** [](counter-agg-api-utilities)
---
## **with_bounds() **[](counter-agg-with-bounds)
```SQL ,ignore
timescale_analytics_experimental.with_bounds(
    summary CounterSummary,
    bounds TSTZRANGE,
) RETURNS CounterSummary
```

A utility function to add bounds to an already-computed `CounterSummary`. The bounds represent the outer limits of the timestamps allowed for this `CounterSummary` as well as the edges of the range to extrapolate to in functions that do that.

### Required Arguments
|Name| Type |Description|
|---|---|---|
| `summary` | `CounterSummary` | The input `CounterSummary`, 
| `bounds` | `TSTZRANGE` |  A range of `timestamptz` representing the largest and smallest allowed times in this `CounterSummary` |

### Returns
|Column|Type|Description|
|---|---|---|
| `counter_agg` | `CounterSummary` |  A CounterSummary object that can be passed to [accessor functions](counter-agg-api-accessors) or other objects in the counter aggregate API|
<br>

### Sample Usage
```SQL ,ignore
SELECT
    id,
    bucket,
    timescale_analytics_experimental.extrapolated_rate(
        timescale_analytics_experimental.with_bounds(
            summary,
            timescale_analytics_experimental.time_bucket_range('15 min'::interval, bucket)
        )
    )
FROM (
    SELECT
        id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val) AS summary
    FROM foo
    GROUP BY id, time_bucket('15 min'::interval, ts)
) t
```
---
# Notes on Parallelism and Ordering [](counter-agg-ordering)

The counter reset calculations we perform require a strict ordering of inputs and therefore the calculations are not parallelizable in the strict Postgres sense. This is because when Postgres does parallelism it hands out rows randomly, basically as it sees them to workers. However, if your parallelism can guarantee disjoint (in time) sets of rows, the algorithm can be parallelized, just so long as within some time range, all rows go to the same worker. This is the case for both [continuous aggregates](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates) and for [distributed hypertables](https://docs.timescale.com/latest/using-timescaledb/distributed-hypertables) (as long as the partitioning keys are in the group by, though the aggregate itself doesn't horribly make sense otherwise).

We throw an error if there is an attempt to combine overlapping `CounterSummaries`, for instance, in our example above, if you were to try to combine summaries across `measure_id`'s it would error (assuming that they had overlapping times). This is because the counter values resetting really only makes sense within a given time series determined by a single `measure_id`. However, once an accessor function is applied, such as `delta`, a sum of deltas may be computed. Similarly, an average or histogram of rates across multiple time series might be a useful calculation to perform. The thing to note is that the counter aggregate and the reset logic should be performed first, then further calculations may be performed on top of that. 

As an example, let's consider that we might want to find which of my counters had the most extreme rates of change in each 15 minute period. For this, we'll want to normalize the rate of change of each measure by dividing it by the average rate of change over all the counters in that 15 minute period. We'll use the normal `avg` function to do this, but we'll use it as a window function like so: 


```SQL ,ignore
WITH t as (SELECT measure_id,
        time_bucket('15 min'::interval, ts) AS bucket,
        timescale_analytics_experimental.rate(
            timescale_analytics_experimental.counter_agg(ts, val)
        ) as rate
    FROM foo
    GROUP BY measure_id), 
SELECT measure_id, 
    bucket,
    rate,
    rate / avg(rate_per_measure) OVER (PARTITION BY bucket) AS normalized_rate -- call normal avg function as a window function to get a 15 min avg to normalize our per-measure rates
FROM t;
```
Still, note that the counter resets are accounted for before applying the `avg` function in order to get our normalized rate. 

Internally, the `CounterSummary` stores:
- the first, second, penultimate, and last points seen
- the sum of all the values at reset points, as well as the number of changes, and number of resets seen.
- A set of 6 values used to compute all the statistical regression parameters using the Youngs-Cramer algorithm.
- Optionally, the bounds as an open-ended range, over which extrapolation should occur and which represents the outer possible limit of times represented in this `CounterSummary` 

In general, the functions support [partial aggregation](https://www.postgresql.org/docs/current/xaggr.html#XAGGR-PARTIAL-AGGREGATES) and partitionwise aggregation in the multinode context, but are not parallelizable (in the Postgres sense, which requires them to accept potentially overlapping input).

Because they require ordered sets, the aggregates build up a buffer of input data, sort it and then perform the proper aggregation steps. In cases where memory is proving to be too small to build up a buffer of points causing OOMs or other issues, a multi-level aggregate can be useful. 

So where I might run into OOM issues if I computed the values over all time like so:


```SQL ,ignore
SELECT measure_id,
    timescale_analytics_experimental.rate(
        timescale_analytics_experimental.counter_agg(ts, val)
    ) as rate
FROM foo
GROUP BY measure_id;
```
If I were to instead, compute the `counter_agg` over, say daily buckets and then combine the aggregates, I might be able to avoid OOM issues, as each day will be computed separately first and then combined, like so: 

```SQL ,ignore
WITH t as (SELECT measure_id,
        time_bucket('1 day'::interval, ts) AS bucket,
        timescale_analytics_experimental.counter_agg(ts, val)
    FROM foo
    GROUP BY measure_id), 
SELECT measure_id, \
    timescale_analytics_experimental.rate(
        timescale_analytics_experimental.counter_agg(counter_agg) --combine the daily `CounterSummaries` to make a full one over all time, accounting for all the resets, then apply the rate function
    )
FROM t;
```

Moving aggregate mode is not supported by `counter_agg` and its use as a window function may be quite inefficient.

---
# Extrapolation Methods Details [](counter-agg-methods)
#TODO
