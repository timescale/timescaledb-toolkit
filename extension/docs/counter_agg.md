# Counter Aggregates [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes)

> [Description](#counter-agg-description)<br>
> [Example Usage](counter-agg-examples)<br>
> [API](#counter-agg-api) <br>
> [Notes on Parallelism and Ordering](#counter-agg-ordering)<br>
> [Extrapolation Calculations and Considerations](#counter-agg-methods)<br>


## Description [](counter-agg-description)

Metrics generally come in a few different varieties, which many systems have come to call *gauges* and *counters*. A gauge is a typical metric that can vary up or down, something like temperature or percent utilization. A counter is meant to be monotonically increasing. So it keeps track of, say, the total number of visitors to a website.

The main difference in processing counters and gauges is that a decrease in the value of a counter (compared to its previous value in the timeseries) is interpreted as a *reset*. This means that the "true value" of the counter after a decrease is the previous value + the current value. A reset could occur due to a server restart or any number of other reasons. Because of the feature of the reset a counter is often analyzed by taking its change over a time period, accounting for resets. (Our `delta` function offers a way to do this).

Accounting for resets is hard in pure SQL, so we've developed aggregate and accessor functions that do the proper calculations for counters. While the aggregate is not parallelizable, it is supported with [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).

Additionally, [see the notes on parallelism and ordering](#counter-agg-ordering) for a deeper dive into considerations for use with parallelism and some discussion of the internal data structures.

---
## Example Usage [](counter-agg-examples)
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

# Command List  [](counter-agg-api)

### [Aggregate Functions](counter-agg-api-aggregates)
> - [counter_agg() (point form)](#counter-agg-point)
> - [counter_agg() (summary form)](#counter-agg-summary)
### [Accessor Functions (A-Z)](counter-agg-api-accessors)
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
### [Utility Functions](counter-agg-api-utilities)
> - [with_bounds()](#counter-agg-with-bounds)
---

