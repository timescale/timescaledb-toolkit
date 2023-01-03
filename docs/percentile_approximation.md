# Approximate Percentiles
> [Why To Use Approximate Percentiles](#why-use)<br>
> [API](#percentile-approx-api) <br>
> [Advanced Usage: Algorithms and How to Choose](#advanced-usage)<br>

###### A note on terminology: Technically, a percentile divides the group into 100 equally sized (by frequency) buckets, while a quantile would divide the group into an arbitrary number of buckets. We use percentile here with the recognition that while quantile is the technically more "correct" term for an arbitrary precision operation, percentile has become more commonly used to describe this type of function.

## Why to Use Approximate Percentiles <a id="why-use"></a>

There are really two things to cover here:  1) [why use percentiles at all](#why-use-percent) and 2) [why use *approximate* percentiles rather than exact percentiles](#why-approximate).

To better understand this, we'll use the common example of a server that's running APIs for a company and tracking the response times for the various APIs it's running. So, for our example, we have a table something like this:

```SQL , non-transactional, ignore-output
SET extra_float_digits = -3; -- use 12 digits of precision to reduce flakiness
SET SESSION TIME ZONE 'UTC'; -- so we get consistent output
CREATE TABLE response_times (
    ts timestamptz,
    api_id int,
    user_id int,
    response_time_ms float
);
-- and we'll make it a hypertable for ease of use in the rest of the example
SELECT create_hypertable('response_times', 'ts');
```
<details> <a id="data-generation"></a>
    <summary> We'll also generate some data to work with here. And insert it into the table (expand for the generation script if you want to see it). </summary>

```SQL , non-transactional, ignore-output
SELECT setseed(0.43); -- do this to make sure we get the same random number for each run so the results are the same

WITH apis as MATERIALIZED (SELECT generate_series(1, 12) as api_id),
users as MATERIALIZED (SELECT generate_series(1, 30) as user_id),
api_users as MATERIALIZED (SELECT * FROM apis JOIN users on api_id % 3 = user_id % 3),  -- users use ~ 1/3 of apis
times as MATERIALIZED (SELECT generate_series('2020-01-01'::timestamptz, '2020-01-02'::timestamptz, '1 minute'::interval) as ts),
raw_joined as MATERIALIZED (SELECT * from api_users CROSS JOIN times ORDER BY api_id, user_id, ts),
generated_data as MATERIALIZED (
SELECT ts + '5 min'::interval * random() as ts,
    api_id,
    user_id,
    10 * api_id * user_id   / (1+(extract(hour FROM ts)::int % api_id)) * random() as response_time
FROM raw_joined
ORDER BY api_id, user_id, ts)

INSERT INTO response_times SELECT * FROM generated_data;
```
It's not the most representative of data sets, but it'll do and have some interesting features for us to look at.
</details>

---
### Why use percentiles? <a id="why-use-percent"></a>

In general, percentiles are useful for understanding the distribution of your data, for instance the 50% percentile, aka median of the data can be a more useful measure than average when there are outliers that would dramatically impact the average, but have a much smaller impact on the median. The median or 50th percentile means that in an ordered list of your data half of the data will be greater and half less, the 10% percentile would mean that 10% would fall below and 90% above the value returned and the 99th percentile would mean that 1% is above the value returned, 99% below. Outliers have less of an impact because their magnitude doesn't affect their percentile, only their order in the set, so the skew introduced by uncommon very large or very small values is reduced or eliminated.

Let's look at an example with our generated data set, and lets say we want to find the worst apis, in an hour segment, so that we can identify poor performance, we'll start by using the Postgres [percentile_disc]() function for our percentiles:

```SQL
SELECT
    time_bucket('1 h'::interval, ts) as bucket,
    api_id,
    avg(response_time_ms),
    percentile_disc(0.5) WITHIN GROUP (ORDER BY response_time_ms) as median
FROM response_times
GROUP BY 1, 2
ORDER BY 3 DESC LIMIT 15;
```
```output, precision(2: 7)
         bucket         | api_id |      avg      |    median
------------------------+--------+---------------+--------------
 2020-01-01 00:00:00+00 |     12 |  963.71332589 | 718.523974458
 2020-01-01 12:00:00+00 |     12 | 960.321550984 | 702.553342115
 2020-01-01 00:00:00+00 |     11 | 869.080106405 | 672.323915584
 2020-01-01 11:00:00+00 |     11 | 812.398067226 | 601.097789543
 2020-01-01 22:00:00+00 |     11 | 807.601702923 |   588.4594427
 2020-01-01 09:00:00+00 |      9 | 734.571525228 | 568.587417008
 2020-01-01 18:00:00+00 |      9 |  729.74167841 | 579.954580675
 2020-01-01 10:00:00+00 |     10 |  706.33545502 | 530.221293445
 2020-01-01 20:00:00+00 |     10 |  703.37743915 | 547.222908361
 2020-01-01 00:00:00+00 |      9 | 699.838199982 | 512.966472958
 2020-01-01 00:00:00+00 |     10 | 693.538069163 | 520.245282353
 2020-01-02 00:00:00+00 |     11 | 664.649986691 | 526.017052809
 2020-01-01 08:00:00+00 |      8 | 614.010225183 | 450.329133442
 2020-01-01 16:00:00+00 |      8 | 600.166598131 | 448.352142719
 2020-01-01 00:00:00+00 |      8 | 598.260875149 | 430.921181959
 ```

So, this returns some interesting results, maybe something like what those of you who read over our [data generation](#data-generation) code would expect. Given how we generate the data, we expect that the larger `api_ids` will have longer generated response times but that it will be cyclic with `hour % api_id`, so we can see that here.

But what happens if we introduce some aberrant data points? They could have come from anywhere, maybe a user ran a weird query, maybe there's an odd bug in the code that causes some timings to get multiplied in an odd code path, who knows, here we'll introduce just 10 outlier points out of half a million:

```SQL , non-transactional, ignore-output
SELECT setseed(0.43); --make sure we've got a consistent seed so the output is consistent.
WITH rand_points as (SELECT ts, api_id, user_id FROM response_times ORDER BY random() LIMIT 10)
UPDATE response_times SET response_time_ms = 10000 * response_time_ms WHERE (ts, api_id, user_id) IN (SELECT * FROM rand_points);
```
```SQL
SELECT
    time_bucket('1 h'::interval, ts) as bucket,
    api_id,
    avg(response_time_ms),
    percentile_disc(0.5) WITHIN GROUP (ORDER BY response_time_ms) as median
FROM response_times
GROUP BY 1, 2
ORDER BY 3 DESC LIMIT 15;
```

```output, precision(2: 7)
         bucket         | api_id |      avg      |     median
------------------------+--------+---------------+---------------
 2020-01-01 09:00:00+00 |      9 | 11508.5077421 | 568.587417008
 2020-01-01 13:00:00+00 |     11 | 11406.1365163 | 218.613331575
 2020-01-01 00:00:00+00 |      8 | 10795.1549884 | 430.921181959
 2020-01-01 02:00:00+00 |     11 | 6982.65943397 | 231.997136085
 2020-01-01 21:00:00+00 |      8 | 4166.71533182 | 80.9020478838
 2020-01-01 12:00:00+00 |      5 | 1417.81186885 | 97.1619017291
 2020-01-01 18:00:00+00 |     12 |   1382.216682 | 110.607063032
 2020-01-01 19:00:00+00 |      9 | 1152.86960635 | 300.074082831
 2020-01-01 23:00:00+00 |      6 | 1025.71057197 | 68.2470801603
 2020-01-01 00:00:00+00 |     12 |  963.71332589 | 718.523974458
 2020-01-01 12:00:00+00 |     12 | 960.321550984 | 702.553342115
 2020-01-01 00:00:00+00 |     11 | 869.080106405 | 672.323915584
 2020-01-01 11:00:00+00 |     11 | 812.398067226 | 601.097789543
 2020-01-01 22:00:00+00 |     11 | 807.601702923 |   588.4594427
 2020-01-01 18:00:00+00 |      9 |  729.74167841 | 579.954580675
 ```

Now, `avg` is giving horribly misleading results and not showing us the underlying patterns in our data anymore. But if I order by the `median` instead:
```SQL
SELECT
    time_bucket('1 h'::interval, ts) as bucket,
    api_id,
    avg(response_time_ms),
    percentile_disc(0.5) WITHIN GROUP (ORDER BY response_time_ms) as median
FROM response_times
GROUP BY 1, 2
ORDER BY 4 DESC, 2, 1 LIMIT 15;
```
```output, precision(2: 7)
         bucket         | api_id |      avg      |    median
------------------------+--------+---------------+---------------
 2020-01-01 00:00:00+00 |     12 |  963.71332589 | 718.523974458
 2020-01-01 12:00:00+00 |     12 | 960.321550984 | 702.553342115
 2020-01-01 00:00:00+00 |     11 | 869.080106405 | 672.323915584
 2020-01-01 11:00:00+00 |     11 | 812.398067226 | 601.097789543
 2020-01-01 22:00:00+00 |     11 | 807.601702923 |   588.4594427
 2020-01-01 18:00:00+00 |      9 |  729.74167841 | 579.954580675
 2020-01-01 09:00:00+00 |      9 | 11508.5077421 | 568.587417008
 2020-01-01 20:00:00+00 |     10 |  703.37743915 | 547.222908361
 2020-01-01 10:00:00+00 |     10 |  706.33545502 | 530.221293445
 2020-01-02 00:00:00+00 |     11 | 664.649986691 | 526.017052809
 2020-01-01 00:00:00+00 |     10 | 693.538069163 | 520.245282353
 2020-01-01 00:00:00+00 |      9 | 699.838199982 | 512.966472958
 2020-01-01 08:00:00+00 |      8 | 614.010225183 | 450.329133442
 2020-01-01 16:00:00+00 |      8 | 600.166598131 | 448.352142719
 2020-01-01 00:00:00+00 |      8 | 10795.1549884 | 430.921181959
 ```
 I can see the pattern in my data again! The median was much better at dealing with outliers than `avg` was, and percentiles in general are much less noisy. This becomes even more obvious where we might want to measure the worst case scenario for users. So we might want to use the `max`, but often the 99th percentile value gives a better representation of the *likely* worst outcome for users than the max response time, which might be due to unrealistic parameters, an error, or some other non-representative condition. The maximum response time becomes something useful for engineers to investigate, ie to find errors or other weird outlier use cases, but less useful for, say, measuring overall user experience and how it changes over time. Both are useful for different circumstances, but often the 95th or 99th or other percentile outcome becomes the design parameter and what we measure success against.

---
### Why use *approximate* percentiles? <a id="why-approximate"></a>

One reason that percentiles are less frequently used than, say, average, min, max or other measures of a distribution is that they are significantly more expensive to perform (in terms of cpu and memory) than traditional aggregates. This is because an exact computation of the percentile (using say, Postgres' [`percentile_cont`]() or [`percentile_disc`]() ) requires the full data set as an ordered list. This is unlike, say, the maximum where I can scan my data set and just keep the largest value I see, for percentiles I need to order the entire data set in order to find the 99th percentile or the 50th percentile etc. This also means that the aggregates are not partializable or parallelizable; there isn't a great form that will allow me to compute the exact percentile on part of my data and combine that with information from another part and give me an exact percentile back. I need all the data, ordered appropriately in order to calculate the exact result.

This is where approximation algorithms come into play: they allow for the calculation of a "good enough" percentile without using all of the data and ordering it before returning a result. There are multiple types of approximation algorithms, we've implemented two of them to start ([uddsketch]() and [tdigest]()), but if you're just getting started, we recommend trying out our [default implementation](), which uses the `uddsketch` implementation, but doesn't require twiddling of various knobs by the user. We believe this will be good enough for most cases, but if you run into an edge case or want different tradeoffs in terms of accuracy etc. we recommend reading [about the algorithms and tradeoffs below]() .

Let's look back at our example from above and use our approximation algorithm alongside:

```SQL
SELECT
    time_bucket('1 h'::interval, ts) as bucket,
    api_id,
    avg(response_time_ms),
    percentile_disc(0.5) WITHIN GROUP (ORDER BY response_time_ms) as true_median,
    approx_percentile(0.5, percentile_agg(response_time_ms))  as approx_median
FROM response_times
GROUP BY 1, 2
ORDER BY 5 DESC LIMIT 15;
```
```output, precision(2: 7)
         bucket         | api_id |      avg      |  true_median  | approx_median
------------------------+--------+---------------+---------------+---------------
 2020-01-01 00:00:00+00 |     12 |  963.71332589 | 718.523974458 | 717.572650369
 2020-01-01 12:00:00+00 |     12 | 960.321550984 | 702.553342115 | 694.973827589
 2020-01-01 00:00:00+00 |     11 | 869.080106405 | 672.323915584 | 673.086719213
 2020-01-01 22:00:00+00 |     11 | 807.601702923 |   588.4594427 | 592.217599089
 2020-01-01 11:00:00+00 |     11 | 812.398067226 | 601.097789543 | 592.217599089
 2020-01-01 18:00:00+00 |      9 |  729.74167841 | 579.954580675 | 592.217599089
 2020-01-01 09:00:00+00 |      9 | 11508.5077421 | 568.587417008 | 573.566636623
 2020-01-01 20:00:00+00 |     10 |  703.37743915 | 547.222908361 | 555.503056905
 2020-01-01 10:00:00+00 |     10 |  706.33545502 | 530.221293445 | 538.008361239
 2020-01-02 00:00:00+00 |     11 | 664.649986691 | 526.017052809 | 525.842421172
 2020-01-01 00:00:00+00 |     10 | 693.538069163 | 520.245282353 | 521.064633515
 2020-01-01 00:00:00+00 |      9 | 699.838199982 | 512.966472958 | 521.064633515
 2020-01-01 08:00:00+00 |      8 | 614.010225183 | 450.329133442 | 444.021967419
 2020-01-01 16:00:00+00 |      8 | 600.166598131 | 448.352142719 | 444.021967419
 2020-01-01 00:00:00+00 |      8 | 10795.1549884 | 430.921181959 | 430.038193446
 ```
Pretty darn close! We can definitely still see the patterns in the data. Note that the calling conventions are a bit different for ours, partially because it's no longer an [ordered set aggregate](), and partially because we use [two-step aggregation](), see the [API documentation]() below for exactly how to use.

The approximation algorithms can provide better performance than algorithms that need the whole sorted data set, especially on very large data sets that can't be easily sorted in memory. Not only that, but they are able to be incorporated into [continuous aggregates](), because they have partializable forms, can be used in [parallel]() and [partitionwise]() aggregation. They are used very frequently in continuous aggregates as that's where they give the largest benefit over the usual Postgres percentile algorithms, which can't be used at all because they require the entire ordered data set to function.

Let's do this with our example, we can't use `percentile_disc` anymore as ordered set aggregates are not supported.

```SQL , non-transactional, ignore-output
CREATE MATERIALIZED VIEW response_times_hourly
WITH (timescaledb.continuous)
AS SELECT
    time_bucket('1 h'::interval, ts) as bucket,
    api_id,
    avg(response_time_ms),
    percentile_agg(response_time_ms)
FROM response_times
GROUP BY 1, 2;
```
Note that we only do the aggregation step of our [two-step aggregation](), we'll save the accessor step for our selects from the view, and we'll start by just getting the same data as our previous example like so:

```SQL
SELECT
    bucket,
    api_id,
    avg,
    approx_percentile(0.5, percentile_agg) as approx_median
FROM response_times_hourly
ORDER BY 4 DESC, 2, 1 LIMIT 15;
```
```output, precision(2: 7)
         bucket         | api_id |      avg      | approx_median
------------------------+--------+---------------+---------------
 2020-01-01 00:00:00+00 |     12 |  963.71332589 | 717.572650369
 2020-01-01 12:00:00+00 |     12 | 960.321550984 | 694.973827589
 2020-01-01 00:00:00+00 |     11 | 869.080106405 | 673.086719213
 2020-01-01 18:00:00+00 |      9 |  729.74167841 | 592.217599089
 2020-01-01 11:00:00+00 |     11 | 812.398067226 | 592.217599089
 2020-01-01 22:00:00+00 |     11 | 807.601702923 | 592.217599089
 2020-01-01 09:00:00+00 |      9 | 11508.5077421 | 573.566636623
 2020-01-01 20:00:00+00 |     10 |  703.37743915 | 555.503056905
 2020-01-01 10:00:00+00 |     10 |  706.33545502 | 538.008361239
 2020-01-02 00:00:00+00 |     11 | 664.649986691 | 525.842421172
 2020-01-01 00:00:00+00 |      9 | 699.838199982 | 521.064633515
 2020-01-01 00:00:00+00 |     10 | 693.538069163 | 521.064633515
 2020-01-01 08:00:00+00 |      8 | 614.010225183 | 444.021967419
 2020-01-01 16:00:00+00 |      8 | 600.166598131 | 444.021967419
 2020-01-01 00:00:00+00 |      8 | 10795.1549884 | 430.038193446
```

So, that's nifty, and much faster, especially for large data sets. But what's even cooler is I can do aggregates over the aggregates and speed those up, let's look at the median by `api_id`:
```SQL
SELECT
    api_id,
    approx_percentile(0.5, rollup(percentile_agg)) as approx_median
FROM response_times_hourly
GROUP BY api_id
ORDER BY api_id;
```
```output
 api_id | approx_median
--------+---------------
      1 | 54.5702804443
      2 | 80.1171187405
      3 | 97.0755568949
      4 | 91.0573557571
      5 | 110.331520385
      6 | 117.623597735
      7 | 110.331520385
      8 | 117.623597735
      9 | 133.685458898
     10 | 117.623597735
     11 | 125.397626136
     12 | 133.685458898
```

You'll notice that I didn't include the average response time here, that's because `avg` is not a [two-step aggregate](), and doesn't actually give you the average if you stack calls using it. But it turns out, we can derive the true average from the sketch we use to calculate the approximate percentiles! (We call that accessor function `mean` because there would otherwise be odd conflicts with `avg` in terms of how they're called).
```SQL
SELECT
    api_id,
    mean(rollup(percentile_agg)) as avg,
    approx_percentile(0.5, rollup(percentile_agg)) as approx_median
FROM response_times_hourly
GROUP BY api_id
ORDER BY api_id;
```
```output, precision(1: 7)
 api_id |      avg      | approx_median
--------+---------------+---------------
      1 | 71.5532290753 | 54.5702804443
      2 | 116.144620055 | 80.1171187405
      3 | 151.694318353 | 97.0755568949
      4 | 151.805468188 | 91.0573557571
      5 | 240.732188975 | 110.331520385
      6 | 242.390944182 | 117.623597735
      7 | 204.316670161 | 110.331520385
      8 | 791.721302735 | 117.623597735
      9 |  730.10776889 | 133.685458898
     10 | 237.621813524 | 117.623597735
     11 | 1006.15878094 | 125.397626136
     12 | 308.595292221 | 133.685458898
```

We have several other accessor functions, including `error` which returns the maximum relative error for the percentile estimate, `num_vals` which returns the number of elements in the estimator, and perhaps the most interesting one, `approx_percentile_rank`, which gives the hypothetical percentile for a given value. Let's say we really don't want our apis to go over 1s in response time (1000 ms), we can use that to figure out what fraction of users waited over a second for each api:

```SQL
SELECT
    api_id,
    ((1 - approx_percentile_rank(1000, rollup(percentile_agg))) * 100)::numeric(6,2) as percent_over_1s
FROM response_times_hourly
GROUP BY api_id
ORDER BY api_id;
```
```output
 api_id | percent_over_1s
--------+-----------------
      1 |            0.00
      2 |            0.00
      3 |            0.00
      4 |            0.42
      5 |            1.61
      6 |            2.59
      7 |            2.90
      8 |            3.20
      9 |            4.47
     10 |            4.42
     11 |            5.84
     12 |            4.97
```


## API <a id="percentile-approx-api"></a>
Aggregate Functions <a id="aggregate-functions">
> - [percentile_agg (point form)](#point-form)
> - [rollup (summary form)](#summary-form)

Accessor Functions <a id="accesor-functions">

> - [error](#error)
> - [mean](#mean)
> - [num_vals](#num-vals)
> - [approx_percentile](#approx_percentile)
> - [approx_percentile_rank](#approx_percentile-at-value)


---
## **percentile_agg (point form)** <a id="point-form"></a>
```SQL ,ignore
percentile_agg(
    value DOUBLE PRECISION
) RETURNS UddSketch
```

This is the default percentile aggregation function. Under the hood, it uses the [UddSketch algorithm](/docs/uddsketch.md) with 200 buckets and an initial max error of 0.001. This should be good for most common use cases of percentile approximation. For more advanced usage of the uddsketch algorithm or use cases for other percentile approximation algorithms see [advanced usage](#advanced-usage). This is the aggregation step of the [two-step aggregate](/docs/two-step_aggregation.md), it is usually used with the [approx_percentile()](#approx_percentile) accessor function in order to extract an approximate percentile, however it is in a form that can be re-aggregated using the [summary form](#summary-form) of the function and any of the other [accessor functions](#accessor-functions).


### Required Arguments <a id="point-form-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `value` | `DOUBLE PRECISION` |  Column to aggregate.
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `percentile_agg` | `UddSketch` | A UddSketch object which may be passed to other percentile approximation APIs|

Because the `percentile_agg` function uses the [UddSketch algorithm](/docs/uddsketch.md), it returns the UddSketch data structure for use in further calls.
<br>

### Sample Usages <a id="point-form-examples"></a>

Get the approximate first percentile using the `percentile_agg()` point form plus the [`approx_percentile`](#approx_percentile) accessor function.
```SQL
SELECT
    approx_percentile(0.01, percentile_agg(data))
FROM generate_series(0, 100) data;
```
```output
approx_percentile
-------------------
             0.999
```

They are often used to create [continuous aggregates]() after which we can use multiple [accessors](#accessor-functions) for [retrospective analysis](/docs/two-step_aggregation.md#retrospective-analysis).

```SQL ,ignore
CREATE MATERIALIZED VIEW foo_hourly
WITH (timescaledb.continuous)
AS SELECT
    time_bucket('1 h'::interval, ts) as bucket,
    percentile_agg(value) as pct_agg
FROM foo
GROUP BY 1;
```
---

## **rollup (summary form)** <a id="summary-form"></a>
```SQL ,ignore
rollup(
    sketch uddsketch
) RETURNS UddSketch
```

This will combine multiple outputs from the [point form](#point-form) of the `percentile_agg()` function, this is especially useful for re-aggregation in the [continuous aggregate]() context (ie bucketing by a larger [`time_bucket`](), or re-grouping on other dimensions included in an aggregation).

### Required Arguments <a id="summary-form-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `sketch` | `UddSketch` | The already constructed uddsketch from a previous [percentile_agg()](#point-form) call. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `uddsketch` | `UddSketch` | A UddSketch object which may be passed to other UddSketch APIs. |

Because the `percentile_agg` function uses the [UddSketch algorithm](/docs/uddsketch.md), `rollup` returns the UddSketch data structure for use in further calls.
<br>

### Sample Usages <a id="summary-form-examples"></a>
Let's presume we created the [continuous aggregate]() in the [point form example](#point-form-examples):

We can then rollup function to re-aggregate the results from the `foo_hourly` view and the [`approx_percentile`](#approx_percentile) accessor function to get the 95th and 99th percentiles over each day:

```SQL , ignore
SELECT
    time_bucket('1 day'::interval, bucket) as bucket,
    approx_percentile(0.95, rollup(pct_agg)) as p95,
    approx_percentile(0.99, rollup(pct_agg)) as p99
FROM foo_hourly
GROUP BY 1;
```

---


## **error** <a id="error"></a>

```SQL ,ignore
error(sketch UddSketch) RETURNS DOUBLE PRECISION
```

This returns the maximum relative error that a percentile estimate will have (relative to the correct value). This means the actual value will fall in the range defined by `approx_percentile(sketch) +/- approx_percentile(sketch)*error(sketch)`.

### Required Arguments <a id="error-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to determine the error of, usually from a [`percentile_agg()`](#aggregate-functions) call. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `error` | `DOUBLE PRECISION` | The maximum relative error of any percentile estimate. |
<br>

### Sample Usages <a id="error-examples"></a>

```SQL
SELECT error(percentile_agg(data))
FROM generate_series(0, 100) data;
```
```output
 error
-------
 0.001
```

---
## **mean** <a id="mean"></a>

```SQL ,ignore
mean(sketch UddSketch) RETURNS DOUBLE PRECISION
```

Get the exact average of all the values in the percentile estimate. (Percentiles returned are estimates, the average is exact.

### Required Arguments <a id="mean-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` |  The sketch to extract the mean value from, usually from a [`percentile_agg()`](#aggregate-functions) call. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `mean` | `DOUBLE PRECISION` | The average of the values in the percentile estimate. |
<br>

### Sample Usage <a id="mean-examples"></a>

```SQL
SELECT mean(percentile_agg(data))
FROM generate_series(0, 100) data;
```
```output
 mean
------
 50
```
## **num_vals** <a id="num-vals"></a>

```SQL ,ignore
num_vals(sketch UddSketch) RETURNS DOUBLE PRECISION
```

Get the number of values contained in a percentile estimate.

### Required Arguments <a id="num-vals-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to extract the number of values from, usually from a [`percentile_agg()`](#aggregate-functions) call. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `uddsketch_count` | `DOUBLE PRECISION` | The number of values in the percentile estimate |
<br>

### Sample Usage <a id="num-vals-examples"></a>

```SQL
SELECT num_vals(percentile_agg(data))
FROM generate_series(0, 100) data;
```
```output
 num_vals
-----------
       101
```

---
---
## **approx_percentile** <a id="approx_percentile"></a>

```SQL ,ignore
approx_percentile(
    percentile DOUBLE PRECISION,
    sketch  uddsketch
) RETURNS DOUBLE PRECISION
```

Get the approximate value at a percentile from a percentile estimate.

### Required Arguments <a id="approx_percentile-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `approx_percentile` | `DOUBLE PRECISION` | The desired percentile (0.0-1.0) to approximate. |
| `sketch` | `UddSketch` | The sketch to compute the approx_percentile on, usually from a [`percentile_agg()`](#aggregate-functions) call. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `approx_percentile` | `DOUBLE PRECISION` | The estimated value at the requested percentile. |
<br>

### Sample Usage <a id="approx_percentile-examples"></a>

```SQL
SELECT
    approx_percentile(0.01, percentile_agg(data))
FROM generate_series(0, 100) data;
```
```output
approx_percentile
-------------------
             0.999
```

---
## **approx_percentile_rank** <a id="approx_percentile_rank"></a>

```SQL ,ignore
approx_percentile_rank(
    value DOUBLE PRECISION,
    sketch UddSketch
) RETURNS UddSketch
```

Estimate what percentile a given value would be located at in a UddSketch.

### Required Arguments <a id="approx_percentile_rank-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `value` | `DOUBLE PRECISION` |  The value to estimate the percentile of. |
| `sketch` | `UddSketch` | The sketch to compute the percentile on. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `approx_percentile_rank` | `DOUBLE PRECISION` | The estimated percentile associated with the provided value. |
<br>

### Sample Usage <a id="approx_percentile_rank-examples"></a>

```SQL
SELECT
    approx_percentile_rank(99, percentile_agg(data))
FROM generate_series(0, 100) data;
```
```output
 approx_percentile_rank
------------------------
         0.985148514851
```



## Advanced Usage: Percentile Approximation Algorithms and How to Choose <a id="advanced-usage"></a>
While the simple `percentile_agg` interface will be sufficient for many users, we do provide more specific APIs for advanced users who want more control of how their percentile approximation is computed and how much space the intermediate representation uses.  We currently provide implementations of the following percentile approximation algorithms:

- [T-Digest](/docs/tdigest.md) – This algorithm buckets data more aggressively toward the center of the quantile range, giving it greater accuracy near the tails (i.e. 0.001 or 0.995).
- [UddSketch](/docs/uddsketch.md) – This algorithm uses exponentially sized buckets to guarantee the approximation falls within a known error range, relative to the true discrete percentile.

There are different tradeoffs that each algorithm makes, and different use cases where each will shine.  The doc pages above each link to the research papers fully detailing the algorithms if you want all the details.  However, at a higher level, here are some of the differences to consider when choosing an algorithm:
1) First off, it's interesting to note that the formal definition for a percentile is actually impercise, and there are different methods for determining what the true percentile actually is.  In Postgres, given a target percentile 'p', `percentile_disc` will return the smallest element of a set such that 'p' percent of the set is less than that element, while `percentile_cont` will return an interpolated value between the two nearest matches for 'p'.  The difference here isn't usually that interesting in practice, but if it matters to your use case, then keep in mind that TDigest will approximate the continuous percentile while UddSketch provides an estimate of the discrete value.
2) It's also important to consider the types of percentiles you're most interested in.  In particular, TDigest is optimized to trade off more accurate estimates at the extremes with weaker estimates near the median.  If your work flow involves estimating 99th percentiles, this is probably a good trade off.  However if you're more concerned about getting highly accurate median estimates, UddSketch is probably a better fit.
3) UddSketch has a stable bucketing function, so it will always return the same quantile estimate for the same underlying data, regardless of how it is ordered or reaggregated.  TDigest, on the other hand, builds up incremental buckets based on the average of nearby points, which will result in (usually subtle) differences in estimates based on the same data, unless the order and batching of the aggregation is strictly controlled (which can be difficult to do in Postgres).  Therefore, if having stable estimates is important to you, UddSketch will likely be required.
4) Trying to calculate precise error bars for TDigest can be difficult, especially when merging multiple subdigests into a larger one (this can come about either through summary aggregation or just parallelization of the normal point aggregate).  If being able to tightly characterize your error is important, UddSketch will likely be the desired algorithm.
5) That being said, the fact that UddSketch uses exponential bucketing to provide a guaranteed relative error can cause some wildly varying absolute errors if the data set covers a large range.  For instance if the data is evenly distributed over the range [1,100], estimates at the high end of the percentile range would have about 100 times the absolute error of those at the low end of the range.  This gets much more extreme if the data range is [0,100].  If having a stable absolute error is important to your use case, consider TDigest.
6) While both implementation will likely get smaller and/or faster with future optimizations, in general UddSketch will end up with a smaller memory footprint than TDigest, and a correspondingly smaller disk footprint for any continuous aggregates.  This is one of the main reasons that the default `percentile_agg` uses UddSketch, and is a pretty good reason to prefer that algorithm if your use case doesn't clearly benefit from TDigest.  Regardless of the algorithm, the best way to improve the accuracy of your percentile estimates is to increase the number of buckets, and UddSketch gives you more leeway to do so.
