# Approximate Percentiles
> [Why To Use Approximate Percentiles](#why-use)<br>
> [API](#percentile-approx-api) <br>
> [Advanced Usage: Algorithms and How to Choose](#advanced-usage)<br>

###### A note on terminology: Technically, a percentile divides the group into 100 equally sized (by frequency) buckets, while a quantile would divide the group into an arbitrary number of buckets, we use percentile here with the recognition that quantile the technically more "correct" one for anything with more precision than the hundredths place, but we've chosen to go with the more commonly used percentile throughout so that we aren't switching back and forth all the time.

## Why to Use Approximate Percentiles <a id="why-use"></a>

There are really two things to cover here:  1) [why use percentiles at all](#why-use-percent) and 2) [why use *approximate* percentiles rather than exact percentiles](#why-approximate).

To better understand this, we'll use the common example of a server that's running APIs for a company and tracking the response times for the various APIs it's running. So, for our example, we have a table something like this:

```SQL , non-transactional, ignore-output
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
```output
         bucket         | api_id |        avg        |       median
------------------------+--------+-------------------+--------------------
 2020-01-01 00:00:00+00 |     12 | 963.7133258896499 |  718.5239744582293
 2020-01-01 12:00:00+00 |     12 | 960.3215509841885 |  702.5533421145439
 2020-01-01 00:00:00+00 |     11 |  869.080106405452 |  672.3239155837705
 2020-01-01 11:00:00+00 |     11 | 812.3980672261094 |  601.0977895428585
 2020-01-01 22:00:00+00 |     11 |   807.60170292334 |  588.4594427003582
 2020-01-01 09:00:00+00 |      9 | 734.5715252282495 |  568.5874170077199
 2020-01-01 18:00:00+00 |      9 |  729.741678409908 |  579.9545806754643
 2020-01-01 10:00:00+00 |     10 | 706.3354550201176 |  530.2212934454012
 2020-01-01 20:00:00+00 |     10 | 703.3774391502177 |  547.2229083613446
 2020-01-01 00:00:00+00 |      9 | 699.8381999820344 |  512.9664729583037
 2020-01-01 00:00:00+00 |     10 |  693.538069162809 |  520.2452823525146
 2020-01-02 00:00:00+00 |     11 | 664.6499866911574 |  526.0170528091467
 2020-01-01 08:00:00+00 |      8 | 614.0102251830702 | 450.32913344205554
 2020-01-01 16:00:00+00 |      8 | 600.1665981310882 |  448.3521427192275
 2020-01-01 00:00:00+00 |      8 |  598.260875149253 | 430.92118195891203
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

```output
         bucket         | api_id |        avg         |       median
------------------------+--------+--------------------+--------------------
 2020-01-01 09:00:00+00 |      9 | 11508.507742106061 |  568.5874170077199
 2020-01-01 13:00:00+00 |     11 | 11406.136516310735 |  218.6133315749926
 2020-01-01 00:00:00+00 |      8 | 10795.154988400745 | 430.92118195891203
 2020-01-01 02:00:00+00 |     11 |  6982.659433972783 | 231.99713608466845
 2020-01-01 21:00:00+00 |      8 |  4166.715331816164 |  80.90204788382914
 2020-01-01 12:00:00+00 |      5 |   1417.81186884559 |  97.16190172907324
 2020-01-01 18:00:00+00 |     12 | 1382.2166820029004 |  110.6070630315704
 2020-01-01 19:00:00+00 |      9 | 1152.8696063521115 | 300.07408283070794
 2020-01-01 23:00:00+00 |      6 | 1025.7105719733838 |  68.24708016025141
 2020-01-01 00:00:00+00 |     12 |    963.71332588965 |  718.5239744582293
 2020-01-01 12:00:00+00 |     12 |   960.321550984189 |  702.5533421145439
 2020-01-01 00:00:00+00 |     11 |  869.0801064054518 |  672.3239155837705
 2020-01-01 11:00:00+00 |     11 |  812.3980672261094 |  601.0977895428585
 2020-01-01 22:00:00+00 |     11 |    807.60170292334 |  588.4594427003582
 2020-01-01 18:00:00+00 |      9 |  729.7416784099084 |  579.9545806754643
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
```output
         bucket         | api_id |        avg         |       median
------------------------+--------+--------------------+--------------------
 2020-01-01 00:00:00+00 |     12 |    963.71332588965 |  718.5239744582293
 2020-01-01 12:00:00+00 |     12 |   960.321550984189 |  702.5533421145439
 2020-01-01 00:00:00+00 |     11 |  869.0801064054518 |  672.3239155837705
 2020-01-01 11:00:00+00 |     11 |  812.3980672261094 |  601.0977895428585
 2020-01-01 22:00:00+00 |     11 |    807.60170292334 |  588.4594427003582
 2020-01-01 18:00:00+00 |      9 |  729.7416784099084 |  579.9545806754643
 2020-01-01 09:00:00+00 |      9 | 11508.507742106061 |  568.5874170077199
 2020-01-01 20:00:00+00 |     10 |  703.3774391502178 |  547.2229083613446
 2020-01-01 10:00:00+00 |     10 |  706.3354550201176 |  530.2212934454012
 2020-01-02 00:00:00+00 |     11 |  664.6499866911576 |  526.0170528091467
 2020-01-01 00:00:00+00 |     10 |  693.5380691628102 |  520.2452823525146
 2020-01-01 00:00:00+00 |      9 |  699.8381999820344 |  512.9664729583037
 2020-01-01 08:00:00+00 |      8 |  614.0102251830706 | 450.32913344205554
 2020-01-01 16:00:00+00 |      8 |   600.166598131088 |  448.3521427192275
 2020-01-01 00:00:00+00 |      8 | 10795.154988400745 | 430.92118195891203
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
```output
                  bucket         | api_id |        avg         |    true_median     |   approx_median
------------------------+--------+--------------------+--------------------+--------------------
 2020-01-01 00:00:00+00 |     12 |    963.71332588965 |  718.5239744582293 |   717.572650368603
 2020-01-01 12:00:00+00 |     12 |   960.321550984189 |  702.5533421145439 |   694.973827588948
 2020-01-01 00:00:00+00 |     11 |  869.0801064054518 |  672.3239155837705 |  673.0867192130734
 2020-01-01 22:00:00+00 |     11 |    807.60170292334 |  588.4594427003582 |  592.2175990890378
 2020-01-01 11:00:00+00 |     11 |  812.3980672261094 |  601.0977895428585 |  592.2175990890378
 2020-01-01 18:00:00+00 |      9 |  729.7416784099084 |  579.9545806754643 |  592.2175990890378
 2020-01-01 09:00:00+00 |      9 | 11508.507742106061 |  568.5874170077199 |  573.5666366228246
 2020-01-01 20:00:00+00 |     10 |  703.3774391502178 |  547.2229083613446 |  555.5030569048633
 2020-01-01 10:00:00+00 |     10 |  706.3354550201176 |  530.2212934454012 |  538.0083612387158
 2020-01-02 00:00:00+00 |     11 |  664.6499866911576 |  526.0170528091467 |  525.8424211721613
 2020-01-01 00:00:00+00 |     10 |  693.5380691628102 |  520.2452823525146 |  521.0646335153127
 2020-01-01 00:00:00+00 |      9 |  699.8381999820344 |  512.9664729583037 |  521.0646335153127
 2020-01-01 08:00:00+00 |      8 |  614.0102251830706 | 450.32913344205554 | 444.02196741884205
 2020-01-01 16:00:00+00 |      8 |   600.166598131088 |  448.3521427192275 | 444.02196741884205
 2020-01-01 00:00:00+00 |      8 | 10795.154988400745 | 430.92118195891203 | 430.03819344582666
 ```
Pretty darn close! We can definitely still see the patterns in the data. Note that the calling conventions are a bit different for ours, partially because it's no longer an [ordered set aggregate](), and partially because we use [two-step aggregation](), see the [API documentation]() below for exactly how to use.

The approximation algorithms can provide better performance than algorithms that need the whole sorted data set, especially on very large data sets that can't be easily sorted in memory. Not only that, but they are able to be incorporated into [continuous aggregates](), because they have partializable forms, can be used in [parallel]() and [partitionwise]() aggregation. They are used very frequently in continous aggregates as that's where they give the largest benefit over the usual Postgres percentile algorithms, which can't be used at all because they require the entire ordered data set to function.

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
```output
         bucket         | api_id |        avg         |   approx_median
------------------------+--------+--------------------+--------------------
 2020-01-01 00:00:00+00 |     12 |    963.71332588965 |   717.572650368603
 2020-01-01 12:00:00+00 |     12 |   960.321550984189 |   694.973827588948
 2020-01-01 00:00:00+00 |     11 |  869.0801064054518 |  673.0867192130734
 2020-01-01 18:00:00+00 |      9 |  729.7416784099084 |  592.2175990890378
 2020-01-01 11:00:00+00 |     11 |  812.3980672261094 |  592.2175990890378
 2020-01-01 22:00:00+00 |     11 |    807.60170292334 |  592.2175990890378
 2020-01-01 09:00:00+00 |      9 | 11508.507742106061 |  573.5666366228246
 2020-01-01 20:00:00+00 |     10 |  703.3774391502178 |  555.5030569048633
 2020-01-01 10:00:00+00 |     10 |  706.3354550201176 |  538.0083612387158
 2020-01-02 00:00:00+00 |     11 |  664.6499866911576 |  525.8424211721613
 2020-01-01 00:00:00+00 |      9 |  699.8381999820344 |  521.0646335153127
 2020-01-01 00:00:00+00 |     10 |  693.5380691628102 |  521.0646335153127
 2020-01-01 08:00:00+00 |      8 |  614.0102251830706 | 444.02196741884205
 2020-01-01 16:00:00+00 |      8 |   600.166598131088 | 444.02196741884205
 2020-01-01 00:00:00+00 |      8 | 10795.154988400745 | 430.03819344582666
```

So, that's nifty, and much faster, especially for large data sets. But what's even cooler is I can do aggregates over the aggregates and speed those up, let's look at the median by `api_id`:
```SQL
SELECT
    api_id,
    approx_percentile(0.5, percentile_agg(percentile_agg)) as approx_median
FROM response_times_hourly
GROUP BY api_id
ORDER BY api_id;
```
```output
 api_id |   approx_median
--------+--------------------
      1 |  54.57028044425955
      2 |  80.11711874054421
      3 |  97.07555689493275
      4 |  91.05735575711857
      5 | 110.33152038490607
      6 | 117.62359773545333
      7 | 110.33152038490607
      8 | 117.62359773545333
      9 | 133.68545889817688
     10 | 117.62359773545333
     11 | 125.39762613589876
     12 | 133.68545889817688
```

You'll notice that I didn't include the average response time here, that's because `avg` is not a [two-step aggregate](), and doesn't actually give you the average if you stack calls using it. But it turns out, we can derive the true average from the sketch we use to calculate the approximate percentiles! (We call that accessor function `mean` because there would otherwise be odd conflicts with `avg` in terms of how they're called).
```SQL
SELECT
    api_id,
    mean(percentile_agg(percentile_agg)) as avg,
    approx_percentile(0.5, percentile_agg(percentile_agg)) as approx_median
FROM response_times_hourly
GROUP BY api_id
ORDER BY api_id;
```
```output
 api_id |        avg         |   approx_median
--------+--------------------+--------------------
      1 |  71.55322907526073 |  54.57028044425955
      2 | 116.14462005516907 |  80.11711874054421
      3 |  151.6943183529463 |  97.07555689493275
      4 | 151.80546818776742 |  91.05735575711857
      5 |  240.7321889752658 | 110.33152038490607
      6 | 242.39094418199042 | 117.62359773545333
      7 | 204.31667016105945 | 110.33152038490607
      8 |  791.7213027346409 | 117.62359773545333
      9 |  730.1077688899509 | 133.68545889817688
     10 |   237.621813523762 | 117.62359773545333
     11 | 1006.1587809419942 | 125.39762613589876
     12 |   308.595292220514 | 133.68545889817688
```

We have several other accessor functions, including `error` which returns the maximum relative error for the percentile estimate, `num_vals` which returns the number of elements in the estimator, and perhaps the most interesting one, `approx_percentile_rank`, which gives the hypothetical percentile for a given value. Let's say we really don't want our apis to go over 1s in response time (1000 ms), we can use that to figure out what fraction of users waited over a second for each api:

```SQL
SELECT
    api_id,
    ((1 - approx_percentile_rank(1000, percentile_agg(percentile_agg))) * 100)::numeric(6,2) as percent_over_1s
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
> - [percentile_agg - point form](#point-form)
> - [percentile_agg - summary form](#summary-form)

Accessor Functions <a id="accesor-functions">

> - [error](#error)
> - [mean](#mean)
> - [num_vals](#num-vals)
> - [approx_percentile](#approx_percentile)
> - [approx_percentile_rank](#approx_percentile-at-value)


---
## **percentile_agg (point form) ** <a id="point-form"></a>
```SQL ,ignore
percentile_agg(
    value DOUBLE PRECISION
) RETURNS UddSketch
```

This is the default percentile aggregation function. Under the hood, it uses the [UddSketch algorithm](/extension/docs/uddsketch.md) with 200 buckets and an initial max error of 0.001. This should be good for most common use cases of percentile approximation. For more advanced usage of the uddsketch algorithm or use cases for other percentile approximation algorithms see [advanced usage](#advanced-usage). This is the aggregation step of the [two-step aggregate](/extension/docs/two-step_aggregation.md), it is usually used with the [approx_percentile()](#approx_percentile) accessor function in order to extract an approximate percentile, however it is in a form that can be re-aggregated using the [summary form](#summary-form) of the function and any of the other [accessor functions](#accessor-functions).


### Required Arguments <a id="point-form-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `value` | `DOUBLE PRECISION` |  Column to aggregate.
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `percentile_agg` | `UddSketch` | A UddSketch object which may be passed to other percentile approximation APIs|

Because the `percentile_agg` function uses the [UddSketch algorithm](/extension/docs/uddsketch.md), it returns the UddSketch data structure for use in further calls.
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

They are often used to create [continuous aggregates]() after which we can use multiple [accessors](#accessor-functions) for [retrospective analysis](/extension/docs/two-step_aggregation.md#retrospective-analysis).

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

## **percentile_agg (summary form)** <a id="summary-form"></a>
```SQL ,ignore
percentile_agg(
    sketch uddsketch
) RETURNS UddSketch
```

This will combine multiple outputs from the [point form](#point-form) of the `percentile_agg()` function, this is especially useful for re-aggregation in the [continuous aggregate]() context (ie bucketing by a larger [`time_bucket`](), or re-grouping on other dimensions included in an aggregation).

### Required Arguments <a id="summary-form-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `sketch` | `UddSketch` | The already constructed uddsketch from a previous [percentile_agg() (point form)](#point-form) call. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `uddsketch` | `UddSketch` | A UddSketch object which may be passed to other UddSketch APIs. |

Because the `percentile_agg` function uses the [UddSketch algorithm](/extension/docs/uddsketch.md), it returns the UddSketch data structure for use in further calls.
<br>

### Sample Usages <a id="summary-form-examples"></a>
Let's presume we created the [continuous aggregate]() in the [point form example](#point-form-examples):

We can then use the summary form of the percentile_agg function to re-aggregate the results from the `foo_hourly` view and the [`approx_percentile`](#approx_percentile) accessor function to get the 95th and 99th percentiles over each day:

```SQL , ignore
SELECT
    time_bucket('1 day'::interval, bucket) as bucket,
    approx_percentile(0.95, percentile_agg(pct_agg)) as p95,
    approx_percentile(0.99, percentile_agg(pct_agg)) as p99
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
----------------------------
         0.9851485148514851
```



## Advanced Usage: Percentile Approximation Algorithms and How to Choose <a id="advanced-usage"></a>
While the simple `percentile_agg` interface will be sufficient for many users, we provide two different lower level algorithms for percentile approximation for advanced users who want more control of how the approximation is performed:

- [T-Digest](/extension/docs/tdigest.md) [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes) – A quantile estimate sketch optimized to provide more accurate estimates near the tails (i.e. 0.001 or 0.995) than conventional approaches. ([Methods](tdigest#tdigest_api))
- [UddSketch](/extension/docs/uddsketch.md) – A quantile estimate sketch which provides a guaranteed maximum relative error. ([Methods](uddsketch.md#uddsketch_api))


Their docs pages provide links to full papers explaining the algorithms. When trying to figure out which is best for your use case, often the best thing to do is to construct some simple queries using each of the candidate algorithms as well as the exact [`percentile_cont`]() that Postgres provides in order to understand the error tradeoffs for your typical data distribution. However, there are a few things to note about the two algorithms:
1) UddSketch provides a maximum relative error for all of its estimates, this can grow relatively large if you have a large range of values, and a lot of values. You can tune the amount / range of data you can best represent by tuning the number of buckets, however UddSketch will almost always have some error, as it is always returning the middle of the bucket so that it can provide consistent error bounding.
2) Tdigest can often provide better absolute estimates than UddSketch, but in edge cases it can provide very large errors, if you happen to have a data distribution that it doesn't get along with, UddSketch can also have large error for certain data distributions, but it will tell you that it does. Tdigest is also prone to error if you have multiple re-aggregation steps, especially if some of the digests have highly disparate numbers of values and it is not a perfectly "stackable" aggregate, in that multiple re-aggregation steps can lead to different results than if you performed the algorithm directly on the underlying data (data order input can even sometimes have an affect though this is usually smaller). UddSketch does not have this problem, it will provide the same result no matter the reaggregation steps or order of input.