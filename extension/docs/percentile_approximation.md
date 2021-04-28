# Approximate Percentiles
> [Why To Use Approximate Percentiles](#why-use)<br>
> [API](#percentile-approx-api) <br> 
> [Advanced Usage: Algorithms and How to Choose](#algorithms)<br>

###### A note on terminology: Technically, a percentile divides the group into 100 equally sized (by frequency) buckets, while a quantile would divide the group into an arbitrary number of buckets, we use percentile here with the recognition that quantile the technically more "correct" one for anything with more precision than the hundredths place, but we've chosen to go with the more commonly used percentile throughout so that we aren't switching back and forth all the time.

## Why to Use Approximate Percentiles <a id="why-use"></a>

There are really two things to cover here:  1) [why use percentiles at all](#why-use-percent) and 2) [why use *approximate* percentiles rather than exact percentiles](#why-approximate).

To better understand this, we'll use the common example of a server that's running APIs for a company and tracking the response times for the various APIs it's running. So, for our example, we have a table something like this: 

```SQL , non-transactional, ignore-output
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

WITH apis as (SELECT generate_series(1, 12) as api_id), 
users as (SELECT generate_series(1, 30) as user_id), 
api_users as (SELECT * FROM apis JOIN users on api_id % 3 = user_id % 3),  -- users use ~ 1/3 of apis
times as (SELECT generate_series('2020-01-01'::timestamptz, '2020-01-02'::timestamptz, '1 minute'::interval) as ts),
raw_joined as (SELECT * from api_users CROSS JOIN times ORDER BY api_id, user_id, ts),
generated_data as (
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
 2020-01-01 00:00:00-05 |     12 | 957.3300074102655 |  714.7792776452128
 2020-01-01 12:00:00-05 |     12 | 952.2981359887485 |  699.4961287221906
 2020-01-01 00:00:00-05 |     11 | 833.4469472490307 |  627.2483710078043
 2020-01-01 11:00:00-05 |     11 |  825.591115495885 |   586.144811277349
 2020-01-01 22:00:00-05 |     11 | 818.2994213030709 |    627.57106274261
 2020-01-01 00:00:00-05 |      9 |  721.585843481516 |  558.8211532758091
 2020-01-01 09:00:00-05 |      9 | 709.0516956912517 |  531.0335521031565
 2020-01-01 00:00:00-05 |     10 | 704.4238835054174 |  519.6845181743456
 2020-01-01 18:00:00-05 |      9 |  697.206933868508 |  514.1638163837733
 2020-01-01 20:00:00-05 |     10 | 695.8335731245254 |  504.3263060866259
 2020-01-01 10:00:00-05 |     10 | 689.5839792685773 | 484.66032971925443
 2020-01-01 00:00:00-05 |      8 |  614.043390687057 |  458.7690229164113
 2020-01-01 08:00:00-05 |      8 | 606.2064760230946 |  441.5522223012488
 2020-01-01 16:00:00-05 |      8 | 604.1360469272314 |  443.9829264778399
 2020-01-01 01:00:00-05 |     12 | 518.4530780875233 | 395.26063972604095
 ```

So, this returns some interesting results, maybe something like what those of you who read over our [data generation](#data-generation) code would expect. Given how we generate the data, we expect that the larger `api_ids` will have longer generated response times but that it will be cyclic with `hour % api_id`, so we can see that here. 

But what happens if we introduce some aberrant data points? They could have come from anywhere, maybe a user ran a weird query, maybe there's an odd bug in the code that causes some timings to get multiplied in an odd code path, who knows, here we'll introduce just 10 outlier points out of half a million:

```SQL , non-transactional
SELECT setseed(0.43); --make sure we've got a consistent seed so the output is consistent.
WITH rand_points as (SELECT ts, api_id, user_id FROM response_times ORDER BY random() LIMIT 10)
UPDATE response_times SET response_time_ms = 10000 * response_time_ms WHERE (ts, api_id, user_id) IN (SELECT * FROM rand_points); 
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
 2020-01-01 11:00:00-05 |     10 | 3997.6772824820628 |  266.2854035932881
 2020-01-01 18:00:00-05 |      3 |  3825.633846384715 |  187.5657753606962
 2020-01-01 14:00:00-05 |      3 |  1522.639783107579 |  62.67658879283886
 2020-01-01 20:00:00-05 |     12 | 1424.5403899234598 |  82.09740378674013
 2020-01-01 15:00:00-05 |      9 | 1102.3972294841913 |   82.0713285695133
 2020-01-01 00:00:00-05 |     12 |  957.3300074102655 |  714.7792776452128
 2020-01-01 12:00:00-05 |     12 |   952.298135988748 |  699.4961287221906
 2020-01-01 01:00:00-05 |      8 |  900.1847737194663 | 240.30560772652052
 2020-01-01 10:00:00-05 |      6 |  885.9202907985747 |  76.30628539306436
 2020-01-01 00:00:00-05 |     11 |  833.4469472490299 |  627.2483710078043
 2020-01-01 11:00:00-05 |     11 |  825.5911154958851 |   586.144811277349
 2020-01-01 22:00:00-05 |     11 |  818.2994213030717 |    627.57106274261
 2020-01-01 00:00:00-05 |      9 |  721.5858434815163 |  558.8211532758091
 2020-01-01 09:00:00-05 |      9 |  709.0516956912516 |  531.0335521031565
 2020-01-01 00:00:00-05 |     10 |  704.4238835054174 |  519.6845181743456
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
ORDER BY 4 DESC LIMIT 15;
```
```output
         bucket         | api_id |        avg        |       median       
------------------------+--------+-------------------+--------------------
 2020-01-01 00:00:00-05 |     12 | 957.3300074102655 |  714.7792776452128
 2020-01-01 12:00:00-05 |     12 |  952.298135988748 |  699.4961287221906
 2020-01-01 22:00:00-05 |     11 | 818.2994213030717 |    627.57106274261
 2020-01-01 00:00:00-05 |     11 | 833.4469472490299 |  627.2483710078043
 2020-01-01 11:00:00-05 |     11 | 825.5911154958851 |   586.144811277349
 2020-01-01 00:00:00-05 |      9 | 721.5858434815163 |  558.8211532758091
 2020-01-01 09:00:00-05 |      9 | 709.0516956912516 |  531.0335521031565
 2020-01-01 00:00:00-05 |     10 | 704.4238835054174 |  519.6845181743456
 2020-01-01 18:00:00-05 |      9 |  697.206933868508 |  514.1638163837733
 2020-01-01 20:00:00-05 |     10 | 695.8335731245255 |  504.3263060866259
 2020-01-01 10:00:00-05 |     10 |  689.583979268577 | 484.66032971925443
 2020-01-01 00:00:00-05 |      8 | 614.0433906870569 |  458.7690229164113
 2020-01-01 16:00:00-05 |      8 | 604.1360469272315 |  443.9829264778399
 2020-01-01 08:00:00-05 |      8 | 606.2064760230944 |  441.5522223012488
 2020-01-01 01:00:00-05 |     12 | 518.4530780875242 | 395.26063972604095
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
         bucket         | api_id |        avg        |    true_median     |   approx_median    
------------------------+--------+-------------------+--------------------+--------------------
 2020-01-01 00:00:00-05 |     12 | 957.3300074102655 |  714.7792776452128 |  717.5726503686043
 2020-01-01 12:00:00-05 |     12 |  952.298135988748 |  699.4961287221906 |  694.9738275889493
 2020-01-01 22:00:00-05 |     11 | 818.2994213030717 |    627.57106274261 |  631.3586942706065
 2020-01-01 00:00:00-05 |     11 | 833.4469472490299 |  627.2483710078043 |  631.3586942706065
 2020-01-01 11:00:00-05 |     11 | 825.5911154958851 |   586.144811277349 |   592.217599089039
 2020-01-01 00:00:00-05 |      9 | 721.5858434815163 |  558.8211532758091 |  555.5030569048643
 2020-01-01 09:00:00-05 |      9 | 709.0516956912516 |  531.0335521031565 |  538.0083612387168
 2020-01-01 00:00:00-05 |     10 | 704.4238835054174 |  519.6845181743456 |  521.0646335153136
 2020-01-01 18:00:00-05 |      9 |  697.206933868508 |  514.1638163837733 |  521.0646335153136
 2020-01-01 20:00:00-05 |     10 | 695.8335731245255 |  504.3263060866259 |  512.5971054008514
 2020-01-01 10:00:00-05 |     10 |  689.583979268577 | 484.66032971925443 |  480.8186373535084
 2020-01-01 00:00:00-05 |      8 | 614.0433906870569 |  458.7690229164113 |  458.4604589902221
 2020-01-01 08:00:00-05 |      8 | 606.2064760230944 |  441.5522223012488 | 444.02196741884285
 2020-01-01 16:00:00-05 |      8 | 604.1360469272315 |  443.9829264778399 | 444.02196741884285
 2020-01-01 01:00:00-05 |     12 | 518.4530780875242 | 395.26063972604095 |  390.6742117791449
 ```
Pretty darn close! We can definitely still see the patterns in the data. Note that the calling conventions are a bit different for ours, partially because it's no longer an [ordered set aggregate](), and partially because we use [two-step aggregation](), see the [API documentation]() below for exactly how to use. 

The approximation algorithms can provide better performance than algorithms that need the whole sorted data set, especially on very large data sets that can't be easily sorted in memory. Not only that, but  they are able to be incorporated into [continuous aggregates](), because they have partializable forms, can be used in [parallel]() and [partitionwise]() aggregation, and can be [re-aggregated](link to example?) over larger time frames, or over different axes. 

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
GROUP BY 1, 2
```
Note that we only do the aggregation step of our [two-step aggregation](), we'll save the accessor step for our selects from the view, and we'll start by just getting the same data as our previous example like so:

```SQL
SELECT
    bucket,
    api_id,
    avg,
    approx_percentile(0.5, percentile_agg) as approx_median
FROM response_times_hourly
ORDER BY 4 DESC LIMIT 15;
```
```output
         bucket         | api_id |        avg        |   approx_median    
------------------------+--------+-------------------+--------------------
 2020-01-01 00:00:00-05 |     12 |  957.330007410265 |  717.5726503686043
 2020-01-01 12:00:00-05 |     12 | 952.2981359887486 |  694.9738275889493
 2020-01-01 00:00:00-05 |     11 | 833.4469472490314 |  631.3586942706065
 2020-01-01 22:00:00-05 |     11 | 818.2994213030719 |  631.3586942706065
 2020-01-01 11:00:00-05 |     11 | 825.5911154958854 |   592.217599089039
 2020-01-01 00:00:00-05 |      9 | 721.5858434815136 |  555.5030569048643
 2020-01-01 09:00:00-05 |      9 | 709.0516956912521 |  538.0083612387168
 2020-01-01 18:00:00-05 |      9 | 697.2069338685094 |  521.0646335153136
 2020-01-01 00:00:00-05 |     10 | 704.4238835054173 |  521.0646335153136
 2020-01-01 20:00:00-05 |     10 | 695.8335731245246 |  512.5971054008514
 2020-01-01 10:00:00-05 |     10 | 689.5839792685764 |  480.8186373535084
 2020-01-01 00:00:00-05 |      8 | 614.0433906870558 |  458.4604589902221
 2020-01-01 08:00:00-05 |      8 | 606.2064760230948 | 444.02196741884285
 2020-01-01 16:00:00-05 |      8 | 604.1360469272308 | 444.02196741884285
 2020-01-01 00:00:00-05 |      7 |  518.260705892737 |  390.6742117791449
 ```

So, that's nifty, and much faster, especially for large data sets. But what's even cooler is I can do aggregates over the aggregates and speed those up, let's look at the median by `api_id`: 
```SQL
SELECT
    api_id,
    approx_percentile(0.5, percentile_agg(percentile_agg)) as approx_median
FROM response_times_hourly
GROUP BY api_id;
```
```output
 api_id |   approx_median    
--------+--------------------
      1 | 54.570280444259595
      2 |  80.11711874054426
      3 | 103.49151551904819
      4 |  91.05735575711863
      5 | 110.33152038490614
      6 |  117.6235977354534
      7 | 110.33152038490614
      8 |  117.6235977354534
      9 | 142.52105459674107
     10 |  117.6235977354534
     11 |   133.685458898177
     12 |   133.685458898177
```

You'll notice that I didn't include the average response time here, that's because `avg` is not a [two-step aggregate](), and doesn't actually give you the average if you stack calls using it. But it turns out, we can derive the true average from the sketch we use to calculate the approximate percentiles! (We call that accessor function `mean` because there would otherwise be odd conflicts with `avg` in terms of how they're called).
```SQL 
SELECT
    api_id,
    mean(percentile_agg(percentile_agg)) as avg,
    approx_percentile(0.5, percentile_agg(percentile_agg)) as approx_median
FROM response_times_hourly
GROUP BY api_id;
```
```output
 api_id |        avg         |   approx_median    
--------+--------------------+--------------------
      1 |  72.33502661563753 | 54.570280444259595
      2 | 116.19832913566916 |  80.11711874054426
      3 |  359.6251042486034 | 103.49151551904819
      4 | 151.34792844065697 |  91.05735575711863
      5 | 180.45501348645712 | 110.33152038490614
      6 |  243.3608186338227 |  117.6235977354534
      7 | 203.59208681247378 | 110.33152038490614
      8 | 234.95379212561386 |  117.6235977354534
      9 |  290.9090915449227 | 142.52105459674107
     10 | 408.90710586132195 |  117.6235977354534
     11 | 267.13533347022087 |   133.685458898177
     12 |  312.5189463905521 |   133.685458898177
```

We have several other accessor functions, including `error` which returns the maximum relative error for the percentile estimate, `num_vals` which returns the number of elements in the estimator, and perhaps the most interesting one, `approx_percentile_at_value`, which gives the hypothetical percentile for a given value. Let's say we really don't want our apis to go over 1s in response time (1000 ms), we can use that to figure out what fraction of users waited over a second for each api:

```SQL
SELECT
    api_id,
    (1 - approx_percentile_at_value(percentile_agg(percentile_agg), 1000)) * 100 as percent_over_1s
FROM response_times_hourly
GROUP BY api_id;
```


## API <a id="percentile-approx-api"></a>


## Advanced Usage: Percentile Approximation Algorithms and How to Choose <a id="algorithms"></a>
