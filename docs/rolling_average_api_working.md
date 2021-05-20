
# Info dump on rolling average APIs #

Rolling averages are currntly nasty to do with with timescaledb (user complaint https://news.ycombinator.com/item?id=27051005).  In our timeseries API we will eventually provide a function like
```SQL
moving_average(window => '30 minutes', slide => '5 minutes', data)
```
However, because set-returning aggregates cannot not exist in Postgres, this will not work outside of the timeseries API. Currently, doing rolling average properly requires windowed aggregates. In base SQL it is a real PITA because you have to do sum and count separately and then divide them yourself.

```SQL
SELECT
    time_bucket('5 minutes', time) as bucket, 
    sum(sum(value)) OVER thirty_minutes / sum(count(value)) OVER thirty_minutes as rolling_average
FROM data
GROUP BY 1
WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes', time) RANGE '30 minutes' PRECEDING);
```
Ideally, to do a thirty-minute rolling average every 5 minutes we would provide an API like:

```SQL
SELECT
    time_bucket('5 minutes', time) as bucket,
    rolling_average('5 minutes', value) OVER thirty_minutes
FROM data
GROUP BY bucket
WINDOW thirty_minutes as (ORDER BY ts RANGE '30 minutes' PRECEDING);
```
However, this once again runs into postgres limitations: we need to aggregate over the `value` column in order for this to query to be correctly executed; the `rolling_average()` executes strictly after the `GROUP BY`, and will only see things within its 5-minute group. To fix this issue we need a seperate aggregation step. First we'll aggregate the data into 5-minute summaries, then we'll re-aggregate over 30-minute windows of summaries
```SQL
SELECT
    time_bucket('5 minutes'::interval, time) as bucket,
    average(
        rolling(stats_agg(value)) OVER thirty_minutes
    )
FROM foo
GROUP BY bucket
WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING);
```
While we could create a dedicated `rolling_average()` function used like
```SQL
SELECT
    time_bucket('5 minutes'::interval, time) as bucket,
    rolling_average(stats_agg(value)) OVER thirty_minutes
FROM foo
GROUP BY bucket
WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING);
```
for non-trivial cases, where you want to gather multiple statistics over the same data, this ends up significantly less readable, compare
```SQL
SELECT
    time_bucket('5 minutes'::interval, ts) as bucket, 
    rolling_average(stats_agg(value)) OVER thirty_minutes, 
    rolling_stddev(stats_agg(value)) OVER thirty_minutes,
    rolling_approx_percentile(0.1, percentile_agg(val1)) OVER thirty_minutes,
    rolling_approx_percentile(0.9, percentile_agg(val1)) OVER thirty_minutes
FROM foo
GROUP BY 1
WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING);
```
to
```SQL
SELECT
    bucket, 
    average(rolling_stats),
    stddev(rolling_stats),
    approx_percentile(0.1, rolling_percentile),
    approx_percentile(0.9, rolling_percentile)
FROM (
    SELECT
        time_bucket('5 minutes'::interval, ts) as bucket,
        rolling(stats_agg(value)) OVER thirty_minutes as rolling_stats,
        rolling(percentile_agg(value)) OVER thirty_minutes as rolling_percentile
    FROM foo
    GROUP BY 1
    WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING)
) aggs;
```
since in real world, and all our documentation, we expect to see multi-statistic queries, we plan to optimize for readability in this case, and have seperate rollup and query steps.

Seperating out the re-aggregation step also allows for more powerful composition, for instance:
```SQL
SELECT
    bucket, 
    average(rolling_stats) as rolling_average,
    average(rolling(rolling_stats) OVER (ORDER BY bucket)) AS cumulative_average, 
    average(rolling(rolling_stats) OVER ()) as full_set_average,
    average(rolling_stats) / average(rolling(rolling_stats) OVER ()) as normalized_average
FROM (
    SELECT
        time_bucket('5 minutes'::interval, ts) as bucket,
        rolling(stats_agg(value)) OVER thirty_minutes as rolling_stats
    FROM foo
    GROUP BY 1
    WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING)
) aggs;
```


### A note on style and semantics

```SQL
SELECT
    bucket, 
    average(rolling_stats),
    stddev(rolling_stats),
    approx_percentile(0.1, rolling_percentile),
    approx_percentile(0.9, rolling_percentile)
FROM (
    SELECT
        time_bucket('5 minutes'::interval, ts) as bucket,
        rolling(stats_agg(value)) OVER thirty_minutes as rolling_stats,
        rolling(percentile_agg(value)) OVER thirty_minutes as rolling_percentile
    FROM foo
    GROUP BY 1
    WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING)
) aggs;
```
is equivalent to

```SQL
WITH aggs as (
    SELECT
        time_bucket('5 minutes'::interval, ts) as bucket,
        rolling(stats_agg(value)) OVER thirty_minutes as rolling_stats,
        rolling(percentile_agg(value)) OVER thirty_minutes as rolling_percentile
    FROM foo
    GROUP BY 1
    WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING)
)
SELECT
    bucket, 
    average(rolling_stats),
    stddev(rolling_stats),
    approx_percentile(0.1, rolling_percentile),
    approx_percentile(0.9, rolling_percentile)
FROM aggs;
```

which is also equivalent to, for understanding the order of operations here

```SQL
WITH aggs as (
    SELECT
        time_bucket('5 minutes'::interval, ts) as bucket,
        stats_agg(value),
        percentile_agg(value)
    FROM foo
    GROUP BY 1
), 
rolling_aggs as (
    SELECT
        bucket
        rolling(stats_agg) OVER thirty_minutes as rolling_stats,
        rolling(percentile_agg) OVER thirty_minutes as rolling_percentile
    FROM aggs
    WINDOW thirty_minutes as (ORDER BY bucket RANGE '30 minutes' PRECEDING)
)
SELECT
    bucket, 
    average(rolling_stats),
    stddev(rolling_stats),
    approx_percentile(0.1, rolling_percentile),
    approx_percentile(0.9, rolling_percentile)
FROM rolling_aggs;
```

which is also equivalent to:

```SQL
SELECT
    bucket, 
    average(rolling_stats),
    stddev(rolling_stats),
    approx_percentile(0.1, rolling_percentile),
    approx_percentile(0.9, rolling_percentile)
FROM (
    SELECT
        bucket,
        rolling(stats_agg) OVER thirty_minutes as rolling_stats,
        rolling(percentile_agg) OVER thirty_minutes as rolling_percentile
    FROM (
        SELECT
            time_bucket('5 minutes'::interval, ts) as bucket,
            stats_agg(value),
            percentile_agg(value)
        FROM foo
        GROUP BY 1
    ) aggs
    WINDOW thirty_minutes as (ORDER BY time_bucket('5 minutes'::interval, ts) RANGE '30 minutes' PRECEDING)
) rolling_aggs;
```