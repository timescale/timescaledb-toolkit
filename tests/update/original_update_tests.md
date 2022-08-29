# Original Update Tests



```sql,creation,min-toolkit-version=1.4.0
SET TIME ZONE 'UTC';
CREATE TABLE test_data(ts timestamptz, val DOUBLE PRECISION);
    INSERT INTO test_data
        SELECT '2020-01-01 00:00:00+00'::timestamptz + i * '1 hour'::interval,
        100 + i % 100
    FROM generate_series(0, 10000) i;
	
CREATE MATERIALIZED VIEW regression_view AS
    SELECT
        counter_agg(ts, val) AS countagg,
        hyperloglog(1024, val) AS hll,
        time_weight('locf', ts, val) AS twa,
        uddsketch(100, 0.001, val) as udd,
        tdigest(100, val) as tdig,
        stats_agg(val) as stats
    FROM test_data;
```



```sql,validation,min-toolkit-version=1.4.0
SELECT
    num_resets(countagg),
    distinct_count(hll),
    average(twa),
    approx_percentile(0.1, udd),
    approx_percentile(0.1, tdig),
    kurtosis(stats)
FROM regression_view;
```

```output
 num_resets | distinct_count | average | approx_percentile  | approx_percentile  |      kurtosis
------------+----------------+---------+--------------------+--------------------+--------------------
        100 |            100 |   149.5 | 108.96220333142547 | 109.50489521100047 | 1.7995661075080858
```
