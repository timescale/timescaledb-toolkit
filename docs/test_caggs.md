# Continuous aggregation tests

This document serves as a driver for allowing our doctester to verify the behavior of some of our features on continuous aggregates.  It is not intended to serve as documentation, though it does present an example of using continuous aggregates with some toolkit code.

We're also going to adjust extra_float_digits to use 12 significant digits.  This prevents some spurious failures in the skewness and kurtosis accessors used below.
```SQL ,non-transactional,ignore-output
SET extra_float_digits = -3;
```

## Setup table
```SQL ,non-transactional,ignore-output
SET TIME ZONE 'UTC';
CREATE TABLE test(time TIMESTAMPTZ, value1 DOUBLE PRECISION, value2 DOUBLE PRECISION);
SELECT create_hypertable('test', 'time');
```

## Setup continuous aggs
```SQL ,non-transactional,ignore-output
CREATE MATERIALIZED VIEW weekly_aggs
WITH (timescaledb.continuous)
AS SELECT
    time_bucket('7 day'::interval, time) as week,
    hyperloglog(64, value1) as hll,
    counter_agg(time, value1) as counter,
    stats_agg(value1, value2) as stats,
    timevector(time, value2) as tvec
FROM test
GROUP BY time_bucket('7 day'::interval, time);
```

## Populate table

```SQL ,non-transactional,ignore-output
INSERT INTO test
    SELECT '2020-01-01'::TIMESTAMPTZ + '1 hour'::INTERVAL * row_number() OVER (),
        v.b, v.b::DOUBLE PRECISION/v.a::DOUBLE PRECISION
    FROM (SELECT a, generate_series(a, 100) AS b FROM generate_series(1, 100) a) v;
```

## Validate continuous aggs

```SQL
SELECT week, distinct_count(hll), rate(counter), skewness_x(stats, 'population')
FROM weekly_aggs
WHERE week > '2020-06-01'::TIMESTAMPTZ
ORDER BY week;
```

```output
          week          | distinct_count |       rate        |    skewness_x
------------------------+----------------+-------------------+-------------------
 2020-06-08 00:00:00+00 |             49 | 0.000627079174983 |  0.0970167274813
 2020-06-15 00:00:00+00 |             45 |  0.00065369261477 | -0.0885157388226
 2020-06-22 00:00:00+00 |             42 | 0.000680306054558 |  0.0864685035294
 2020-06-29 00:00:00+00 |             36 | 0.000706919494345 | -0.0257336371983
 2020-07-06 00:00:00+00 |             31 | 0.000971390552229 |   0.169001960922
 2020-07-13 00:00:00+00 |             28 |   0.0011626746507 |  0.0432068720231
 2020-07-20 00:00:00+00 |             22 |  0.00168330006653 |   0.344413728361
 2020-07-27 00:00:00+00 |             10 |  0.00432471264368 |   0.624916113283
```

```SQL
SELECT distinct_count(rollup(hll)), stderror(rollup(hll))
FROM weekly_aggs;
```

```output
 distinct_count | stderror
----------------+----------
            115 |     0.13
```

```SQL
SELECT num_resets(rollup(counter))
FROM weekly_aggs;
```

```output
 num_resets
------------
         98
```

```SQL
SELECT average_y(rollup(stats)), stddev_y(rollup(stats)), skewness_y(rollup(stats), 'population'), kurtosis_y(rollup(stats), 'population')
FROM weekly_aggs;
```

```output
 average_y |   stddev_y    |   skewness_y    |   kurtosis_y
-----------+---------------+-----------------+----------------
        67 | 23.6877840059 | -0.565748443434 | 2.39964349376
```

```SQL
SELECT week, count(*)
FROM (
    SELECT week, unnest(tvec)
    FROM weekly_aggs
    WHERE week > '2020-06-01'::TIMESTAMPTZ
) s
GROUP BY week
ORDER BY week;
```
```output
          week          | count 
------------------------+-------
 2020-06-08 00:00:00+00 |   168
 2020-06-15 00:00:00+00 |   168
 2020-06-22 00:00:00+00 |   168
 2020-06-29 00:00:00+00 |   168
 2020-07-06 00:00:00+00 |   168
 2020-07-13 00:00:00+00 |   168
 2020-07-20 00:00:00+00 |   168
 2020-07-27 00:00:00+00 |    59
```
