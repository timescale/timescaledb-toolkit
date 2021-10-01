# Continuous aggregation tests

This document serves as a driver for allowing our doctester to verify the behavior of some of our features on continuous aggregates.  It is not intended to serve as documentation, though it does present an example of using continuous aggregates with some toolkit code.

## Setup table
Remove this first section once these features are stabilized.
```SQL ,non-transactional,ignore-output
SET search_path TO "$user", public, toolkit_experimental;
SET timescaledb_toolkit_acknowledge_auto_drop TO 'true';
```

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
    stats_agg(value1, value2) as stats
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
SELECT week, distinct_count(hll), rate(counter), skewness_x(stats)
FROM weekly_aggs
WHERE week > '2020-06-01'::TIMESTAMPTZ;
```

```output
          week          | distinct_count |         rate          |      skewness_x       
------------------------+----------------+-----------------------+-----------------------
 2020-06-08 00:00:00+00 |             73 | 0.0006270791749833666 |   0.09701672748134149
 2020-06-15 00:00:00+00 |             70 |  0.000653692614770459 |  -0.08851573882262405
 2020-06-22 00:00:00+00 |             68 | 0.0006803060545575516 |   0.08646850352936841
 2020-06-29 00:00:00+00 |             36 | 0.0007069194943446441 | -0.025733637198277898
 2020-07-06 00:00:00+00 |             31 | 0.0009713905522288756 |   0.16900196092210715
 2020-07-13 00:00:00+00 |             28 |  0.001162674650698603 |  0.043206872023093014
 2020-07-20 00:00:00+00 |             22 | 0.0016833000665335994 |    0.3444137283612257
 2020-07-27 00:00:00+00 |             10 |  0.004324712643678161 |    0.6249161132826231
```

```SQL 
SELECT distinct_count(rollup(hll)), stderror(rollup(hll))
FROM weekly_aggs;
```

```output
 distinct_count | stderror 
----------------+----------
            123 |     0.13
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
SELECT average_y(rollup(stats)), stddev_y(rollup(stats)), skewness_y(rollup(stats)), kurtosis_y(rollup(stats))
FROM weekly_aggs;
```

```output
 average_y |     stddev_y      |     skewness_y      |     kurtosis_y     
-----------+-------------------+---------------------+--------------------
        67 | 23.68778400591983 | -0.5657484434338034 | 2.3996434937611384
```
