# State Aggregation [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

# Test table

Examples below are tested against the following tables:

```SQL ,non-transactional
SET TIME ZONE 'UTC';
CREATE TABLE states_test(ts TIMESTAMPTZ, state TEXT);
INSERT INTO states_test VALUES
    ('2020-01-01 00:00:00+00', 'START'),
    ('2020-01-01 00:00:11+00', 'OK'),
    ('2020-01-01 00:01:00+00', 'ERROR'),
    ('2020-01-01 00:01:03+00', 'OK'),
    ('2020-01-01 00:02:00+00', 'STOP');
CREATE TABLE states_test_2(ts TIMESTAMPTZ, state TEXT);
INSERT INTO states_test_2 VALUES
    ('2019-12-31 00:00:00+00', 'START'),
    ('2019-12-31 00:00:11+00', 'OK'),
    ('2019-12-31 00:02:00+00', 'STOP'),
    ('2019-12-31 00:01:03+00', 'OK');
CREATE TABLE states_test_3(ts TIMESTAMPTZ, state TEXT);
INSERT INTO states_test_3 VALUES
    ('2019-12-31 00:00:11+00', 'UNUSED'),
    ('2019-12-31 00:01:00+00', 'START');
CREATE TABLE states_test_4(ts TIMESTAMPTZ, state BIGINT);
INSERT INTO states_test_4 VALUES
    ('2020-01-01 00:00:00+00', 4),
    ('2020-01-01 00:00:11+00', 51351),
    ('2020-01-01 00:01:00+00', 2),
    ('2020-01-01 00:01:03+00', 51351),
    ('2020-01-01 00:02:00+00', -9);
CREATE TABLE states_test_5(ts TIMESTAMPTZ, state BIGINT);
INSERT INTO states_test_5 VALUES
    ('2020-01-01 00:00:00+00', 4),
    ('2020-01-01 00:00:11+00', 51351),
    ('2020-01-01 00:01:00+00', 2),
    ('2020-01-01 00:02:03+00', 51351),
    ('2020-01-01 00:02:05+00', -9);
```

## Functions

### duration_in

Compute the amount of time spent in a state as INTERVAL.

```SQL
SELECT toolkit_experimental.duration_in('ERROR', toolkit_experimental.state_agg(ts, state)) FROM states_test;
```
```output
 interval
----------
 00:00:03
```
```SQL
SELECT toolkit_experimental.duration_in(2, toolkit_experimental.state_agg(ts, state)) FROM states_test_4;
```
```output
 interval
----------
 00:00:03
```

Extract as number of seconds:

```SQL
SELECT
  EXTRACT(epoch FROM
    toolkit_experimental.duration_in('ERROR', toolkit_experimental.state_agg(ts, state))
  )::INTEGER
FROM states_test;
```
```output
 seconds
---------
       3
```

#### duration_in for a range
```SQL
SELECT toolkit_experimental.duration_in('OK', toolkit_experimental.timeline_agg(ts, state), '2020-01-01 00:01:00+00', '2020-01-03 00:01:00+00') FROM states_test;
```
```output
 duration_in
-------------
 00:00:57
```
```SQL
SELECT toolkit_experimental.duration_in('OK', toolkit_experimental.timeline_agg(ts, state), '2020-01-01 00:01:00+00') FROM states_test;
```
```output
 duration_in
-------------
 00:00:57
```
```SQL
SELECT toolkit_experimental.duration_in(51351, toolkit_experimental.timeline_agg(ts, state), '2020-01-01 00:01:00+00', '2020-01-03 00:01:00+00') FROM states_test_4;
```
```output
 duration_in
-------------
 00:00:57
```
```SQL
SELECT toolkit_experimental.duration_in(51351, toolkit_experimental.timeline_agg(ts, state), '2020-01-01 00:01:00+00') FROM states_test_4;
```
```output
 duration_in
-------------
 00:00:57
```

### into_values

```SQL
SELECT state, duration FROM toolkit_experimental.into_values(
    (SELECT toolkit_experimental.state_agg(ts, state) FROM states_test))
    ORDER BY state, duration;
```
```output
 state | duration
-------+-----------
 ERROR |   3000000
 OK    | 106000000
 START |  11000000
 STOP  |         0
```
```SQL
SELECT state, duration FROM toolkit_experimental.into_int_values(
    (SELECT toolkit_experimental.state_agg(ts, state) FROM states_test_4))
    ORDER BY state, duration;
```
```output
 state | duration
-------+-----------
   -9 |         0
    2 |   3000000
    4 |  11000000
51351 | 106000000
```

### state_timeline

```SQL
SELECT state, start_time, end_time FROM toolkit_experimental.state_timeline(
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test))
    ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
START | 2020-01-01 00:00:00+00 | 2020-01-01 00:00:11+00
   OK | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
ERROR | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
   OK | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
 STOP | 2020-01-01 00:02:00+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT state, start_time, end_time FROM toolkit_experimental.state_int_timeline(
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_4))
    ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
    4 | 2020-01-01 00:00:00+00 | 2020-01-01 00:00:11+00
51351 | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
    2 | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
51351 | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
   -9 | 2020-01-01 00:02:00+00 | 2020-01-01 00:02:00+00
```


```SQL
SELECT state, start_time, end_time FROM toolkit_experimental.state_timeline(
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2))
    ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
START | 2019-12-31 00:00:00+00 | 2019-12-31 00:00:11+00
   OK | 2019-12-31 00:00:11+00 | 2019-12-31 00:02:00+00
 STOP | 2019-12-31 00:02:00+00 | 2019-12-31 00:02:00+00
```

## state_periods

```SQL
SELECT start_time, end_time
FROM toolkit_experimental.state_periods(
    'OK',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT start_time, end_time
FROM toolkit_experimental.state_periods(
    51351,
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_4)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT start_time, end_time
FROM toolkit_experimental.state_periods(
    'ANYTHING',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
```

## interpolated_state_timeline

```SQL
SELECT state, start_time, end_time FROM toolkit_experimental.interpolated_state_timeline(
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '1 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3)
)
ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
START | 2019-12-31 00:00:00+00 | 2020-01-01 00:00:11+00
   OK | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
ERROR | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
   OK | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
 STOP | 2020-01-01 00:02:00+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT state, start_time, end_time FROM toolkit_experimental.interpolated_state_timeline(
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '5 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3)
)
ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
START | 2019-12-31 00:00:00+00 | 2020-01-01 00:00:11+00
   OK | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
ERROR | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
   OK | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
 STOP | 2020-01-01 00:02:00+00 | 2020-01-05 00:00:00+00
```

```SQL
SELECT state, start_time, end_time FROM toolkit_experimental.interpolated_state_timeline(
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '1 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2)
)
ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
 STOP | 2019-12-31 00:00:00+00 | 2020-01-01 00:00:00+00
START | 2020-01-01 00:00:00+00 | 2020-01-01 00:00:11+00
   OK | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
ERROR | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
   OK | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
 STOP | 2020-01-01 00:02:00+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT state, start_time, end_time FROM toolkit_experimental.interpolated_state_timeline(
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '5 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2)
)
ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
 STOP | 2019-12-31 00:00:00+00 | 2020-01-01 00:00:00+00
START | 2020-01-01 00:00:00+00 | 2020-01-01 00:00:11+00
   OK | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
ERROR | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
   OK | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
 STOP | 2020-01-01 00:02:00+00 | 2020-01-05 00:00:00+00
```


## interpolated_state_periods

```SQL
SELECT start_time, end_time FROM toolkit_experimental.interpolated_state_periods(
    'OK',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '1 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT start_time, end_time FROM toolkit_experimental.interpolated_state_periods(
    'START',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '5 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_3)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2019-12-31 00:00:00+00 | 2020-01-01 00:00:11+00
```

```SQL
SELECT start_time, end_time FROM toolkit_experimental.interpolated_state_periods(
    'STOP',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '1 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2019-12-31 00:00:00+00 | 2020-01-01 00:00:00+00
2020-01-01 00:02:00+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT start_time, end_time FROM toolkit_experimental.interpolated_state_periods(
    'STOP',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test),
    '2019-12-31', '5 days',
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2),
    (SELECT toolkit_experimental.timeline_agg(ts, state) FROM states_test_2)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2019-12-31 00:00:00+00 | 2020-01-01 00:00:00+00
2020-01-01 00:02:00+00 | 2020-01-05 00:00:00+00
```

## rolllup

```SQL
WITH buckets AS (SELECT
    date_trunc('minute', ts) as dt,
    toolkit_experimental.state_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts))
SELECT toolkit_experimental.duration_in(
    'START',
    toolkit_experimental.rollup(buckets.sa)
)
FROM buckets;
```
```output
 interval
----------
 00:00:11
```

```SQL
WITH buckets AS (SELECT
    date_trunc('minute', ts) as dt,
    toolkit_experimental.state_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts))
SELECT toolkit_experimental.duration_in(
    'OK',
    toolkit_experimental.rollup(buckets.sa)
)
FROM buckets;
```
```output
 interval
----------
 00:01:46
```

```SQL
WITH buckets AS (SELECT
    date_trunc('minute', ts) as dt,
    toolkit_experimental.timeline_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts))
SELECT toolkit_experimental.state_timeline(
    toolkit_experimental.rollup(buckets.sa)
)
FROM buckets;
```
```output
                      state_timeline
-----------------------------------------------------------
(START,"2020-01-01 00:00:00+00","2020-01-01 00:00:11+00")
   (OK,"2020-01-01 00:00:11+00","2020-01-01 00:01:00+00")
(ERROR,"2020-01-01 00:01:00+00","2020-01-01 00:01:03+00")
   (OK,"2020-01-01 00:01:03+00","2020-01-01 00:02:00+00")
 (STOP,"2020-01-01 00:02:00+00","2020-01-01 00:02:00+00")
```

```SQL
WITH buckets AS (SELECT
    date_trunc('minute', ts) as dt,
    toolkit_experimental.timeline_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts)
HAVING date_trunc('minute', ts) != '2020-01-01 00:01:00+00'::timestamptz)
SELECT toolkit_experimental.state_timeline(
    toolkit_experimental.rollup(buckets.sa)
)
FROM buckets;
```
```output
                      state_timeline
-----------------------------------------------------------
(START,"2020-01-01 00:00:00+00","2020-01-01 00:00:11+00")
   (OK,"2020-01-01 00:00:11+00","2020-01-01 00:02:00+00")
 (STOP,"2020-01-01 00:02:00+00","2020-01-01 00:02:00+00")
```

```SQL
WITH buckets AS (SELECT
    date_trunc('minute', ts) as dt,
    toolkit_experimental.timeline_agg(ts, state) AS sa
FROM states_test_5
GROUP BY date_trunc('minute', ts)
HAVING date_trunc('minute', ts) != '2020-01-01 00:01:00+00'::timestamptz)
SELECT toolkit_experimental.state_int_timeline(
    toolkit_experimental.rollup(buckets.sa)
)
FROM buckets;
```
```output
                      state_timeline
-----------------------------------------------------------
    (4,"2020-01-01 00:00:00+00","2020-01-01 00:00:11+00")
(51351,"2020-01-01 00:00:11+00","2020-01-01 00:02:05+00")
   (-9,"2020-01-01 00:02:05+00","2020-01-01 00:02:05+00")
```
