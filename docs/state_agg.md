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
CREATE TABLE states_test_5(ts TIMESTAMPTZ, state BIGINT); -- states_test with integer states
INSERT INTO states_test_5 VALUES
    ('2020-01-01 00:00:00+00', 4),
    ('2020-01-01 00:00:11+00', 51351),
    ('2020-01-01 00:01:00+00', 2),
    ('2020-01-01 00:02:03+00', 51351),
    ('2020-01-01 00:02:05+00', -9);
CREATE TABLE states_test_6(ts TIMESTAMPTZ, state BIGINT); -- states_test_3 with integer states
INSERT INTO states_test_6 VALUES
    ('2019-12-31 00:00:11+00', 456789),
    ('2019-12-31 00:01:00+00', 4);
```

## Functions

### duration_in

Compute the amount of time spent in a state as INTERVAL.

```SQL
SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'ERROR') FROM states_test;
```
```output
 interval
----------
 00:00:03
```
```SQL
SELECT toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 2) FROM states_test_4;
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
    toolkit_experimental.duration_in(toolkit_experimental.compact_state_agg(ts, state), 'ERROR')
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
SELECT duration_in(state_agg(ts, state), 'OK', '2020-01-01 00:01:00+00', '2 days') FROM states_test;
```
```output
 duration_in
-------------
 00:00:57
```
```SQL
SELECT duration_in(state_agg(ts, state), 'OK', '2020-01-01 00:01:00+00', NULL) FROM states_test;
```
```output
 duration_in
-------------
 00:00:57
```
```SQL
SELECT duration_in(state_agg(ts, state), 'OK', '2020-01-01 00:01:00+00') FROM states_test;
```
```output
 duration_in
-------------
 00:00:57
```
```SQL
SELECT duration_in(state_agg(ts, state), 51351, '2020-01-01 00:01:00+00', '2 days') FROM states_test_4;
```
```output
 duration_in
-------------
 00:00:57
```
```SQL
SELECT duration_in(state_agg(ts, state), 51351, '2020-01-01 00:01:00+00', NULL) FROM states_test_4;
```
```output
 duration_in
-------------
 00:00:57
```

```SQL
SELECT duration_in(state_agg(ts, state), 'OK', '2020-01-01 00:00:15+00', '30 seconds') FROM states_test;
```
```output
 duration_in
-------------
 00:00:30
```

```SQL
SELECT duration_in(state_agg(ts, state), 51351, '2020-01-01 00:00:15+00', '1 minute 1 second') FROM states_test_4;
```
```output
 duration_in
-------------
 00:00:58
```

```SQL
SELECT duration_in(state_agg(ts, state), 'OK', '2020-01-01 00:00:15+00', '1 minute 1 second') FROM states_test;
```
```output
 duration_in
-------------
 00:00:58
```

```SQL
SELECT (SELECT state_agg(ts, state) FROM states_test) -> duration_in('OK'::text, '2020-01-01 00:00:15+00', '1 minute 1 second');
```
```output
 ?column?
-------------
 00:00:58
```

```SQL
SELECT (SELECT state_agg(ts, state) FROM states_test) -> duration_in('OK');
```
```output
 ?column?
-------------
 00:01:46
```

### into_values

```SQL
SELECT state, duration FROM toolkit_experimental.into_values(
    (SELECT toolkit_experimental.compact_state_agg(ts, state) FROM states_test))
    ORDER BY state, duration;
```
```output
 state | duration
-------+-----------
 ERROR |  00:00:03
 OK    |  00:01:46
 START |  00:00:11
 STOP  |  00:00:00
```
```SQL
SELECT state, duration FROM into_int_values(
    (SELECT state_agg(ts, state) FROM states_test_4))
    ORDER BY state, duration;
```
```output
 state | duration
-------+-----------
   -9 |  00:00:00
    2 |  00:00:03
    4 |  00:00:11
51351 |  00:01:46
```
```SQL
SELECT (state_agg(ts, state) -> into_values()).* FROM states_test ORDER BY state;
```
```output
 state | duration
-------+----------
 ERROR | 00:00:03
 OK    | 00:01:46
 START | 00:00:11
 STOP  | 00:00:00
```

### state_timeline

```SQL
SELECT (state_agg(ts, state) -> state_timeline()).* FROM states_test;
```
```output
 state |       start_time       |        end_time
-------+------------------------+------------------------
 START | 2020-01-01 00:00:00+00 | 2020-01-01 00:00:11+00
 OK    | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
 ERROR | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
 OK    | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
 STOP  | 2020-01-01 00:02:00+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT state, start_time, end_time FROM state_timeline(
    (SELECT state_agg(ts, state) FROM states_test))
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
SELECT state, start_time, end_time FROM state_int_timeline(
    (SELECT state_agg(ts, state) FROM states_test_4))
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
SELECT state, start_time, end_time FROM state_timeline(
    (SELECT state_agg(ts, state) FROM states_test_2))
    ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
START | 2019-12-31 00:00:00+00 | 2019-12-31 00:00:11+00
   OK | 2019-12-31 00:00:11+00 | 2019-12-31 00:02:00+00
 STOP | 2019-12-31 00:02:00+00 | 2019-12-31 00:02:00+00
```

### state_in

```SQL
SELECT state_at(
    (SELECT state_agg(ts, state) FROM states_test),
    '2020-01-01 00:01:02+00'
);
```
```output
 state_at
----------
 ERROR
```
```SQL
SELECT state_at_int(
    (SELECT state_agg(ts, state) FROM states_test_5),
    '2020-01-01 00:01:02+00'
);
```
```output
 state_at
----------
 2
```
```SQL
SELECT state_at(
    (SELECT state_agg(ts, state) FROM states_test),
    '2020-01-01 00:01:00+00'
);
```
```output
 state_at
----------
 ERROR
```
```SQL
SELECT state_at(
    (SELECT state_agg(ts, state) FROM states_test),
    '2020-01-01 00:00:05+00'
);
```
```output
 state_at
----------
 START
```
```SQL
SELECT state_at(
    (SELECT state_agg(ts, state) FROM states_test),
    '2020-01-01 00:00:00+00'
);
```
```output
 state_at
----------
 START
```
```SQL
SELECT state_at(
    (SELECT state_agg(ts, state) FROM states_test),
    '2019-12-31 23:59:59.999999+00'
);
```
```output
 state_at
----------
 
```
```SQL
SELECT state_at(
    (SELECT state_agg(ts, state) FROM states_test),
    '2025-01-01 00:00:00+00'
);
```
```output
 state_at
----------
 STOP
```
```SQL
SELECT (SELECT state_agg(ts, state) FROM states_test) -> state_at('2025-01-01 00:00:00+00');
```
```output
 ?column?
----------
 STOP
```

## state_periods

```SQL
SELECT start_time, end_time
FROM state_periods(
    (SELECT state_agg(ts, state) FROM states_test),
    'OK'
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
SELECT ((SELECT state_agg(ts, state) FROM states_test) -> state_periods('OK')).*;
```
```output
       start_time       |        end_time
------------------------+------------------------
 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT start_time, end_time
FROM state_periods(
    (SELECT state_agg(ts, state) FROM states_test_4),
    51351
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
FROM state_periods(
    (SELECT state_agg(ts, state) FROM states_test),
    'ANYTHING'
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
```

## interpolated_state_timeline

```SQL
SELECT state, start_time, end_time FROM interpolated_state_timeline(
    (SELECT state_agg(ts, state) FROM states_test),
    '2019-12-31', '1 days',
    (SELECT state_agg(ts, state) FROM states_test_3)
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
SELECT ((SELECT state_agg(ts, state) FROM states_test) -> interpolated_state_timeline(
    '2019-12-31', '1 days',
    (SELECT state_agg(ts, state) FROM states_test_3)
)).*
ORDER BY start_time;
```
```output
 state |       start_time       |        end_time
-------+------------------------+------------------------
 START | 2019-12-31 00:00:00+00 | 2020-01-01 00:00:11+00
 OK    | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
 ERROR | 2020-01-01 00:01:00+00 | 2020-01-01 00:01:03+00
 OK    | 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
 STOP  | 2020-01-01 00:02:00+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT state, start_time, end_time FROM interpolated_state_int_timeline(
    (SELECT state_agg(ts, state) FROM states_test_5),
    '2019-12-31', '1 days',
    (SELECT state_agg(ts, state) FROM states_test_6)
)
ORDER BY start_time;
```
```output
state | start_time             | end_time
------+------------------------+-----------------------
    4 | 2019-12-31 00:00:00+00 | 2020-01-01 00:00:11+00
51351 | 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
    2 | 2020-01-01 00:01:00+00 | 2020-01-01 00:02:03+00
51351 | 2020-01-01 00:02:03+00 | 2020-01-01 00:02:05+00
   -9 | 2020-01-01 00:02:05+00 | 2020-01-01 00:02:05+00
```

```SQL
SELECT state, start_time, end_time FROM interpolated_state_timeline(
    (SELECT state_agg(ts, state) FROM states_test),
    '2019-12-31', '5 days',
    (SELECT state_agg(ts, state) FROM states_test_3)
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
SELECT state, start_time, end_time FROM interpolated_state_timeline(
    (SELECT state_agg(ts, state) FROM states_test),
    '2019-12-31', '1 days',
    (SELECT state_agg(ts, state) FROM states_test_2)
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
SELECT state, start_time, end_time FROM interpolated_state_timeline(
    (SELECT state_agg(ts, state) FROM states_test),
    '2019-12-31', '5 days',
    (SELECT state_agg(ts, state) FROM states_test_2)
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

```SQL
SELECT (state_agg(ts, state) -> state_periods('OK')).* FROM states_test;
```
```output
       start_time       |        end_time
------------------------+------------------------
 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
```

## interpolated_state_periods

```SQL
SELECT start_time, end_time FROM interpolated_state_periods(
    (SELECT state_agg(ts, state) FROM states_test),
    'OK',
    '2019-12-31', '1 days',
    (SELECT state_agg(ts, state) FROM states_test_3)
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
SELECT ((SELECT state_agg(ts, state) FROM states_test) -> interpolated_state_periods(
    'OK',
    '2019-12-31', '1 days',
    (SELECT state_agg(ts, state) FROM states_test_3)
)).*
ORDER BY start_time;
```
```output
       start_time       |        end_time
------------------------+------------------------
 2020-01-01 00:00:11+00 | 2020-01-01 00:01:00+00
 2020-01-01 00:01:03+00 | 2020-01-01 00:02:00+00
```

```SQL
SELECT start_time, end_time FROM interpolated_state_periods(
    (SELECT state_agg(ts, state) FROM states_test),
    'START',
    '2019-12-31', '5 days',
    (SELECT state_agg(ts, state) FROM states_test_3)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2019-12-31 00:00:00+00 | 2020-01-01 00:00:11+00
```

```SQL
SELECT start_time, end_time FROM interpolated_state_periods(
    (SELECT state_agg(ts, state) FROM states_test_5),
    4,
    '2019-12-31', '5 days',
    (SELECT state_agg(ts, state) FROM states_test_6)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2019-12-31 00:00:00+00 | 2020-01-01 00:00:11+00
```

```SQL
SELECT start_time, end_time FROM interpolated_state_periods(
    (SELECT state_agg(ts, state) FROM states_test),
    'STOP',
    '2019-12-31', '1 days',
    (SELECT state_agg(ts, state) FROM states_test_2)
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
SELECT start_time, end_time FROM interpolated_state_periods(
    (SELECT state_agg(ts, state) FROM states_test),
    'STOP',
    '2019-12-31', '5 days',
    (SELECT state_agg(ts, state) FROM states_test_2)
)
ORDER BY start_time;
```
```output
start_time             | end_time
-----------------------+-----------------------
2019-12-31 00:00:00+00 | 2020-01-01 00:00:00+00
2020-01-01 00:02:00+00 | 2020-01-05 00:00:00+00
```

## rollup

```SQL
WITH buckets AS (SELECT
    date_trunc('minute', ts) as dt,
    toolkit_experimental.compact_state_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts))
SELECT toolkit_experimental.duration_in(
    toolkit_experimental.rollup(buckets.sa),
    'START'
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
    toolkit_experimental.compact_state_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts))
SELECT toolkit_experimental.duration_in(
    toolkit_experimental.rollup(buckets.sa),
    'OK'
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
    state_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts))
SELECT state_timeline(
    rollup(buckets.sa)
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
    state_agg(ts, state) AS sa
FROM states_test
GROUP BY date_trunc('minute', ts)
HAVING date_trunc('minute', ts) != '2020-01-01 00:01:00+00'::timestamptz)
SELECT state_timeline(
    rollup(buckets.sa)
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
    state_agg(ts, state) AS sa
FROM states_test_5
GROUP BY date_trunc('minute', ts)
HAVING date_trunc('minute', ts) != '2020-01-01 00:01:00+00'::timestamptz)
SELECT state_int_timeline(
    rollup(buckets.sa)
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

## With continuous aggregate

```SQL ,non-transactional,ignore-output
CREATE TABLE email_status (
  id BIGINT,
  ts TIMESTAMPTZ,
  status TEXT
);
SELECT create_hypertable('email_status','ts');

INSERT INTO email_status("ts", "id", "status")
VALUES
('2022-01-11 11:51:12',1,'draft'),
('2022-01-11 11:53:23',1,'queued'),
('2022-01-11 11:57:46',1,'sending'),
('2022-01-11 11:57:50',1,'sent'),
('2022-01-11 11:52:12',2,'draft'),
('2022-01-11 11:58:23',2,'queued'),
('2022-01-11 12:00:46',2,'sending'),
('2022-01-11 12:01:03',2,'bounced');
```

```SQL ,non-transactional,ignore-output
CREATE MATERIALIZED VIEW sa
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT time_bucket('1 minute'::interval, ts) AS bucket,
  id,
  state_agg(ts, status) AS agg
FROM email_status
GROUP BY bucket, id;
```

```SQL
SELECT rollup(agg) -> duration_in('draft') FROM sa WHERE id = 1;
```
```output
 ?column?
----------
 00:02:11
```

```SQL
SELECT (state_timeline(rollup(agg))).* FROM sa WHERE id = 2;
```
```output
  state  |       start_time       |        end_time
---------+------------------------+------------------------
 draft   | 2022-01-11 11:52:12+00 | 2022-01-11 11:58:23+00
 queued  | 2022-01-11 11:58:23+00 | 2022-01-11 12:00:46+00
 sending | 2022-01-11 12:00:46+00 | 2022-01-11 12:01:03+00
 bounced | 2022-01-11 12:01:03+00 | 2022-01-11 12:01:03+00
```
