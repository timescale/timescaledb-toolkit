# Gauge Aggregates [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

A gauge is a metric similar to a counter, with the primary difference being
that it measures a value that varies up and down over time, rather than an
ever-increasing COUNT of the number of times something happened.
Examples include resource utilization metrics, precipitation levels,
or temperatures.

`gauge_agg` currently shares implementation with `counter_agg` but without the
resetting logic.  This means it enforces ordering even though that is not
necessarily required for all gauge aggregates.  We may offer an additional
unordered gauge aggregate in the future.

# Test table

Examples below are tested against the following table:

```SQL ,non-transactional
SET TIME ZONE 'UTC';
CREATE TABLE gauge_test (
    measure_id      BIGINT,
    ts              TIMESTAMPTZ ,
    val             DOUBLE PRECISION,
    PRIMARY KEY (measure_id, ts)
);
INSERT INTO gauge_test SELECT 1, '2020-01-03 UTC'::timestamptz + make_interval(days=>v), v + 1000 FROM generate_series(1,10) v;
INSERT INTO gauge_test SELECT 2, '2020-01-03 UTC'::timestamptz + make_interval(days=>v), v + 2000 FROM generate_series(1,10) v;
INSERT INTO gauge_test SELECT 3, '2020-01-03 UTC'::timestamptz + make_interval(days=>v), v + 3000 FROM generate_series(1,10) v;
```

## Functions

### delta

```SQL
SELECT toolkit_experimental.delta(toolkit_experimental.gauge_agg(ts, val)) FROM gauge_test;
```
```output
 delta
-------
 -1991
```

### idelta_left

```SQL
SELECT toolkit_experimental.idelta_left(toolkit_experimental.gauge_agg(ts, val)) FROM gauge_test;
```
```output
 idelta_left
-------------
        1002
```

### idelta_right

```SQL
SELECT toolkit_experimental.idelta_right(toolkit_experimental.gauge_agg(ts, val)) FROM gauge_test;
```
```output
 idelta_right
--------------
         1010
```

### rollup

```SQL
WITH t as (SELECT date_trunc('minute', ts), toolkit_experimental.gauge_agg(ts, val) as agg FROM gauge_test group by 1)
    SELECT toolkit_experimental.delta(toolkit_experimental.rollup(agg)) FROM t;
```
```output
 rollup delta
--------------
            9
```
