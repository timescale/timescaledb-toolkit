# Heartbeat Aggregation

## Description

TimescaleDB Toolkit provides the `heartbeat_agg` aggregate to evaluate and track the liveness state of an underlying system based on a set of heartbeat timestamps.

The aggregate constructs a timeline by trating each heartbeat as "live" for a specified duration following the timestamp. Any point in the specified overall time range that does not closely follow a heartbeat is considered "dead" (downtime). This is particularly useful for tracking system health or finding exact intervals where a device went offline. 

## Details

Timescale's `heartbeat_agg` is implemented as an aggregate function that takes incoming heartbeat timestamps and groups them into contiguous live intervals.

It requires to specify an aggregation window (`agg_start` and `agg_duration`). All heartbeat points passed to the aggregate must occur strictly withins this bounding interval. The state collects these points and processes them in batches, combining overlapping liveness intervals into a consolidated timeline of up and down states.

Because hearbeat logs are often grouped by time buckets, the toolkit also provides a rollup function to combine multiple sub-aggregates into a single, unified liveness timeline. Additionally, interpolation functions allow to bridge the liveness gap seamlessly across adjacent time buckets.

## Usage Example

For this example, assume we have a table called `system_health` containing periodic ping timestamps from a device. We want to construct an aggregate of system liveness over a 2-hour window, assuming a system is unhealthy if it hasn't reported in for 10 minutes. 

```SQL
CREATE TABLE system_health(heartbeat TIMESTAMPTZ);

INSERT INTO system_health VALUES
    ('2020-01-01 00:02:20 UTC'),
    ('2020-01-01 00:10:00 UTC'),
    ('2020-01-01 00:17:00 UTC'),
    ('2020-01-01 00:50:30 UTC'),
    ('2020-01-01 01:00:00 UTC');
```

We can aggregate this data using `heartbeat_agg` and immediately inspect the ranges where the system was considered "live" using the `live_ranges` accessor:

```SQL
SELECT live_ranges(
    heartbeat_agg(
        heartbeat, 
        '2020-01-01 00:00:00 UTC', 
        '2h', 
        '10m'
    )
)
FROM system_health;
```
```
                     live_ranges
-----------------------------------------------------
 ("2020-01-01 01:02:20+01","2020-01-01 01:27:00+01")
 ("2020-01-01 01:50:30+01","2020-01-01 02:10:00+01")
(2 rows)
```

We can also query the total uptime and downtime for that aggregate:

```SQL
WITH agg AS (
    SELECT heartbeat_agg(heartbeat, '2020-01-01 00:00:00 UTC', '2h', '10m') as hb
    FROM system_health
)
SELECT uptime(hb), downtime(hb) FROM agg;
```
```
  uptime  | downtime
----------+----------
 00:44:10 | 01:15:50
(1 row)
```

## Continuos Aggregate Example

Timescale continuos aggregates provide an easy way to keep heartbeat sketches up to date as new telemetry arrives.

First, we create a hypertable and insert data:

```SQL
CREATE TABLE heartbeats(time TIMESTAMPTZ, device_id INT);
SELECT create_hypertable('heartbeats', 'time');
```
```
    create_hypertable
--------------------------
 (50,public,heartbeats,t)
(1 row)
```
We create a continuos aggregate view that summarizes system health into 1-hour buckets. We define liveness as having received a ping in the last 5 minutes.

```SQL
CREATE MATERIALIZED VIEW hourly_health
WITH (timescaledb.continuous, timescaledb.materialized_only=false)
AS SELECT
    device_id,
    time_bucket('1 hour'::interval, time) as bucket,
    heartbeat_agg(time, time_bucket('1 hour'::interval, time), '1 hour', '5 minutes') as health_sketch
FROM heartbeats
GROUP BY device_id, time_bucket('1 hour'::interval, time);
```

We can later combine these hourly sketches acroos larger periods using the `rollup` funcion:

```SQL
SELECT
    device_id,
    uptime(rollup(health_sketch)) as total_daily_uptime,
    num_gaps(rollup(health_sketch)) as total_outages
FROM hourly_health
WHERE bucket >= '2020-01-01 00:00:00' AND bucket < '2020-01-02 00:00:00'
GROUP BY device_id;
```
```
 device_id | total_daily_uptime | total_outages
-----------+--------------------+---------------
(0 rows)
```

## Command List

### Aggregate Functions

- `heartbeat_agg`
- `rollup`

### Accessor Functions

- `dead_ranges`
- `downtime`
- `interpolate`
- `interpolated_downtime`
- `interpolated_uptime`
- `live_at`
- `live_ranges`
- `num_gaps`
- `num_live_ranges`
- `trim_to`
- `uptime`

## API