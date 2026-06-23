# Heartbeat Aggregation

## Description

TimescaleDB Toolkit provides the `heartbeat_agg` aggregate to evaluate and track the liveness state of an underlying system based on a set of heartbeat timestamps.

The aggregate constructs a timeline by traiting each heartbeat as "live" for a specified duration following the timestamp. Any point in the specified overall time range that does not fall within `heartbeat_liveness` duration after a heartbeat is considered "dead" (downtime). This is particularly useful for tracking system health or finding exact intervals where a device went offline. 

## Details

TimescaleDB Toolkit `heartbeat_agg` is implemented as an aggregate function that takes incoming heartbeat timestamps and groups them into contiguous live intervals.

It requires to specify an aggregation window (`agg_start` and `agg_duration`). All heartbeat points passed to the aggregate must occur strictly withins this bounding interval. Note that `agg_duration` must be strictly greater than `heartbeat_liveness`. The state collects these points and processes them in batches, combining overlapping liveness intervals into a consolidated timeline of up and down states.

Because heartbeat logs are often grouped by time buckets, the toolkit also provides a rollup function to combine multiple sub-aggregates into a single, unified liveness timeline. Additionally, interpolation functions allow bridging the liveness gap seamlessly across adjacent time buckets.

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
SET TIMEZONE TO 'UTC';

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
 ("2020-01-01 00:02:20+00","2020-01-01 00:27:00+00")
 ("2020-01-01 00:50:30+00","2020-01-01 01:10:00+00")
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

Accessor functions can also be called using the `->` arrow operator, which is useful for chaining:

```SQL
WITH agg AS (
    SELECT heartbeat_agg(heartbeat, '2020-01-01 00:00:00 UTC', '2h', '10m') as hb
    FROM system_health
)
SELECT hb -> uptime() AS uptime, hb -> downtime() AS downtime FROM agg;
```
```
  uptime  | downtime
----------+----------
 00:44:10 | 01:15:50
(1 row)
```

## Continuous Aggregate Example

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

We create a continuous aggregate view that summarizes system health into 1-hour buckets. We define liveness as having received a ping in the last 5 minutes.

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

> **Note:** The query above returns 0 rows because no data has been inserted into `heartbeats` yet. Once the hypertable is populated with telemetry, `hourly_health` will contain rows and this query
> will return results per device. 

## API
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

## heartbeat_agg

```SQL
heartbeat_agg(
    heartbeat TIMESTAMPTZ,
    agg_start TIMESTAMPTZ,
    agg_duration INTERVAL,
    heartbeat_liveness INTERVAL
) RETURNS HeartbeatAgg
```

Constructs and returns a `HeartbeatAgg` to evaluate liveness over a specific time bound.

### Required Arguments

| Name | Type | Description |
|------|------|-------------|
| heartbeat | TIMESTAMPTZ | The column containing the timestamps of the heartbeats. |
| agg_start | TIMESTAMPTZ | The start of the time range over which this aggregate is tracking liveness. |
| agg_duration | INTERVAL | The length of the bounding time range. All heartbeats evaluated must fall within agg_start and agg_start + agg_duration. Must be strictly greater than heartbeat_liveness. |
| heartbeat_liveness | INTERVAL | How long the system is considered to be live after each heartbeat. |

## rollup

```SQL
rollup(
    agg HeartbeatAgg
) RETURNS HeartbeatAgg
```

Combines multiple `HeartbeatAgg` objects into a single aggregate.

### Required Arguments

| Name | Type | Description |
|------|------|-------------|
| agg | HeartbeatAgg | Previously constructed HeartbeatAgg objects (often grouped by time buckets). |

## dead_ranges

```SQL
dead_ranges(agg HeartbeatAgg) RETURNS TABLE (start TIMESTAMPTZ, end TIMESTAMPTZ)
```

Returns the intervals where the system was considered offline or down within the aggregated time range. If no heartbeats were recorded, returns the entire aggregation window as a single dead range.

## downtime

```SQL
downtime(agg HeartbeatAgg) RETURNS INTERVAL
```

Returns the total cumulative downtime (the sum of all dead ranges) during the aggregated interval.

## interpolate

```SQL
interpolate(
    agg HeartbeatAgg,
    pred HeartbeatAgg DEFAULT NULL
) RETURNS HeartbeatAgg
```

Adjust the start of the current aggregate's liveness range based on the final heartbeat of a preceding aggregate (`pred`). This connects cross-bucket gaps if a heartbeat near the end of a previous bucket should provide liveness that spills into the current bucket. If `pred` is NULL (e.g., for the first row when used with `LAG()`), the aggregate is returned unchanged.


## interpolated_downtime

```SQL
interpolated_downtime(
    agg HeartbeatAgg,
    pred HeartbeatAgg DEFAULT NULL
) RETURNS INTERVAL
```

Returns the total downtime of the aggregate, adjusting for any continuous liveness spilling over from the given preceding aggregate. If `pred` is NULL, behaves identically to `downtime`.


## interpolated_uptime

```SQL
interpolated_uptime(
    agg HeartbeatAgg,
    pred HeartbeatAgg DEFAULT NULL
) RETURNS INTERVAL
```

Returns the total uptime of the aggregate, adjusted based on interpolation from the preceding aggregate. If `pred` is NULL, behaves identically to `uptime`.

## live_at

```SQL
live_at(
    agg HeartbeatAgg,
    test TIMESTAMPTZ
) RETURNS BOOLEAN
```

Evaluates if the system was live at a specific timestamp. The tested timestamp must fall within the aggregate's covered range (`[agg_start, agg_start + agg_duration]`); querying outside this range raises an error. Live interval ends are exclusive, so a timestamp exactly equal to the end of a live range returns `false`. 

## live_ranges

```SQL
live_ranges(agg HeartbeatAgg) RETURNS TABLE (start TIMESTAMPTZ, end TIMESTAMPTZ)
```

Returns a set of intervals representing the exact periods when the system was considered up.

## num_gaps

```SQL
num_gaps(agg HeartbeatAgg) RETURNS BIGINT
```

Returns the number of distinct downtime gaps within the aggregate window. If no heartbeats were recorded, returns 1 (the entire window counts as a single gap).

## num_live_ranges

```SQL
num_live_ranges(agg HeartbeatAgg) RETURNS BIGINT
```

Returns the number of distinct continuous liveness intervals.

## trim_to

Trims the aggregate to a narrower time range, discarding any ranges outside the new bounds. You cannot trim beyond the original bounding box of the aggregate.

The function has two calling conventions with different signatures depending on how it is invoked.

**Inline form**:

```SQL
trim_to(
    agg HeartbeatAgg,
    start TIMESTAMPTZ DEFAULT NULL,
    duration INTERVAL DEFAULT NULL
) RETURNS HeartbeatAgg
```

Both `start` and `duration` are optional. Omitting `start` anchors the trim at the aggregate's own start time, which allows trimming with a duration alone:

```SQL
-- Trim to the first 30 minutes of the aggregate's own window
SELECT trim_to(agg, duration => '30m') FROM ...
```

**Arrow operator form:**

```SQL
agg -> trim_to(start TIMESTAMPTZ, duration INTERVAL DEFAULT NULL)
```

When using `->`, `start` is **required**. Because the accessor must convert the interval to an integer offset at call time, it needs a concrete reference timestamp to do so. Calling `agg -> trim_to(duration => '30m')` without a start timestamp is not supported and will fail.

```SQL
-- Trim starting at a specific timestamp
SELECT agg -> trim_to('2020-01-01 01:00:00 UTC'::TIMESTAMPTZ) FROM ...

-- Trim starting at a specific timestamp with a duration
SELECT agg -> trim_to('2020-01-01 01:00:00 UTC'::TIMESTAMPTZ, '30m') FROM ...

```

## uptime

```SQL
uptime(agg HeartbeatAgg) RETURNS INTERVAL
```

Returns the total cumulative uptime (the sum of all live ranges) during the aggregate interval.
