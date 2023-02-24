# Candlestick Tests

## Get candlestick values from tick data


```sql,creation,min-toolkit-version=1.15.0
CREATE TABLE liveness(heartbeat TIMESTAMPTZ, start TIMESTAMPTZ);
INSERT INTO liveness VALUES
    ('01-01-2020 0:2:20 UTC', '01-01-2020 0:0 UTC'),
    ('01-01-2020 0:10 UTC', '01-01-2020 0:0 UTC'),
    ('01-01-2020 0:17 UTC', '01-01-2020 0:0 UTC'),
    ('01-01-2020 0:30 UTC', '01-01-2020 0:30 UTC'),
    ('01-01-2020 0:35 UTC', '01-01-2020 0:30 UTC'),
    ('01-01-2020 0:40 UTC', '01-01-2020 0:30 UTC'),
    ('01-01-2020 0:35 UTC', '01-01-2020 0:30 UTC'),
    ('01-01-2020 0:40 UTC', '01-01-2020 0:30 UTC'),
    ('01-01-2020 0:40 UTC', '01-01-2020 0:30 UTC'),
    ('01-01-2020 0:50:30 UTC', '01-01-2020 0:30 UTC'),
    ('01-01-2020 1:00:30 UTC', '01-01-2020 1:00 UTC'),
    ('01-01-2020 1:08 UTC', '01-01-2020 1:00 UTC'),
    ('01-01-2020 1:18 UTC', '01-01-2020 1:00 UTC'),
    ('01-01-2020 1:28 UTC', '01-01-2020 1:00 UTC'),
    ('01-01-2020 1:38:01 UTC', '01-01-2020 1:30 UTC'),
    ('01-01-2020 1:40 UTC', '01-01-2020 1:30 UTC'),
    ('01-01-2020 1:40:01 UTC', '01-01-2020 1:30 UTC'),
    ('01-01-2020 1:50:01 UTC', '01-01-2020 1:30 UTC'),
    ('01-01-2020 1:57 UTC', '01-01-2020 1:30 UTC'),
    ('01-01-2020 1:59:50 UTC', '01-01-2020 1:30 UTC');

CREATE MATERIALIZED VIEW hb AS
    SELECT start,
        heartbeat_agg(heartbeat, start, '30m', '10m') AS agg
    FROM liveness
    GROUP BY start;
```

```sql,validation,min-toolkit-version=1.15.0
SELECT
  start,
  uptime(agg),
  interpolated_uptime(agg, LAG(agg) OVER (ORDER by start))
FROM hb
ORDER BY start;
```

```output
         start          |  uptime  | interpolated_uptime 
------------------------+----------+---------------------
 2020-01-01 00:00:00+00 | 00:24:40 | 00:24:40
 2020-01-01 00:30:00+00 | 00:29:30 | 00:29:30
 2020-01-01 01:00:00+00 | 00:29:30 | 00:30:00
 2020-01-01 01:30:00+00 | 00:21:59 | 00:29:59
 ```
