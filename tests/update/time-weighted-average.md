# Time Weighted Average Tests

## Test integral and interpolated integral

```sql,creation,min-toolkit-version=1.15.0
CREATE TABLE time_weight_test(time timestamptz, value double precision, bucket timestamptz);
INSERT INTO time_weight_test VALUES
    ('2020-1-1 8:00'::timestamptz, 10.0, '2020-1-1'::timestamptz),
    ('2020-1-1 12:00'::timestamptz, 40.0, '2020-1-1'::timestamptz),
    ('2020-1-1 16:00'::timestamptz, 20.0, '2020-1-1'::timestamptz),
    ('2020-1-2 2:00'::timestamptz, 15.0, '2020-1-2'::timestamptz),
    ('2020-1-2 12:00'::timestamptz, 50.0, '2020-1-2'::timestamptz),
    ('2020-1-2 20:00'::timestamptz, 25.0, '2020-1-2'::timestamptz),
    ('2020-1-3 10:00'::timestamptz, 30.0, '2020-1-3'::timestamptz),
    ('2020-1-3 12:00'::timestamptz, 0.0, '2020-1-3'::timestamptz), 
    ('2020-1-3 16:00'::timestamptz, 35.0, '2020-1-3'::timestamptz);
CREATE MATERIALIZED VIEW twa AS (
    SELECT bucket, time_weight('linear', time, value) as agg 
    FROM time_weight_test 
    GROUP BY bucket
);
```

```sql,validation,min-toolkit-version=1.15.0
SELECT
    bucket,
    interpolated_integral(
        agg,
        bucket,
        '1 day'::interval, 
        LAG(agg) OVER (ORDER BY bucket),
        LEAD(agg) OVER (ORDER BY bucket),
        'hours')
FROM twa
ORDER BY bucket;
```

```output
         bucket         | interpolated_integral
------------------------+-----------------------
 2020-01-01 00:00:00+00 |                   364
 2020-01-02 00:00:00+00 |     758.8571428571429
 2020-01-03 00:00:00+00 |     382.1428571428571
```

```sql,validation,min-toolkit-version=1.15.0
SELECT bucket, integral(agg, 'hrs') FROM twa ORDER BY bucket;
```

```output
         bucket         | integral
------------------------+----------
 2020-01-01 00:00:00+00 |      220
 2020-01-02 00:00:00+00 |      625
 2020-01-03 00:00:00+00 |      100
```
