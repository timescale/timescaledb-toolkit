There are different types of metrics: gauges & counters
counters reset - require in order aggregation (happens internally)
similar calculations on each
```SQL
CREATE VIEW counters AS SELECT time_bucket('1 hour', ts) as bucket, metric_id, metric_agg('counter',ts, val) as c FROM foo_counters GROUP BY bucket, metric_id;
SELECT metric_id, counter_agg(c) FROM foo GROUP BY metric_id;

SELECT *, rate(c), delta(c), irate(c, 'lead'), irate(c, 'trail'), idelta(c, 'lead'), idelta(c, 'trail'), 
    extrapolated_rate(c, method=>'prom', edges=> time_bucket_range('1 hour', bucket)), 
    extrapolated_delta(c, method=>'prom', edges=> time_bucket_range('1 hour', bucket)), 
    deriv(c) -- need regr parameters
FROM counters;

CREATE VIEW gauges AS SELECT time_bucket('1 hour', ts) as bucket, metric_id, gauge_agg(ts, val) as g FROM foo_gauge GROUP BY bucket, metric_id;

CREATE VIEW both as SELECT * FROM counters UNION ALL SELECT * FROM gauges;

```