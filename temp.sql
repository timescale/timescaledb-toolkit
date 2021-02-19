WITH t as (
    SELECT time_bucket('5 min', ts) as bucket, id, time_weight(ts, value, method=>'locf') as tw, sum (value) as sum
    FROM foo
    WHERE ts > '2020-10-01'
        AND ts <= '2020-10-02'
    [AND id IN ('foo', 'bar', 'baz')]
    GROUP BY 1, 2
) SELECT
    bucket,
    id,
    average(
        with_bounds(
            tw,
            bounds => time_bucket_range(bucket, '5 min'),
            prev => (
                SELECT tspoint(ts, value)
                FROM foo f
                WHERE f.id = t.id
                    AND f.ts < '2020-10-01'
                ORDER BY ts DESC
                LIMIT 1
            )
        ) OVER (PARTITION BY id ORDER BY bucket ASC)
    )    
FROM t;