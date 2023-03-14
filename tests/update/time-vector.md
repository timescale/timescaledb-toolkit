# Time Vector Tests

```sql,creation
CREATE TABLE time_vector_data(time TIMESTAMPTZ, value DOUBLE PRECISION);
INSERT INTO time_vector_data VALUES
    ('2020-1-1 UTC', 30.0),
    ('2020-1-2 UTC', 45.0),
    ('2020-1-3 UTC', NULL),
    ('2020-1-4 UTC', 55.5),
    ('2020-1-5 UTC', 10.0);
```

```sql,validation
SELECT unnest(timevector(time,value))::TEXT FROM time_vector_data;
```

```output
             unnest
---------------------------------
 ("2020-01-01 00:00:00+00",30)
 ("2020-01-02 00:00:00+00",45)
 ("2020-01-03 00:00:00+00",NaN)
 ("2020-01-04 00:00:00+00",55.5)
 ("2020-01-05 00:00:00+00",10)
 ```

```sql,creation
CREATE TABLE tv_rollup_data(time TIMESTAMPTZ, value DOUBLE PRECISION, bucket INTEGER);
INSERT INTO tv_rollup_data VALUES
    ('2020-1-1 UTC', 30.0, 1),
    ('2020-1-2 UTC', 45.0, 1),
    ('2020-1-3 UTC', NULL, 2),
    ('2020-1-4 UTC', 55.5, 2),
    ('2020-1-5 UTC', 10.0, 3),
    ('2020-1-6 UTC', 13.0, 3),
    ('2020-1-7 UTC', 71.0, 4),
    ('2020-1-8 UTC', 0.0, 4);
```

```sql,validation
SELECT unnest(rollup(tvec))::TEXT
   FROM (
       SELECT timevector(time, value) AS tvec
       FROM tv_rollup_data 
       GROUP BY bucket 
       ORDER BY bucket
   ) s;
```

```output
            unnest
-------------------------------
 ("2020-01-01 00:00:00+00",30)
 ("2020-01-02 00:00:00+00",45)
 ("2020-01-03 00:00:00+00",NaN)
 ("2020-01-04 00:00:00+00",55.5)
 ("2020-01-05 00:00:00+00",10)
 ("2020-01-06 00:00:00+00",13)
 ("2020-01-07 00:00:00+00",71)
 ("2020-01-08 00:00:00+00",0)
 ```
