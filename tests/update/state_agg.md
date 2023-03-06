# `state_agg` tests

```sql,creation,min-toolkit-version=1.15.0
CREATE TABLE states_test(ts TIMESTAMPTZ, state TEXT);
INSERT INTO states_test VALUES
    ('2020-01-01 00:00:00+00', 'START'),
    ('2020-01-01 00:00:11+00', 'OK'),
    ('2020-01-01 00:01:00+00', 'ERROR'),
    ('2020-01-01 00:01:03+00', 'OK'),
    ('2020-01-01 00:02:00+00', 'STOP');

CREATE TABLE agg(sa StateAgg);
INSERT INTO agg SELECT state_agg(ts, state) FROM states_test;
```

```sql,validation,min-toolkit-version=1.15.0
SELECT (state_timeline(sa)).* FROM agg;
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
