# State Aggregation [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

# Test table

Examples below are tested against the following table:

```SQL ,non-transactional
SET TIME ZONE 'UTC';
CREATE TABLE states_test(ts TIMESTAMPTZ, state TEXT);
INSERT INTO states_test VALUES
    ('2020-01-01 00:00:00+00', 'START'),
    ('2020-01-01 00:00:11+00', 'OK'),
    ('2020-01-01 00:01:00+00', 'ERROR'),
    ('2020-01-01 00:01:03+00', 'OK'),
    ('2020-01-01 00:02:00+00', 'STOP');
```

## Functions

### duration_in

Compute the amount of time spent in a state as INTERVAL.

```SQL
SELECT toolkit_experimental.duration_in('ERROR', toolkit_experimental.state_agg(ts, state)) FROM states_test;
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
    toolkit_experimental.duration_in('ERROR', toolkit_experimental.state_agg(ts, state))
  )::INTEGER
FROM states_test;
```
```output
 seconds
---------
       3
```

### into_values

```SQL
SELECT state, duration FROM toolkit_experimental.into_values(
    (SELECT toolkit_experimental.state_agg(ts, state) FROM states_test))
    ORDER BY state, duration;
```
```output
 state | duration
-------+-----------
 ERROR |   3000000
 OK    | 106000000
 START |  11000000
 STOP  |         0
```
