# funnels [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

- Current status:	prototype
- Effort remaining:	lots

## Purpose

TODO

## Use cases

TODO - the examples below are just speculative.

### Test Data

Examples below are tested against the following data:

```SQL ,non-transactional
DROP TABLE IF EXISTS funnel_test;
CREATE TABLE funnel_test(
	"time" TIMESTAMPTZ,
	"user" INTEGER,
	"event" TEXT);
INSERT INTO funnel_test VALUES
	('2020-01-01 00:00:00+00', 1, 'LOGIN'),
	('2020-01-02 00:00:00+00', 1, 'LOGIN'),
	('2020-01-02 01:00:00+00', 1, 'SEARCH'),
	('2020-01-02 03:00:00+00', 1, 'BUY'),

	('2020-01-03 00:00:00+00', 2, 'LOGIN'),
	('2020-01-03 01:00:00+00', 2, 'SEARCH'),
	('2020-01-03 01:30:00+00', 2, 'BUY'),
	('2020-01-20 00:00:00+00', 2, 'LOGIN'),

	('2020-02-01 01:00:00+00', 3, 'SEARCH'),
	('2020-02-14 01:30:00+00', 3, 'BUY'),

	('2020-02-15 00:00:00+00', 4, 'LOGIN'),
	('2020-02-28 00:00:00+00', 4, 'LOGIN');
```

### two LOGIN events within one week

```SQL
SELECT a.user
FROM funnel_test AS a
    JOIN funnel_test AS b
    ON a.user = b.user
WHERE
    a.event = 'LOGIN'
    AND b.event = 'LOGIN'
    AND a.time <> b.time
    AND a.time - b.time > '0 week'::interval
    AND a.time - b.time < '1 week'::interval
;
```
```output
 userid
--------
      1
```

```SQL
SELECT toolkit_experimental.within_interval(
	'LOGIN', 'LOGIN', '1 week',
	toolkit_experimental.funnel_agg("user", event, time))
	FROM funnel_test;
```
```output
 userid
--------
      1
```

### two LOGIN events in a row

```SQL
WITH t AS (
    SELECT *, row_number() OVER (PARTITION BY f.user ORDER BY time)
    FROM funnel_test AS f
)
SELECT a.user
FROM t AS a
    JOIN t AS b ON a.user = b.user
WHERE
    a.event = 'LOGIN'
    AND b.event = 'LOGIN'
    AND a.row_number - b.row_number > 0
    AND a.row_number - b.row_number <= 1
;
```
```output
 user
------
    1
    4
```

```SQL
SELECT toolkit_experimental.consecutive(
	'LOGIN', 'LOGIN',
	toolkit_experimental.funnel_agg("user", event, time))
	userid
	FROM funnel_test
	ORDER BY userid;
```
```output
 userid
--------
      1
      4
```

### BUY event within one hour of SEARCH

```SQL
SELECT toolkit_experimental.within_interval(
	'SEARCH', 'BUY', '1 hour',
	toolkit_experimental.funnel_agg("user", event, time))
	FROM funnel_test;
```
```output
 within_interval
-----------------
               2
```

### BUY event immediately following SEARCH

```SQL
SELECT toolkit_experimental.consecutive(
	'SEARCH', 'BUY',
	toolkit_experimental.funnel_agg("user", event, time))
	userid
	FROM funnel_test
	ORDER BY userid;
```
```output
 userid
--------
      1
      2
      3
```

### from David

https://github.com/timescale/timescaledb-toolkit/pull/474#discussion_r925692902

```SQL ,ignore
CREATE MATERIALIZED VIEW funnels AS SELECT user, toolkit_experimental.funnel_agg(event, time) funnel
FROM funnel_test
GROUP BY user;

-- Then we can search with the WHERE clause:

SELECT * FROM funnels p

WHERE funnel ? event('LOGIN') -> event('SEARCH', within => '1 hour'::interval)
OR funnel ? event('LOGIN') -> event('LOGIN', within => '1 hour'::interval, consecutive => true);
```

Trying to inch my way toward this.  This works, but output is empty!  Huh?

```SQL
CREATE MATERIALIZED VIEW funnels AS SELECT user, toolkit_experimental.funnel_agg("user", event, time) funnel
FROM funnel_test
GROUP BY user;

SELECT * FROM funnels;
```
```output
```

### into_values

```SQL
SELECT handle, event, time FROM
	toolkit_experimental.into_values(
		(SELECT toolkit_experimental.funnel_agg("user", event, time) FROM funnel_test))
	ORDER BY time, handle, event;
```
```output
 handle |  event | time
--------+--------+------------------------
      1 |  LOGIN | 2020-01-01 00:00:00+00
      1 |  LOGIN | 2020-01-02 00:00:00+00
      1 | SEARCH | 2020-01-02 01:00:00+00
      1 |    BUY | 2020-01-02 03:00:00+00
      2 |  LOGIN | 2020-01-03 00:00:00+00
      2 | SEARCH | 2020-01-03 01:00:00+00
      2 |    BUY | 2020-01-03 01:30:00+00
      2 |  LOGIN | 2020-01-20 00:00:00+00
      3 | SEARCH | 2020-02-01 01:00:00+00
      3 |    BUY | 2020-02-14 01:30:00+00
      4 |  LOGIN | 2020-02-15 00:00:00+00
      4 |  LOGIN | 2020-02-28 00:00:00+00
```

## Implementation

### Current status

This is just an exploratory prototype.

Time to implement a new function over `FunnelAggregate` is on the order of a
week.  If `FunnelAggregate` is insufficient as is, time to implement goes up.

Currently requires rows to appear ordered by time.

### Next steps

Next step is to resolve these questions and publish a first experimental
function (or functions).

#### Questions

What use cases do we want to support?

Is storing an `INTEGER` handle right for those?  No.  Aggregate should only
cover event and time, with users (or sessions) coming from GROUP BY (see
David's example above).  At that point this looks a lot like a timevector, and
the duplication is a shame (but it only supports floats...).

Which use case do we want to start with?

`within_interval` is just off the top of epg's head.  What does a useful
interface look like?  David offered an idea above.

## Performance (aspirational)

What requirements do we have?

### Space

Current implementation stores `16 + E + 24N` bytes where N is the number of
events and E is the number of bytes required to store one copy of each event
name.

However, building the aggregate currently requires in memory a separate copy
of each event name per event (plus `String` and padding overhead) as well as
two more copies of the unique event names only (i.e. `2E`), plus the overhead
of one `std::collections::HashMap`.

There are tricks we can use to reduce the transient memory footprint, but that
gets pretty hairy, and I think we can reduce the footprint a lot more by using
event id rather than name.  See "Event id" under "Alternatives", below.

## Alternatives

### Event id rather than name

How much burden is this on the user?  If they have a table like this:

```SQL ,non-transactional
DROP TABLE IF EXISTS funnel_events;
CREATE TABLE funnel_events(
	"id" INTEGER,
	"name" TEXT);
INSERT INTO funnel_events VALUES
	(1, 'LOGIN'),
	(2, 'BUY');
```

A query like this seems to work (I didn't try it out all the way, just hacked
what I had to accept `INTEGER` instead of `TEXT` with hard-coded result and
that much at least worked!):

```SQL
SELECT toolkit_experimental.within_interval(
	(SELECT id FROM funnel_events WHERE name = 'LOGIN'),
	(SELECT id FROM funnel_events WHERE name = 'LOGIN'),
	'1 week',
	toolkit_experimental.funnel_agg2(
		"user",
		(SELECT id FROM funnel_events WHERE name = event),
		time))
	FROM funnel_test;
```
```output
 within_interval
-----------------
               1
```

If we're only looking at processing 1 million rows, we're only talking about
savings of megabytes, which is probably not worth the effort nowadays.  But if
we're talking about billions of rows and reducing 10s of gigabytes to only a
few gigabytes, that starts to sound worth it.

I implemented this and tracked the resident set size of this and the strings
version of the program with 10 million rows of data and found the int form to
consume only 20% the memory the strings form consumes:

- 3136 MB for strings
-  641 MB for ints
