# funnels [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

- Current status:	prototype
- Effort remaining:	lots

## Purpose

TODO

## Use cases

TODO

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
	('2020-01-20 00:00:00+00', 2, 'LOGIN');
```

### within_interval

Name is terrible, this is just a first test.

Looking for rows where one event occurs within an interval of another, e.g.
- two LOGIN events within one week
- BUY event within one hour of SEARCH

#### two LOGIN events within one week

```SQL
SELECT toolkit_experimental.within_interval(
	'LOGIN', 'LOGIN', '1 week',
	toolkit_experimental.funnel_agg("user", event, time))
	FROM funnel_test;
```
```output
 within_interval
-----------------
	       1
```

#### BUY event within one hour of SEARCH

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
      1 |	 BUY | 2020-01-02 03:00:00+00
      2 |  LOGIN | 2020-01-03 00:00:00+00
      2 | SEARCH | 2020-01-03 01:00:00+00
      2 |	 BUY | 2020-01-03 01:30:00+00
      2 |  LOGIN | 2020-01-20 00:00:00+00
```

## Implementation

### Current status

This is just an exploratory prototype.

Time to implement a new function over `FunnelAggregate` is on the order of a
week.  If `FunnelAggregate` is insufficient as is, time to implement goes up.

### Next steps

Next step is to resolve these questions and publish a first experimental
function (or functions).

#### Questions

What use cases do we want to support?

Is storing an `INTEGER` handle right for those?  I'm not sure what users need
to join matching events with.  Examples above treat `handle` as a direct user
id, but it could also be a session id which is joined with some other table to
match to a user, class of user, etc.

Which use case do we want to start with?

`within_interval` is just off the top of epg's head.  What does a useful
interface look like?

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

```SQL ,ignore
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

```SQL ,ignore
SELECT toolkit_experimental.within_interval(
	(SELECT id FROM funnel_events WHERE name = 'LOGIN'),
	(SELECT id FROM funnel_events WHERE name = 'LOGIN'),
	'1 week',
	toolkit_experimental.funnel_agg(
		"user",
		(SELECT id FROM funnel_events WHERE name = event),
		time))
	FROM funnel_test;
```

If we're only looking at processing 1 million rows, we're only talking about
savings of megabytes, which is probably not worth the effort nowadays.  But if
we're talking about billions of rows and reducing 10s of gigabytes to only a
few gigabytes, that starts to sound worth it.
