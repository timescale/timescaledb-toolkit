# OHLC [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

- Current status: scoping
- Effort remaining:  some

## Purpose

- What problem is the user trying to solve?
  Many of our examples and tutorials show how to do basic OHLC bucketing, saving at least four columns using
  MAX/MIN/FIRST/LAST as the functions. It's a repetitive processes and some functions don't work well with
  re-aggregation - and even when they do (MAX/MIN), most of our documentation/context warns users against using
  functions to re-aggregate unless they were designed to do so.

- How will this be used?
  Users with financial time-series data.

- What kind of SQL are they going to write?
```SQL
SELECT time_bucket('1 minute', time),
       symbol,
       ohlc(ts, price)
FROM stocks_real_time
GROUP BY time_bucket, symbol
```

- Is there pure SQL query we are simplifying?
  The SQL is largely unchanged (though perhaps less often duplicated). The main purpose of this aggregate would be to
  make `rollup` work with these queries rather than defining a CAgg at varying levels of granularity.

## Use cases

### simple use case

Create CAgg with OHLC info at 1m level
```SQL
CREATE MATERIALIZED VIEW ohlc_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS time,
       symbol,
       ohlc(ts, price)
FROM stocks_real_time
GROUP BY time, symbol
```

Query at the same 1m level
```SQL
SELECT symbol,
       open(ohlc),
       high(ohlc),
       low(ohlc),
       close(ohlc)
FROM ohlc_1m
GROUP BY symbol
```

Or rollup and query at 1h level (rather than requiring a second CAgg at the 1h level to be created)
```SQL
SELECT time_bucket('1 hour', time),
       symbol,
       open(rollup(ohlc)),
       high(rollup(ohlc)),
       low(rollup(ohlc)),
       close(rollup(ohlc))
FROM ohlc_1m
GROUP BY symbol
```

Is it worth creating `open`, `high`, `low`, `close` when `first`, `max`, `min`, `last` exist?

`OPEN` and `CLOSE` are keywords in the SQL Standard, but I think we could still define new hyperfunctions with them
https://www.postgresql.org/docs/current/sql-keywords-appendix.html.

### complex use cases
See "Alternatives" section for notes on OHLCV, where incorporating volume would introduce some complexity.

### edge cases

## Common functionality


### rollup
`rollup` is the functionality that makes this aggregate worth implementing.

### into_values / unnest

Seems unnecessary for this aggregate

## Implementation plan
- create `ohlc` aggregate
- create `rollup` for OHLC type aggregates
- possibly create `open`, `high`, `low`, `close` accessors

### Current status
Unimplemented

### Next steps

First step is a simple use case in `toolkit_experimental`.

Other steps may include:
- expanded functionality
- adjusting based on user feedback
- optimization

And finally:  stabilization or removal.

## Performance (aspirational)

Expect that performance will be very similar to computing OHLC by hand, but once we've established a method for
benchmarking, we should confirm.

## Alternatives

Could also make an `ohlcv` (with volume). The volume component may complicate some aspects of accessor/re-aggregation
implementations.

Offering an `ohlc` aggregate first and then an `ohlcv` later are not mutually exclusive options, and beginning with just
`ohlc` makes the implementation of the aggregate very simple.
