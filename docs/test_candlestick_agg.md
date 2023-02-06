# Candlestick Continuous Aggregation Tests

## Setup table
```SQL,non-transactional,ignore-output
SET TIME ZONE 'UTC';
CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NULL
);
SELECT create_hypertable('stocks_real_time','time');
CREATE INDEX ix_symbol_time ON stocks_real_time (symbol, time DESC);

CREATE TABLE company (
  symbol TEXT NOT NULL,
  name TEXT NOT NULL
);
```

## Setup Continuous Aggs
```SQL,non-transactional,ignore-output
CREATE MATERIALIZED VIEW cs
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute'::interval, "time") AS ts,
  symbol,
  candlestick_agg("time", price, day_volume) AS candlestick
FROM stocks_real_time
GROUP BY ts, symbol;
```

## Insert data into table
```SQL,non-transactional,ignore-output
INSERT INTO stocks_real_time("time","symbol","price","day_volume")
VALUES
('2023-01-11 17:59:57-06','AAPL',133.445,NULL),
('2023-01-11 17:59:55-06','PFE',47.38,NULL),
('2023-01-11 17:59:54-06','AMZN',95.225,NULL),
('2023-01-11 17:59:52-06','AAPL',29.82,NULL);
```

```SQL,non-transactional,ignore-output
INSERT INTO company("symbol","name")
VALUES
('AAPL','Apple'),
('PFE','Pfizer'),
('AMZN','Amazon');
```
## Query by-minute continuous aggregate over stock trade data for ohlc prices along with timestamps 

```SQL,non-transactional,ignore-output
SELECT ts,
  symbol,
    open_time(candlestick),
    open(candlestick),
    high_time(candlestick),
    high(candlestick),
    low_time(candlestick),
    low(candlestick),
    close_time(candlestick),
    close(candlestick)
FROM cs;
```

```output
           ts           | symbol |       open_time        |  open  |       high_time        |  high   |        low_time        |  low   |       close_time       |  close
------------------------+--------+------------------------+--------+------------------------+---------+------------------------+--------+------------------------+---------
 2023-01-11 23:59:00+00 | PFE    | 2023-01-11 23:59:55+00 |  47.38 | 2023-01-11 23:59:55+00 |   47.38 | 2023-01-11 23:59:55+00 |  47.38 | 2023-01-11 23:59:55+00 |   47.38
 2023-01-11 23:59:00+00 | AAPL   | 2023-01-11 23:59:52+00 |  29.82 | 2023-01-11 23:59:57+00 | 133.445 | 2023-01-11 23:59:52+00 |  29.82 | 2023-01-11 23:59:57+00 | 133.445
 2023-01-11 23:59:00+00 | AMZN   | 2023-01-11 23:59:54+00 | 95.225 | 2023-01-11 23:59:54+00 |  95.225 | 2023-01-11 23:59:54+00 | 95.225 | 2023-01-11 23:59:54+00 |  95.225
```

## Roll up your by minute continuous agg into daily buckets and return the volume weighted average price for AAPL and its high price

```SQL,non-transactional,ignore-output
SELECT
    time_bucket('1 day'::interval, ts) AS daily_bucket,
    symbol,
    vwap(rollup(candlestick)),
	high(rollup(candlestick))
FROM cs
WHERE symbol = 'AAPL'
GROUP BY daily_bucket,symbol;
```

```output
      daily_bucket      | symbol | vwap |  high
------------------------+--------+------+---------
 2023-01-11 00:00:00+00 | AAPL   | NULL | 133.445
```
