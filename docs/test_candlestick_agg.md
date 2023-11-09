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
```

## Setup Continuous Aggs
```SQL,non-transactional,ignore-output
CREATE MATERIALIZED VIEW cs
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
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
('2023-01-11 18:59:59+00','AAPL',140,20),
('2023-01-11 18:23:58+00','AAPL',100,10),
('2023-01-11 17:59:57+00','AAPL',133.445,NULL),
('2023-01-11 17:59:55+00','PFE',47.38,2000),
('2023-01-11 12:15:55+00','PFE',1,23),
('2023-01-11 12:00:52+00','AAPL',29.82,NULL),
('2023-01-11 11:12:12+00','PFE',47.38,14),
('2023-01-11 11:01:50+00','AMZN',95.25,1000),
('2023-01-11 11:01:32+00','AMZN',92,NULL),
('2023-01-11 11:01:30+00','AMZN',75.225,NULL);
```
## Query by-minute continuous aggregate over stock trade data for ohlc prices along with timestamps 

```SQL,non-transactional
SELECT ts,
    symbol,
    open_time(candlestick),
    open(candlestick),
    high_time(candlestick),
    high(candlestick),
    low_time(candlestick),
    low(candlestick),
    close_time(candlestick),
    close(candlestick),
	volume(candlestick)
FROM cs;
```

```output
           ts           | symbol |       open_time        |  open   |       high_time        |  high   |        low_time        |   low   |       close_time       |  close  | volume
------------------------+--------+------------------------+---------+------------------------+---------+------------------------+---------+------------------------+---------+--------
 2023-01-11 12:15:00+00 | PFE    | 2023-01-11 12:15:55+00 |       1 | 2023-01-11 12:15:55+00 |       1 | 2023-01-11 12:15:55+00 |       1 | 2023-01-11 12:15:55+00 |       1 |     23
 2023-01-11 17:59:00+00 | PFE    | 2023-01-11 17:59:55+00 |   47.38 | 2023-01-11 17:59:55+00 |   47.38 | 2023-01-11 17:59:55+00 |   47.38 | 2023-01-11 17:59:55+00 |   47.38 |   2000
 2023-01-11 11:01:00+00 | AMZN   | 2023-01-11 11:01:30+00 |  75.225 | 2023-01-11 11:01:50+00 |   95.25 | 2023-01-11 11:01:30+00 |  75.225 | 2023-01-11 11:01:50+00 |   95.25 |
 2023-01-11 18:59:00+00 | AAPL   | 2023-01-11 18:59:59+00 |     140 | 2023-01-11 18:59:59+00 |     140 | 2023-01-11 18:59:59+00 |     140 | 2023-01-11 18:59:59+00 |     140 |     20
 2023-01-11 11:12:00+00 | PFE    | 2023-01-11 11:12:12+00 |   47.38 | 2023-01-11 11:12:12+00 |   47.38 | 2023-01-11 11:12:12+00 |   47.38 | 2023-01-11 11:12:12+00 |   47.38 |     14
 2023-01-11 17:59:00+00 | AAPL   | 2023-01-11 17:59:57+00 | 133.445 | 2023-01-11 17:59:57+00 | 133.445 | 2023-01-11 17:59:57+00 | 133.445 | 2023-01-11 17:59:57+00 | 133.445 |
 2023-01-11 18:23:00+00 | AAPL   | 2023-01-11 18:23:58+00 |     100 | 2023-01-11 18:23:58+00 |     100 | 2023-01-11 18:23:58+00 |     100 | 2023-01-11 18:23:58+00 |     100 |     10
 2023-01-11 12:00:00+00 | AAPL   | 2023-01-11 12:00:52+00 |   29.82 | 2023-01-11 12:00:52+00 |   29.82 | 2023-01-11 12:00:52+00 |   29.82 | 2023-01-11 12:00:52+00 |   29.82 |
```
