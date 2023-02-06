# Candlestick Tests

## Get candlestick values from tick data


```sql,creation,min-toolkit-version=1.14.0
CREATE TABLE stocks_real_time(time TIMESTAMPTZ, symbol TEXT, price DOUBLE PRECISION,day_volume DOUBLE PRECISION);
INSERT INTO stocks_real_time VALUES
    ('2023-01-11',	'AAPL',	133.445,10),	
    ('2023-01-11',	'PFE',    47.38,2),	
    ('2023-01-11',	'AMZN',	95.225,1),
    ('2023-01-11',	'INTC',	29.82,NULL),
    ('2023-01-11',	'MSFT',	235.5,100),
    ('2023-01-11',	'TSLA',	123.085,NULL),	
    ('2023-01-11',	'AAPL',	133.44,20);

CREATE MATERIALIZED VIEW candlestick AS
    SELECT symbol,
        candlestick_agg("time", price, day_volume) AS candlestick
    FROM stocks_real_time
    GROUP BY symbol;
```

```sql,validation,min-toolkit-version=1.14.0
SELECT
  symbol,
  open(candlestick),
  high(candlestick),
  low(candlestick),
  close(candlestick),
  volume(candlestick)
FROM cs;
```

```output
 symbol |  open   |  high   |   low   |  close  | volume
--------+---------+---------+---------+---------+--------
 PFE    |   47.38 |   47.38 |   47.38 |   47.38 |      2
 AMZN   |  95.225 |  95.225 |  95.225 |  95.225 |      1
 MSFT   |   235.5 |   235.5 |   235.5 |   235.5 |    100
 AAPL   | 133.445 | 133.445 |  133.44 | 133.445 |     30
 TSLA   | 123.085 | 123.085 | 123.085 | 123.085 |   NULL
 INTC   |   29.82 |   29.82 |   29.82 |   29.82 |   NULL
 ```
