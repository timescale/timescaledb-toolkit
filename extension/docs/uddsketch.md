# UddSketch

> [Description](#uddsketch-description)<br>
> [Details](#uddsketch-details)<br>
> [Example](#uddsketch-example)<br>
> [Example in a Continuous Aggregates](#uddsketch-cagg-example)<br>
> [API](#uddsketch-api)

## Description <a id="uddsketch-description"></a>

[UddSketch](https://arxiv.org/pdf/2004.08604.pdf) is a specialization of the [DDSketch](https://arxiv.org/pdf/1908.10693.pdf) data structure.  It follows the same approach of breaking the data range into a series of logarithmically sized buckets such that it can guarantee a maximum relative error for any percentile estimate as long as it knows which bucket that percentile falls in.

Where UddSketch differs from DDSketch in its behavior when the number of buckets required by a set of values exceeds some predefined maximum.  In these cirumstances DDSketch will maintain it's original error bound, but only for a subset of the range of percentiles.  UddSketch, on the other hand, will combine buckets in such a way that it loosens the error bound, but can still estimate all percentile values.

As an example, assume both sketches were trying to capture an large set of values to be able to estimate percentiles with 1% relative error but were given too few buckets to do so.  The DDSketch implementation would still guarantee 1% relative error, but may only be able to provides estimates in the range (0.05, 0.95).  The UddSketch implementation however, might end up only able to guarantee 2% relative error, but would still be able to estimate all percentiles at that error.

## Details <a id="uddsketch-details"></a>

Timescale's UddSketch implementation is provided as an aggregate function in PostgreSQL.  It does not support moving-aggregate mode, and is not a ordered-set aggregate.  It currently only works with `DOUBLE PRECISION` types, but we're intending to relax this constraint as needed.  UddSketches are partializable and are good candidates for [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).

It's also worth noting that attempting to set the relative error too small or large can result in breaking behavior.  For this reason, the error is required to fall into the range [1.0e-12, 1.0).

## Usage Example <a id="uddsketch-example"></a>

For this example we're going to start with a table containing some NOAA weather data for a few weather stations across the US over the past 20 years.

```SQL ,ignore
\d weather;
```
```
                         Table "public.weather"
 Column  |            Type             | Collation | Nullable | Default
---------+-----------------------------+-----------+----------+---------
 station | text                        |           |          |
 name    | text                        |           |          |
 date    | timestamp without time zone |           |          |
 prcp    | double precision            |           |          |
 snow    | double precision            |           |          |
 tavg    | double precision            |           |          |
 tmax    | double precision            |           |          |
 tmin    | double precision            |           |          |
```

Now let's create some UddSketches for our different stations and verify that they're receiving data.

```SQL ,ignore
CREATE VIEW daily_rain AS
    SELECT name, uddsketch(100, 0.005, prcp)
    FROM weather
    GROUP BY name;

SELECT
    name,
    num_vals(uddsketch),
    error(uddsketch)
FROM daily_rain;
```
```
                 name                  | num_vals |               error
---------------------------------------+-----------+---------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US |      7671 |  0.0199975003624472
 LITCHFIELD PARK, AZ US                |      5904 |               0.005
 NY CITY CENTRAL PARK, NY US           |      7671 | 0.03997901311671962
 MIAMI INTERNATIONAL AIRPORT, FL US    |      7671 | 0.03997901311671962
(4 rows)
```

Notice that 100 buckets proved to be insufficient to maintain 0.5% relative error for three of our data sets, but they've automatically adjusted their bucket size to maintain the desired bucket limit.

We can then check some rainfall percentiles to see how our stations compare.
```SQL ,ignore
SELECT
    name,
    approx_percentile(0.6, uddsketch)
FROM daily_rain;
```
```
                 name                  |             approx_percentile
---------------------------------------+----------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 0.009850446542334412
 LITCHFIELD PARK, AZ US                |                    0
 NY CITY CENTRAL PARK, NY US           |                    0
 MIAMI INTERNATIONAL AIRPORT, FL US    |                    0
(4 rows)
```
```SQL ,ignore
SELECT
    name,
    approx_percentile(0.9, uddsketch)
FROM daily_rain;
```
```
                 name                  |           approx_percentile
---------------------------------------+--------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 0.3072142710699281
 LITCHFIELD PARK, AZ US                |                  0
 NY CITY CENTRAL PARK, NY US           | 0.4672895773464223
 MIAMI INTERNATIONAL AIRPORT, FL US    | 0.5483701300878486
(4 rows)
```
```SQL ,ignore
SELECT
    name,
    approx_percentile( 0.995, uddsketch)
FROM daily_rain;
```
```
                 name                  |           approx_percentile
---------------------------------------+--------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 1.1969797510556823
 LITCHFIELD PARK, AZ US                | 0.7671946655927083
 NY CITY CENTRAL PARK, NY US           | 2.3145312888530807
 MIAMI INTERNATIONAL AIRPORT, FL US    | 2.9423518191328113
(4 rows)
```

## Example Using TimeScale Continuous Aggregates <a id="uddsketch-cagg-example"></a>
To have a UddSketch over a PostgresQL table which automatically updates as more data is added, we can make use of continuous aggregates.  First, let us create a simple hypertable:

```SQL ,non-transactional,ignore-output
SET TIME ZONE 'UTC';
CREATE TABLE test(time TIMESTAMPTZ, value DOUBLE PRECISION);
SELECT create_hypertable('test', 'time');
```

Now we'll create a continuous aggregate which will group all the points for each week into a UddSketch:
```SQL ,non-transactional,ignore-output
CREATE MATERIALIZED VIEW weekly_sketch
WITH (timescaledb.continuous)
AS SELECT
    time_bucket('7 day'::interval, time) as week,
    uddsketch(100, 0.005, value) as sketch
FROM test
GROUP BY time_bucket('7 day'::interval, time);
```

Next we'll use one of our utility functions, `generate_periodic_normal_series`, to add some data to the table.  Using default arguments, this function will add 28 days of data points at 10 minute intervals.
```SQL ,non-transactional
INSERT INTO test
    SELECT time, value
    FROM timescale_analytics_experimental.generate_periodic_normal_series('2020-01-01 UTC'::timestamptz, rng_seed => 12345678); 
```
```
INSERT 0 4032
```

Finally, we can query the aggregate to see various approximate percentiles from different weeks.
```SQL
SELECT 
    week,
    error(sketch), 
    approx_percentile(0.01, sketch) AS low, 
    approx_percentile(0.5, sketch) AS mid, 
    approx_percentile(0.99, sketch) AS high 
FROM weekly_sketch
ORDER BY week;
```
```output
          week          | error |        low        |        mid         |        high        
------------------------+-------+-------------------+--------------------+--------------------
 2019-12-30 00:00:00+00 | 0.005 | 808.3889305072331 |  1037.994095858188 | 1280.5527834239035
 2020-01-06 00:00:00+00 | 0.005 | 858.3773394302965 |  1091.213645863754 | 1306.4218833642865
 2020-01-13 00:00:00+00 | 0.005 | 816.5134423716273 | 1058.9631440308738 | 1293.4226606442442
 2020-01-20 00:00:00+00 | 0.005 | 731.4599430896668 |   958.188678537264 | 1205.9785918127336
 2020-01-27 00:00:00+00 | 0.005 | 688.8626877028054 |  911.4568854686239 | 1135.7472981488002
```

We can also combine the weekly aggregates to run queries on the entire data:
```SQL
SELECT 
    error(a.uddsketch), 
    approx_percentile(0.01, a.uddsketch) AS low, 
    approx_percentile(0.5, a.uddsketch) AS mid, 
    approx_percentile(0.99, a.uddsketch) AS high 
FROM (SELECT uddsketch(sketch) FROM weekly_sketch) AS a;
```
```output
 error |       low        |        mid         |        high        
-------+------------------+--------------------+--------------------
 0.005 | 753.736403199032 | 1027.6657963969128 | 1280.5527834239035
```


## Command List (A-Z) <a id="uddsketch-api"></a>
Aggregate Functions
> - [uddsketch - point form](#uddsketch-point)
> - [uddsketch - summary form](#uddsketch-summary)

Accessor Functions
> - [approx_percentile](#approx_percentile)
> - [approx_percentile_rank](#approx_percentile_rank)
> - [error](#error)
> - [mean](#mean)
> - [num_vals](#num-vals)

---

## **uddsketch (point form) ** <a id="uddsketch-point"></a>
```SQL ,ignore
uddsketch(
    size INTEGER,
    max_error DOUBLE PRECISION,
    value DOUBLE PRECISION
) RETURNS UddSketch
```

This will construct and return a new UddSketch with at most `size` buckets.  The maximum relative error of the UddSketch will be bounded by `max_error` unless it is impossible to do so while with the bucket bound.  If the sketch has had to combine buckets, the new error can be found with the [uddsketch_error](#error) command.

### Required Arguments <a id="uddsketch-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `size` | `INTEGER` | Maximum number of buckets in the sketch.  Providing a larger value here will make it more likely that the aggregate will able to maintain the desired error, though will potentially increase the memory usage. |
| `max_error` | `DOUBLE PRECISION` | This is the starting maximum relative error of the sketch, as a multiple of the actual value.  The true error may exceed this if too few buckets are provided for the data distribution. |
| `value` | `DOUBLE PRECISION` |  Column to aggregate.
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `uddsketch` | `UddSketch` | A UddSketch object which may be passed to other UddSketch APIs. |
<br>

### Sample Usages <a id="uddsketch-examples"></a>
For this example assume we have a table 'samples' with a column 'data' holding `DOUBLE PRECISION` values.  The following will simply return a sketch over that column

```SQL ,ignore
SELECT uddsketch(100, 0.01, data) FROM samples;
```

It may be more useful to build a view from the aggregate that we can later pass to other uddsketch functions.

```SQL ,ignore
CREATE VIEW sketch AS
    SELECT uddsketch(100, 0.01, data)
    FROM samples;
```

---

## **uddsketch (summary form)** <a id="uddsketch-summary"></a>
```SQL ,ignore
uddsketch(
    sketch uddsketch
) RETURNS UddSketch
```

This will combine multiple already constructed UddSketches, they must have the same size in order to be combined. This is very useful for re-aggregating already constructed uddsketches using the [point form](#uddsketch-point).

### Required Arguments <a id="uddsketch-summary-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `sketch` | `UddSketch` | The already constructed uddsketch from a previous [uddsketch() (point form)](#uddsketch-point) call. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `uddsketch` | `UddSketch` | A UddSketch object which may be passed to other UddSketch APIs. |
<br>

### Sample Usages <a id="uddsketch-summary-examples"></a>
For this example assume we have a table 'samples' with a column 'data' holding `DOUBLE PRECISION` values, and an 'id' column that holds the what series the data belongs to, we can create a view to get the UddSketches for each `id` using the [point form](#uddsketch-point) like so:

```SQL ,ignore
CREATE VIEW sketch AS
    SELECT 
        id, 
        uddsketch(100, 0.01, data) as sketched
    FROM samples
    GROUP BY id;
```

Then we can use that view to get the full aggregate like so: 

```SQL ,ignore
SELECT uddsketch(sketched)
FROM sketch;
```

---

## **approx_percentile** <a id="approx_percentile"></a>

```SQL ,ignore
approx_percentile(
    percentile DOUBLE PRECISION, 
    sketch  uddsketch
) RETURNS DOUBLE PRECISION
```

Get the approximate value at a percentile from a UddSketch.

### Required Arguments <a id="approx_percentile-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `percentile` | `DOUBLE PRECISION` | The desired percentile (0.0-1.0) to approximate. |
| `sketch` | `UddSketch` | The sketch to compute the approx_percentile on. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `approx_percentile` | `DOUBLE PRECISION` | The estimated value at the requested percentile. |
<br>

### Sample Usage <a id="approx_percentile-examples"></a>

```SQL
SELECT approx_percentile(
    0.90,
    uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
```
```output
           approx_percentile
--------------------
  90.93094205022494
```

---

## **approx_percentile_rank** <a id="approx_percentile_rank"></a>

```SQL ,ignore
approx_percentile_rank(
    value DOUBLE PRECISION,
    sketch UddSketch
) RETURNS UddSketch
```

Estimate what percentile a given value would be located at in a UddSketch.

### Required Arguments <a id="approx_percentile_rank-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `value` | `DOUBLE PRECISION` |  The value to estimate the percentile of. |
| `sketch` | `UddSketch` | The sketch to compute the percentile on. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `approx_percentile_rank` | `DOUBLE PRECISION` | The estimated percentile associated with the provided value. |
<br>

### Sample Usage <a id="approx_percentile_rank-examples"></a>

```SQL
SELECT approx_percentile_rank(
    90,
    uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
```
```output
 approx_percentile_rank
-------------------
             0.89
```

---

## **error** <a id="error"></a>

```SQL ,ignore
error(sketch UddSketch) RETURNS DOUBLE PRECISION
```

This returns the maximum relative error that a percentile estimate will have (relative to the correct value).  This will initially be the same as the `max_error` used to construct the UddSketch, but if the sketch has needed to combine buckets this function will return the new maximum error.

### Required Arguments <a id="error-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to determine the error of. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `error` | `DOUBLE PRECISION` | The maximum relative error of any percentile estimate. |
<br>

### Sample Usages <a id="error-examples"></a>

```SQL
SELECT error(
    uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
```
```output
 error
-------
  0.01
```

---

## **mean** <a id="mean"></a>

```SQL ,ignore
mean(sketch UddSketch) RETURNS DOUBLE PRECISION
```

Get the average of all the values contained in a UddSketch.

### Required Arguments <a id="mean-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` |  The sketch to extract the mean value from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `mean` | `DOUBLE PRECISION` | The average of the values entered into the UddSketch. |
<br>

### Sample Usage <a id="mean-examples"></a>

```SQL
SELECT mean(
    uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
```
```output
 mean
------
 50.5
```

---

## **num_vals** <a id="num-vals"></a>

```SQL ,ignore
num_vals(sketch UddSketch) RETURNS DOUBLE PRECISION
```

Get the number of values contained in a UddSketch.

### Required Arguments <a id="num-vals-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to extract the number of values from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `uddsketch_count` | `DOUBLE PRECISION` | The number of values entered into the UddSketch. |
<br>

### Sample Usage <a id="num-vals-examples"></a>

```SQL
SELECT num_vals(
    uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
```
```output
 num_vals
-----------
       100
```

---
