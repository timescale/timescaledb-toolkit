# UddSketch [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes)

> [Description](#uddsketch-description)<br>
> [Details](#uddsketch-details)<br>
> [Example](#uddsketch-example)<br>
> [API](#uddsketch-api)

## Description [](uddsketch-description)

[UddSketch](https://arxiv.org/pdf/2004.08604.pdf) is a specialization of the [DDSketch](https://arxiv.org/pdf/1908.10693.pdf) data structure.  It follows the same approach of breaking the data range into a series of logarithmically sized buckets such that it can guarantee a maximum relative error for any quantile estimate as long as it knows which bucket that quantile falls in.

Where UddSketch differs from DDSketch in its behavior when the number of buckets required by a set of values exceeds some predefined maximum.  In these cirumstances DDSketch will maintain it's original error bound, but only for a subset of the range of quantiles.  UddSketch, on the other hand, will combine buckets in such a way that it loosens the error bound, but can still estimate all quantile values.

As an example, assume both sketches were trying to capture an large set of values to be able to estimate quantiles with 1% relative error but were given too few buckets to do so.  The DDSketch implementation would still guarantee 1% relative error, but may only be able to provides estimates in the range (0.05, 0.95).  The UddSketch implementation however, might end up only able to guarantee 2% relative error, but would still be able to estimate all quantiles at that error.

## Details [](uddsketch-details)

Timescale's UddSketch implementation is provided as an aggregate function in PostgreSQL.  It does not support moving-aggregate mode, and is not a ordered-set aggregate.  It currently only works with `DOUBLE PRECISION` types, but we're intending to relax this constraint as needed.  UddSketches are partializable and are good candidates for [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).

## Usage Example [](uddsketch-example)

For this example we're going to start with a table containing some NOAA weather data for a few weather stations across the US over the past 20 years.

```SQL
timescale_analytics=> \d weather;
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

```SQL
timescale_analytics=> CREATE VIEW daily_rain AS
    SELECT name, timescale_analytics_experimental.uddsketch(100, 0.005, prcp)
    FROM weather
    GROUP BY name;
CREATE VIEW
timescale_analytics=> SELECT
    name,
    timescale_analytics_experimental.get_count(uddsketch),
    timescale_analytics_experimental.error(uddsketch)
FROM daily_rain;
                 name                  | get_count |               error
---------------------------------------+-----------+---------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US |      7671 |  0.0199975003624472
 LITCHFIELD PARK, AZ US                |      5904 |               0.005
 NY CITY CENTRAL PARK, NY US           |      7671 | 0.03997901311671962
 MIAMI INTERNATIONAL AIRPORT, FL US    |      7671 | 0.03997901311671962
(4 rows)
```

Notice that 100 buckets proved to be insufficient to maintain 0.5% relative error for three of our data sets, but they've automatically adjusted their bucket size to maintain the desired bucket limit.

We can then check some rainfall quantiles to see how our stations compare.
```SQL
timescale_analytics=> SELECT
    name,
    timescale_analytics_experimental.quantile(uddsketch, 0.6)
FROM daily_rain;
                 name                  |             quantile
---------------------------------------+----------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 0.009850446542334412
 LITCHFIELD PARK, AZ US                |                    0
 NY CITY CENTRAL PARK, NY US           |                    0
 MIAMI INTERNATIONAL AIRPORT, FL US    |                    0
(4 rows)

timescale_analytics=> SELECT
    name,
    timescale_analytics_experimental.quantile(uddsketch, 0.9)
FROM daily_rain;
                 name                  |           quantile
---------------------------------------+--------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 0.3072142710699281
 LITCHFIELD PARK, AZ US                |                  0
 NY CITY CENTRAL PARK, NY US           | 0.4672895773464223
 MIAMI INTERNATIONAL AIRPORT, FL US    | 0.5483701300878486
(4 rows)

timescale_analytics=> SELECT
    name,
    timescale_analytics_experimental.quantile(uddsketch, 0.995)
FROM daily_rain;
                 name                  |           quantile
---------------------------------------+--------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 1.1969797510556823
 LITCHFIELD PARK, AZ US                | 0.7671946655927083
 NY CITY CENTRAL PARK, NY US           | 2.3145312888530807
 MIAMI INTERNATIONAL AIRPORT, FL US    | 2.9423518191328113
(4 rows)
```

## Command List (A-Z) [](uddsketch-api)
> - [uddsketch](#uddsketch)
> - [uddsketch_count](#uddsketch_count)
> - [uddsketch_error](#uddsketch_error)
> - [uddsketch_mean](#uddsketch_mean)
> - [uddsketch_quantile](#uddsketch_quantile)
> - [uddsketch_quantile_at_value](#uddsketch_quantile_at_value)


---
## **uddsketch** [](uddsketch)
```SQL
timescale_analytics_experimental.uddsketch(
    size INTEGER,
    max_error DOUBLE PRECISION,
    value DOUBLE PRECISION
) RETURNS UddSketch
```

This will construct and return a new UddSketch with at most `size` buckets.  The maximum relative error of the UddSketch will be bounded by `max_error` unless it is impossible to do so while with the bucket bound.  If the sketch has had to combine buckets, the new error can be found with the [uddsketch_error](#uddsketch_error) command.

### Required Arguments [](uddsketch-required-arguments)
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

### Sample Usages [](uddsketch-examples)
For this examples assume we have a table 'samples' with a column 'weights' holding `DOUBLE PRECISION` values.  The following will simply return a sketch over that column

```SQL
SELECT timescale_analytics_experimental.uddsketch(100, 0.01, data) FROM samples;
```

It may be more useful to build a view from the aggregate that we can later pass to other uddsketch functions.

```SQL
CREATE VIEW sketch AS
    SELECT timescale_analytics_experimental.uddsketch(100, 0.01, data)
    FROM samples;
```

---
## **uddsketch_count** [](uddsketch_count)

```SQL
timescale_analytics_experimental.get_count(sketch UddSketch) RETURNS DOUBLE PRECISION
```

Get the number of values contained in a UddSketch.

### Required Arguments [](uddsketch_count-required-arguments)
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to extract the number of values from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `uddsketch_count` | `DOUBLE PRECISION` | The number of values entered into the UddSketch. |
<br>

### Sample Usage [](uddsketch_count-examples)

```SQL
SELECT timescale_analytics_experimental.get_count(
    timescale_analytics_experimental.uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
 get_count
-----------
       100
(1 row)
```

---

## **uddsketch_error** [](uddsketch_error)

```SQL
timescale_analytics_experimental.error(sketch UddSketch) RETURNS DOUBLE PRECISION
```

This returns the maximum relative error that a quantile estimate will have (relative to the correct value).  This will initially be the same as the `max_error` used to construct the UddSketch, but if the sketch has needed to combine buckets this function will return the new maximum error.

### Required Arguments [](uddsketch_error-required-arguments)
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to determine the error of. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `uddsketch_error` | `DOUBLE PRECISION` | The maximum relative error of any quantile estimate. |
<br>

### Sample Usages [](uddsketch_error-examples)

```SQL
SELECT timescale_analytics_experimental.error(
    timescale_analytics_experimental.uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
 error
-------
  0.01
(1 row)
```

---
## **uddsketch_mean** [](uddsketch_mean)

```SQL
timescale_analytics_experimental.mean(sketch UddSketch) RETURNS DOUBLE PRECISION
```

Get the average of all the values contained in a UddSketch.

### Required Arguments [](uddsketch_mean-required-arguments)
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` |  The sketch to extract the mean value from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `mean` | `DOUBLE PRECISION` | The average of the values entered into the UddSketch. |
<br>

### Sample Usage [](uddsketch_mean-examples)

```SQL
SELECT timescale_analytics_experimental.mean(
    timescale_analytics_experimental.uddsketch(100, 0.01, data)
) FROM generate_series(1, 100) data;
 mean
------
 50.5
(1 row)
```

---
## **uddsketch_quantile** [](uddsketch_quantile)

```SQL
timescale_analytics_experimental.quantile(
    sketch UddSketch,
    quantile DOUBLE PRECISION
) RETURNS UddSketch
```

Get the approximate value at a quantile from a UddSketch.

### Required Arguments [](uddsketch_quantile-required-arguments)
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to compute the quantile on. |
| `quantile` | `DOUBLE PRECISION` | The desired quantile (0.0-1.0) to approximate. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `quantile` | `DOUBLE PRECISION` | The estimated value at the requested quantile. |
<br>

### Sample Usage [](uddsketch_quantile-examples)

```SQL
SELECT timescale_analytics_experimental.quantile(
    timescale_analytics_experimental.uddsketch(100, 0.01, data),
    0.90
) FROM generate_series(1, 100) data;
           quantile
--------------------
  89.13032933635797
(1 row)
```

---
## **uddsketch_quantile_at_value** [](uddsketch_quantile_at_value)

```SQL
timescale_analytics_experimental.quantile_at_value(
    sketch UddSketch,
    value DOUBLE PRECISION
) RETURNS UddSketch
```

Estimate what quantile a given value would be located at in a UddSketch.

### Required Arguments [](uddsketch_quantile_at_value-required-arguments)
|Name|Type|Description|
|---|---|---|
| `sketch` | `UddSketch` | The sketch to compute the quantile on. |
| `value` | `DOUBLE PRECISION` |  The value to estimate the quantile of. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `quantile_at_value` | `DOUBLE PRECISION` | The estimated quantile associated with the provided value. |
<br>

### Sample Usage [](uddsketch_quantile_at_value-examples)

```SQL
SELECT timescale_analytics_experimental.quantile_at_value(
    uddsketch(100, 0.01, data),
    90
) FROM generate_series(1, 100) data;
 quantile_at_value
-------------------
             0.89
(1 row)
```
