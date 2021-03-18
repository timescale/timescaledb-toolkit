# T-Digest [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes)

> [Description](#tdigest-description)<br>
> [Details](#tdigest-details)<br>
> [Example](#tdigest-example)<br>
> [API](#tdigest-api)

## Description [](tdigest-description)

Timescale analytics provides an implementation of the [t-digest data structure](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) for quantile approximations.  A t-digest is a space efficient aggregation which provides increased resolution at the edges of the distribution.  This allows for more accurate estimates of extreme quantiles than traditional methods.

## Details [](tdigest-details)

Timescale's t-digest is implemented as an aggregate function in PostgreSQL.  They do not support moving-aggregate mode, and are not ordered-set aggregates.  Presently they are restricted to float values, but the goal is to make them polymorphic.  They are partializable and are good candidates for [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).

## Usage Example [](tdigest-example)

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

Now let's create some t-digests for our different stations and verify that they're receiving data.

```SQL ,ignore
CREATE VIEW high_temp AS
    SELECT name, timescale_analytics_experimental.tdigest(100, tmax)
    FROM weather
    GROUP BY name;

SELECT
    name,
    timescale_analytics_experimental.get_count(tdigest)
FROM high_temp;
```
```
                 name                  | get_count
---------------------------------------+-----------
 PORTLAND INTERNATIONAL AIRPORT, OR US |      7671
 LITCHFIELD PARK, AZ US                |      5881
 NY CITY CENTRAL PARK, NY US           |      7671
 MIAMI INTERNATIONAL AIRPORT, FL US    |      7671
(4 rows)
```

We can then check to see the 99.5 percentile high temperature for each location.
```SQL ,ignore
SELECT
    name,
    timescale_analytics_experimental.quantile(tdigest, 0.995)
FROM high_temp;
```
```
                 name                  |           quantile
---------------------------------------+--------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US |   98.4390837104072
 LITCHFIELD PARK, AZ US                | 114.97809722222223
 NY CITY CENTRAL PARK, NY US           |  95.86391321044545
 MIAMI INTERNATIONAL AIRPORT, FL US    |  95.04283854166665
(4 rows)
```
Or even check to see what quantile 90F would fall at in each city.
```SQL ,ignore
SELECT
    name,
    timescale_analytics_experimental.quantile_at_value(tdigest, 90.0)
FROM high_temp;
```
```
                 name                  |  quantile_at_value
---------------------------------------+--------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 0.9609990016734108
 LITCHFIELD PARK, AZ US                | 0.5531621580122781
 NY CITY CENTRAL PARK, NY US           | 0.9657150306348585
 MIAMI INTERNATIONAL AIRPORT, FL US    | 0.8093468908877591
(4 rows)
```

## Command List (A-Z) [](tdigest-api)
> - [tdigest](#tdigest)
> - [tdigest_count](#tdigest_count)
> - [tdigest_max](#tdigest_max)
> - [tdigest_mean](#tdigest_mean)
> - [tdigest_min](#tdigest_min)
> - [tdigest_quantile](#tdigest_quantile)
> - [tdigest_quantile_at_value](#tdigest_quantile_at_value)
> - [tdigest_sum](#tdigest_sum)


---
## **tdigest** [](tdigest)
```SQL ,ignore
timescale_analytics_experimental.tdigest(
    buckets INTEGER,
    value DOUBLE PRECISION
) RETURNS TDigest
```

This will construct and return a TDigest with the specified number of buckets over the given values.

### Required Arguments [](tdigest-required-arguments)
|Name| Type |Description|
|---|---|---|
| `buckets` | `INTEGER` | Number of buckets in the digest.  Increasing this will provide more accurate quantile estimates, but will require more memory.|
| `value` | `DOUBLE PRECISION` |  Column to aggregate.
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `tdigest` | `TDigest` | A t-digest object which may be passed to other t-digest APIs. |
<br>

### Sample Usages [](tdigest-examples)
For this examples assume we have a table 'samples' with a column 'weights' holding `DOUBLE PRECISION` values.  The following will simply return a digest over that column

```SQL ,ignore
SELECT timescale_analytics_experimental.tdigest(100, data) FROM samples;
```

It may be more useful to build a view from the aggregate that we can later pass to other tdigest functions.

```SQL ,ignore
CREATE VIEW digest AS
    SELECT timescale_analytics_experimental.tdigest(100, data)
    FROM samples;
```

---

## **tdigest_min** [](tdigest_min)

```SQL ,ignore
timescale_analytics_experimental.get_min(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the minimum value from a t-digest.

### Required Arguments [](tdigest_min-required-arguments)
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to extract the min value from. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `tdigest_min` | `DOUBLE PRECISION` | The minimum value entered into the t-digest. |
<br>

### Sample Usages [](tdigest-min-examples)

```SQL
SELECT timescale_analytics_experimental.get_min(timescale_analytics_experimental.tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 tdigest_min
-------------
           1
```
---
## **tdigest_max** [](tdigest_max)

```SQL ,ignore
timescale_analytics_experimental.get_max(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the maximum value from a t-digest.

### Required Arguments [](tdigest_max-required-arguments)
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to extract the max value from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `max` | `DOUBLE PRECISION` | The maximum value entered into the t-digest. |
<br>

### Sample Usage [](tdigest_max-examples)

```SQL
SELECT timescale_analytics_experimental.get_max(timescale_analytics_experimental.tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 get_max
---------
     100
```
---
## **tdigest_count** [](tdigest_count)

```SQL ,ignore
timescale_analytics_experimental.get_count(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the number of values contained in a t-digest.

### Required Arguments [](tdigest_count-required-arguments)
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to extract the number of values from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `count` | `DOUBLE PRECISION` | The number of values entered into the t-digest. |
<br>

### Sample Usage [](tdigest_count-examples)

```SQL
SELECT timescale_analytics_experimental.get_count(timescale_analytics_experimental.tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 get_count
-----------
       100
```

---
## **tdigest_mean** [](tdigest_mean)

```SQL ,ignore
timescale_analytics_experimental.mean(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the average of all the values contained in a t-digest.

### Required Arguments [](tdigest_mean-required-arguments)
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` |  The digest to extract the mean value from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `mean` | `DOUBLE PRECISION` | The average of the values entered into the t-digest. |
<br>

### Sample Usage [](tdigest_mean-examples)

```SQL
SELECT timescale_analytics_experimental.mean(timescale_analytics_experimental.tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 mean
------
 50.5
```

---
## **tdigest_sum** [](tdigest_sum)

```SQL ,ignore
timescale_analytics_experimental.sum(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the sum of all the values in a t-digest

### Required Arguments [](tdigest_sum-required-arguments)
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` |  The digest to compute the sum on. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `sum` | `DOUBLE PRECISION` | The sum of the values entered into the t-digest. |
<br>

### Sample Usage [](tdigest_sum-examples)

```SQL
SELECT timescale_analytics_experimental.sum(timescale_analytics_experimental.tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
  sum
------
 5050
```

---
## **tdigest_quantile** [](tdigest_quantile)

```SQL ,ignore
timescale_analytics_experimental.quantile(
    digest TDigest,
    quantile DOUBLE PRECISION
) RETURNS TDigest
```

Get the approximate value at a quantile from a t-digest

### Required Arguments [](tdigest_quantile-required-arguments)
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to compute the quantile on. |
| `quantile` | `DOUBLE PRECISION` | The desired quantile (0.0-1.0) to approximate. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `quantile` | `DOUBLE PRECISION` | The estimated value at the requested quantile. |
<br>

### Sample Usage [](tdigest_quantile-examples)

```SQL
SELECT timescale_analytics_experimental.quantile(timescale_analytics_experimental.tdigest(100, data), 0.90)
FROM generate_series(1, 100) data;
```
```output
 quantile
----------
     90.5
```

---
## **tdigest_quantile_at_value** [](tdigest_quantile_at_value)

```SQL ,ignore
timescale_analytics_experimental.quantile_at_value(
    digest TDigest,
    value DOUBLE PRECISION
) RETURNS TDigest
```

Estimate what quantile a given value would be located at in a t-digest.

### Required Arguments [](tdigest_quantile_at_value-required-arguments)
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to compute the quantile on. |
| `value` | `DOUBLE PRECISION` |  The value to estimate the quantile of. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `quantile_at_value` | `DOUBLE PRECISION` | The estimated quantile associated with the provided value. |
<br>

### Sample Usage [](tdigest_quantile_at_value-examples)

```SQL
SELECT timescale_analytics_experimental.quantile_at_value(timescale_analytics_experimental.tdigest(100, data), 90)
FROM generate_series(1, 100) data;
```
```output
 quantile_at_value
-------------------
             0.895
```
