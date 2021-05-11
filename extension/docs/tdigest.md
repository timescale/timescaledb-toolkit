# T-Digest [<sup><mark>experimental</mark></sup>](/extension/docs/README.md#tag-notes)

> [Description](#tdigest-description)<br>
> [Details](#tdigest-details)<br>
> [Example](#tdigest-example)<br>
> [Continuous Aggregate Example](#tdigest-cagg-example)<br>
> [API](#tdigest-api)

## Description <a id="tdigest-description"></a>

Timescale analytics provides an implementation of the [t-digest data structure](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) for quantile approximations.  A t-digest is a space efficient aggregation which provides increased resolution at the edges of the distribution.  This allows for more accurate estimates of extreme quantiles than traditional methods.

## Details <a id="tdigest-details"></a>

Timescale's t-digest is implemented as an aggregate function in PostgreSQL.  They do not support moving-aggregate mode, and are not ordered-set aggregates.  Presently they are restricted to float values, but the goal is to make them polymorphic.  They are partializable and are good candidates for [continuous aggregation](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).

One additional thing to note about TDigests is that they are somewhat dependant on the order of inputs.  The percentile approximations should be nearly equal for the same underlying data, especially at the extremes of the quantile range where the TDigest is inherently more accurate, they are unlikely to be identical if built in a different order.  While this should have little effect on the accuracy of the estimates, it is worth noting that repeating the creation of the TDigest might have subtle differences if the call is being parallelized by Postgres.  Similarly, building a TDigest by combining several subdigests using the [summary aggregate](#tdigest-summary) is likely to produce a subtley different result than combining all of the underlying data using a single [point aggregate](#tdigest).

## Usage Example <a id="tdigest-example"></a>

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
    SELECT name, tdigest(100, tmax)
    FROM weather
    GROUP BY name;

SELECT
    name,
    num_vals(tdigest)
FROM high_temp;
```
```
                 name                  | num_vals
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
    approx_percentile(0.995, tdigest)
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
    approx_percentile_at_value(90.0, tdigest)
FROM high_temp;
```
```
                 name                  |  approx_percentile_at_value
---------------------------------------+--------------------
 PORTLAND INTERNATIONAL AIRPORT, OR US | 0.9609990016734108
 LITCHFIELD PARK, AZ US                | 0.5531621580122781
 NY CITY CENTRAL PARK, NY US           | 0.9657150306348585
 MIAMI INTERNATIONAL AIRPORT, FL US    | 0.8093468908877591
(4 rows)
```

## Example Using TimeScale Continuous Aggregates (tdigest-cagg-example)
Timescale [continuous aggregates](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates)
provide an easy way to keep a tdigest up to date as more data is added to a table.  The following example
shows how this might look in practice.  The first step is to create a Timescale hypertable to store our data.

```SQL ,non-transactional,ignore-output
SET TIME ZONE 'UTC';
CREATE TABLE test(time TIMESTAMPTZ, value DOUBLE PRECISION);
SELECT create_hypertable('test', 'time');
```

Next a materialized view with the timescaledb.continuous property is added.  This will automatically keep itself,
including the tdigest in this case, up to date as data is added to the table.
```SQL ,non-transactional,ignore-output
CREATE MATERIALIZED VIEW weekly_sketch
WITH (timescaledb.continuous)
AS SELECT
    time_bucket('7 day'::interval, time) as week,
    tdigest(100, value) as digest
FROM test
GROUP BY time_bucket('7 day'::interval, time);
```

Next a utility function, `generate_periodic_normal_series`, is called to generate some data.  When called in
this manner the function will return 28 days worth of data points spaced 10 minutes apart.  These points are
generate by adding a random point (with a normal distribution and standard deviation of 100) to a sine wave
which oscilates between 900 and 1100 over the period of a day.
```SQL ,non-transactional
INSERT INTO test
    SELECT time, value
    FROM timescale_analytics_experimental.generate_periodic_normal_series('2020-01-01 UTC'::timestamptz, rng_seed => 543643); 
```
```
INSERT 0 4032
```

Finally, a query is run over the aggregate to see various approximate percentiles from different weeks.
```SQL
SELECT 
    week,
    approx_percentile(0.01, digest) AS low, 
    approx_percentile(0.5, digest) AS mid, 
    approx_percentile(0.99, digest) AS high 
FROM weekly_sketch
ORDER BY week;
```
```output
         week          |        low        |        mid         |        high        
-----------------------+-------------------+--------------------+--------------------
2019-12-30 00:00:00+00 | 783.2075197029583 | 1030.4505832620227 | 1276.7865808567146
2020-01-06 00:00:00+00 | 865.2941219994462 | 1096.0356855737048 |  1331.649176312383
2020-01-13 00:00:00+00 | 834.6747915021757 |  1060.024660266383 |    1286.1810386717
2020-01-20 00:00:00+00 | 728.2421431793433 |  955.3913494459423 |  1203.730690023456
2020-01-27 00:00:00+00 | 655.1143367116582 |  903.4836014674186 | 1167.7058289748031

```

It is also possible to combine the weekly aggregates to run queries on the entire data:
```SQL
SELECT 
    approx_percentile(0.01, combined.digest) AS low, 
    approx_percentile(0.5, combined.digest) AS mid, 
    approx_percentile(0.99, combined.digest) AS high 
FROM (SELECT tdigest(digest) AS digest FROM weekly_sketch) AS combined;
```
```output
       low        |        mid         |        high        
------------------+--------------------+--------------------
746.7844638729881 | 1026.6100299252928 | 1294.5391132795592
```


## Command List (A-Z) <a id="tdigest-api"></a>
Aggregate Functions
> - [tdigest - point form](#tdigest)
> - [tdigest - summary form](#tdigest-summary)

Accessor Functions
> - [approx_percentile](#tdigest_quantile)
> - [approx_percentile_at_value](#tdigest_quantile_at_value)
> - [max_val](#tdigest_max)
> - [mean](#tdigest_mean)
> - [min_val](#tdigest_min)
> - [num_vals](#tdigest_count)

---

## **tdigest - point form** <a id="tdigest"></a>
```SQL ,ignore
tdigest(
    buckets INTEGER,
    value DOUBLE PRECISION
) RETURNS TDigest
```

This will construct and return a TDigest with the specified number of buckets over the given values.

### Required Arguments <a id="tdigest-required-arguments"></a>
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

### Sample Usages <a id="tdigest-examples"></a>
For this example, assume we have a table 'samples' with a column 'weights' holding `DOUBLE PRECISION` values.  The following will simply return a digest over that column

```SQL ,ignore
SELECT tdigest(100, data) FROM samples;
```

It may be more useful to build a view from the aggregate that can later be passed to other tdigest functions.

```SQL ,ignore
CREATE VIEW digest AS
    SELECT tdigest(100, data)
    FROM samples;
```

---

## **tdigest (summary form)** <a id="tdigest-summary"></a>
```SQL ,ignore
tdigest(
    digest TDigest
) RETURNS TDigest
```

This will combine multiple already constructed TDigests, if they were created with the same size. This is very useful for re-aggregating digests already constructed using the [point form](#tdigest).  Note that the resulting digest may be subtley different from a digest constructed directly from the underlying points, as noted in the [details section](#tdigest-details) above.

### Required Arguments <a id="tdigest-summary-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `digest` | `TDigest` | Previously constructed TDigest objects. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `tdigest` | `TDigest` | A TDigest representing all of the underlying data from all the subaggregates. |
<br>

### Sample Usages <a id="tdigest-summary-examples"></a>
This example assumes a table 'samples' with a column 'data' holding `DOUBLE PRECISION` values and an 'id' column that holds the what series the data belongs to.  A view to get the TDigests for each `id` using the [point form](#tdigest-point) can be created like so:

```SQL ,ignore
CREATE VIEW digests AS
    SELECT 
        id, 
        tdigest(100, data) as digest
    FROM samples
    GROUP BY id;
```

That view can then be used to get the full aggregate like so: 

```SQL ,ignore
SELECT tdigest(digest)
FROM digests;
```

---

## **approx_percentile** <a id="tdigest_quantile"></a>

```SQL ,ignore
approx_percentile(
    quantile DOUBLE PRECISION,
    digest TDigest
) RETURNS TDigest
```

Get the approximate value at a quantile from a t-digest

### Required Arguments <a id="tdigest_quantile-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `quantile` | `DOUBLE PRECISION` | The desired quantile (0.0-1.0) to approximate. |
| `digest` | `TDigest` | The digest to compute the quantile on. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `approx_percentile` | `DOUBLE PRECISION` | The estimated value at the requested quantile. |
<br>

### Sample Usage <a id="tdigest_quantile-examples"></a>

```SQL
SELECT approx_percentile(0.90, tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 approx_percentile
----------
     90.5
```

---

## **approx_percentile_at_value** <a id="tdigest_quantile_at_value"></a>

```SQL ,ignore
approx_percentile_at_value(
    value DOUBLE PRECISION,
    digest TDigest
) RETURNS TDigest
```

Estimate what quantile a given value would be located at in a t-digest.

### Required Arguments <a id="tdigest_quantile_at_value-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `value` | `DOUBLE PRECISION` |  The value to estimate the quantile of. |
| `digest` | `TDigest` | The digest to compute the quantile on. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `approx_percentile_at_value` | `DOUBLE PRECISION` | The estimated quantile associated with the provided value. |
<br>

### Sample Usage <a id="tdigest_quantile_at_value-examples"></a>

```SQL
SELECT approx_percentile_at_value(90, tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 approx_percentile_at_value
-------------------
             0.895
```

## **max_val** <a id="tdigest_max"></a>

```SQL ,ignore
max_val(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the maximum value from a t-digest.

### Required Arguments <a id="tdigest_max-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to extract the max value from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `max_val` | `DOUBLE PRECISION` | The maximum value entered into the t-digest. |
<br>

### Sample Usage <a id="tdigest_max-examples"></a>

```SQL
SELECT max_val(tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 max_val
---------
     100
```

---

## **mean** <a id="tdigest_mean"></a>

```SQL ,ignore
mean(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the average of all the values contained in a t-digest.

### Required Arguments <a id="tdigest_mean-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` |  The digest to extract the mean value from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `mean` | `DOUBLE PRECISION` | The average of the values entered into the t-digest. |
<br>

### Sample Usage <a id="tdigest_mean-examples"></a>

```SQL
SELECT mean(tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 mean
------
 50.5
```

---

## **min_val** <a id="tdigest_min"></a>

```SQL ,ignore
min_val(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the minimum value from a t-digest.

### Required Arguments <a id="tdigest_min-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to extract the min value from. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `min_val` | `DOUBLE PRECISION` | The minimum value entered into the t-digest. |
<br>

### Sample Usages <a id="tdigest-min-examples"></a>

```SQL
SELECT min_val(tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 min_val
-----------
         1
```

---

## **num_vals** <a id="tdigest_count"></a>

```SQL ,ignore
num_vals(digest TDigest) RETURNS DOUBLE PRECISION
```

Get the number of values contained in a t-digest.

### Required Arguments <a id="tdigest_count-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `digest` | `TDigest` | The digest to extract the number of values from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `num_vals` | `DOUBLE PRECISION` | The number of values entered into the t-digest. |
<br>

### Sample Usage <a id="tdigest_count-examples"></a>

```SQL
SELECT num_vals(tdigest(100, data))
FROM generate_series(1, 100) data;
```
```output
 num_vals
-----------
       100
```

---
