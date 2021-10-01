# Timeseries [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

> [Description](#timeseries-description)<br>
> [Timeseries Pipelines](#timeseries-pipelines)<br>
> [Example](#timeseries-example)<br>
> [API](#timeseries-api)

## Description <a id="timeseries-description"></a>

A timeseries is an intermediate representation of a particular value over time used by the extension.  It is a space efficient representation used to store the result of analytic functions such as [asap_smooth]((asap.md#asap_smooth)) or [lttb]((lttb.md#lttb)).  Data can also be directly aggregated into a timeseries and passed to functions which support this representation.  The [unnest](#timeseries_unnest) API can be used to get the data back from a timeseries.

## Timeseries Pipelines <a id="timeseries-pipelines"></a>

In an attempt to streamline the timeseries interface and make them as easy to use as possible, we've provided a custom operator `->` for applying common operations to timeseries and chaining such operations together.  This is much more fully documented in the [timeseries pipeline elements](timeseries_pipeline_elements.md) page.

## Usage Example <a id="timeseries-example"></a>

For this example, let's start with a table containing some random test data.

```SQL ,non-transactional,ignore-output
SET TIME ZONE 'UTC';
CREATE TABLE test(time TIMESTAMPTZ, value DOUBLE PRECISION);
```

```SQL ,non-transactional
INSERT INTO test
    SELECT time, value
    FROM toolkit_experimental.generate_periodic_normal_series('2020-01-01 UTC'::timestamptz, rng_seed => 11111);
```
```output
INSERT 0 4032
```

While still expermental, we'll need to set this before creating our view:
```SQL , non-transactional,ignore-output
    SET timescaledb_toolkit_acknowledge_auto_drop TO 'true';
```

Now lets capture this data into a time series which we'll store in a view.

```SQL ,non-transactional,ignore-output
CREATE VIEW series AS SELECT toolkit_experimental.timeseries(time, value) FROM test;
```

We can now use this timeseries to efficiently move the data around to other functions.

```SQL
SELECT time, value::numeric(10,2) FROM
toolkit_experimental.unnest((SELECT toolkit_experimental.lttb(timeseries, 20) FROM series));
```
```output
          time          |       value
------------------------+--------------------
2020-01-01 00:00:00+00 | 1038.44
2020-01-02 04:20:00+00 | 1325.44
2020-01-03 14:00:00+00 |  708.82
2020-01-04 18:30:00+00 | 1328.28
2020-01-05 16:40:00+00 |  802.20
2020-01-07 06:00:00+00 | 1298.02
2020-01-09 11:20:00+00 |  741.08
2020-01-10 18:40:00+00 | 1357.05
2020-01-13 08:30:00+00 |  780.32
2020-01-14 03:40:00+00 | 1408.34
2020-01-15 01:50:00+00 |  895.15
2020-01-16 20:30:00+00 | 1335.22
2020-01-18 07:20:00+00 |  823.08
2020-01-19 18:10:00+00 | 1245.79
2020-01-21 10:00:00+00 |  666.48
2020-01-22 23:10:00+00 | 1182.87
2020-01-24 09:00:00+00 |  736.47
2020-01-26 05:20:00+00 | 1197.26
2020-01-28 08:10:00+00 |  659.63
2020-01-28 23:50:00+00 |  956.29
```


## Command List (A-Z) <a id="timeseries-api"></a>
Aggregate Functions
> - [timeseries (point form)](#timeseries)
> - [rollup (summary form)](#timeseries-summary)

Accessor Functions
> - [unnest](#timeseries_unnest)


---

## **timeseries (point form)** <a id="timeseries"></a>
```SQL ,ignore
timeseries(
    time TIMESTAMPTZ,
    value DOUBLE PRECISION
) RETURNS TimeSeries
```

This will construct and return timeseries object containing the passed in time, value pairs.

### Required Arguments <a id="timeseries-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `time` | `TIMESTAMPTZ` | Time column to aggregate. |
| `value` | `DOUBLE PRECISION` | Value column to aggregate. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `timeseries` | `Timeseries` | A timeseries object which can be efficiently used by any of our timeseries operations. |
<br>

### Sample Usages <a id="timeseries-examples"></a>
For this example, assume we have a table 'samples' with two columns, 'time' and 'weight'.  The following will return that table as a timeseries.

```SQL ,ignore
SELECT toolkit_experimental.timeseries(time, weight) FROM samples;
```

---

## **rollup (summary form)** <a id="timeseries-summary"></a>
```SQL ,ignore
rollup(
    series timeseries
) RETURNS timeseries
```

This will combine multiple already constructed timeseriess. This is very useful for re-aggregating series already constructed using the [point form](#timeseries).

### Required Arguments <a id="timeseries-summary-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `series` | `timeseries` | Previously constructed timeseries objects. |
<br>

### Returns

|Column|Type|Description|
|---|---|---|
| `timeseries` | `timeseries` | A timeseries combining all the underlying series. |
<br>

### Sample Usages <a id="timeseries-summary-examples"></a>
This example assumes a table 'samples' with columns 'time', 'data', and 'batch'.  We can create a view containing timeseries for each batch like so:

```SQL ,ignore
CREATE VIEW series AS
    SELECT
        batch,
        toolkit_experimental.timeseries(time, data) as batch_series
    FROM samples
    GROUP BY batch;
```

If we want to operate over the combination of all batches, we can get the timeseries for this as follows:

```SQL ,ignore
SELECT rollup(batch_series)
FROM series;
```

---

## **unnest** <a id="timeseries_unnest"></a>

```SQL ,ignore
unnest(
    series timeseries
) RETURNS TABLE("time" timestamp with time zone, value double precision)
```

The unnest function is used to get the (time, value) pairs back out of a timeseries object.

### Required Arguments <a id="timeseries_unnest-required-arguments"></a>
|Name|Type|Description|
|---|---|---|
| `series` | `timeseries` | The series to return the data from. |
<br>

### Returns
|Column|Type|Description|
|---|---|---|
| `unnest` | `TABLE` | The (time,value) records contained in the timeseries. |
<br>

### Sample Usage <a id="timeseries_unnest-examples"></a>

```SQL
SELECT toolkit_experimental.unnest(
    (SELECT toolkit_experimental.timeseries(a.time, a.value)
    FROM
        (SELECT time, value
        FROM toolkit_experimental.generate_periodic_normal_series('2020-01-01 UTC'::timestamptz, 45654))
        a)
    )
LIMIT 10;
```
```output
                 unnest
-----------------------------------------------
 ("2020-01-01 00:00:00+00",1009.8399687963981)
 ("2020-01-01 00:10:00+00",873.6326953620166)
 ("2020-01-01 00:20:00+00",1045.8138997857413)
 ("2020-01-01 00:30:00+00",1075.472021940188)
 ("2020-01-01 00:40:00+00",956.0229773008177)
 ("2020-01-01 00:50:00+00",878.215079403259)
 ("2020-01-01 01:00:00+00",1067.8120522056508)
 ("2020-01-01 01:10:00+00",1102.3464544566375)
 ("2020-01-01 01:20:00+00",952.9509636893868)
 ("2020-01-01 01:30:00+00",1031.9006507123047)
```
