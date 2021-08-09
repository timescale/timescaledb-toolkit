# Timeseries Pipelines [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

> [Description](#timeseries-pipeline-description)<br>
> [Example](#timeseries-pipeline-example)<br>
> [Pipeline Elements](#timeseries-pipeline-elements)

## Description <a id="timeseries-pipeline-description"></a>

Timescale timeseries objects are just a convenient and efficient way of tracking a single value over time and are detailed a bit more [here](timeseries.md).  One of our primary goals with timeseries is that they should be easy and efficient to perform basic operations on, and that is where pipelines enter the picture.  At its simplest, a pipeline is just a timeseries connected to a [pipeline element](#timeseries-pipeline-elements) via the pipeline operator `|>`.  However, most pipeline operations output new timeseries, so it's possible to chain many pipeline elements together such that the output from one element become the input to the next.

### A note on operator associativity and grouping

Due to limitations in the PostgresQL parser, custom operators are required to be left associative.  The following pipeline will always result in `elementA` being applied to `timeseries` and then `elementB` being applied to the result.

```SQL ,ignore
SELECT timeseries |> elementA |> elementB;
```

However, it is possible to explicitly group elements using parentheses:

```SQL ,ignore
SELECT timeseries |> (elementA |> elementB);
```

This will result in a pipeline object being created from elements A and B, which will then be applied to the timeseries.  While we don't presently take maximum advantage of this internally, these multiple element pipelines should enable optimizations moving forward.  Therefore, this second form should be preferred where possible.

## Usage Example <a id="timeseries-pipeline-example"></a>

For this example let start with a table of temperatures collected from different devices at different times.

```SQL ,non-transactional,ignore-output
SET TIME ZONE 'UTC';
CREATE TABLE test_data(time TIMESTAMPTZ, device INTEGER, temperature DOUBLE PRECISION);
```

In order to have some nominally interesting data to look at, let's populate this table with random data covering 30 days of readings over 10 devices.

```SQL ,non-transactional,ignore-output
SELECT setseed(0.456);
INSERT INTO test_data 
    SELECT
        '2020-01-01 00:00:00+00'::timestamptz + ((random() * 2592000)::int * '1 second'::interval),
        floor(random() * 10 + 1),
        50 + random() * 20
    FROM generate_series(1,10000);
```

Now suppose we want to know how much the temperature fluctuates on a daily basis for each device.  Using timeseries and pipelines can simplify the process of finding the answer:
```SQL ,non-transactional,ignore-output
SET timescaledb_toolkit_acknowledge_auto_drop TO 'true';
CREATE VIEW daily_delta AS 
    SELECT device, 
        toolkit_experimental.timeseries(time, temperature)
            |> (toolkit_experimental.sort() 
            |>  toolkit_experimental.resample_to_rate('trailing_average', '24 hours', true) 
            |>  toolkit_experimental.fill_holes('interpolate') 
            |>  toolkit_experimental.delta()) AS deltas
    FROM test_data
    GROUP BY device;
```

This command creates a timeseries from the time and temperature columns (grouped by device), sorts them in increasing time, aggregates them as a daily average, interpolates the values for any missing days, and computes the deltas between days.  Now we can look at the deltas for a specific device:

```SQL
SELECT time, value::numeric(4,2) AS delta FROM toolkit_experimental.unnest_series((SELECT deltas FROM daily_delta WHERE device = 3));
```
```output
          time          | delta 
------------------------+-------
 2020-01-02 00:00:00+00 | -0.54
 2020-01-03 00:00:00+00 |  0.29
 2020-01-04 00:00:00+00 | -0.25
 2020-01-05 00:00:00+00 |  0.07
 2020-01-06 00:00:00+00 |  0.80
 2020-01-07 00:00:00+00 | -0.27
 2020-01-08 00:00:00+00 | -2.55
 2020-01-09 00:00:00+00 |  3.51
 2020-01-10 00:00:00+00 | -0.78
 2020-01-11 00:00:00+00 | -0.39
 2020-01-12 00:00:00+00 |  0.55
 2020-01-13 00:00:00+00 | -0.87
 2020-01-14 00:00:00+00 |  1.17
 2020-01-15 00:00:00+00 | -2.49
 2020-01-16 00:00:00+00 |  0.10
 2020-01-17 00:00:00+00 |  1.09
 2020-01-18 00:00:00+00 | -0.09
 2020-01-19 00:00:00+00 |  1.14
 2020-01-20 00:00:00+00 | -1.23
 2020-01-21 00:00:00+00 | -0.29
 2020-01-22 00:00:00+00 | -0.37
 2020-01-23 00:00:00+00 |  1.48
 2020-01-24 00:00:00+00 | -0.52
 2020-01-25 00:00:00+00 |  1.34
 2020-01-26 00:00:00+00 | -0.95
 2020-01-27 00:00:00+00 | -0.65
 2020-01-28 00:00:00+00 | -0.42
 2020-01-29 00:00:00+00 |  1.42
 2020-01-30 00:00:00+00 | -0.66
```

Or even run one of our device's deltas through lttb to get a nice graphable set of points:
```SQL
SELECT (deltas |> toolkit_experimental.lttb(10))::TEXT FROM daily_delta where device = 7;
```
```output
                                                                            text                                                                                                                                                                                                                                                                                               
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 [{"ts":"2020-01-02 00:00:00+00","val":0.5555802022457712},{"ts":"2020-01-05 00:00:00+00","val":-1.4688929826077484},{"ts":"2020-01-08 00:00:00+00","val":2.416048415988122},{"ts":"2020-01-09 00:00:00+00","val":-3.0046993833401174},{"ts":"2020-01-14 00:00:00+00","val":0.22758839123397223},{"ts":"2020-01-17 00:00:00+00","val":-2.1256090660578124},{"ts":"2020-01-19 00:00:00+00","val":1.2272792346941657},{"ts":"2020-01-25 00:00:00+00","val":-3.1053238977555324},{"ts":"2020-01-26 00:00:00+00","val":1.2629388469236815},{"ts":"2020-01-30 00:00:00+00","val":-0.7042437967407409}]
```

## Current Pipeline Elements(A-Z) <a id="timeseries-pipeline-elements"></a>

As of the current timescale release, these elements are all [experimental](/docs/README.md#tag-notes).


> - [delta](#timeseries_pipeline_delta)
> - [fill_holes](#timeseries_pipeline_fill_holes)
> - [lttb](#timeseries_pipeline_lttb)
> - [resample_to_rate](#timeseries_pipeline_resample_to_rate)
> - [sort](#sort)


---

## **delta** <a id="timeseries_pipeline_delta"></a>
```SQL ,ignore
delta(
) RETURNS TimeseriesPipelineElement
```

This element will return a new timeseries where each point is the difference between the current and preceeding value in the input timeseries.  The new series will be one point shorter as it will not have a preceding value to return a delta for the first point.

### Required Arguments <a id="timeseries_pipeline_delta-arguments"></a>
|Name| Type |Description|
|---|---|---|
<br>

### Pipeline Execution Returns <a id="timeseries_pipeline_delta-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timeseries` | `Timeseries` | The result of applying this pipeline element will be a new time series where each point contains the difference in values from the prior point in the input timeseries. |
<br>

### Sample Usage <a id="timeseries_pipeline_delta-examples"></a>
```SQL
SELECT time, value
FROM toolkit_experimental.unnest_series(
    (SELECT toolkit_experimental.timeseries('2020-01-01'::timestamptz + step * '1 day'::interval, step * step) 
        |> toolkit_experimental.delta() 
    FROM generate_series(1, 5) step)
);
```
```output
          time          | value 
------------------------+-------
 2020-01-03 00:00:00+00 |     3
 2020-01-04 00:00:00+00 |     5
 2020-01-05 00:00:00+00 |     7
 2020-01-06 00:00:00+00 |     9
```

---

## **fill_holes** <a id="timeseries_pipeline_fill_holes"></a>
```SQL ,ignore
fill_holes(
    fill_method TEXT
) RETURNS TimeseriesPipelineElement
```

This element will take in a normal timeseries (such as the result of a [resample_to_rate](#timeseries_pipeline_resample_to_rate) pipeline element), and fill in any implicit gaps according to the requested `fill_method`.  Calling this on a non-normal timeseries will produce an error.

Valid fill methods are:
| Method | Description |
|---|---|
| `locf` | Fill gaps with the last valid preceeding value. |
| `interpolate` | Compute the missing value linearly from the immediately bounding values |

### Required Arguments <a id="timeseries_pipeline_fill_holes-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `fill_method` | `TEXT` | Case insensitive match for one of the fill methods above. |
<br>

### Pipeline Execution Returns <a id="timeseries_pipeline_fill_holes-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timeseries` | `Timeseries` | This creates a complete normal timeseries (no missing values) from the input series.. |
<br>

### Sample Usage <a id="timeseries_pipeline_fill_holes-examples"></a>
```SQL
SELECT time, value
FROM toolkit_experimental.unnest_series(
    (SELECT toolkit_experimental.timeseries('2020-01-01'::timestamptz + step * step * '1 hour'::interval, step * step) 
        |> (toolkit_experimental.resample_to_rate('nearest', '1 hour', true)
        |>  toolkit_experimental.fill_holes('locf'))
    FROM generate_series(1, 3) step)
);
```
```output
          time          | value 
------------------------+-------
 2020-01-01 01:00:00+00 |     1
 2020-01-01 02:00:00+00 |     1
 2020-01-01 03:00:00+00 |     1
 2020-01-01 04:00:00+00 |     4
 2020-01-01 05:00:00+00 |     4
 2020-01-01 06:00:00+00 |     4
 2020-01-01 07:00:00+00 |     4
 2020-01-01 08:00:00+00 |     4
 2020-01-01 09:00:00+00 |     9
```

---

## **lttb** <a id="timeseries_pipeline_lttb"></a>
```SQL ,ignore
lttb(
    resolution int,
) RETURNS TimeseriesPipelineElement
```

This element will return a [largest triangle three buckets](lttb.md#description) approximation of a given timeseries.  Its behavior is the same as the lttb function documented [here](lttb.md#lttb), save that it expects the series to be sorted.

```SQL ,ignore
SELECT lttb(time, value, 40) FROM data;
```
is equivalent to
```SQL ,ignore
SELECT timeseries(time, value) |> sort() |> lttb() FROM data;
```

### Required Arguments <a id="timeseries_pipeline_lttb-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `resolution` | `INTEGER` | Number of points the output should have. |
<br>

### Pipeline Execution Returns <a id="timeseries_pipeline_lttb-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timeseries` | `Timeseries` | The result of applying this pipeline element will be a new timeseries with `resolution` point that is visually similar to the input series. |
<br>

### Sample Usage <a id="timeseries_pipeline_lttb-examples"></a>
```SQL
SELECT time, value
FROM toolkit_experimental.unnest_series(
    (SELECT toolkit_experimental.timeseries('2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int), 10 + 5 * cos(foo)) 
        |> toolkit_experimental.lttb(4) 
    FROM generate_series(1,11,0.1) foo)
);
```
```output
          time          |       value
------------------------+--------------------
 2020-01-11 00:00:00+00 |   12.7015115293407
 2020-02-01 00:00:00+00 |  5.004324248633603
 2020-03-03 00:00:00+00 | 14.982710485116087
 2020-04-20 00:00:00+00 | 10.022128489940254
```

---

## **resample_to_rate** <a id="timeseries_pipeline_resample_to_rate"></a>
```SQL ,ignore
resample_to_rate(
    resample_method TEXT,
    interval INTERVAL,
    snap_to_rate BOOL
) RETURNS TimeseriesPipelineElement
```

This element will operate over a timeseries, returning a new series with points exactly `interval` units apart.  The target timestamp for the first point of this range will either be the first timestamp from the input range if `snap_to_rate` is false, or the `interval` truncated timestamp containing that time if `snap_to_rate` is true.  The value for the new points will be computed from all the points in the input series which fall into the resulting interval, using the `resample_method` as follows:

| Method | Description | Interval range |
|---|---|---|
| `average` | An average of all the values closest to the target timestamp | Each point of the result covers the values with times +/- `interval` / 2 in the input series |
| `nearest` | The value of the closest point in the input series.  If the two nearest points are equdistant, this becomes the average of their values. | Only points +/- `interval` / 2 are considered candidates for `nearest`.  If there are no points in this range in the input series, the output series will not have a value for that timestamp |
| `weighted_average` | Similar to average, but weights points on how close they are to the target time.  A point matching the target time would be full weight, while one on the edge of the interval only recieves 0.1 weight (weights for other values grow linearly between these extremes as they approach the target). | Like average, each point in the output series will aggregate the points of the input series with times +/- `interval` / 2 |
| `trailing_average` | In this case, each point of the result is determined by the average of the points in the `interval` following the target time. | Each point covers the target time + `interval` in the input series |

In all cases, if there are no points in the input series in the interval range of a particular target time, there will be no point at that time in the output series.

### Required Arguments <a id="timeseries_pipeline_resample_to_rate-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `resample_method` | `TEXT` | Case insensitive match for one of the methods above. |
| `interval` | `INTERVAL` | The rate to resample to.  Note that this must be a stable interval, meaning it can't use time units greater than hours (days are unstable due to DST) |
| `snap_to_rate` | `BOOL` | Whether the resulting points should be multiples of `interval` (if true), else `interval` offsets from the first point in the input series. |
<br>

### Pipeline Execution Returns <a id="timeseries_pipeline_resample_to_rate-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timeseries` | `Timeseries` | A new pipeline with `interval` spaced points generated from the input series |
<br>

### Sample Usage <a id="timeseries_pipeline_resample_to_rate-examples"></a>
```SQL
SELECT time, value::numeric(4,2)
FROM toolkit_experimental.unnest_series(
    (SELECT toolkit_experimental.timeseries('2020-01-01'::TIMESTAMPTZ + step *step * step * '1 minute'::interval, step) 
        |> toolkit_experimental.resample_to_rate('weighted_average', '1 hour', true) 
    FROM generate_series(1,10) step)
);
```
```output
          time          | value 
------------------------+-------
 2020-01-01 00:00:00+00 |  1.59
 2020-01-01 01:00:00+00 |  4.00
 2020-01-01 02:00:00+00 |  5.00
 2020-01-01 04:00:00+00 |  6.00
 2020-01-01 06:00:00+00 |  7.00
 2020-01-01 09:00:00+00 |  8.00
 2020-01-01 12:00:00+00 |  9.00
 2020-01-01 17:00:00+00 | 10.00
```

---

## **sort** <a id="timeseries_pipeline_sort"></a>
```SQL ,ignore
sort(
) RETURNS TimeseriesPipelineElement
```

This element takes in a timeseries and returns a timeseries consisting of the same points, but in order of increasing time values.

### Required Arguments <a id="timeseries_pipeline_sort-arguments"></a>
|Name| Type |Description|
|---|---|---|
<br>

### Pipeline Execution Returns <a id="timeseries_pipeline_sort-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timeseries` | `Timeseries` | The result of applying this pipeline element will be a time sorted version of the incoming timeseries. |
<br>

### Sample Usage <a id="timeseries_pipeline_sort-examples"></a>
```SQL
SELECT time, value
FROM toolkit_experimental.unnest_series(
    (SELECT toolkit_experimental.timeseries('2020-01-06'::timestamptz - step * '1 day'::interval, step * step) 
        |> toolkit_experimental.sort() 
    FROM generate_series(1, 5) step)
);
```
```output
          time          | value 
------------------------+-------
 2020-01-01 00:00:00+00 |    25
 2020-01-02 00:00:00+00 |    16
 2020-01-03 00:00:00+00 |     9
 2020-01-04 00:00:00+00 |     4
 2020-01-05 00:00:00+00 |     1
```

---
