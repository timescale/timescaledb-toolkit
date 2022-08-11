# Timevector Pipelines [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

> [Description](#timevector-pipeline-description)<br>
> [Example](#timevector-pipeline-example)<br>
> [Pipeline Elements](#timevector-pipeline-elements)

## Description <a id="timevector-pipeline-description"></a>

Timescale timevector objects are just a convenient and efficient way of tracking a single value over time and are detailed a bit more [here](timevector.md).  One of our primary goals with timevector is that they should be easy and efficient to perform basic operations on, and that is where pipelines enter the picture.  At its simplest, a pipeline is just a timevector connected to a [pipeline element](#timevector-pipeline-elements) via the pipeline operator `->`.  However, most pipeline operations output new timevector, so it's possible to chain many pipeline elements together such that the output from one element become the input to the next.

### A note on operator associativity and grouping

Due to limitations in the PostgresQL parser, custom operators are required to be left associative.  The following pipeline will always result in `elementA` being applied to `timevector` and then `elementB` being applied to the result.

```SQL ,ignore
SELECT timevector -> elementA -> elementB;
```

However, it is possible to explicitly group elements using parentheses:

```SQL ,ignore
SELECT timevector -> (elementA -> elementB);
```

This will result in a pipeline object being created from elements A and B, which will then be applied to the timevector.  While we don't presently take maximum advantage of this internally, these multiple element pipelines should enable optimizations moving forward.  Therefore, this second form should be preferred where possible.

## Usage Example <a id="timevector-pipeline-example"></a>

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

Now suppose we want to know how much the temperature fluctuates on a daily basis for each device.  Using timevector and pipelines can simplify the process of finding the answer:
```SQL ,non-transactional,ignore-output
CREATE VIEW daily_delta AS
    SELECT device,
        timevector(time, temperature)
            -> (toolkit_experimental.sort()
            ->  toolkit_experimental.delta()) AS deltas
    FROM test_data
    GROUP BY device;
```

This command creates a timevector from the time and temperature columns (grouped by device), sorts them in increasing time, and computes the deltas between values.  Now we can look at the deltas for a specific device.  Note that the output for this test is inaccurate as we've removed some of the pipeline elements for the moment.

```SQL,ignore-output
SELECT time, value::numeric(4,2) AS delta FROM unnest((SELECT deltas FROM daily_delta WHERE device = 3));
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
SELECT (deltas -> toolkit_experimental.lttb(10))::TEXT FROM daily_delta where device = 7;
```
```output
                                                                            text
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  (version:1,num_points:10,flags:1,internal_padding:(0,0,0),points:[(ts:"2020-01-01 01:25:10+00",val:6.071850341376361),(ts:"2020-01-01 06:42:42+00",val:-19.012231731606803),(ts:"2020-01-05 07:18:48+00",val:15.050657902599482),(ts:"2020-01-10 09:35:14+00",val:-17.350077317333685),(ts:"2020-01-13 05:26:49+00",val:17.4527246179904),(ts:"2020-01-17 06:52:46+00",val:-19.59155342245161),(ts:"2020-01-21 12:43:25+00",val:18.586476656935602),(ts:"2020-01-24 09:45:35+00",val:-17.787766631363837),(ts:"2020-01-30 14:00:56+00",val:-15.147139203422384),(ts:"2020-01-30 23:50:41+00",val:10.993553071510647)],null_val:[0,0])
```

## Current Pipeline Elements(A-Z) <a id="timevector-pipeline-elements"></a>

As of the current timescale release, these elements are all [experimental](/docs/README.md#tag-notes).


> - [delta](#timevector_pipeline_delta)
> - [lttb](#timevector_pipeline_lttb)
> - [sort](#sort)


---

## **delta** <a id="timevector_pipeline_delta"></a>
```SQL ,ignore
delta(
) RETURNS TimevectorPipelineElement
```

This element will return a new timevector where each point is the difference between the current and preceeding value in the input timevector.  The new series will be one point shorter as it will not have a preceding value to return a delta for the first point.

### Required Arguments <a id="timevector_pipeline_delta-arguments"></a>
|Name| Type |Description|
|---|---|---|
<br>

### Pipeline Execution Returns <a id="timevector_pipeline_delta-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timevector` | `Timevector` | The result of applying this pipeline element will be a new time series where each point contains the difference in values from the prior point in the input timevector. |
<br>

### Sample Usage <a id="timevector_pipeline_delta-examples"></a>
```SQL
SELECT time, value
FROM unnest(
    (SELECT timevector('2020-01-01'::timestamptz + step * '1 day'::interval, step * step)
        -> toolkit_experimental.delta()
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

## **lttb** <a id="timevector_pipeline_lttb"></a>
```SQL ,ignore
lttb(
    resolution int,
) RETURNS TimevectorPipelineElement
```

This element will return a [largest triangle three buckets](lttb.md#description) approximation of a given timevector.  Its behavior is the same as the lttb function documented [here](lttb.md#lttb), save that it expects the series to be sorted.

```SQL ,ignore
SELECT lttb(time, value, 40) FROM data;
```
is equivalent to
```SQL ,ignore
SELECT timevector(time, value) -> sort() -> lttb() FROM data;
```

### Required Arguments <a id="timevector_pipeline_lttb-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `resolution` | `INTEGER` | Number of points the output should have. |
<br>

### Pipeline Execution Returns <a id="timevector_pipeline_lttb-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timevector` | `Timevector` | The result of applying this pipeline element will be a new timevector with `resolution` point that is visually similar to the input series. |
<br>

### Sample Usage <a id="timevector_pipeline_lttb-examples"></a>
```SQL
SELECT time, value
FROM unnest(
    (SELECT timevector('2020-01-01 UTC'::TIMESTAMPTZ + make_interval(days=>(foo*10)::int), 10 + 5 * cos(foo))
        -> toolkit_experimental.lttb(4)
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

## **sort** <a id="timevector_pipeline_sort"></a>
```SQL ,ignore
sort(
) RETURNS TimevectorPipelineElement
```

This element takes in a timevector and returns a timevector consisting of the same points, but in order of increasing time values.

### Required Arguments <a id="timevector_pipeline_sort-arguments"></a>
|Name| Type |Description|
|---|---|---|
<br>

### Pipeline Execution Returns <a id="timevector_pipeline_sort-returns"></a>

|Column|Type|Description|
|---|---|---|
| `timevector` | `Timevector` | The result of applying this pipeline element will be a time sorted version of the incoming timevector. |
<br>

### Sample Usage <a id="timevector_pipeline_sort-examples"></a>
```SQL
SELECT time, value
FROM unnest(
    (SELECT timevector('2020-01-06'::timestamptz - step * '1 day'::interval, step * step)
        -> toolkit_experimental.sort()
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
