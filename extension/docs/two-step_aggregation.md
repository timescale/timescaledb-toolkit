# Two-Step Aggregation - What It Is and Why We Use It

## What is a Two-Step Aggregate <a id="two-step-description"></a>
You may have noticed that many of our aggregate functions have two parts to them; first an aggregation step and then second an accessor. For instance:

```SQL , ignore
SELECT average(time_weight('LOCF', value)) as time_weighted_average FROM foo;
-- or
SELECT approx_percentile(0.5, percentile_agg(value)) as median FROM bar;
```

In each case there is an inner aggregate function (`time_weight` / `percentile_agg`) and an outer call to an accessor function (`average` / `approx_percentile`). We use this calling convention in multiple places throughout the Timescale Analytics project. 

The inner aggregate call creates a machine-readable partial form that can be used for multiple purposes. The two-step calling convention is slightly longer than a hypothetical one-step one where we just called `time_weighted_average('LOCF', value)` or `percentile_agg(0.5, val)` directly (these functions don't exist, don't try to use them).

While the one-step calling convention is easier for the simple case, it becomes much more difficult and hard to reason about for slightly more complex use-cases detailed in the next section. We wanted the calling convention to remain consistent and easy to reason about so you can take advantage of the same functions even as you start doing more complicated analyses.  This also to keeps the docs consistent and prevents adding special cases everywhere. 

## Why We Use Two-Step Aggregates <a id="two-step-philosophy"></a>
Interestingly, almost all Postgres aggregates do a version of this [under the hood already](https://www.postgresql.org/docs/current/xaggr.html), where they have an internal state used for aggregation and then a final function that displays the output to the user. 

So why do we make this calling convention explicit? 

1) It allows different accessor function calls to use the same internal state and not redo work. 
2) It cleanly distinguishes the parameters that affect the aggregate and those that only affect the accessor.
3) It makes it explicit how and when aggregates can be re-aggregated or "stacked" on themselves with logically consistent results. This also helps them better integrate with [continuous aggregates](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates).
4) It allows for better retrospective analysis of downsampled data in [continuous aggregates](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates). 

That might have been gibberish to some, so let's unpack it a bit. 

### Accessor functions with additional parameters <a id="philosophy-accessor-funcs"></a>
The way the optimizer works, if you run an aggregate like:
```SQL , ignore
SELECT avg(val), sum(val), count(val) FROM foo;
```
The internal state of the `avg` is actually the `sum` and the `count` and it just returns `sum / count` in the final step of the aggregate. The optimizer knows, when these functions are used, that it doesn't need to run separate aggregates for each, it can use the same internal function and extract the results it needs. This is great! It can save a lot of work. The problem comes when we do something like `percentile_agg` where we have multiple `approx_percentiles` ie:

```SQL , ignore
SELECT 
    approx_percentile(0.1, percentile_agg(val)) as p10, 
    approx_percentile(0.5, percentile_agg(val)) as p50, 
    approx_percentile(0.9, percentile_agg(val)) as p90 
FROM foo;
```
Because the aggregate step is the same for all three of the calls, the optimizer can combine all the calls, or I can do so explicitly:

```SQL , ignore
WITH pct as (SELECT percentile_agg(val) as approx FROM foo)
SELECT 
    approx_percentile(0.1, approx) as p10, 
    approx_percentile(0.5, approx) as p50, 
    approx_percentile(0.9, approx) as p90 
FROM pct;
```
But the work done in each case will be the same.

If we were to use the one-step calling convention, the extra input of the percentile we're trying to extract would comletely confuse the optimizer, and it would have to redo all the calculation inside the aggregate for each of the values you wanted to extract.

So, if it were framed like this: 
```SQL , ignore
-- NB: THIS IS AN EXAMPLE OF AN API WE DECIDED NOT TO USE, IT DOES NOT WORK
SELECT 
    approx_percentile(0.1, val) as p10, 
    approx_percentile(0.5, val) as p50, 
    approx_percentile(0.9, val) as p90 
FROM foo;
```
the optimizer would be forced to build up the necessary internal state three times rather than just once.

This is even more apparent when you want to use multiple accessor functions, which may have different numbers or types of inputs:

```SQL , ignore
SELECT 
    approx_percentile(0.1, percentile_agg(val)) as p10, 
    approx_percentile(0.5, percentile_agg(val)) as p50, 
    approx_percentile(0.9, percentile_agg(val)) as p90, 
    error(percentile_agg(val)), 
    approx_percentile_rank(10000, percentile_agg(val)) as percentile_at_threshold
FROM foo;
```
The optimizer can easily optimize away the redundant `percentile_agg(val)` calls, but would have much more trouble in the one-step approach.

### Explicit association of parameters with either the aggregation or access step <a id="philosophy-explicit-association"></a>
This leads us to our second benefit of the two-step approach. A number of our accessor functions (both completed and planned) take inputs that don't affect how we aggregate the underlying data, but do affect how we extract data from the computed aggregate. If we combine everything into one function, it makes it less clear which is which. 

Now, our `percentile_agg` implementation uses the `uddsketch` algorithm under the hood and has some default values for parameters, namely the number of buckets it stores and the target error, but there are cases where we might want to use the full algorithm with custom parameters like so: 
```SQL , ignore
SELECT
    approx_percentile(0.5, uddsketch(1000, 0.001, val)) as median, -- 1000 buckets, 0.001 relative error target
    approx_percentile(0.9, uddsketch(1000, 0.001, val)) as p90, 
    approx_percentile(0.5, uddsketch(100, 0.01, val)) as less_accurate_median -- modify the terms for the aggregate get a new approximation
FROM foo;
```
Here we can see which parameters are for the `uddsketch` aggregate (the number of buckets and the target error), and which arguments are for`approx_percentile` (the approx_percentile we want to extract). The optimizer will correctly combine the calls for the first two `uddsketch` calls but not for the third. It is also more clear to the user what is going on, and that I can't set my target error at read time, but rather only at calculation time (this is especially helpful for understanding the behavior of [continuous aggregates](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates)). 

Combining all of these into one function, so we can use the one-step approach, can get unwieldy and unclear very quickly (ie imagine something like `approx_percentile_uddsketch(0.5, 1000, 0.001)`).
<br>
### Stacked aggregates and [continuous aggregate](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates) integration <a id="philosophy-reagg"></a>
Aggregates can be divided into two classes: ones that are "stackable" in their final form and ones that are not.
What I'm calling stackable aggregates are ones like `sum`, `min`, `max` etc. that can be re-aggregated on themselves at different groupings without losing their meaning, ie:

```SQL , ignore
SELECT sum(val) FROM foo;
-- is equivalent to:
SELECT sum(sum) 
FROM 
    (SELECT id, sum(val) 
    FROM foo
    GROUP BY id) s
```

A non-stackable aggregate like `avg` doesn't have this property:
```SQL , ignore
SELECT avg(val) FROM foo;
-- is NOT equivalent to:
SELECT avg(avg) 
FROM 
    (SELECT id, avg(val) 
    FROM foo
    GROUP BY id) s;
```

Or to say it more succinctly: the `sum` of a `sum` is the `sum` but the `avg` of an `avg` is not the `avg`. This is the difference between stackable and non-stackable aggregates.

This is not to say that the `avg` of an `avg` is not a useful piece of information, it can be in some cases, but it isn't always what you want and it can be difficult to actually get the true value for non-stackable aggregates, for instance, for `avg` we can take the `count` and `sum` and divide the `sum` by the `count`, but for many aggregates this is not so obvious and for something like `percentile_agg` __LINK__ with a one-step aggregate, the user would simply have to re-implement most of the algorithm in SQL in order to get the result they want. 

Two-step aggregates expose the internal, re-aggregateable form to the user so they can much more easily do this work, so we've tried to provide two-step aggregates wherever we can. This is especially useful for working with [continuous aggregates](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates), so if I create a continuous aggregate like so:

```SQL , ignore
CREATE MATERIALIZED VIEW foo_15
WITH (timescaledb.continuous)
AS SELECT id,
    time_bucket('15 min'::interval, ts) as bucket,
    sum(val),
    percentile_agg(val)
FROM foo
GROUP BY id, time_bucket('15 min'::interval, ts);
```

And I want to do a second level of aggregation, say over a day, I can do it over the resulting aggregate with the `percentile_agg` function:
```SQL , ignore
SELECT id, time_bucket('1 day'::interval, bucket) as bucket, 
    sum(sum),
    approx_percentile(percentile_agg(percentile_agg), 0.5) as median
FROM foo_15
GROUP BY id, time_bucket('1 day'::interval, bucket)
```


##### NB: There are some two-step aggregates like `tdigest` __ADD LINK? and expose and other bits...__ when we document that function where two-step aggregation can lead to more error or different results, because the algorithm is not deterministic in its re-aggregation, but we will note that clearly in the documentation when that happens, it is unusual.

### Retrospective analysis over downsampled data <a id="philosophy-retro"></a>
[Continuous aggregates](https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates) (or separate aggregation tables powered by a cron job or [user-defined action]( __LINK__ ) ) aren't just used for speeding up queries, they're also used for [data retention]( __LINK__ ). But this can mean that they are very difficult to modify as your data ages. Unfortunately this is also when you are learning more things about the analysis you want to do on your data. By keeping them in their raw aggregate form, the user has the flexibility to apply different accessors to do retrospective analysis. With a one-step aggregate the user needs to determine, say, which percentiles are important when we create the continous aggregate, with a two-step aggregate the user can simply determine they're going to want an approximate percentile, and then determine when doing the analysis whether they want the median, the 90th, 95th or 1st percentile. No need to modify the aggregate or try to re-calculate from data that may no longer exist in the system. 
