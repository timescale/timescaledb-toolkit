# Statistical Aggregates

## Common 1-D Statistical Functions
- `average`
- `sum`
- `num_vals`
- `stddev`(population and sample)
- `variance` (population and sample )

## 2-D Statistical Regression Functions
- `slope`
- `intercept`
- `x_intercept`
- `corr` (correlation coefficient)
- `covariance` (population  and sample)
- `determination_coeff`

In order to make common statistical aggregates easier to work with in window functions and continuous aggregates, Toolkit provides common statistical aggregates in a slightly different form than  otherwise available in PostgreSQL/TimescaleDB. They are re-implemented within the [two-step aggregates framework](docs/two-step_aggregation.md)which exposes a summary form to the user which can then have multiple accessors. 

```SQL, non-transactional
CREATE TABLE foo (
    t timestamptz,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION
);
```

In order to run any of these statistical functions you must first perform the `stats_agg` aggregate with either one or two variables, following the general SQL framework for these things, when being used for statistical regression with two dimensions, the dependent variable comes first and the independent variable second, ie:

```SQL, ignore-output
SELECT toolkit_experimental.stats_agg(y, x) FROM foo;
```

As with other aggregates in the Toolkit, you can use any of the accessors on the results of the aggregation, so: 

```SQL, ignore-output
SELECT toolkit_experimental.average(
    toolkit_experimental.stats_agg(x)
) FROM foo;
```
will give you the average of column `x`. While this is slightly more complex for the simple case, many of the results of these aggregates are not combinable in their final forms, the output of the `stats_agg` aggregate is combinable, which means we can do tumbling window aggregates with them and re-combine them when they are used in continuous aggregates. 

In the 2-D case, you can access single variable statistics by calling the function with `_x` or `_y` like so:

```SQL, ignore-output
SELECT toolkit_experimental.average_x(
    toolkit_experimental.stats_agg(y, x)
) FROM foo;
```

Statistics involving both variables (the ones only available in the 2-D case) are called normally:
```SQL, ignore-output
SELECT toolkit_experimental.slope(
    toolkit_experimental.stats_agg(y, x)
) FROM foo;
```

For those statistics which have variants for either the sample or population we have made these accessible via a separate variable ie:

```SQL, ignore-output
SELECT toolkit_experimental.covariance(
    toolkit_experimental.stats_agg(y, x),
    'population'
) FROM foo;
```

The default for all of these is 'population' (the abbreviations 'pop' and 'samp' are also acceptable). The default means the function may also be called without the second argument, like so:

```SQL, ignore-output
SELECT toolkit_experimental.covariance(
    toolkit_experimental.stats_agg(y, x)
) FROM foo;
```

Which will still return the population covariance.


This is a minimum working version of the documentation for now, another working document can be found [here](docs/rolling_average_api_working.md), which goes into the window function usecase and some of the reasoning behind our naming decisions. Please feel free to open issues or discussions if you have questions or comments on the current API. We will further develop the documentation as we stabilize these functions over the coming releases. 


