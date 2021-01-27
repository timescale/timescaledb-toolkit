# T-Digest API <sup><mark>experimental</mark></sup>

>:TOPLIST:
> ### Command List (A-Z)
> - [t_digest](#t_digest)
> - [tdigest_count](#tdigest_count)
> - [tdigest_max](#tdigest_max)
> - [tdigest_mean](#tdigest_mean)
> - [tdigest_min](#tdigest_min)
> - [tdigest_quantile](#tdigest_quantile)
> - [tdigest_quantile_at_value](#tdigest_quantile_at_value)
> - [tdigest_sum](#tdigest_sum)


## Digest management [](tdigest-api-management)

## t_digest() [](t_digest)

This is a Postgresql custom aggregate containing a digest of the specified column.

### Required Arguments [](t_digest-required-arguments)
|Name|Description|
|---|---|
| `buckets` | (INTEGER) Number of buckets in the digest.  Increasing this will provide more accurate quantile estimates, but will require more memory.|
| `column_name` | (NAME)  Column to aggregate.|

### Returns

| Column | Description |
|---|---|
| `digest` | (T-DIGEST) A t-digest object which may be passed to other t-digest APIs. |



### Sample Usage [](t_digest-examples)

TODO

## Digest Querying

## tdigest_min [](tdigest_min)

Get the minimum value from a t-digest.

### Required Arguments [](tdigest_min-required-arguments)
|Name|Description|
|---|---|
| `digest` | (T-DIGEST) The digest to extract the min value from. |

### Returns

| Column | Description |
|---|---|
| `min` | (FLOAT) The minimum value entered into the t-digest. |

### Sample Usage [](tdigest_min-examples)

TODO

## tdigest_max [](tdigest_max)

Get the maximum value from a t-digest.

### Required Arguments [](tdigest_max-required-arguments)
|Name|Description|
|---|---|
| `digest` | (T-DIGEST) The digest to extract the max value from. |

### Returns

| Column | Description |
|---|---|
| `max` | (FLOAT) The maximum value entered into the t-digest. |

### Sample Usage [](tdigest_max-examples)

TODO

## tdigest_count [](tdigest_count)

Get the number of values contained in a t-digest.

### Required Arguments [](tdigest_count-required-arguments)
|Name|Description|
|---|---|
| `digest` | (T-DIGEST) The digest to extract the number of values from. |

### Returns

| Column | Description |
|---|---|
| `count` | (FLOAT) The number of values entered into the t-digest. |

### Sample Usage [](tdigest_count-examples)

TODO

## tdigest_mean [](tdigest_mean)

Get the average of all the values contained in a t-digest.

### Required Arguments [](tdigest_mean-required-arguments)
|Name|Description|
|---|---|
| `digest` | (T-DIGEST) The digest to extract the mean value from. |

### Returns

| Column | Description |
|---|---|
| `mean` | (FLOAT) The average of the values entered into the t-digest. |

### Sample Usage [](tdigest_mean-examples)

TODO

## tdigest_sum [](tdigest_sum)

Get the sum of all the values in a t-digest

### Required Arguments [](tdigest_sum-required-arguments)
|Name|Description|
|---|---|
| `digest` | (T-DIGEST) The digest to compute the sum on. |

### Returns

| Column | Description |
|---|---|
| `sum` | (FLOAT) The sum of the values entered into the t-digest. |

### Sample Usage [](tdigest_sum-examples)

TODO

## tdigest_quantile [](tdigest_quantile)

Get an approximate quantile from a t-digest

### Required Arguments [](tdigest_quantile-required-arguments)
|Name|Description|
|---|---|
| `digest` | (T-DIGEST) The digest to compute the quantile on. |
| `quantile` | (FLOAT) The desired quantile (0.0-1.0) to approximate. |

### Returns

| Column | Description |
|---|---|
| `quantile` | (FLOAT) The estimated value at the requested quantile. |

### Sample Usage [](tdigest_quantile-examples)

TODO

## tdigest_quantile_at_value [](tdigest_quantile_at_value)

Estimate what quantile given value would be located at in a t-digest.

### Required Arguments [](tdigest_quantile_at_value-required-arguments)
|Name|Description|
|---|---|
| `digest` | (T-DIGEST) The digest to compute the quantile on. |
| `value` | (FLOAT) The value to estimate the quantile of. |

### Returns

| Column | Description |
|---|---|
| `quantile_at_value` | (FLOAT) The estimated quantile associated with the provided value. |

### Sample Usage [](tdigest_quantile_at_value-examples)

TODO
