# T-digest #

Timescale analytics provides an implementation of the t-digest data structure <link> for quantile approximations.  A t-digest is a space efficient aggregation which provides increased resolution at the edges of the distribution.  This allows for more accurate estimates of extreme quantiles than traditional methods.

## Details ##

Timescale's t-digest is implemented as an aggregate function in PostgreSQL.  They do not support moving-aggregate mode, and are not ordered-set aggregates.  Presently they are restricted to float values, but the goal is to make them polymorphic.  They are partializable and are good candidates for continuous aggregation <link>.

## Creation of t-digests ##

T-digest creation is similar to creating other aggregates in Postgresql.  The only additional bit of information needed is a digest size, which is used to increase the resolution of the digest.  Increasing this value will provide more accurate estimates, at the cost of having a larger digest.

    SELECT t_digest(<digest size>, <column>) FROM <table> 

See the [API documentation](/extension/docs/tdigest_api.md#t_digest) for more detail.

## Using t-digest objects in PostgresQL ##

Once you have a t-digest, you can use it to generate estimates for different quantiles.

    SELECT tdigest_quantile(<digest>, <target quantiles>)

You can also perform the inverse operation and approximate what quantile a particular value would appear at.

    SELECT tdigest_quantile_at_value(<digest>, <quantile>)

Beyond this, you can also get exact min, max, count, sum, and mean from a t-digest.  The functions to access these are tdigest_min, tdigest_max, tdigest_count, tdigest_sum, and tdigest_mean.  They all just take the digest as an argument.

    SELECT tdigest_min(<digest>)

Example usage:

Given a table `test` which has some column of double precision values `data`, the following example shows a t-digest in use.


    CREATE VIEW digest AS SELECT t_digest(100, data) FROM test;
    SELECT tdigest_min(t_digest) AS min_val, tdigest_max(t_digest) AS max_val, tdigest_mean(t_digest) as mean_val FROM digest;
    SELECT tdigest_quantile(t_digest, 0.01) AS one_pecentile, tdigest_quantile(t_digest, 0.1) AS tenth_pecentile FROM digest;
    ˇSELECT tdigest_quantile_at_value(t_digest, 10000) AS quantile_for_10k FROM digest;

For a complete list of T-Digest functions and their syntax, see the [API page for T-Digest](/extension/docs/tdigest_api.md).
