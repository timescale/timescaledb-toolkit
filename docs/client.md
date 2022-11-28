# Client-side aggregation [<sup><mark>experimental</mark></sup>](/docs/README.md#tag-notes)

- Current status: prototype
- Effort remaining: lots

## Purpose

We have long suspected it might be valuable to allow building aggregates
client-side rather than requiring all data be stored in postgres and
aggregated within the toolkit.

https://github.com/timescale/timescaledb-toolkit/issues/485 recently came in
adding weight to this idea.  Because this customer requests tdigest, that's
what we'll use for prototyping.

## Use cases

Quoting the above customer:

"In some cases it is not possible to transfer all the non-aggregated data to
TimescaleDB due to it's amount and/or limited connectivity."

## Questions

- Do we want to support a public crate?
  - What does that mean?
  - Do we need to monitor an email address?
  - What promise would we make on response time?
  - Is this materially different from what we've already signed up for by
    publishing on github?
  - How do we handle ownership of the crates.io credentials?

- Which license do we use?
  - Some of our code is already a derived work - do we permissively license it
    all, or restrict some of it?

- Wire protocol maintenance
  - This is a problem we already have, we just didn't realize it, as it is
    already possible to construct our aggregates and INSERT them, and they
    also in pg dumps; at the moment, you can restore those dumps, though we
    haven't made any promise about it.  On our stabilized aggregates, users
    may assume that is stabilized, too.
  - Is there a practical concern here?  Or do we just say "not supported"?
  - Is it possible to crash the extension with invalid inputs?
  - If we commit to a public wire protocol, shouldn't we avoid the
    Rust-specific ron and go for something more common?

## Proposal

As a first step, build a crate which externalizes tdigest aggregate creation.

```rust
let mut digester = tdigest::Builder::with_size(N);
loop {
    digester.push(value);
}
send_to_postgres(format!("INSERT INTO digests VALUES ({})", digester.build().format_for_postgres()));
```

In order to provide that API, we must first reorganize the tdigest
implementation so that all business logic is in the tdigest crate.  Some is
currently in the pgx extension crate.

For each aggregate, the transient state is actually a Builder pattern hidden
hidden behind pgx machinery.

On this branch, I've moved TDigestTransState into tdigest::Builder.

Currently, we use default ron behavior to serialize the raw implementation
details of the pg_type .  Users can insert inconsistent data now, and it
doesn't look like we validate that at insertion time.

We should reconsider this for all pg_types regardless of the overall client
project.  Is it possible NOT to offer serialized insertion at all?  If so,
turning that off would be a good first step.

Then we can enable it just where we want to.

We should put more thought into the serialization format we intentionally
support.  Currently it contains redundancy which we can eliminate by
implementing serialization carefully rather than relying on defaults.

## Proof of concept

This is a simple demonstration of inserting serialized tdigest into a table,
showing that it works the same way as an aggregate built by the extension.

```SQL ,non-transactional
CREATE TABLE test (data DOUBLE PRECISION);
INSERT INTO test SELECT generate_series(0.01, 1, 0.01);

CREATE VIEW digest AS SELECT tdigest(100, data) FROM test;

CREATE TABLE digest2 (tdigest tdigest);
INSERT INTO digest2 VALUES ('(version:1,max_buckets:100,count:100,sum:50.50000000000001,min:0.01,max:1,centroids:[(mean:0.01,weight:1),(mean:0.02,weight:1),(mean:0.03,weight:1),(mean:0.04,weight:1),(mean:0.05,weight:1),(mean:0.06,weight:1),(mean:0.07,weight:1),(mean:0.08,weight:1),(mean:0.09,weight:1),(mean:0.1,weight:1),(mean:0.11,weight:1),(mean:0.12,weight:1),(mean:0.13,weight:1),(mean:0.14,weight:1),(mean:0.15,weight:1),(mean:0.16,weight:1),(mean:0.17,weight:1),(mean:0.18,weight:1),(mean:0.19,weight:1),(mean:0.2,weight:1),(mean:0.21,weight:1),(mean:0.22,weight:1),(mean:0.23,weight:1),(mean:0.24,weight:1),(mean:0.25,weight:1),(mean:0.26,weight:1),(mean:0.27,weight:1),(mean:0.28,weight:1),(mean:0.29,weight:1),(mean:0.3,weight:1),(mean:0.31,weight:1),(mean:0.32,weight:1),(mean:0.33,weight:1),(mean:0.34,weight:1),(mean:0.35,weight:1),(mean:0.36,weight:1),(mean:0.37,weight:1),(mean:0.38,weight:1),(mean:0.39,weight:1),(mean:0.4,weight:1),(mean:0.41,weight:1),(mean:0.42,weight:1),(mean:0.43,weight:1),(mean:0.44,weight:1),(mean:0.45,weight:1),(mean:0.46,weight:1),(mean:0.47,weight:1),(mean:0.48,weight:1),(mean:0.49,weight:1),(mean:0.5,weight:1),(mean:0.51,weight:1),(mean:0.525,weight:2),(mean:0.545,weight:2),(mean:0.565,weight:2),(mean:0.585,weight:2),(mean:0.605,weight:2),(mean:0.625,weight:2),(mean:0.64,weight:1),(mean:0.655,weight:2),(mean:0.675,weight:2),(mean:0.69,weight:1),(mean:0.705,weight:2),(mean:0.72,weight:1),(mean:0.735,weight:2),(mean:0.75,weight:1),(mean:0.76,weight:1),(mean:0.775,weight:2),(mean:0.79,weight:1),(mean:0.8,weight:1),(mean:0.815,weight:2),(mean:0.83,weight:1),(mean:0.84,weight:1),(mean:0.85,weight:1),(mean:0.86,weight:1),(mean:0.87,weight:1),(mean:0.88,weight:1),(mean:0.89,weight:1),(mean:0.9,weight:1),(mean:0.91,weight:1),(mean:0.92,weight:1),(mean:0.93,weight:1),(mean:0.94,weight:1),(mean:0.95,weight:1),(mean:0.96,weight:1),(mean:0.97,weight:1),(mean:0.98,weight:1),(mean:0.99,weight:1),(mean:1,weight:1)])');
```

```SQL
SELECT
                    min_val(tdigest),
                    max_val(tdigest),
                    num_vals(tdigest)
                    FROM digest;
```
```output
 min_val | max_val | num_vals
---------+---------+----------
    0.01 |       1 |      100
```

Inserting serialized tdigest into table behaves the same:

```SQL
SELECT
                    min_val(tdigest),
                    max_val(tdigest),
                    num_vals(tdigest)
                    FROM digest2;
```
```output
 min_val | max_val | num_vals
---------+---------+----------
    0.01 |       1 |      100
```
