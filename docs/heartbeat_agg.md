# Heartbeat Aggregation

## Description

TimescaleDB Toolkit provides the `heartbeat_agg` aggregate to evaluate and track the liveness state of an underlying system based on a set of heartbeat timestamps.

The aggregate constructs a timeline by trating each heartbeat as "live" for a specified duration following the timestamp. Any point in the specified overall time range that does not closely follow a heartbeat is considered "dead" (downtime). This is particularly useful for tracking system health or finding exact intervals where a device went offline. 

## Details

Timescale's `heartbeat_agg` is implemented as an aggregate function that takes incoming heartbeat timestamps and groups them into contiguous live intervals.

It requires to specify an aggregation window (`agg_start` and `agg_duration`). All heartbeat points passed to the aggregate must occur strictly withins this bounding interval. The state collects these points and processes them in batches, combining overlapping liveness intervals into a consolidated timeline of up and down states.

## Usage Example


## API