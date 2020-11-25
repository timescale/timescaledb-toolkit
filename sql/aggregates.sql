CREATE AGGREGATE approx_median(size int, value DOUBLE PRECISION)
(
    sfunc=tdigest_trans,
    stype=internal,
    finalfunc=tdigest_final,
    serialfunc = tdigest_serialize,
    deserialfunc = tdigest_deserialize
)
