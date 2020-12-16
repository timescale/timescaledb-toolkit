CREATE AGGREGATE t_digest(size int, value DOUBLE PRECISION)
(
    sfunc=tdigest_trans,
    stype=internal,
    finalfunc=tdigest_final,
    combinefunc=tdigest_combine,
    serialfunc = tdigest_serialize,
    deserialfunc = tdigest_deserialize
)
