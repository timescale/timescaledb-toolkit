CREATE AGGREGATE t_digest(size int, value DOUBLE PRECISION)
(
    sfunc=tdigest_trans,
    stype=internal,
    finalfunc=tdigest_final,
    combinefunc=tdigest_combine,
    serialfunc = tdigest_serialize,
    deserialfunc = tdigest_deserialize
);

CREATE AGGREGATE hyperloglog(size int, value AnyElement)
(
    stype = internal,
    sfunc=hyperloglog_trans,
    finalfunc = hyperloglog_final,
    combinefunc = hyperloglog_combine,
    serialfunc = hyperloglog_serialize,
    deserialfunc = hyperloglog_deserialize
);
