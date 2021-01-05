CREATE TYPE TimescaleTDigest;

CREATE OR REPLACE FUNCTION TimescaleTDigest_in(cstring) RETURNS TimescaleTDigest IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'timescaletdigest_in_wrapper';
CREATE OR REPLACE FUNCTION TimescaleTDigest_out(TimescaleTDigest) RETURNS CString IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'timescaletdigest_out_wrapper';

CREATE TYPE TimescaleTDigest (
    INTERNALLENGTH = variable,
    INPUT = TimescaleTDigest_in,
    OUTPUT = TimescaleTDigest_out,
    STORAGE = extended
);

CREATE TYPE Hyperloglog;

CREATE OR REPLACE FUNCTION Hyperloglog_in(cstring) RETURNS Hyperloglog IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'hyperloglog_in_wrapper';
CREATE OR REPLACE FUNCTION Hyperloglog_out(Hyperloglog) RETURNS CString IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'hyperloglog_out_wrapper';

CREATE TYPE Hyperloglog (
    INTERNALLENGTH = variable,
    INPUT = Hyperloglog_in,
    OUTPUT = Hyperloglog_out,
    STORAGE = extended
);