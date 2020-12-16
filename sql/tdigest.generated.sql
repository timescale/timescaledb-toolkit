CREATE TYPE tstdigest;
CREATE OR REPLACE FUNCTION tstdigest_in(cstring) RETURNS tstdigest IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'tstdigest_in_wrapper';
CREATE OR REPLACE FUNCTION tstdigest_out(tstdigest) RETURNS cstring IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'tstdigest_out_wrapper';
CREATE TYPE tstdigest (
                                INTERNALLENGTH = variable,
                                INPUT = tstdigest_in,
                                OUTPUT = tstdigest_out,
                                STORAGE = extended
                            );
-- ./src/tdigest.rs:47:0
CREATE OR REPLACE FUNCTION "tdigest_trans"("state" internal, "size" int, "value" double precision) RETURNS internal LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_trans_wrapper';
-- ./src/tdigest.rs:78:0
CREATE OR REPLACE FUNCTION "tdigest_final"("state" internal) RETURNS tsTDigest LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_final_wrapper';
-- ./src/tdigest.rs:101:0
CREATE OR REPLACE FUNCTION "tdigest_combine"("state1" internal, "state2" internal) RETURNS internal LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_combine_wrapper';
-- ./src/tdigest.rs:142:0
CREATE OR REPLACE FUNCTION "tdigest_serialize"("state" internal) RETURNS bytea STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_serialize_wrapper';
-- ./src/tdigest.rs:160:0
CREATE OR REPLACE FUNCTION "tdigest_deserialize"("bytes" bytea, "_internal" internal) RETURNS internal LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_deserialize_wrapper';
-- ./src/tdigest.rs:218:0
CREATE OR REPLACE FUNCTION "tdigest_quantile"("digest" tsTDigest, "quantile" double precision) RETURNS double precision STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_quantile_wrapper';
-- ./src/tdigest.rs:228:0
CREATE OR REPLACE FUNCTION "tdigest_quantile_at_value"("digest" tsTDigest, "value" double precision) RETURNS double precision STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_quantile_at_value_wrapper';
-- ./src/tdigest.rs:238:0
CREATE OR REPLACE FUNCTION "tdigest_count"("digest" tsTDigest) RETURNS double precision STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_count_wrapper';
-- ./src/tdigest.rs:246:0
CREATE OR REPLACE FUNCTION "tdigest_min"("digest" tsTDigest) RETURNS double precision STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_min_wrapper';
-- ./src/tdigest.rs:254:0
CREATE OR REPLACE FUNCTION "tdigest_max"("digest" tsTDigest) RETURNS double precision STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_max_wrapper';
-- ./src/tdigest.rs:262:0
CREATE OR REPLACE FUNCTION "tdigest_mean"("digest" tsTDigest) RETURNS double precision STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_mean_wrapper';
-- ./src/tdigest.rs:274:0
CREATE OR REPLACE FUNCTION "tdigest_sum"("digest" tsTDigest) RETURNS double precision STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'tdigest_sum_wrapper';
-- ./src/tdigest.rs:282:0
CREATE OR REPLACE FUNCTION "hello_statscale"() RETURNS text STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'hello_statscale_wrapper';
CREATE SCHEMA IF NOT EXISTS "tests";
-- ./src/tdigest.rs:296:4
CREATE OR REPLACE FUNCTION tests."test_hello_statscale"() RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'test_hello_statscale_wrapper';
-- ./src/tdigest.rs:301:4
CREATE OR REPLACE FUNCTION tests."test_aggregate"() RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'test_aggregate_wrapper';
