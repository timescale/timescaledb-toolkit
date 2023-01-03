CREATE EXTENSION timescaledb;
CREATE EXTENSION timescaledb_toolkit;
SET SESSION TIMEZONE TO 'UTC';

-- utility for generating random numbers
CREATE SEQUENCE rand START 567;
CREATE FUNCTION test_random() RETURNS float AS
    'SELECT ((nextval(''rand'')*34567)%1000)::float/1000'
LANGUAGE SQL;
