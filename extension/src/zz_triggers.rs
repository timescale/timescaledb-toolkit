use pgx::*;

/// nop function that forces the extension binary to be loaded, this ensures
/// that if a user sees the error message the guc will in fact be active
#[pg_extern]
fn timescaledb_toolkit_probe() {

}

extension_sql!(r#"
GRANT USAGE ON SCHEMA toolkit_experimental TO PUBLIC;
CREATE OR REPLACE FUNCTION disallow_experimental_dependencies()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
DECLARE
  guc_set TEXT;
  experimental_schema_id oid;
BEGIN

  guc_set := current_setting('timescaledb_toolkit_acknowledge_auto_drop', true);
  IF guc_set IS NOT NULL AND guc_set = 'on' THEN
    RETURN;
  END IF;

  SELECT oid schema_oid
  INTO experimental_schema_id
  FROM pg_catalog.pg_namespace
  WHERE nspname='toolkit_experimental'
  LIMIT 1;

  IF EXISTS (
    SELECT top_dep.objid, top_dep.deptype, top_dep.refobjid
    FROM pg_catalog.pg_depend top_dep
    WHERE top_dep.refobjid=experimental_schema_id
    AND top_dep.objid IN (
      SELECT depend.refobjid dep_id
      FROM pg_catalog.pg_depend depend
      INNER JOIN (
        SELECT obj.objid id
        FROM pg_event_trigger_ddl_commands() as obj
        WHERE NOT obj.in_extension
      ) created ON created.id = depend.objid))
  THEN
    PERFORM timescaledb_toolkit_probe();
    RAISE EXCEPTION 'features in toolkit_experimental are unstable, and objects depending on them will be deleted on extension update (there will be a DROP SCHEMA toolkit_experimental CASCADE), which on Cloud can happen at any time.'
      USING DETAIL='If you really want to do this, and are willing to accept the possibility that objects so created may be deleted without warning, set timescaledb_toolkit_acknowledge_auto_drop to ''true''.';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION disallow_experimental_view_dependencies()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
  guc_set TEXT;
  experimental_schema_id oid;
BEGIN

  guc_set := current_setting('timescaledb_toolkit_acknowledge_auto_drop', true);
  IF guc_set IS NOT NULL AND guc_set = 'on' THEN
    RETURN;
  END IF;

  SELECT oid schema_oid
  INTO experimental_schema_id
  FROM pg_catalog.pg_namespace
  WHERE nspname='toolkit_experimental'
  LIMIT 1;

  -- views do not depend directly on objects, instead the rewrite rule depends
  -- on both the view, and the dependencies, so check for that
  IF EXISTS (
    SELECT top_dep.objid, top_dep.deptype, top_dep.refobjid
    FROM pg_catalog.pg_depend top_dep
    WHERE top_dep.refobjid=experimental_schema_id
    AND top_dep.objid IN (
      SELECT depend2.refobjid dep_id
      FROM pg_catalog.pg_depend depend
      INNER JOIN (
        SELECT obj.objid id
        FROM pg_event_trigger_ddl_commands() as obj
        WHERE NOT obj.in_extension
      ) created ON created.id = depend.refobjid
      INNER JOIN pg_catalog.pg_depend depend2 ON depend2.objid = depend.objid)
    )
  THEN
    PERFORM timescaledb_toolkit_probe();
    RAISE EXCEPTION 'features in toolkit_experimental are unstable, and objects depending on them will be deleted on extension update (there will be a DROP SCHEMA toolkit_experimental CASCADE), which on Cloud can happen at any time.'
        USING DETAIL='If you really want to do this, and are willing to accept the possibility that objects so created may be deleted without warning, set timescaledb_toolkit_acknowledge_auto_drop to ''true''.';
  END IF;
END;
$$;

CREATE EVENT TRIGGER disallow_experimental_deps ON ddl_command_end
  WHEN tag IN ('CREATE AGGREGATE', 'CREATE CAST',
    'CREATE FUNCTION', 'CREATE INDEX', 'CREATE MATERIALIZED VIEW',
    'CREATE OPERATOR', 'CREATE PROCEDURE', 'CREATE TABLE', 'CREATE TABLE AS',
    'CREATE TRIGGER', 'CREATE TYPE', 'CREATE VIEW')
  EXECUTE FUNCTION disallow_experimental_dependencies();

CREATE EVENT TRIGGER disallow_experimental_dependencies_on_views ON ddl_command_end
  WHEN tag IN ('CREATE MATERIALIZED VIEW', 'CREATE VIEW')
  EXECUTE FUNCTION disallow_experimental_view_dependencies();
"#,
name = "warning_trigger",
finalize,
);
