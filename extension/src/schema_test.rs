#[cfg(any(test, feature = "pg_test"))]
use pgx::*;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::collections::HashSet;

    use pgx::*;
    use pgx_macros::pg_test;

    #[pg_extern(schema="toolkit_experimental")]
    fn expected_failure() -> i32 { 1 }

    #[pg_test(error = "features in toolkit_experimental are unstable, and objects depending on them will be deleted on extension update (there will be a DROP SCHEMA toolkit_experimental CASCADE), which on Forge can happen at any time.")]
    fn should_fail_blocks_view() {
        Spi::execute(|client| {
            let _ = client.select(
                "CREATE VIEW failed AS SELECT toolkit_experimental.expected_failure();",
               None,
                None);
        })
    }

    // Test that any new features are added to the the experimental schema
    #[pg_test]
    fn test_schema_qualification() {
        Spi::execute(|client| {
            let released_features: HashSet<_> = RELEASED_FEATURES.iter().cloned().collect();
            let unexpected_features: Vec<_> = client
                .select(
                    "SELECT pg_catalog.pg_describe_object(classid, objid, 0) \
                    FROM pg_catalog.pg_extension e, pg_catalog.pg_depend d \
                    WHERE e.extname='timescaledb_toolkit' \
                    AND refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass \
                    AND d.refobjid = e.oid \
                    AND deptype = 'e'
                    ORDER BY 1",
                    None,
                    None,
                ).filter_map(|row| {
                    let val: String = row.by_ordinal(1).unwrap().value().unwrap();

                    if released_features.contains(&*val) {
                        return None
                    }

                    if val.starts_with("schema")
                        && val.strip_prefix("schema ") == Some("toolkit_experimental") {
                        return None
                    }

                    if val.starts_with("schema")
                        && val.strip_prefix("schema ") == Some("tests") {
                        return None
                    }

                    let type_prefix = "type toolkit_experimental.";
                    if val.starts_with(type_prefix)
                        && val.strip_prefix(type_prefix).is_some() {
                            return None
                    }

                    let function_prefix = "function toolkit_experimental.";
                    if val.starts_with(function_prefix)
                        && val.strip_prefix(function_prefix).is_some() {
                            return None
                    }

                    let operator_prefix = "operator toolkit_experimental.";
                    if val.starts_with(operator_prefix)
                        && val.strip_prefix(operator_prefix).is_some() {
                            return None
                    }

                    // Allow all `->` operators for now; it's the accessor that
                    // will be unstable.
                    if val.starts_with("operator ->(") {
                        return None
                    }
                    if val.starts_with("function arrow_") {
                        return None
                    }

                    // Allow all casts now, it's the types that'll be unstable
                    if val.starts_with("cast from toolkit_experimental.") {
                        return None
                    }

                    // ignore the pgx test schema
                    let test_prefix = "function tests.";
                    if val.starts_with(test_prefix)
                        && val.strip_prefix(test_prefix).is_some() {
                            return None
                    }

                    Some(val)
                }).collect();

            if unexpected_features.is_empty() {
                return
            }

            panic!("unexpectedly released features: {:#?}", unexpected_features)
        });
    }

    // list of features that are released and can be in places other than the
    // experimental schema
    // TODO it may pay to auto-discover this list based on the previous version of
    //      the extension, once we have a released extension
    static RELEASED_FEATURES: &[&str] = &[
        "event trigger disallow_experimental_deps",
        "event trigger disallow_experimental_dependencies_on_views",
        "function disallow_experimental_dependencies()",
        "function disallow_experimental_view_dependencies()",
        "function timescaledb_toolkit_probe()",
        "function approx_percentile(double precision,uddsketch)",
        "function approx_percentile_rank(double precision,uddsketch)",
        "function error(uddsketch)",
        "function mean(uddsketch)",
        "function num_vals(uddsketch)",
        "function percentile_agg(double precision)",
        "function percentile_agg(uddsketch)",
        "function percentile_agg_trans(internal,double precision)",
        "function uddsketch(integer,double precision,double precision)",
        "function rollup(uddsketch)",
        "function uddsketch_combine(internal,internal)",
        "function uddsketch_compound_trans(internal,uddsketch)",
        "function uddsketch_deserialize(bytea,internal)",
        "function uddsketch_final(internal)",
        "function uddsketch_in(cstring)",
        "function uddsketch_out(uddsketch)",
        "function uddsketch_serialize(internal)",
        "function uddsketch_trans(internal,integer,double precision,double precision)",
        "type uddsketch",
        "function approx_percentile(double precision,tdigest)",
        "function approx_percentile_rank(double precision,tdigest)",
        "function max_val(tdigest)",
        "function min_val(tdigest)",
        "function mean(tdigest)",
        "function num_vals(tdigest)",
        "function tdigest(integer,double precision)",
        "function rollup(tdigest)",
        "function tdigest_combine(internal,internal)",
        "function tdigest_compound_combine(internal,internal)",
        "function tdigest_compound_deserialize(bytea,internal)",
        "function tdigest_compound_final(internal)",
        "function tdigest_compound_serialize(internal)",
        "function tdigest_compound_trans(internal,tdigest)",
        "function tdigest_deserialize(bytea,internal)",
        "function tdigest_final(internal)",
        "function tdigest_in(cstring)",
        "function tdigest_out(tdigest)",
        "function tdigest_serialize(internal)",
        "function tdigest_trans(internal,integer,double precision)",
        "type tdigest",
        "function average(timeweightsummary)",
        "function time_weight(text,timestamp with time zone,double precision)",
        "function rollup(timeweightsummary)",
        "function time_weight_combine(internal,internal)",
        "function time_weight_final(internal)",
        "function time_weight_summary_trans(internal,timeweightsummary)",
        "function time_weight_trans(internal,text,timestamp with time zone,double precision)",
        "function time_weight_trans_deserialize(bytea,internal)",
        "function time_weight_trans_serialize(internal)",
        "function timeweightsummary_in(cstring)",
        "function timeweightsummary_out(timeweightsummary)",
        "type timeweightsummary",
        "operator ->(toolkit_experimental.timevector,toolkit_experimental.unstabletimevectorpipeline)",
        "operator ->(toolkit_experimental.unstabletimevectorpipeline,toolkit_experimental.unstabletimevectorpipeline)",
        "operator ->>(regproc,regproc)",
        "operator ->>(regproc,toolkit_experimental.unstabletimevectorpipeline)",
        "operator ->>(toolkit_experimental.timevector,regproc)",
        "operator ->>(toolkit_experimental.unstabletimevectorpipeline,regproc)",
        "operator ->(toolkit_experimental.timevector,toolkit_experimental.pipelinethenstatsagg)",
        "operator ->(toolkit_experimental.unstabletimevectorpipeline,toolkit_experimental.pipelinethenstatsagg)",
        "function corr(countersummary)",
        "function counter_agg(timestamp with time zone,double precision)",
        "function counter_agg(timestamp with time zone,double precision,tstzrange)",
        "function counter_agg_combine(internal,internal)",
        "function counter_agg_final(internal)",
        "function counter_agg_summary_trans(internal,countersummary)",
        "function counter_agg_trans(internal,timestamp with time zone,double precision,tstzrange)",
        "function counter_agg_trans_no_bounds(internal,timestamp with time zone,double precision)",
        "function counter_summary_trans_deserialize(bytea,internal)",
        "function counter_summary_trans_serialize(internal)",
        "function counter_zero_time(countersummary)",
        "function countersummary_in(cstring)",
        "function countersummary_out(countersummary)",
        "function delta(countersummary)",
        "function extrapolated_delta(countersummary,text)",
        "function extrapolated_rate(countersummary,text)",
        "function idelta_left(countersummary)",
        "function idelta_right(countersummary)",
        "function intercept(countersummary)",
        "function irate_left(countersummary)",
        "function irate_right(countersummary)",
        "function num_changes(countersummary)",
        "function num_elements(countersummary)",
        "function num_resets(countersummary)",
        "function rate(countersummary)",
        "function rollup(countersummary)",
        "function slope(countersummary)",
        "function time_delta(countersummary)",
        "function with_bounds(countersummary,tstzrange)",
        "type countersummary",
        "function distinct_count(hyperloglog)",
        "function hyperloglog(integer,anyelement)",
        "function hyperloglog_combine(internal,internal)",
        "function hyperloglog_deserialize(bytea,internal)",
        "function hyperloglog_final(internal)",
        "function hyperloglog_in(cstring)",
        "function hyperloglog_out(hyperloglog)",
        "function hyperloglog_serialize(internal)",
        "function hyperloglog_trans(internal,integer,anyelement)",
        "function hyperloglog_union(internal,hyperloglog)",
        "function rollup(hyperloglog)",
        "function stderror(hyperloglog)",
        "type hyperloglog",
        "function average(statssummary1d)",
        "function average_x(statssummary2d)",
        "function average_y(statssummary2d)",
        "function corr(statssummary2d)",
        "function covariance(statssummary2d,text)",
        "function determination_coeff(statssummary2d)",
        "function intercept(statssummary2d)",
        "function kurtosis(statssummary1d,text)",
        "function kurtosis_x(statssummary2d,text)",
        "function kurtosis_y(statssummary2d,text)",
        "function num_vals(statssummary1d)",
        "function num_vals(statssummary2d)",
        "function rolling(statssummary1d)",
        "function rolling(statssummary2d)",
        "function rollup(statssummary1d)",
        "function rollup(statssummary2d)",
        "function skewness(statssummary1d,text)",
        "function skewness_x(statssummary2d,text)",
        "function skewness_y(statssummary2d,text)",
        "function slope(statssummary2d)",
        "function stats1d_combine(internal,internal)",
        "function stats1d_final(internal)",
        "function stats1d_inv_trans(internal,double precision)",
        "function stats1d_summary_inv_trans(internal,statssummary1d)",
        "function stats1d_summary_trans(internal,statssummary1d)",
        "function stats1d_trans(internal,double precision)",
        "function stats1d_trans_deserialize(bytea,internal)",
        "function stats1d_trans_serialize(internal)",
        "function stats2d_combine(internal,internal)",
        "function stats2d_final(internal)",
        "function stats2d_inv_trans(internal,double precision,double precision)",
        "function stats2d_summary_inv_trans(internal,statssummary2d)",
        "function stats2d_summary_trans(internal,statssummary2d)",
        "function stats2d_trans(internal,double precision,double precision)",
        "function stats2d_trans_deserialize(bytea,internal)",
        "function stats2d_trans_serialize(internal)",
        "function stats_agg(double precision)",
        "function stats_agg(double precision,double precision)",
        "function stats_agg_no_inv(double precision)",
        "function stats_agg_no_inv(double precision,double precision)",
        "function statssummary1d_in(cstring)",
        "function statssummary1d_out(statssummary1d)",
        "function statssummary2d_in(cstring)",
        "function statssummary2d_out(statssummary2d)",
        "function stddev(statssummary1d,text)",
        "function stddev_x(statssummary2d,text)",
        "function stddev_y(statssummary2d,text)",
        "function sum(statssummary1d)",
        "function sum_x(statssummary2d)",
        "function sum_y(statssummary2d)",
        "function variance(statssummary1d,text)",
        "function variance_x(statssummary2d,text)",
        "function variance_y(statssummary2d,text)",
        "function x_intercept(statssummary2d)",
        "type statssummary1d",
        "type statssummary2d",
        "function finalize_with_average(toolkit_experimental.unstabletimevectorpipeline,toolkit_experimental.pipelinethenaverage)",
        "function finalize_with_num_vals(toolkit_experimental.unstabletimevectorpipeline,toolkit_experimental.pipelinethennumvals)",
        "function finalize_with_sum(toolkit_experimental.unstabletimevectorpipeline,toolkit_experimental.pipelinethensum)",
    ];
}
