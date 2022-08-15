// This file serves as the canonical database for what functionality Toolkit has
// stabilized and in which version they were stabilized. The file is consumed by
// by post-install as well as a number of tests that check if our stabilization
// guarantees are being upheld. These different usages require different views
// of the same info, so to avoid parsing issues the stabilization data is
// exposed as macros that're left to the other files to interpret.
//
// XXX this file is used as multiple modules. Search for `#[path = "..."]`
//     directives before adding new macros to make sure that all relevant usages
//     can handle it.

crate::functions_stabilized_at! {
    STABLE_FUNCTIONS
    "1.9.0" => {
        accessorunnest_in(cstring),
        accessorunnest_out(accessorunnest),
        arrow_timevector_unnest(timevector_tstz_f64,accessorunnest),
        rollup(timevector_tstz_f64),
        timevector(timestamp with time zone,double precision),
        timevector_combine(internal,internal),
        timevector_tstz_f64_compound_trans(internal,timevector_tstz_f64),
        timevector_deserialize(bytea,internal),
        timevector_final(internal),
        timevector_tstz_f64_in(cstring),
        timevector_tstz_f64_out(timevector_tstz_f64),
        timevector_serialize(internal),
        timevector_tstz_f64_trans(internal,timestamp with time zone,double precision),
        unnest(timevector_tstz_f64),
        unnest(),
        lttb(timestamp with time zone,double precision,integer),
        lttb(timevector_tstz_f64,integer),
        lttb_final(internal),
        lttb_trans(internal,timestamp with time zone,double precision,integer),
    }
    "1.8.0" => {
    }
    "1.7.0" => {
    }
    "1.6.0" => {
    }
    "1.5" => {
    }
    "prehistory" => {
        approx_percentile(double precision,uddsketch),
        approx_percentile_rank(double precision,uddsketch),
        error(uddsketch),
        mean(uddsketch),
        num_vals(uddsketch),
        percentile_agg(double precision),
        percentile_agg_trans(internal,double precision),
        uddsketch(integer,double precision,double precision),
        rollup(uddsketch),
        uddsketch_combine(internal,internal),
        uddsketch_compound_trans(internal,uddsketch),
        uddsketch_deserialize(bytea,internal),
        uddsketch_final(internal),
        uddsketch_in(cstring),
        uddsketch_out(uddsketch),
        uddsketch_serialize(internal),
        uddsketch_trans(internal,integer,double precision,double precision),
        approx_percentile(double precision,tdigest),
        approx_percentile_rank(double precision,tdigest),
        max_val(tdigest),
        min_val(tdigest),
        mean(tdigest),
        num_vals(tdigest),
        tdigest(integer,double precision),
        rollup(tdigest),
        tdigest_combine(internal,internal),
        tdigest_compound_combine(internal,internal),
        tdigest_compound_deserialize(bytea,internal),
        tdigest_compound_final(internal),
        tdigest_compound_serialize(internal),
        tdigest_compound_trans(internal,tdigest),
        tdigest_deserialize(bytea,internal),
        tdigest_final(internal),
        tdigest_in(cstring),
        tdigest_out(tdigest),
        tdigest_serialize(internal),
        tdigest_trans(internal,integer,double precision),
        average(timeweightsummary),
        time_weight(text,timestamp with time zone,double precision),
        rollup(timeweightsummary),
        time_weight_combine(internal,internal),
        time_weight_final(internal),
        time_weight_summary_trans(internal,timeweightsummary),
        time_weight_trans(internal,text,timestamp with time zone,double precision),
        time_weight_trans_deserialize(bytea,internal),
        time_weight_trans_serialize(internal),
        timeweightsummary_in(cstring),
        timeweightsummary_out(timeweightsummary),
        corr(countersummary),
        counter_agg(timestamp with time zone,double precision),
        counter_agg(timestamp with time zone,double precision,tstzrange),
        counter_agg_combine(internal,internal),
        counter_agg_final(internal),
        counter_agg_summary_trans(internal,countersummary),
        counter_agg_trans(internal,timestamp with time zone,double precision,tstzrange),
        counter_agg_trans_no_bounds(internal,timestamp with time zone,double precision),
        counter_summary_trans_deserialize(bytea,internal),
        counter_summary_trans_serialize(internal),
        counter_zero_time(countersummary),
        countersummary_in(cstring),
        countersummary_out(countersummary),
        delta(countersummary),
        extrapolated_delta(countersummary,text),
        extrapolated_rate(countersummary,text),
        idelta_left(countersummary),
        idelta_right(countersummary),
        intercept(countersummary),
        irate_left(countersummary),
        irate_right(countersummary),
        num_changes(countersummary),
        num_elements(countersummary),
        num_resets(countersummary),
        rate(countersummary),
        rollup(countersummary),
        slope(countersummary),
        time_delta(countersummary),
        with_bounds(countersummary,tstzrange),
        hyperloglog(integer,anyelement),
        hyperloglog_combine(internal,internal),
        hyperloglog_deserialize(bytea,internal),
        hyperloglog_final(internal),
        hyperloglog_in(cstring),
        hyperloglog_out(hyperloglog),
        hyperloglog_serialize(internal),
        hyperloglog_trans(internal,integer,anyelement),
        hyperloglog_union(internal,hyperloglog),
        rollup(hyperloglog),
        stderror(hyperloglog),
        average(statssummary1d),
        average_x(statssummary2d),
        average_y(statssummary2d),
        corr(statssummary2d),
        covariance(statssummary2d,text),
        determination_coeff(statssummary2d),
        intercept(statssummary2d),
        kurtosis(statssummary1d,text),
        kurtosis_x(statssummary2d,text),
        kurtosis_y(statssummary2d,text),
        num_vals(statssummary1d),
        num_vals(statssummary2d),
        rolling(statssummary1d),
        rolling(statssummary2d),
        rollup(statssummary1d),
        rollup(statssummary2d),
        skewness(statssummary1d,text),
        skewness_x(statssummary2d,text),
        skewness_y(statssummary2d,text),
        slope(statssummary2d),
        stats1d_combine(internal,internal),
        stats1d_final(internal),
        stats1d_inv_trans(internal,double precision),
        stats1d_summary_inv_trans(internal,statssummary1d),
        stats1d_summary_trans(internal,statssummary1d),
        stats1d_trans(internal,double precision),
        stats1d_trans_deserialize(bytea,internal),
        stats1d_trans_serialize(internal),
        stats2d_combine(internal,internal),
        stats2d_final(internal),
        stats2d_inv_trans(internal,double precision,double precision),
        stats2d_summary_inv_trans(internal,statssummary2d),
        stats2d_summary_trans(internal,statssummary2d),
        stats2d_trans(internal,double precision,double precision),
        stats2d_trans_deserialize(bytea,internal),
        stats2d_trans_serialize(internal),
        stats_agg(double precision),
        stats_agg(double precision,double precision),
        stats_agg_no_inv(double precision),
        stats_agg_no_inv(double precision,double precision),
        statssummary1d_in(cstring),
        statssummary1d_out(statssummary1d),
        statssummary2d_in(cstring),
        statssummary2d_out(statssummary2d),
        stddev(statssummary1d,text),
        stddev_x(statssummary2d,text),
        stddev_y(statssummary2d,text),
        sum(statssummary1d),
        sum_x(statssummary2d),
        sum_y(statssummary2d),
        variance(statssummary1d,text),
        variance_x(statssummary2d,text),
        variance_y(statssummary2d,text),
        x_intercept(statssummary2d),
        distinct_count(hyperloglog),
    }
}

crate::types_stabilized_at! {
    STABLE_TYPES
    "1.9.0" => {
        timevector_tstz_f64,
        accessorunnest
    }
    "1.8.0" => {
    }
    "1.7.0" => {
    }
    "1.6.0" => {
    }
    "1.5" => {
    }
    "prehistory" => {
        uddsketch,
        tdigest,
        timeweightsummary,
        countersummary,
        hyperloglog,
        statssummary1d,
        statssummary2d,
    }
}

crate::operators_stabilized_at! {
    STABLE_OPERATORS
    "1.9.0" => {
        "->"(timevector_tstz_f64,accessorunnest),
    }
    "1.8.0" => {
    }
    "1.7.0" => {
    }
    "1.6.0" => {
    }
    "1.5" => {
    }
    "prehistory" => {
    }
}