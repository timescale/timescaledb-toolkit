
use pgx::*;

use crate::{
    json_inout_funcs, pg_type, flatten,
};

use time_series::{TSPoint, NormalTimeSeries};

use flat_serialize::*;

pg_type! {
    #[derive(Debug)]
    struct SortedTimeseries {
        num_points: u64,  // required to be aligned
        points: [TSPoint; self.num_points],
    }
}

json_inout_funcs!(SortedTimeseries);

// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
pub mod timescale_analytics_experimental {
    pub(crate) use super::*;

    varlena_type!(SortedTimeseries);
    varlena_type!(NormalizedTimeSeries);
}

#[pg_extern(name = "unnest_series", schema = "timescale_analytics_experimental")]
pub fn unnest_sorted_series(
    series: timescale_analytics_experimental::SortedTimeseries,
) -> impl std::iter::Iterator<Item = (name!(time,pg_sys::TimestampTz),name!(value,f64))> + '_ {
    series.points.iter().map(|p| (p.ts, p.val))
}

// TODO: Can we have a single time-series object which can store either an
// explicit or normal timeseries (without being stupidly inefficient)
pg_type! {
    #[derive(Debug)]
    struct NormalizedTimeSeries {
        start_ts: i64,
        step_interval: i64,
        num_vals: u64,  // required to be aligned
        values: [f64; self.num_vals],
    }
}

json_inout_funcs!(NormalizedTimeSeries);

impl<'input> NormalizedTimeSeries<'input> {
    #[allow(dead_code)]
    pub fn to_normal_time_series(&self) -> NormalTimeSeries {
        NormalTimeSeries {
            start_ts: *self.start_ts,
            step_interval: *self.step_interval,
            values: self.values.to_vec(),
        }
    }

    pub fn from_normal_time_series(series: &NormalTimeSeries) -> NormalizedTimeSeries<'input> {
        unsafe {
            flatten!(
                NormalizedTimeSeries {
                    start_ts: &series.start_ts,
                    step_interval: &series.step_interval,
                    num_vals: &(series.values.len() as u64),
                    values: &series.values,
                }
            )
        }
    }
}

#[pg_extern(name = "unnest_series", schema = "timescale_analytics_experimental")]
pub fn unnest_normalized_series(
    series: timescale_analytics_experimental::NormalizedTimeSeries,
) -> impl std::iter::Iterator<Item = (name!(time,pg_sys::TimestampTz),name!(value,f64))> + '_ {
    (0..*series.num_vals).map(move |i| {
        let num_steps = i as i64;
        let step_interval = *series.step_interval;
        (*series.start_ts + num_steps * step_interval, series.values[i as usize])
    })
}