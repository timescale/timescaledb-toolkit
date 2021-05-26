
use pgx::*;

use crate::{
    json_inout_funcs, pg_type, flatten,
};

use time_series::{TSPoint, TimeSeries as InternalTimeSeries, ExplicitTimeSeries, NormalTimeSeries};

use flat_serialize::*;

pg_type! {
    #[derive(Debug)]
    struct TimeSeries {
        #[flat_serialize::flatten]
        series : SeriesType<'a>,
    }
}

json_inout_funcs!(TimeSeries);

flat_serialize_macro::flat_serialize! {
    #[derive(Debug)]
    #[derive(serde::Serialize, serde::Deserialize)]
    #[flat_serialize::field_attr(
        fixed = r##"#[serde(deserialize_with = "crate::serialization::serde_reference_adaptor::deserialize")]"##,
        variable = r##"#[serde(deserialize_with = "crate::serialization::serde_reference_adaptor::deserialize_slice")]"##,
    )]
    enum SeriesType {
        type_id: u64,
        SortedSeries: 1 {
            num_points: u64,  // required to be aligned
            points: [TSPoint; self.num_points],
        },
        NormalSeries: 2 {
            start_ts: i64,
            step_interval: i64,
            num_vals: u64,  // required to be aligned
            values: [f64; self.num_vals],
        },
    }
}

// hack to allow us to qualify names with "timescale_analytics_experimental"
// so that pgx generates the correct SQL
pub mod timescale_analytics_experimental {
    pub(crate) use super::*;
    varlena_type!(TimeSeries);
}

impl<'input> TimeSeries<'input> {
    #[allow(dead_code)]
    pub fn to_internal_time_series(&self) -> InternalTimeSeries {
        match self.series {
            SeriesType::SortedSeries{points, ..} => 
                InternalTimeSeries::Explicit(
                    ExplicitTimeSeries {
                        ordered: true,
                        points: points.to_vec(),
                    }
                ),
            SeriesType::NormalSeries{start_ts, step_interval, values, ..} =>
                InternalTimeSeries::Normal(
                    NormalTimeSeries {
                        start_ts: *start_ts,
                        step_interval: *step_interval,
                        values: values.to_vec(),
                    }   
                ),
        }
    }

    pub fn from_internal_time_series(series: InternalTimeSeries) -> TimeSeries<'input> {
        unsafe {
            match series {
                InternalTimeSeries::Explicit(series) => {
                    if !series.ordered {
                        panic!("No time series type for unordered point yet");
                    }
                    flatten!(
                        TimeSeries {
                            series: SeriesType::SortedSeries {
                                num_points: &(series.points.len() as u64),
                                points: &series.points,
                            }
                        }
                    )
                },
                InternalTimeSeries::Normal(series) => {
                    flatten!(
                        TimeSeries {
                            series : SeriesType::NormalSeries {
                                start_ts: &series.start_ts,
                                step_interval: &series.step_interval,
                                num_vals: &(series.values.len() as u64),
                                values: &series.values,
                            }
                        }
                    )
                }
            }
        }
    }
}

#[pg_extern(schema = "timescale_analytics_experimental")]
pub fn unnest_series(
    series: timescale_analytics_experimental::TimeSeries,
) -> impl std::iter::Iterator<Item = (name!(time,pg_sys::TimestampTz),name!(value,f64))> + '_ {
    let iter: Box<dyn Iterator<Item=_>> = match series.series {
        SeriesType::SortedSeries{points, ..} => 
            Box::new(points.iter().map(|points| (points.ts, points.val))),

        SeriesType::NormalSeries{start_ts, step_interval, num_vals, values} =>
            Box::new((0..*num_vals).map(move |i| {
                let num_steps = i as i64;
                let step_interval = *step_interval;
                (*start_ts + num_steps * step_interval, values[i as usize])
            })),
    };
    iter
}
