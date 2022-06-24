use pgx::*;
use serde::{Deserialize, Serialize};

use aggregate_builder::aggregate;
use flat_serialize::*;

use crate::{
    flatten,
    palloc::{Inner, Internal},
    pg_type, ron_inout_funcs,
};

use toolkit_experimental::*;

// type_complexity is upset about the complex Iterator
#[allow(clippy::type_complexity)]
#[pg_schema]
pub mod toolkit_experimental {
    use super::*;
    use pg_sys::{TimeOffset, TimestampTz};

    use forecast::triple::*;

    pg_type! {
        #[derive(Debug)]
        struct TripleForecast<'input> {
            computations_len: u64,
            computations: [forecast::triple::Computation; self.computations_len],
            start_time: TimestampTz,
            interval: TimeOffset,
            conf: Config,
        }
    }

    ron_inout_funcs!(TripleForecast);

    #[pg_extern(name = "forecast")]
    pub fn forecast_triple(
        agg: TripleForecast<'_>,
        n: i32,
    ) -> impl std::iter::Iterator<
        Item = (
            name!(time, crate::raw::TimestampTz),
            name!(forecast, f64),
            name!(smoothed, f64),
            name!(trend_factor, f64),
            name!(seasonal_index, f64),
        ),
    > + '_ {
        assert!(n > 0, "number of forecast to make must be > 0");
        let n = n as usize;
        let mut time = agg.start_time;
        let interval = agg.interval;
        // TODO I made forecast::Triple in context of a stand-alone program and now seeing it in an
        //  aggregate, it makes less sense.  Probably makes more sense to put model-building into
        //  impl TripleForecastTransState and forecasting into impl TripleForecast.
        let forecast = forecast::Triple::from(agg);
        forecast.forecast_iter(n).unwrap().map(move |computation| {
            time += interval;
            (
                time.into(),
                computation.forecasted_value,
                computation.smoothed,
                computation.trend_factor,
                computation.seasonal_index,
            )
        })
    }

    #[pg_extern(name = "into_values")]
    pub fn into_values_triple(
        agg: TripleForecast<'_>,
    ) -> impl std::iter::Iterator<
        Item = (
            name!(time, crate::raw::TimestampTz),
            name!(value, f64),
            name!(smoothed, f64),
            name!(error, f64),
            name!(trend_factor, f64),
            name!(seasonal_index, f64),
            name!(forecasted_value, f64),
        ),
    > + '_ {
        let mut time = agg.start_time;
        let interval = agg.interval;
        let forecast = forecast::Triple::from(agg);
        let computations: Vec<forecast::triple::Computation> =
            forecast.computations_iter().unwrap().cloned().collect();
        time -= computations.len() as pg_sys::TimestampTz * interval;
        computations.into_iter().map(move |computation| {
            time += interval;
            (
                time.into(),
                computation.value,
                computation.smoothed,
                computation.error,
                computation.trend_factor,
                computation.seasonal_index,
                computation.forecasted_value,
            )
        })
    }
}

#[aggregate]
impl toolkit_experimental::triple_forecast {
    type State = TripleForecastTransState;

    fn transition(
        state: Option<State>,
        #[sql_type("double precision")] smooth: f64,
        #[sql_type("double precision")] trend_smooth: f64,
        #[sql_type("double precision")] seasonal_smooth: f64,
        #[sql_type("integer")] values_per_season: i32,
        #[sql_type("timestamptz")] ts: crate::raw::TimestampTz,
        #[sql_type("double precision")] value: f64,
    ) -> Option<State> {
        let time = pg_sys::TimestampTz::from(ts);
        let mut state = match state {
            None => {
                let values_per_season = values_per_season as u32;
                TripleForecastTransState {
                    forecast: forecast::Triple::new(forecast::triple::Config {
                        smooth,
                        trend_smooth,
                        seasonal_smooth,
                        values_per_season,
                    }),
                    last_time: time,
                    // Signal to the Some arm when it runs for the first time so it can establish the interval.
                    interval: 0,
                }
            }
            Some(mut state) => {
                let interval = time - state.last_time;
                assert!(interval > 0, "please respect time's arrow");
                if state.interval == 0 {
                    // First time in Some arm:  establish this interval.
                    state.interval = interval;
                } else {
                    // Interval established:  ensure it doesn't change.
                    assert_eq!(interval, state.interval, "interval must not vary");
                }
                state.last_time = time;
                state
            }
        };
        state.forecast.observe(value);
        //t.minimize();
        Some(state)
    }

    fn finally(state: Option<&mut State>) -> Option<TripleForecast<'static>> {
        state.map(|s| TripleForecast::from(s.clone()))
    }
}

// Intermediate state kept in postgres.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct TripleForecastTransState {
    forecast: forecast::Triple,
    /// The last time value we see becomes [TripleForecastData::start_time].
    last_time: pg_sys::TimestampTz,
    /// This becomes [TripleForecastData::interval].
    interval: pg_sys::TimeOffset,
}

impl From<TripleForecastTransState> for TripleForecast<'_> {
    fn from(
        TripleForecastTransState {
            forecast,
            last_time: start_time,
            interval,
        }: TripleForecastTransState,
    ) -> Self {
        let (conf, computations) = forecast.destruct();
        let computations_len = computations.len() as u64;
        unsafe {
            flatten!(TripleForecast {
                conf,
                computations_len,
                computations: (&*computations).into(),
                start_time,
                interval,
            })
        }
    }
}

impl From<TripleForecast<'_>> for forecast::Triple {
    fn from(internal: TripleForecast<'_>) -> Self {
        Self::construct((
            internal.conf.clone(),
            // TODO `internal` is already either a clone or the original moved in:  how can we avoid this pointless clone?
            internal.computations.as_slice().to_vec(),
        ))
    }
}
