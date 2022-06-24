use std::convert::TryFrom as _;

use serde::{Deserialize, Serialize};

use flat_serialize_macro::FlatSerializable;

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, FlatSerializable)]
#[repr(C)]
pub struct Config {
    // https://www.itl.nist.gov/div898/handbook/pmc/section4/pmc431.htm
    //     "But there are better search methods, such as the Marquardt procedure"
    //     "In general, most well designed statistical software programs should
    //      be able to find the value of α that minimizes the MSE."
    // TODO Guess we better do that then!  Not sure of all the optiosn yet, but
    //  Marquardt implementations are available at least in Fortran and C.
    pub smooth: f64,
    pub trend_smooth: f64,
    pub seasonal_smooth: f64,
    // TODO std::num::NonZeroU32
    pub values_per_season: u32,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, FlatSerializable)]
#[repr(C)]
pub struct Computation {
    pub value: f64,
    pub smoothed: f64,
    /// [Self::smoothed] - [Self::value] - square it and add use for MSE.
    pub error: f64,
    pub trend_factor: f64,
    pub seasonal_index: f64,
    /// This is the value we'd forecast for this value had we not an actual value.
    pub forecasted_value: f64,
}

pub(super) mod t {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    pub struct Triple {
        c: Config,
        state: State,
    }

    impl Triple {
        /// Panics unless:
        /// - 0 <= [Config::smooth] <= 1
        /// - 0 <= [Config::trend_smooth] <= 1
        /// - [Config::values_per_season] > 0
        pub fn new(c: Config) -> Self {
            assert!(c.smooth >= 0.0 && c.smooth <= 1.0, "0 <= smoothing <= 1");
            assert!(
                c.trend_smooth >= 0.0 && c.trend_smooth <= 1.0,
                "0 <= trend_smooth <= 1"
            );
            // TODO what about c.seasonal_smooth?
            assert!(c.values_per_season > 0, "values per season must be > 0");
            Self {
                c,
                state: State::Initializing(vec![]),
            }
        }

        pub fn observe(&mut self, value: f64) {
            match &mut self.state {
                State::Initializing(values) => {
                    values.push(value);
                    // TODO Allow using more values for initialization than the minimum.
                    if values.len() == 2 * self.c.values_per_season as usize {
                        self.state = State::Initialized(initialize(&self.c, values))
                    }
                }
                State::Initialized(i) => {
                    i.observe(&self.c, value);
                }
            }
        }

        pub fn minimize(&mut self) {
            if let State::Initialized(i) = &mut self.state {
                i.minimize(&self.c)
            }
        }

        /// Panics if uninitialized.
        pub fn destruct(self) -> (Config, Vec<Computation>) {
            match self.state {
                State::Initializing(_) => panic!("not initialized"),
                State::Initialized(Initialized { computations }) => (self.c, computations),
            }
        }

        /// Put back together after [Self::destruct].
        /// Panics if `computations` doesn't contain at least twice as many
        /// items as twice [Config::values_per_season].
        pub fn construct((c, computations): (Config, Vec<Computation>)) -> Self {
            assert!(computations.len() >= 2 * c.values_per_season as usize);
            let state = State::Initialized(Initialized { computations });
            Self { c, state }
        }

        // TODO We could expose the two states to callers, so attempts to call [computations_iter]
        //   and [forecast_extend] before initialization is complete wouldn't even compile, but that
        //   might be unnecessarily complicated.
        //   Similar todo in extension/src/forecast.rs whose suggestion probably makes more sense.

        pub fn computations_iter(&self) -> Option<impl std::iter::Iterator<Item = &Computation>> {
            match &self.state {
                State::Initializing(_) => None,
                State::Initialized(i) => Some(i.computations.iter()),
            }
        }

        /// Panics if uninitialized.
        pub fn forecast_iter(self, n: usize) -> Option<ForecastIterator> {
            match self.state {
                State::Initializing(_) => panic!("not initialized"),
                State::Initialized(forecast) => Some(ForecastIterator {
                    c: self.c,
                    forecast,
                    n,
                }),
            }
        }
    }
}

pub struct ForecastIterator {
    c: Config,
    forecast: Initialized,
    n: usize,
}

impl Iterator for ForecastIterator {
    type Item = Computation;
    fn next(&mut self) -> Option<Self::Item> {
        if self.n == 0 {
            None
        } else {
            self.n -= 1;
            self.forecast
                .observe(&self.c, self.forecast.forecast(&self.c));
            self.forecast.minimize(&self.c);
            Some(self.forecast.computations.last().unwrap().clone())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
enum State {
    Initializing(Vec<f64>),
    Initialized(Initialized),
}

struct Smoothed {
    value: f64,
    smoothed: f64,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct Initialized {
    computations: Vec<Computation>,
}

impl Initialized {
    pub fn observe(&mut self, conf: &Config, value: f64) -> &Computation {
        let seasonal_index = self.seasonal_index_for_next_observation(conf);
        let len = self.computations.len();
        let prev = &self.computations[len - 1];
        let smoothed = smooth(conf, prev, seasonal_index, value);
        let computation = Computation {
            value,
            smoothed: smoothed.smoothed,
            error: smoothed.smoothed - value,
            trend_factor: self.compute_trend(conf, &smoothed),
            seasonal_index: compute_seasonal_index(conf, seasonal_index, &smoothed),
            forecasted_value: self.forecast(conf),
        };
        self.computations.push(computation);
        &self.computations[len]
    }

    pub fn forecast(&self, conf: &Config) -> f64 {
        let s_t = self.computations[self.computations.len() - 1].smoothed;
        let b_t = self.computations[self.computations.len() - 1].trend_factor;
        let m: usize = 1;
        let m_f = 1.0;
        let seasonal_index = self.computations
            [self.computations.len() - conf.values_per_season as usize + m]
            .seasonal_index;
        //(S_t + m * b_t) * I_{t-L+m}
        (s_t + m_f * b_t) * seasonal_index
    }

    pub fn minimize(&mut self, conf: &Config) {
        // TODO Why shuffle these around constantly?  We won't need old
        //   computations after all; just use ring buffer?
        for _ in 0..self.computations.len() - 2 * conf.values_per_season as usize {
            self.computations.remove(0);
        }
    }

    fn seasonal_index_for_next_observation(&self, conf: &Config) -> f64 {
        // We use this seasonal_index for [smooth] and [compute_seasonal_index].
        // In both cases, they need I_{t-L} where `t` is the index of the incoming `value`.
        // `computations[computations.len()]` is actually I_{t-1}, so
        // `computations[computations.len() - L]` would be I_{t-1-L}!
        // SO we add one more in here to get the correct seasonal index.
        let t = 1 + self.computations.len();
        let l = conf.values_per_season as usize;
        self.computations[t - l].seasonal_index
    }

    fn compute_trend(&self, conf: &Config, smoothed: &Smoothed) -> f64 {
        let prev = &self
            .computations
            .last()
            .expect("previous compuations available");
        let g = conf.trend_smooth;
        let s_t = smoothed.smoothed;
        let s_t_1 = prev.smoothed;
        // b_t =        γ * (S_t - S_{t-1}) + (1   - γ) * b_{t-1}
        conf.trend_smooth * (s_t - s_t_1) + (1.0 - g) * prev.trend_factor
        // Chatfield (2001) gives the same formula for this one.
    }
}

// TODO This would be cleaner if [smooth] and [compute_seasonal_index]
// chose the seasonal index directly rather than splitting the formula
// between here and there.  The current organization is in support of
// the special intialization case.  Another option might be to put
// those special initial values into a fake [Computation] array such
// that the normal seasonal index offsets land on the initial values.
fn smooth(conf: &Config, prev: &Computation, seasonal_index: f64, value: f64) -> Smoothed {
    let a = conf.smooth;
    let i_t_l = seasonal_index;
    let s_t_1 = prev.smoothed;
    let b_t_1 = prev.trend_factor;
    // This is from NIST Engineering Statistics Handbook:
    //       S_t = α * (y_t / I_{t-L}) + (1   - α) * (S_{t-1}+b_{t-1})
    let smoothed = a * (value / i_t_l) + (1.0 - a) * (s_t_1 + b_t_1);
    // Alternative formula from Chatfield (2001):
    // L_N = α(x_N - I_{N-s}) + (1 - α)(L_{N-1} + T_{N-1})
    // Additive vs. multiplicative?
    Smoothed { value, smoothed }
}

fn compute_seasonal_index(conf: &Config, seasonal_index: f64, smoothed: &Smoothed) -> f64 {
    let y_t = smoothed.value;
    let s_t = smoothed.smoothed;
    // I_t=            β * (y_t / S_t) + (1   -                    β) * I_{t-L}
    conf.seasonal_smooth * (y_t / s_t) + (1.0 - conf.seasonal_smooth) * seasonal_index
    // Alternative formula from Chatfield (2001):
    // I_t = δ(x_t - L_t) + (1 - δ)I_{t-L}
    // Additive vs. multiplicative?
}

fn initialize(conf: &Config, values: &[f64]) -> Initialized {
    let season_size = conf.values_per_season as usize;
    let b = initial_trend_factor(conf, values);
    let mut seasonal_indices = initial_seasonal_indices(conf, values);
    seasonal_indices.reverse();
    let mut values = values.iter().enumerate();
    let (_, value) = values.next().unwrap();
    let value = *value;
    let mut result = Initialized {
        computations: vec![Computation {
            value,
            smoothed: value,
            error: 0.0,
            trend_factor: b,
            seasonal_index: seasonal_indices
                .pop()
                .expect("initial seasonal indices available"),
            forecasted_value: 0.0,
        }],
    };
    for (i, value) in values {
        let prev = &result.computations[i - 1];
        let value = *value;
        let seasonal_index = seasonal_indices
            .pop()
            .unwrap_or_else(|| result.seasonal_index_for_next_observation(conf));
        let smoothed = smooth(conf, prev, seasonal_index, value);
        result.computations.push(Computation {
            value,
            smoothed: smoothed.smoothed,
            error: smoothed.smoothed - value,
            trend_factor: result.compute_trend(conf, &smoothed),
            seasonal_index: compute_seasonal_index(conf, seasonal_index, &smoothed),
            forecasted_value: if result.computations.len() >= season_size {
                result.forecast(conf)
            } else {
                0.0
            },
        });
    }
    result
}

fn initial_trend_factor(conf: &Config, values: &[f64]) -> f64 {
    let mut b = 0.0;
    let values_per_season = f64::from(conf.values_per_season);
    let season_size = conf.values_per_season as usize;
    for i in 0..season_size {
        b += (values[season_size + i] - values[i]) / values_per_season;
    }
    1.0 / values_per_season * b
}

fn initial_seasonal_indices(conf: &Config, values: &[f64]) -> Vec<f64> {
    let season_size = conf.values_per_season as usize;
    assert!(values.len() % season_size == 0);
    let n_seasons = values.len() / season_size;
    let n_seasons_f = f64::from(u32::try_from(n_seasons).expect("number of seasons fits into f64"));
    // Step 1: Compute the averages of each of the 6 years.
    let season_averages = initial_seasonal_averages(conf, n_seasons, values);
    // Step 2: Divide the observations by the appropriate yearly mean.
    // inputs:
    // - y = values[i]		is the observation
    // - t = i			is an index denoting a time period
    // - L = values_per_season	is the number of periods comprising a complete season
    // - m is the number of periods ahead to forecast
    // outputs:
    // - b is the trend factor
    // - I is the seasonal index
    // We need L initial values of I.
    let mut indices = vec![0.0; season_size];
    let mut offset = 0;
    let mut season_averages_i = 0;
    for (i, value) in values.iter().enumerate() {
        if i > 0 && i % season_size == 0 {
            offset += season_size;
            season_averages_i += 1;
        }
        indices[i - offset] += value / season_averages[season_averages_i] / n_seasons_f;
    }
    indices
}

fn initial_seasonal_averages(conf: &Config, n_seasons: usize, values: &[f64]) -> Vec<f64> {
    let values_per_season = f64::from(conf.values_per_season);
    let mut averages = vec![0.0; n_seasons];
    let mut off: usize = 0;
    for (i, value) in values.iter().enumerate() {
        if i > 0 && i % n_seasons == 0 {
            off += n_seasons;
        }
        averages[i - off] += value / values_per_season;
    }
    averages
}
