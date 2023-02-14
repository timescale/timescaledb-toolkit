use pgx::*;

use aggregate_builder::aggregate;
use countminsketch::{CountMinHashFn, CountMinSketch as CountMinSketchInternal};

use crate::{
    flatten,
    palloc::{Inner, Internal},
    pg_type,
    raw::bytea,
    ron_inout_funcs,
};

#[pg_schema]
pub mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct CountMinSketch<'input> {
            width: u32,
            depth: u32,
            counters: [i64; self.width * self.depth],
        }
    }

    impl CountMinSketch<'_> {
        fn new(width: u32, depth: u32, counters: Vec<i64>) -> Self {
            let counters_arr = counters.try_into().unwrap();
            unsafe {
                flatten!(CountMinSketch {
                    width,
                    depth,
                    counters: counters_arr,
                })
            }
        }

        pub fn to_internal_countminsketch(&self) -> CountMinSketchInternal {
            let depth: u64 = self.depth.into();
            let hashfuncs = (1..=depth).map(CountMinHashFn::with_key).collect();

            let mut counters: Vec<Vec<i64>> = Vec::with_capacity(self.depth as usize);
            let row_width = self.width as usize;
            for row in 0..self.depth {
                let row_start = (row * self.width) as usize;
                counters.push(
                    self.counters
                        .iter()
                        .skip(row_start)
                        .take(row_width)
                        .collect(),
                );
            }

            CountMinSketchInternal::new(
                self.width as usize,
                self.depth as usize,
                hashfuncs,
                counters,
            )
        }

        pub fn from_internal_countminsketch(sketch: &mut CountMinSketchInternal) -> Self {
            CountMinSketch::new(
                sketch.width().try_into().unwrap(),
                sketch.depth().try_into().unwrap(),
                sketch.counters().iter().flatten().cloned().collect(),
            )
        }
    }

    ron_inout_funcs!(CountMinSketch);
}

use toolkit_experimental::CountMinSketch;

#[aggregate]
impl toolkit_experimental::count_min_sketch {
    type State = CountMinSketchInternal;

    fn transition(
        state: Option<State>,
        #[sql_type("text")] value: Option<String>,
        #[sql_type("float")] error: f64,
        #[sql_type("float")] probability: f64,
    ) -> Option<State> {
        let value = match value {
            None => return state,
            Some(value) => value,
        };

        let mut state = match state {
            None => CountMinSketchInternal::with_prob(error, probability),
            Some(state) => state,
        };

        state.add_value(value);
        Some(state)
    }

    fn finally(state: Option<&mut State>) -> Option<CountMinSketch<'static>> {
        state.map(CountMinSketch::from_internal_countminsketch)
    }

    const PARALLEL_SAFE: bool = true;

    fn serialize(state: &mut State) -> bytea {
        crate::do_serialize!(state)
    }

    fn deserialize(bytes: bytea) -> State {
        crate::do_deserialize!(bytes, State)
    }

    fn combine(state1: Option<&State>, state2: Option<&State>) -> Option<State> {
        match (state1, state2) {
            (None, None) => None,
            (None, Some(only)) | (Some(only), None) => Some(only.clone()),
            (Some(a), Some(b)) => {
                let (mut a, b) = (a.clone(), b.clone());
                a.combine(b);
                Some(a)
            }
        }
    }
}

#[pg_extern(immutable, parallel_safe, schema = "toolkit_experimental")]
pub fn approx_count<'a>(item: String, aggregate: Option<CountMinSketch<'a>>) -> Option<i64> {
    aggregate.map(|sketch| CountMinSketch::to_internal_countminsketch(&sketch).estimate(item))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    fn test_countminsketch() {
        Spi::connect(|client| {
            client.select("CREATE TABLE test (data TEXT)", None, None);
            client.select("INSERT INTO test SELECT generate_series(1, 100)::TEXT UNION ALL SELECT generate_series(1, 50)::TEXT", None, None);

            let sanity = client
                .select("SELECT COUNT(*) FROM test", None, None)
                .unwrap()
                .first()
                .get_one::<i32>()
                .unwrap();
            assert_eq!(Some(150), sanity);

            client.select(
                "CREATE VIEW sketch AS \
                SELECT toolkit_experimental.count_min_sketch(data, 0.01, 0.01) \
                FROM test",
                None,
                None,
            );

            let sanity = client
                .select("SELECT COUNT(*) FROM sketch", None, None)
                .unwrap()
                .first()
                .get_one::<i32>()
                .unwrap();
            assert!(sanity.unwrap_or(0) > 0);

            let (col1, col2, col3) = client
                .select(
                    "SELECT \
                     toolkit_experimental.approx_count('1', count_min_sketch), \
                     toolkit_experimental.approx_count('51', count_min_sketch), \
                     toolkit_experimental.approx_count('101', count_min_sketch) \
                     FROM sketch",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_three::<i32, i32, i32>()
                .unwrap();

            // 0.01 => error param to the sketch, 150 => number of items added to the sketch
            let err_margin = 0.01 * 150.0;

            let items = [(col1, 2), (col2, 1), (col3, 0)];
            for (approx_count, expected) in items {
                let approx_count = approx_count.unwrap();
                assert!(expected <= approx_count);

                let upper_bound = err_margin + expected as f64;
                let approx_count = approx_count as f64;
                assert!(approx_count < upper_bound);
            }
        });
    }

    #[pg_test]
    fn test_countminsketch_combine() {
        Spi::connect(|client| {
            let combined = client
                .select(
		    "SELECT toolkit_experimental.approx_count('1', toolkit_experimental.count_min_sketch(v::text, 0.01, 0.01))
                     FROM (SELECT * FROM generate_series(1, 100) v \
		             UNION ALL \
                           SELECT * FROM generate_series(1, 100))  u(v)",
                    None,
                    None,
                )
                .unwrap().first()
                .get_one::<i32>().unwrap();

            let expected = 2;
            // 0.01 => error param to the sketch, 200 => number of items added to the sketch
            let err_margin = 0.01 * 200.0;

            let approx_count = combined.unwrap();
            assert!(expected <= approx_count);

            let upper_bound = err_margin + expected as f64;
            let approx_count = approx_count as f64;
            assert!(approx_count < upper_bound);
        });
    }

    #[pg_test]
    fn countminsketch_io_test() {
        Spi::connect(|client| {
            client.select("CREATE TABLE io_test (value TEXT)", None, None);
            client.select("INSERT INTO io_test VALUES ('lorem'), ('ipsum'), ('dolor'), ('sit'), ('amet'), ('consectetur'), ('adipiscing'), ('elit')", None, None);

            let sketch = client
                .select(
                    "SELECT toolkit_experimental.count_min_sketch(value, 0.5, 0.01)::text FROM io_test",
                    None,
                    None,
                )
                .unwrap().first()
                .get_one::<String>().unwrap();

            let expected = "(\
                version:1,\
                width:6,\
                depth:5,\
                counters:[\
                    1,2,2,1,1,1,\
                    0,0,2,3,1,2,\
                    1,0,3,0,4,0,\
                    1,3,2,0,1,1,\
                    0,0,4,3,0,1\
                    ]\
                )";

            assert_eq!(sketch, Some(expected.into()));
        });
    }

    #[pg_test]
    fn test_cms_null_input_yields_null_output() {
        Spi::connect(|client| {
            let output = client
                .select(
                    "SELECT toolkit_experimental.count_min_sketch(NULL::TEXT, 0.1, 0.1)::TEXT",
                    None,
                    None,
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();
            assert_eq!(output, None)
        })
    }

    #[pg_test]
    fn test_approx_count_null_input_yields_null_output() {
        Spi::connect(|client| {
            let output = client
                .select(
                    "SELECT toolkit_experimental.approx_count('1'::text, NULL::toolkit_experimental.countminsketch)",
                    None,
                    None,
                )
                .unwrap().first()
                .get_one::<String>().unwrap();
            assert_eq!(output, None)
        })
    }
}
