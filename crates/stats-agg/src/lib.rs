// stats is a small statistical regression lib that implements the Youngs-Cramer algorithm and is based on the Postgres implementation
// here for 1D regression analysis:
// https://github.com/postgres/postgres/blob/8bdd6f563aa2456de602e78991e6a9f61b8ec86d/src/backend/utils/adt/float.c#L2813
// And here for 2D regression analysis:
// https://github.com/postgres/postgres/blob/472e518a44eacd9caac7d618f1b6451672ca4481/src/backend/utils/adt/float.c#L3260
//

#[derive(Debug, PartialEq)]
pub enum StatsError {
    DoubleOverflow,
}

#[derive(Debug, PartialEq)]
pub struct XYPair {
    pub x: f64,
    pub y: f64,
}
pub mod stats2d;
pub mod stats1d;
