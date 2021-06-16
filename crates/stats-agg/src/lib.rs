// stats is a small statistical regression lib that implements the Youngs-Cramer algorithm and is based on the Postgres implementation
// here for 1D regression analysis:


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

// The threshold at which we should re-calculate when we're doing the inverse transition in a windowed aggregate
// essentially, if we're shifting the data by enough as we remove a value from the aggregate we can end up with 
// extra floating point error because in real arithmetic x = x + C - C 
// but in floating point arithmetic, if C is large compared to x, we can accumulate significant error. 
// In our case, because C is added in the normal transition or combine function, and then removed later in the
// inverse function, we have x + C and C and we are testing the following: C / (x + C) > INV_FLOATING_ERROR_THRESHOLD
// Because of the way that Postgres performs inverse functions, if we return a NULL value, the only thing that happens
// is that the partial will get re-calculated from scratch from the values in the window function. So providing
// the inverse function is purely an optimization. There are several cases where the C/(x + C) is likely to be larger
// than our threshold, but we don't care too much, namely when there are one or two values this can happen frequently,
// but then the cost of recalculation is low, compared to when there are many values in a rolling calculation, so we
// test early in the function for whether we need to recalculate and pass NULL quickly so that we don't affect those
// cases too heavily. 
const INV_FLOATING_ERROR_THRESHOLD : f64 = 0.99;
pub mod stats2d;
pub mod stats1d;
