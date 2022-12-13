// stats is a small statistical regression lib that implements the Youngs-Cramer algorithm and is based on the Postgres implementation
// here for 1D regression analysis:

// And here for 2D regression analysis:
// https://github.com/postgres/postgres/blob/472e518a44eacd9caac7d618f1b6451672ca4481/src/backend/utils/adt/float.c#L3260
//

pub trait FloatLike:
    num_traits::NumOps + num_traits::NumAssignOps + num_traits::Float + From<f64>
{
    /// Shorthand for `<T as From<f64>>::from(val)`
    fn lit(val: f64) -> Self {
        <Self as From<f64>>::from(val)
    }
    fn from_u64(n: u64) -> Self;
}
impl FloatLike for f64 {
    fn from_u64(n: u64) -> Self {
        n as f64
    }
}
impl FloatLike for twofloat::TwoFloat {
    fn from_u64(n: u64) -> Self {
        (n as f64).into()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum StatsError {
    DoubleOverflow,
}

#[derive(Debug, PartialEq, Eq)]
pub struct XYPair<T: FloatLike> {
    pub x: T,
    pub y: T,
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
#[cfg(not(any(test, feature = "pg_test")))]
const INV_FLOATING_ERROR_THRESHOLD: f64 = 0.99;
#[cfg(any(test, feature = "pg_test"))] // don't have a threshold for tests, to ensure the inverse function is better tested
const INV_FLOATING_ERROR_THRESHOLD: f64 = f64::INFINITY;

pub mod stats1d;
pub mod stats2d;

// This will wrap the logic for incrementing the sum for the third moment of a series of floats (i.e. Sum (i=1..N) of (i-avg)^3)
// Math is sourced from https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
mod m3 {
    use super::*;

    // Add a value x to the set.  n, sx, sxx, sx3 are the values from prior to including x.
    pub(crate) fn accum<T: FloatLike>(n: T, sx: T, sxx: T, sx3: T, x: T) -> T {
        let delta = x - (sx / n);
        let n = n + T::one();
        sx3 + delta.powi(3) * (n - T::one()) * (n - T::lit(2.)) / n.powi(2)
            - (T::lit(3.) * delta * sxx / n)
    }
    // Remove a value x from the set.  Here n, sx, sxx are all the values from the set after x has been removed.
    // old_sx3 is the current value prior to the remove (sx3 after the removal is the returned value)
    pub(crate) fn remove<T: FloatLike>(new_n: T, new_sx: T, new_sxx: T, old_sx3: T, x: T) -> T {
        let delta = x - (new_sx / new_n);
        let n = new_n + T::one();
        old_sx3
            - (delta.powi(3) * (n - T::one()) * (n - T::lit(2.)) / n.powi(2)
                - (T::lit(3.) * delta * new_sxx / n))
    }
    // Combine two sets a and b and returns the sx3 for the combined set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn combine<T: FloatLike>(
        na: T,
        nb: T,
        sxa: T,
        sxb: T,
        sxxa: T,
        sxxb: T,
        sx3a: T,
        sx3b: T,
    ) -> T {
        let nx = na + nb;
        let delta = sxb / nb - sxa / na;
        sx3a + sx3b
            + delta.powi(3) * na * nb * (na - nb) / nx.powi(2)
            + (na * sxxb - (nb * sxxa)) * T::lit(3.) * delta / nx
    }
    // This removes set b from a combined set, returning the sx3 of the remaining set a.
    // Note that na, sxa, sxxa are all the values computed on the remaining set.  old_sx3 is the sx3 of the combined set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn remove_combined<T: FloatLike>(
        new_na: T,
        nb: T,
        new_sxa: T,
        sxb: T,
        new_sxxa: T,
        sxxb: T,
        old_sx3: T,
        sx3b: T,
    ) -> T {
        let nx = new_na + nb;
        let delta = sxb / nb - new_sxa / new_na;
        old_sx3
            - (sx3b
                + delta.powi(3) * new_na * nb * (new_na - nb) / nx.powi(2)
                + T::lit(3.) * (new_na * sxxb - (nb * new_sxxa)) * delta / nx)
    }
}

// This will wrap the logic for incrementing the sum for the fourth moment of a series of floats (i.e. Sum (i=1..N) of (i-avg)^4)
// Math is sourced from https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
mod m4 {
    use super::*;

    // Add a value x to the set.  n, sx, sxx, sx3, sx4 are the values from prior to including x.
    pub(crate) fn accum<T: FloatLike>(n: T, sx: T, sxx: T, sx3: T, sx4: T, x: T) -> T {
        let delta = x - (sx / n);
        let n = n + T::one();
        sx4 + delta.powi(4) * (n - T::one()) * (n.powi(2) - T::lit(3.) * n + T::lit(3.)) / n.powi(3)
            + T::lit(6.) * delta.powi(2) * sxx / n.powi(2)
            - T::lit(4.) * delta * sx3 / n
    }
    // Remove a value x from the set.  Here n, sx, sxx, sx3 are all the values from the set after x has been removed.
    // old_sx4 is the current value prior to the remove (sx4 after the removal is the returned value)
    pub(crate) fn remove<T: FloatLike>(
        new_n: T,
        new_sx: T,
        new_sxx: T,
        new_sx3: T,
        old_sx4: T,
        x: T,
    ) -> T {
        let delta = x - (new_sx / new_n);
        let n = new_n + T::one();
        old_sx4
            - (delta.powi(4) * (n - T::one()) * (n.powi(2) - T::lit(3.) * n + T::lit(3.))
                / n.powi(3)
                + T::lit(6.) * delta.powi(2) * new_sxx / n.powi(2)
                - T::lit(4.) * delta * new_sx3 / n)
    }
    // Combine two sets a and b and returns the sx4 for the combined set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn combine<T: FloatLike>(
        na: T,
        nb: T,
        sxa: T,
        sxb: T,
        sxxa: T,
        sxxb: T,
        sx3a: T,
        sx3b: T,
        sx4a: T,
        sx4b: T,
    ) -> T {
        let nx = na + nb;
        let delta = sxb / nb - sxa / na;
        sx4a + sx4b
            + delta.powi(4) * na * nb * (na.powi(2) - na * nb + nb.powi(2)) / nx.powi(3)
            + T::lit(6.) * (na.powi(2) * sxxb + nb.powi(2) * sxxa) * delta.powi(2) / nx.powi(2)
            + T::lit(4.) * (na * sx3b - nb * sx3a) * delta / nx
    }
    // This removes set b from a combined set, returning the sx4 of the remaining set a.
    // Note that na, sxa, sxxa, sx3a are all the values computed on the remaining set.  old_sx4 is the sx4 of the combined set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn remove_combined<T: FloatLike>(
        new_na: T,
        nb: T,
        new_sxa: T,
        sxb: T,
        new_sxxa: T,
        sxxb: T,
        new_sx3a: T,
        sx3b: T,
        old_sx4: T,
        sx4b: T,
    ) -> T {
        let nx = new_na + nb;
        let delta = sxb / nb - new_sxa / new_na;
        old_sx4
            - (sx4b
                + delta.powi(4) * new_na * nb * (new_na.powi(2) - new_na * nb + nb.powi(2))
                    / nx.powi(3)
                + T::lit(6.) * (new_na.powi(2) * sxxb + nb.powi(2) * new_sxxa) * delta.powi(2)
                    / nx.powi(2)
                + T::lit(4.) * (new_na * sx3b - nb * new_sx3a) * delta / nx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twofloat::TwoFloat;

    #[test]
    fn floatlike_lit() {
        assert_eq!(f64::lit(3.), 3.);
        assert_eq!(TwoFloat::lit(3.), TwoFloat::new_add(3., 0.));
    }
}
