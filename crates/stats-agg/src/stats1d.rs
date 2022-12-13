use crate::{m3, m4, FloatLike, StatsError, INV_FLOATING_ERROR_THRESHOLD};
use serde::{Deserialize, Serialize};
use twofloat::TwoFloat;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct StatsSummary1D<T: FloatLike> {
    pub n: u64,
    pub sx: T,
    pub sx2: T,
    pub sx3: T,
    pub sx4: T,
}
impl<T> Default for StatsSummary1D<T>
where
    T: FloatLike,
{
    fn default() -> Self {
        Self::new()
    }
}

// can't make this impl generic without conflicting with the stdlib implementation of From<T> for T
impl From<StatsSummary1D<f64>> for StatsSummary1D<TwoFloat> {
    fn from(input_summary: StatsSummary1D<f64>) -> Self {
        StatsSummary1D {
            n: input_summary.n,
            sx: input_summary.sx.into(),
            sx2: input_summary.sx2.into(),
            sx3: input_summary.sx3.into(),
            sx4: input_summary.sx4.into(),
        }
    }
}
pub fn convert_tf_to_f64(tf: TwoFloat) -> f64 {
    tf.hi() + tf.lo()
}
impl From<StatsSummary1D<TwoFloat>> for StatsSummary1D<f64> {
    fn from(input_summary: StatsSummary1D<TwoFloat>) -> Self {
        StatsSummary1D {
            n: input_summary.n,
            sx: input_summary.sx.into(),
            sx2: input_summary.sx2.into(),
            sx3: input_summary.sx3.into(),
            sx4: input_summary.sx4.into(),
        }
    }
}

impl<T> StatsSummary1D<T>
where
    T: FloatLike,
{
    fn n64(&self) -> T {
        T::from_u64(self.n)
    }

    pub fn new() -> Self {
        StatsSummary1D {
            n: 0,
            sx: T::zero(),
            sx2: T::zero(),
            sx3: T::zero(),
            sx4: T::zero(),
        }
    }

    // we use the Youngs-Cramer method for accumulating the values here to allow for easy computation of variance etc in a numerically robust way.
    // for this part, we've essentially copied the Postgres implementation found: // https://github.com/postgres/postgres/blob/8bdd6f563aa2456de602e78991e6a9f61b8ec86d/src/backend/utils/adt/float.c#L2813
    // Note that the Youngs-Cramer method relies on the sum((x - Sx/n)^2) for which they derive a recurrence relation which is reflected in the algorithm here:
    // the recurrence relation is: sum((x - Sx/n)^2) = Sxx = Sxx_n-1 + 1/(n(n-1)) * (nx - Sx)^2
    pub fn accum(&mut self, p: T) -> Result<(), StatsError> {
        let old = *self;
        self.n += 1;
        self.sx += p;
        if old.n > 0 {
            let tmpx = p * self.n64() - self.sx;
            let scale = T::one() / (self.n64() * old.n64());
            self.sx2 += tmpx * tmpx * scale;
            self.sx3 = m3::accum(old.n64(), old.sx, old.sx2, old.sx3, p);
            self.sx4 = m4::accum(old.n64(), old.sx, old.sx2, old.sx3, old.sx4, p);

            if self.has_infinite() {
                if self.check_overflow(&old, p) {
                    return Err(StatsError::DoubleOverflow);
                }
                // sxx should be set to NaN if any of its inputs are
                // infinite, so if they ended up as infinite and there wasn't an overflow,
                // we need to set them to NaN instead as this implies that there was an
                // infinite input (because they necessarily involve multiplications of
                // infinites, which are NaNs)
                if self.sx2.is_infinite() {
                    self.sx2 = T::nan();
                }
                if self.sx3.is_infinite() {
                    self.sx3 = T::nan();
                }
                if self.sx4.is_infinite() {
                    self.sx4 = T::nan();
                }
            }
        } else {
            // first input, leave sxx alone unless we have infinite inputs
            if !p.is_finite() {
                self.sx2 = T::nan();
                self.sx3 = T::nan();
                self.sx4 = T::nan();
            }
        }
        Result::Ok(())
    }

    fn has_infinite(&self) -> bool {
        self.sx.is_infinite()
            || self.sx2.is_infinite()
            || self.sx3.is_infinite()
            || self.sx4.is_infinite()
    }

    fn check_overflow(&self, old: &Self, p: T) -> bool {
        //Only report overflow if we have finite inputs that lead to infinite results.
        self.has_infinite() && old.sx.is_finite() && p.is_finite()
    }

    // inverse transition function (inverse of accum) for windowed aggregates, return None if we want to re-calculate from scratch
    // we won't modify in place here because of that return bit, it might be that we want to modify accum to also
    // copy just for symmetry.
    // Assumption: no need for Result/error possibility because we can't overflow, as we are doing an inverse operation of something that already happened, so if it worked forward, it should work in reverse?
    // We're extending the Youngs Cramer algorithm here with the algebraic transformation to figure out the reverse calculations.
    // This goes beyond what the PG code does, and is our extension for performance in windowed calculations.

    // There is a case where the numerical error can get very large that we will try to avoid: if we have an outlier value that is much larger than the surrounding values
    // we can get something like: v1 + v2 + v3 + ... vn = outlier + v1 + v2 + v3 + ... + vn - outlier when the outlier is removed from the window. This will cause significant error in the
    // resulting calculation of v1 + ... + vn, more than we're comfortable with, so we'll return None in that case which will force recalculation from scratch of v1 + ... + vn.

    // Algebra for removal:
    // n = n_old + 1 -> n_old = n - 1
    // Sx = Sx_old + x -> Sx_old = Sx - x
    // sum((x - Sx/n)^2) = Sxx = Sxx_old + 1/(n * n_old) * (nx - Sx)^2  -> Sxx_old = Sxx - 1/(n * n_old) * (nx - Sx)^2

    pub fn remove(&self, p: T) -> Option<Self> {
        // if we are trying to remove a nan/infinite input, it's time to recalculate.
        if p.is_nan() || p.is_infinite() {
            return None;
        }
        // if we are removing a value that is very large compared to the sum of the values that we're removing it from,
        // we should probably recalculate to avoid accumulating error. We might want a different test for this, if there
        // is a  way to calculate the error directly, that might be best...
        if p / self.sx > <T as From<f64>>::from(INV_FLOATING_ERROR_THRESHOLD) {
            return None;
        }

        // we can't have an initial value of n = 0 if we're removing something...
        if self.n == 0 {
            panic!(); //perhaps we should do error handling here, but I think this is reasonable as we are assuming that the removal is of an already-added item in the rest of this
        }

        if self.n == 1 {
            return Some(StatsSummary1D::new());
        }

        let mut new = StatsSummary1D {
            n: self.n - 1,
            sx: self.sx - p,
            sx2: T::zero(), // initialize this for now.
            sx3: T::zero(), // initialize this for now.
            sx4: T::zero(), // initialize this for now.
        };

        let tmpx = p * self.n64() - self.sx;
        let scale = (self.n64() * new.n64()).recip();
        new.sx2 = self.sx2 - tmpx * tmpx * scale;
        new.sx3 = m3::remove(new.n64(), new.sx, new.sx2, self.sx3, p);
        new.sx4 = m4::remove(new.n64(), new.sx, new.sx2, new.sx3, self.sx4, p);

        Some(new)
    }

    // convenience function for creating an aggregate from a vector, currently used mostly for testing.
    pub fn new_from_vec(v: Vec<T>) -> Result<Self, StatsError> {
        let mut r = StatsSummary1D::new();
        for p in v {
            r.accum(p)?;
        }
        Result::Ok(r)
    }

    pub fn combine(&self, other: Self) -> Result<Self, StatsError> {
        // TODO: think about whether we want to just modify &self in place here for perf
        // reasons. This is also a set of weird questions around the Rust compiler, so
        // easier to just add the copy trait here, may need to adjust or may make things
        // harder if we do generics.
        if self.n == 0 && other.n == 0 {
            return Ok(StatsSummary1D::new());
        } else if self.n == 0 {
            // handle the trivial n = 0 cases here, and don't worry about divide by zero errors later.
            return Ok(other);
        } else if other.n == 0 {
            return Ok(*self);
        }
        let tmp = self.sx / self.n64() - other.sx / other.n64();
        let n = self.n + other.n;
        let r = StatsSummary1D {
            n,
            sx: self.sx + other.sx,
            sx2: self.sx2
                + other.sx2
                + self.n64() * other.n64() * tmp * tmp
                    / <T as num_traits::cast::NumCast>::from(n).unwrap(),
            sx3: m3::combine(
                self.n64(),
                other.n64(),
                self.sx,
                other.sx,
                self.sx2,
                other.sx2,
                self.sx3,
                other.sx3,
            ),
            sx4: m4::combine(
                self.n64(),
                other.n64(),
                self.sx,
                other.sx,
                self.sx2,
                other.sx2,
                self.sx3,
                other.sx3,
                self.sx4,
                other.sx4,
            ),
        };
        if r.has_infinite() && !self.has_infinite() && !other.has_infinite() {
            return Err(StatsError::DoubleOverflow);
        }
        Ok(r)
    }

    // This is the inverse combine function for use in the window function context when we want to reverse the operation of the normal combine function
    // for re-aggregation over a window, this is what will get called in tumbling window averages for instance.
    // As with any window function, returning None will cause a re-calculation, so we do that in several cases where either we're dealing with infinites or we have some potential problems with outlying sums
    // so here, self is the previously combined StatsSummary, and we're removing the input and returning the part that would have been there before.
    pub fn remove_combined(&self, remove: Self) -> Option<Self> {
        let combined = &self; // just to lessen confusion with naming
                              // handle the trivial n = 0 and equal n cases here, and don't worry about divide by zero errors later.
        if combined.n == remove.n {
            return Some(StatsSummary1D::new());
        } else if remove.n == 0 {
            return Some(*self);
        } else if combined.n < remove.n {
            panic!(); // given that we're always removing things that we've previously added, we shouldn't be able to get a case where we're removing an n that's larger.
        }
        // if the sum we're removing is very large compared to the overall value we need to recalculate, see note on the remove function
        if remove.sx / combined.sx > <T as From<f64>>::from(INV_FLOATING_ERROR_THRESHOLD) {
            return None;
        }
        let mut part = StatsSummary1D {
            n: combined.n - remove.n,
            sx: combined.sx - remove.sx,
            sx2: T::zero(), //just initialize this, for now.
            sx3: T::zero(), //just initialize this, for now.
            sx4: T::zero(), //just initialize this, for now.
        };
        let tmp = part.sx / part.n64() - remove.sx / remove.n64(); //gets squared so order doesn't matter
        part.sx2 =
            combined.sx2 - remove.sx2 - part.n64() * remove.n64() * tmp * tmp / combined.n64();
        part.sx3 = m3::remove_combined(
            part.n64(),
            remove.n64(),
            part.sx,
            remove.sx,
            part.sx2,
            remove.sx2,
            self.sx3,
            remove.sx3,
        );
        part.sx4 = m4::remove_combined(
            part.n64(),
            remove.n64(),
            part.sx,
            remove.sx,
            part.sx2,
            remove.sx2,
            part.sx3,
            remove.sx3,
            self.sx4,
            remove.sx4,
        );

        Some(part)
    }

    pub fn avg(&self) -> Option<T> {
        if self.n == 0 {
            return None;
        }
        Some(self.sx / self.n64())
    }

    pub fn count(&self) -> i64 {
        self.n as i64
    }

    pub fn sum(&self) -> Option<T> {
        if self.n == 0 {
            return None;
        }
        Some(self.sx)
    }

    pub fn var_pop(&self) -> Option<T> {
        if self.n == 0 {
            return None;
        }
        Some(self.sx2 / self.n64())
    }

    pub fn var_samp(&self) -> Option<T> {
        if self.n == 0 {
            return None;
        }
        Some(self.sx2 / (self.n64() - T::one()))
    }

    pub fn stddev_pop(&self) -> Option<T> {
        Some(self.var_pop()?.sqrt())
    }

    pub fn stddev_samp(&self) -> Option<T> {
        Some(self.var_samp()?.sqrt())
    }

    pub fn skewness_pop(&self) -> Option<T> {
        Some(self.sx3 / self.n64() / self.stddev_pop()?.powi(3))
    }

    pub fn skewness_samp(&self) -> Option<T> {
        Some(self.sx3 / (self.n64() - T::one()) / self.stddev_samp()?.powi(3))
    }

    pub fn kurtosis_pop(&self) -> Option<T> {
        Some(self.sx4 / self.n64() / self.stddev_pop()?.powi(4))
    }

    pub fn kurtosis_samp(&self) -> Option<T> {
        Some(self.sx4 / (self.n64() - T::one()) / self.stddev_samp()?.powi(4))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    fn tf(f: f64) -> TwoFloat {
        TwoFloat::new_add(f, 0.0)
    }

    #[track_caller]
    fn assert_close_enough(s1: &StatsSummary1D<f64>, s2: &StatsSummary1D<f64>) {
        assert_eq!(s1.n, s2.n);
        assert_relative_eq!(s1.sx, s2.sx);
        assert_relative_eq!(s1.sx2, s2.sx2);
        assert_relative_eq!(s1.sx3, s2.sx3);
        assert_relative_eq!(s1.sx4, s2.sx4);
    }

    #[track_caller]
    fn assert_close_enough_tf(s1: &StatsSummary1D<TwoFloat>, s2: &StatsSummary1D<TwoFloat>) {
        assert_eq!(s1.n, s2.n);
        assert!((s1.sx - s2.sx).abs() < 10.0 * f64::EPSILON);
        assert!((s1.sx2 - s2.sx2).abs() < 10.0 * f64::EPSILON);
        assert!((s1.sx3 - s2.sx3).abs() < 10.0 * f64::EPSILON);
        assert!((s1.sx4 - s2.sx4).abs() < 10.0 * f64::EPSILON);
    }

    #[test]
    fn test_against_known_vals() {
        let p = StatsSummary1D::new_from_vec(vec![7.0, 18.0, -2.0, 5.0, 3.0]).unwrap();

        assert_eq!(p.n, 5);
        assert_relative_eq!(p.sx, 31.);
        assert_relative_eq!(p.sx2, 218.8);
        assert_relative_eq!(p.sx3, 1057.68);
        assert_relative_eq!(p.sx4, 24016.336);

        let p = p.remove(18.0).unwrap();

        assert_eq!(p.n, 4);
        assert_relative_eq!(p.sx, 13.);
        assert_relative_eq!(p.sx2, 44.75);
        assert_relative_eq!(p.sx3, -86.625);
        assert_relative_eq!(p.sx4, 966.8281249999964);

        let p = p
            .combine(StatsSummary1D::new_from_vec(vec![0.5, 11.0, 6.123]).unwrap())
            .unwrap();

        assert_eq!(p.n, 7);
        assert_relative_eq!(p.sx, 30.623);
        assert_relative_eq!(p.sx2, 111.77425342857143);
        assert_relative_eq!(p.sx3, -5.324891254897949);
        assert_relative_eq!(p.sx4, 3864.054085451184);

        let p = p
            .remove_combined(StatsSummary1D::new_from_vec(vec![5.0, 11.0, 3.0]).unwrap())
            .unwrap();

        assert_eq!(p.n, 4);
        assert_relative_eq!(p.sx, 11.623);
        assert_relative_eq!(p.sx2, 56.96759675000001);
        assert_relative_eq!(p.sx3, -30.055041237374915);
        assert_relative_eq!(p.sx4, 1000.8186787745212);
    }

    #[test]
    fn test_against_known_vals_tf() {
        let p = StatsSummary1D::new_from_vec(vec![tf(7.0), tf(18.0), tf(-2.0), tf(5.0), tf(3.0)])
            .unwrap();

        assert_eq!(p.n, 5);
        assert_relative_eq!(Into::<f64>::into(p.sx), 31.);
        assert_relative_eq!(Into::<f64>::into(p.sx2), 218.8);
        assert_relative_eq!(Into::<f64>::into(p.sx3), 1057.68);
        assert_relative_eq!(Into::<f64>::into(p.sx4), 24016.336);

        let p = p.remove(tf(18.0)).unwrap();

        assert_eq!(p.n, 4);
        assert_relative_eq!(Into::<f64>::into(p.sx), 13.);
        // value is slightly off
        assert_relative_eq!(Into::<f64>::into(p.sx2), 44.75, epsilon = 0.000000000001);
        assert_relative_eq!(Into::<f64>::into(p.sx3), -86.625, epsilon = 0.000000000001);
        assert_relative_eq!(
            Into::<f64>::into(p.sx4),
            966.8281249999964,
            epsilon = 0.000000000001
        );

        let p = p
            .combine(StatsSummary1D::new_from_vec(vec![tf(0.5), tf(11.0), tf(6.123)]).unwrap())
            .unwrap();

        assert_eq!(p.n, 7);
        assert_relative_eq!(Into::<f64>::into(p.sx), 30.623);
        assert_relative_eq!(Into::<f64>::into(p.sx2), 111.77425342857143);
        // slight difference in values here – not sure if twofloat or f64 is more accurate
        assert_relative_eq!(
            Into::<f64>::into(p.sx3),
            -5.324891254897949,
            epsilon = 0.0000000001
        );
        assert_relative_eq!(
            Into::<f64>::into(p.sx4),
            3864.054085451184,
            epsilon = 0.0000000001
        );

        let p = p
            .remove_combined(
                StatsSummary1D::new_from_vec(vec![tf(5.0), tf(11.0), tf(3.0)]).unwrap(),
            )
            .unwrap();

        assert_eq!(p.n, 4);
        assert_relative_eq!(Into::<f64>::into(p.sx), 11.623);
        // f64 gets this slightly over, TF gets this slightly under
        assert_relative_eq!(
            Into::<f64>::into(p.sx2),
            56.96759675000001,
            epsilon = 0.000000000001
        );
        // slight difference in values here – not sure if twofloat or f64 is more accurate
        assert_relative_eq!(
            Into::<f64>::into(p.sx3),
            -30.055041237374915,
            epsilon = 0.0000000001
        );
        assert_relative_eq!(
            Into::<f64>::into(p.sx4),
            1000.8186787745212,
            epsilon = 0.0000000001
        );
    }

    #[test]
    fn test_combine() {
        let p = StatsSummary1D::new_from_vec(vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        let q = StatsSummary1D::new_from_vec(vec![1.0, 2.0]).unwrap();
        let r = StatsSummary1D::new_from_vec(vec![3.0, 4.0]).unwrap();
        assert_close_enough(&q.combine(r).unwrap(), &p);

        let p = StatsSummary1D::new_from_vec(vec![tf(1.0), tf(2.0), tf(3.0), tf(4.0)]).unwrap();
        let q = StatsSummary1D::new_from_vec(vec![tf(1.0), tf(2.0)]).unwrap();
        let r = StatsSummary1D::new_from_vec(vec![tf(3.0), tf(4.0)]).unwrap();
        assert_close_enough_tf(&q.combine(r).unwrap(), &p);
    }
}
