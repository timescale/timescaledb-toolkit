// 2D stats are based on the Youngs-Cramer implementation in PG here:
// https://github.com/postgres/postgres/blob/472e518a44eacd9caac7d618f1b6451672ca4481/src/backend/utils/adt/float.c#L3260
use crate::{m3, m4, FloatLike, StatsError, XYPair, INV_FLOATING_ERROR_THRESHOLD};
use serde::{Deserialize, Serialize};
use twofloat::TwoFloat;

mod stats2d_flat_serialize;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct StatsSummary2D<T: FloatLike> {
    pub n: u64, // count
    pub sx: T,  // sum(x)
    pub sx2: T, // sum((x-sx/n)^2) (sum of squares)
    pub sx3: T, // sum((x-sx/n)^3)
    pub sx4: T, // sum((x-sx/n)^4)
    pub sy: T,  // sum(y)
    pub sy2: T, // sum((y-sy/n)^2) (sum of squares)
    pub sy3: T, // sum((y-sy/n)^3)
    pub sy4: T, // sum((y-sy/n)^4)
    pub sxy: T, // sum((x-sx/n)*(y-sy/n)) (sum of products)
}

impl From<StatsSummary2D<TwoFloat>> for StatsSummary2D<f64> {
    fn from(input_summary: StatsSummary2D<TwoFloat>) -> Self {
        StatsSummary2D {
            n: input_summary.n,
            sx: input_summary.sx.into(),
            sx2: input_summary.sx2.into(),
            sx3: input_summary.sx3.into(),
            sx4: input_summary.sx4.into(),
            sy: input_summary.sy.into(),
            sy2: input_summary.sy2.into(),
            sy3: input_summary.sy3.into(),
            sy4: input_summary.sy4.into(),
            sxy: input_summary.sxy.into(),
        }
    }
}

impl<T: FloatLike> Default for StatsSummary2D<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: FloatLike> StatsSummary2D<T> {
    pub fn new() -> Self {
        StatsSummary2D {
            n: 0,
            sx: T::zero(),
            sx2: T::zero(),
            sx3: T::zero(),
            sx4: T::zero(),
            sy: T::zero(),
            sy2: T::zero(),
            sy3: T::zero(),
            sy4: T::zero(),
            sxy: T::zero(),
        }
    }

    fn n64(&self) -> T {
        T::from_u64(self.n)
    }
    /// accumulate an XYPair into a StatsSummary2D
    /// ```
    /// use stats_agg::*;
    /// use stats_agg::stats2d::*;
    /// let mut p = StatsSummary2D::new();
    /// p.accum(XYPair{x:1.0, y:1.0,}).unwrap();
    /// p.accum(XYPair{x:2.0, y:2.0,}).unwrap();
    /// //we can add in infinite values and it will handle it properly.
    /// p.accum(XYPair{x:f64::INFINITY, y:1.0}).unwrap();
    /// assert_eq!(p.sum().unwrap().x, f64::INFINITY);
    /// assert!(p.sum_squares().unwrap().x.is_nan()); // this is NaN because it involves multiplication of two infinite values
    ///
    /// assert_eq!(p.accum(XYPair{y:f64::MAX, x:1.0,}), Err(StatsError::DoubleOverflow)); // we do error if we actually overflow however
    ///
    ///```
    pub fn accum(&mut self, p: XYPair<T>) -> Result<(), StatsError> {
        let old = *self;
        self.n += 1;
        self.sx += p.x;
        self.sy += p.y;
        if old.n > 0 {
            let tmpx = p.x * self.n64() - self.sx;
            let tmpy = p.y * self.n64() - self.sy;
            let scale = (self.n64() * old.n64()).recip();
            self.sx2 += tmpx * tmpx * scale;
            self.sx3 = m3::accum(old.n64(), old.sx, old.sx2, old.sx3, p.x);
            self.sx4 = m4::accum(old.n64(), old.sx, old.sx2, old.sx3, old.sx4, p.x);
            self.sy2 += tmpy * tmpy * scale;
            self.sy3 = m3::accum(old.n64(), old.sy, old.sy2, old.sy3, p.y);
            self.sy4 = m4::accum(old.n64(), old.sy, old.sy2, old.sy3, old.sy4, p.y);
            self.sxy += tmpx * tmpy * scale;
            if self.has_infinite() {
                if self.check_overflow(&old, p) {
                    return Err(StatsError::DoubleOverflow);
                }
                // sxx, syy, and sxy should be set to NaN if any of their inputs are
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
                if self.sy2.is_infinite() {
                    self.sy2 = T::nan();
                }
                if self.sy3.is_infinite() {
                    self.sy3 = T::nan();
                }
                if self.sy4.is_infinite() {
                    self.sy4 = T::nan();
                }
                if self.sxy.is_infinite() {
                    self.sxy = T::nan();
                }
            }
        } else {
            // first input, leave sxx/syy/sxy alone unless we have infinite inputs
            if !p.x.is_finite() {
                self.sx2 = T::nan();
                self.sx3 = T::nan();
                self.sx4 = T::nan();
                self.sxy = T::nan();
            }
            if !p.y.is_finite() {
                self.sy2 = T::nan();
                self.sy3 = T::nan();
                self.sy4 = T::nan();
                self.sxy = T::nan();
            }
        }
        Result::Ok(())
    }
    fn has_infinite(&self) -> bool {
        self.sx.is_infinite()
            || self.sx2.is_infinite()
            || self.sx3.is_infinite()
            || self.sx4.is_infinite()
            || self.sy.is_infinite()
            || self.sy2.is_infinite()
            || self.sy3.is_infinite()
            || self.sy4.is_infinite()
            || self.sxy.is_infinite()
    }
    fn check_overflow(&self, old: &StatsSummary2D<T>, p: XYPair<T>) -> bool {
        //Only report overflow if we have finite inputs that lead to infinite results.
        ((self.sx.is_infinite()
            || self.sx2.is_infinite()
            || self.sx3.is_infinite()
            || self.sx4.is_infinite())
            && old.sx.is_finite()
            && p.x.is_finite())
            || ((self.sy.is_infinite()
                || self.sy2.is_infinite()
                || self.sy3.is_infinite()
                || self.sy4.is_infinite())
                && old.sy.is_finite()
                && p.y.is_finite())
            || (self.sxy.is_infinite()
                && old.sx.is_finite()
                && p.x.is_finite()
                && old.sy.is_finite()
                && p.y.is_finite())
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
    // Sy / Syy analogous
    // sum((x - Sx/n)(y - Sy/n)) = Sxy = Sxy_old + 1/(n * n_old) * (nx - Sx) * (ny - Sy)  -> Sxy_old = Sxy - 1/(n * n_old) * (nx - Sx) * (ny - Sy)
    pub fn remove(&self, p: XYPair<T>) -> Option<Self> {
        // if we are trying to remove a nan/infinite input, it's time to recalculate.
        if !p.x.is_finite() || !p.y.is_finite() {
            return None;
        }
        // if we are removing a value that is very large compared to the sum of the values that we're removing it from,
        // we should probably recalculate to avoid accumulating error. We might want a different test for this, if there
        // is a  way to calculate the error directly, that might be best...
        let thresh = <T as From<f64>>::from(INV_FLOATING_ERROR_THRESHOLD);
        if p.x / self.sx > thresh || p.y / self.sy > thresh {
            return None;
        }

        // we can't have an initial value of n = 0 if we're removing something...
        if self.n == 0 {
            panic!(); //perhaps we should do error handling here, but I think this is reasonable as we are assuming that the removal is of an already-added item in the rest of this
        }

        // if we're removing the last point we should just return a completely empty value to eliminate any errors, it can only be completely empty at that point.
        if self.n == 1 {
            return Some(StatsSummary2D::new());
        }

        let mut new = StatsSummary2D {
            n: self.n - 1,
            sx: self.sx - p.x,
            sy: self.sy - p.y,
            sx2: T::zero(), // initialize these for now.
            sx3: T::zero(),
            sx4: T::zero(),
            sy2: T::zero(),
            sy3: T::zero(),
            sy4: T::zero(),
            sxy: T::zero(),
        };
        let tmpx = p.x * self.n64() - self.sx;
        let tmpy = p.y * self.n64() - self.sy;
        let scale = (self.n64() * new.n64()).recip();
        new.sx2 = self.sx2 - tmpx * tmpx * scale;
        new.sx3 = m3::remove(new.n64(), new.sx, new.sx2, self.sx3, p.x);
        new.sx4 = m4::remove(new.n64(), new.sx, new.sx2, new.sx3, self.sx4, p.x);
        new.sy2 = self.sy2 - tmpy * tmpy * scale;
        new.sy3 = m3::remove(new.n64(), new.sy, new.sy2, self.sy3, p.y);
        new.sy4 = m4::remove(new.n64(), new.sy, new.sy2, new.sy3, self.sy4, p.y);
        new.sxy = self.sxy - tmpx * tmpy * scale;
        Some(new)
    }

    ///create a StatsSummary2D from a vector of XYPairs
    /// ```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let mut p = StatsSummary2D::new();
    /// p.accum(XYPair{x:1.0, y:1.0,}).unwrap();
    /// p.accum(XYPair{x:2.0, y:2.0,}).unwrap();
    /// p.accum(XYPair{x:3.0, y:3.0,}).unwrap();
    /// let q = StatsSummary2D::new_from_vec(vec![XYPair{x:1.0, y:1.0,}, XYPair{x:2.0, y:2.0,}, XYPair{x:3.0, y:3.0,}]).unwrap();
    /// assert_eq!(p, q);
    ///```
    pub fn new_from_vec(v: Vec<XYPair<T>>) -> Result<Self, StatsError> {
        let mut r = StatsSummary2D::new();
        for p in v {
            r.accum(p)?;
        }
        Result::Ok(r)
    }
    /// combine two StatsSummary2Ds
    /// ```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{x:1.0, y:1.0,}, XYPair{x:2.0, y:2.0,}, XYPair{x:3.0, y:3.0,}, XYPair{x:4.0, y:4.0,}]).unwrap();
    /// let q = StatsSummary2D::new_from_vec(vec![XYPair{x:1.0, y:1.0,}, XYPair{x:2.0, y:2.0,},]).unwrap();
    /// let r = StatsSummary2D::new_from_vec(vec![XYPair{x:3.0, y:3.0,}, XYPair{x:4.0, y:4.0,},]).unwrap();
    /// let r = r.combine(q).unwrap();
    /// assert_eq!(r, p);
    /// ```
    // we combine two StatsSummary2Ds via a generalization of the Youngs-Cramer algorithm, we follow what Postgres does here
    //      n = n1 + n2
    //      sx = sx1 + sx2
    //      sxx = sxx1 + sxx2 + n1 * n2 * (sx1/n1 - sx2/n2)^2 / n
    //      sy / syy analogous
    //      sxy = sxy1 + sxy2 + n1 * n2 * (sx1/n1 - sx2/n2) * (sy1/n1 - sy2/n2) / n
    pub fn combine(&self, other: StatsSummary2D<T>) -> Result<Self, StatsError> {
        // TODO: think about whether we want to just modify &self in place here for perf
        // reasons. This is also a set of weird questions around the Rust compiler, so
        // easier to just add the copy trait here, may need to adjust or may make things
        // harder if we do generics.
        if self.n == 0 && other.n == 0 {
            return Ok(StatsSummary2D::new());
        } else if self.n == 0 {
            // handle the trivial n = 0 cases here, and don't worry about divide by zero errors later.
            return Ok(other);
        } else if other.n == 0 {
            return Ok(*self);
        }
        let tmpx = self.sx / self.n64() - other.sx / other.n64();
        let tmpy = self.sy / self.n64() - other.sy / other.n64();
        let n = self.n + other.n;
        let r = StatsSummary2D {
            n,
            sx: self.sx + other.sx,
            sx2: self.sx2 + other.sx2 + self.n64() * other.n64() * tmpx * tmpx / T::from_u64(n),
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
            sy: self.sy + other.sy,
            sy2: self.sy2 + other.sy2 + self.n64() * other.n64() * tmpy * tmpy / T::from_u64(n),
            sy3: m3::combine(
                self.n64(),
                other.n64(),
                self.sy,
                other.sy,
                self.sy2,
                other.sy2,
                self.sy3,
                other.sy3,
            ),
            sy4: m4::combine(
                self.n64(),
                other.n64(),
                self.sy,
                other.sy,
                self.sy2,
                other.sy2,
                self.sy3,
                other.sy3,
                self.sy4,
                other.sy4,
            ),
            sxy: self.sxy + other.sxy + self.n64() * other.n64() * tmpx * tmpy / T::from_u64(n),
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
    pub fn remove_combined(&self, remove: StatsSummary2D<T>) -> Option<Self> {
        let combined = &self; // just to lessen confusion with naming
                              // handle the trivial n = 0 and equal n cases here, and don't worry about divide by zero errors later.
        if combined.n == remove.n {
            return Some(StatsSummary2D::new());
        } else if remove.n == 0 {
            return Some(*self);
        } else if combined.n < remove.n {
            panic!(); //  given that we're always removing things that we've previously added, we shouldn't be able to get a case where we're removing an n that's larger.
        }
        // if the sum we're removing is very large compared to the overall value we need to recalculate, see note on the remove function
        let thresh = <T as From<f64>>::from(INV_FLOATING_ERROR_THRESHOLD);
        if remove.sx / combined.sx > thresh || remove.sy / combined.sy > thresh {
            return None;
        }
        let mut part = StatsSummary2D {
            n: combined.n - remove.n,
            sx: combined.sx - remove.sx,
            sy: combined.sy - remove.sy,
            sx2: T::zero(), //just initialize these, for now.
            sx3: T::zero(),
            sx4: T::zero(),
            sy2: T::zero(),
            sy3: T::zero(),
            sy4: T::zero(),
            sxy: T::zero(),
        };
        let tmpx = part.sx / part.n64() - remove.sx / remove.n64(); //gets squared so order doesn't matter
        let tmpy = part.sy / part.n64() - remove.sy / remove.n64();
        part.sx2 =
            combined.sx2 - remove.sx2 - part.n64() * remove.n64() * tmpx * tmpx / combined.n64();
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
        part.sy2 =
            combined.sy2 - remove.sy2 - part.n64() * remove.n64() * tmpy * tmpy / combined.n64();
        part.sy3 = m3::remove_combined(
            part.n64(),
            remove.n64(),
            part.sy,
            remove.sy,
            part.sy2,
            remove.sy2,
            self.sy3,
            remove.sy3,
        );
        part.sy4 = m4::remove_combined(
            part.n64(),
            remove.n64(),
            part.sy,
            remove.sy,
            part.sy2,
            remove.sy2,
            part.sy3,
            remove.sy3,
            self.sy4,
            remove.sy4,
        );
        part.sxy =
            combined.sxy - remove.sxy - part.n64() * remove.n64() * tmpx * tmpy / combined.n64();
        Some(part)
    }

    /// offsets all values accumulated in a StatsSummary2D by a given amount in X & Y. This
    /// only works if all values are offset by that amount. This is used for allowing
    /// relative calculations in a local region and then allowing them to be combined with
    /// other regions where all points are offset by the same amount. The main use case
    /// for now is in the counter case where, when partials are combined you can get a new
    /// offset for all points in the counter.
    // Note that when offsetting, the offset of the previous partial  be multiplied by N and added to the Sy value. All the other values are
    // unaffected because they rely on the expression (Y-Sy/N), (and analogous for the X values) which is basically each value subtracted from the
    // average of all values and if all values are shifted by a constant, then the average shifts by the same constant so it cancels out:
    // i.e. If a constant C is added to each Y, then (Y-Sy/N) reduces back to itself as follows:
    //(Y + C) - (Sy + NC)/N
    // Y + C - Sy/N - NC/N
    // Y + C - Sy/N - C
    // Y - Sy/N
    pub fn offset(&mut self, offset: XYPair<T>) -> Result<(), StatsError> {
        self.sx += self.n64() * offset.x;
        self.sy += self.n64() * offset.y;
        if self.has_infinite() && offset.x.is_finite() && offset.y.is_finite() {
            return Err(StatsError::DoubleOverflow);
        }
        Ok(())
    }

    //TODO: Add tests for offsets

    ///returns the sum of squares of both the independent (x) and dependent (y) variables
    ///as an XYPair, where the sum of squares is defined as: sum(x^2) - sum(x)^2 / n)
    ///```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let ssx = (1.0_f64.powi(2) + 2.0_f64.powi(2) + 3.0_f64.powi(2)) - (1.0+2.0+3.0_f64).powi(2)/3.0;
    /// let ssy = (2.0_f64.powi(2) + 4.0_f64.powi(2) + 6.0_f64.powi(2)) - (2.0+4.0+6.0_f64).powi(2)/3.0;
    /// let ssp = p.sum_squares().unwrap();
    /// assert_eq!(ssp.x, ssx);
    /// assert_eq!(ssp.y, ssy);
    /// //empty StatsSummary2Ds return None
    /// assert!(StatsSummary2D::<f64>::new().sum_squares().is_none());
    /// ```
    pub fn sum_squares(&self) -> Option<XYPair<T>> {
        if self.n == 0 {
            return None;
        }
        Some(XYPair {
            x: self.sx2,
            y: self.sy2,
        })
    }
    ///returns the "sum of products" of the dependent * independent variables sum(x * y) - sum(x) * sum(y) / n
    ///```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = (2.0 * 1.0 + 4.0 * 2.0 + 6.0 * 3.0) - (2.0 + 4.0 + 6.0)*(1.0 + 2.0 + 3.0)/3.0;
    /// assert_eq!(p.sumxy().unwrap(), s);
    /// //empty StatsSummary2Ds return None
    /// assert!(StatsSummary2D::<f64>::new().sumxy().is_none());
    /// ```
    pub fn sumxy(&self) -> Option<T> {
        if self.n == 0 {
            return None;
        }
        Some(self.sxy)
    }
    ///returns the averages of the x and y variables
    ///```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let avgx = (1.0 + 2.0 + 3.0)/3.0;
    /// let avgy = (2.0 + 4.0 + 6.0)/3.0;
    /// let avgp = p.avg().unwrap();
    /// assert_eq!(avgp.x, avgx);
    /// assert_eq!(avgp.y, avgy);
    /// //empty StatsSummary2Ds return None
    /// assert!(StatsSummary2D::<f64>::new().avg().is_none());
    /// ```
    pub fn avg(&self) -> Option<XYPair<T>> {
        if self.n == 0 {
            return None;
        }
        Some(XYPair {
            x: self.sx / self.n64(),
            y: self.sy / self.n64(),
        })
    }
    ///returns the count of inputs as an i64
    ///```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;

    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = 3;
    /// assert_eq!(p.count(), s);
    /// //empty StatsSummary2Ds return 0 count
    /// assert_eq!(StatsSummary2D::<f64>::new().count(), 0);
    /// ```
    pub fn count(&self) -> i64 {
        self.n as i64
    }
    ///returns the sums of x and y as an XYPair
    ///```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let sumx = (1.0 + 2.0 + 3.0);
    /// let sumy = (2.0 + 4.0 + 6.0);
    /// let sump = p.sum().unwrap();
    /// assert_eq!(sump.x, sumx);
    /// assert_eq!(sump.y, sumy);
    /// //empty StatsSummary2Ds return None
    /// assert!(StatsSummary2D::<f64>::new().sum().is_none());
    /// ```
    pub fn sum(&self) -> Option<XYPair<T>> {
        if self.n == 0 {
            return None;
        }
        Some(XYPair {
            x: self.sx,
            y: self.sy,
        })
    }

    pub fn var_pop(&self) -> Option<XYPair<T>> {
        if self.n == 0 {
            return None;
        }
        Some(XYPair {
            x: self.sx2 / self.n64(),
            y: self.sy2 / self.n64(),
        })
    }

    pub fn var_samp(&self) -> Option<XYPair<T>> {
        if self.n <= 1 {
            return None;
        }
        Some(XYPair {
            x: self.sx2 / (self.n64() - T::one()),
            y: self.sy2 / (self.n64() - T::one()),
        })
    }

    ///returns the population standard deviation of both the independent and dependent variables as an XYPair
    pub fn stddev_pop(&self) -> Option<XYPair<T>> {
        let var = self.var_pop()?;
        Some(XYPair {
            x: var.x.sqrt(),
            y: var.y.sqrt(),
        })
    }

    ///returns the sample standard deviation of both the independent and dependent variables as an XYPair
    pub fn stddev_samp(&self) -> Option<XYPair<T>> {
        let var = self.var_samp()?;
        Some(XYPair {
            x: var.x.sqrt(),
            y: var.y.sqrt(),
        })
    }

    pub fn skewness_pop(&self) -> Option<XYPair<T>> {
        let stddev = self.stddev_pop()?;
        Some(XYPair {
            x: self.sx3 / self.n64() / stddev.x.powi(3),
            y: self.sy3 / self.n64() / stddev.y.powi(3),
        })
    }

    pub fn skewness_samp(&self) -> Option<XYPair<T>> {
        let stddev = self.stddev_samp()?;
        Some(XYPair {
            x: self.sx3 / (self.n64() - T::one()) / stddev.x.powi(3),
            y: self.sy3 / (self.n64() - T::one()) / stddev.y.powi(3),
        })
    }

    pub fn kurtosis_pop(&self) -> Option<XYPair<T>> {
        let stddev = self.stddev_pop()?;
        Some(XYPair {
            x: self.sx4 / self.n64() / stddev.x.powi(4),
            y: self.sy4 / self.n64() / stddev.y.powi(4),
        })
    }

    pub fn kurtosis_samp(&self) -> Option<XYPair<T>> {
        let stddev = self.stddev_samp()?;
        Some(XYPair {
            x: self.sx4 / (self.n64() - T::one()) / stddev.x.powi(4),
            y: self.sy4 / (self.n64() - T::one()) / stddev.y.powi(4),
        })
    }

    /// returns the correlation coefficient, which is the covariance / (stddev(x) * stddev(y))
    /// Note that it makes no difference whether we choose the sample or
    /// population covariance and stddev, because we end up with a canceling n or n-1 term. This
    /// also allows us to reduce our calculation to the sumxy / sqrt(sum_squares(x)*sum_squares(y))
    pub fn corr(&self) -> Option<T> {
        // empty StatsSummary2Ds, horizontal or vertical lines should return None
        if self.n == 0 || self.sx2 == T::zero() || self.sy2 == T::zero() {
            return None;
        }
        Some(self.sxy / (self.sx2 * self.sy2).sqrt())
    }

    /// returns the slope of the least squares fit line
    pub fn slope(&self) -> Option<T> {
        // the case of a single point will usually be triggered by the the second branch of this (which is also a test for a vertical line)
        //however, in cases where we had an infinite input, we will end up with NaN which is the expected behavior.
        if self.n == 0 || self.sx2 == T::zero() {
            return None;
        }
        Some(self.sxy / self.sx2)
    }

    /// returns the intercept of the least squares fit line
    pub fn intercept(&self) -> Option<T> {
        if self.n == 0 || self.sx2 == T::zero() {
            return None;
        }
        Some((self.sy - self.sx * self.sxy / self.sx2) / self.n64())
    }

    /// returns the x intercept of the least squares fit line
    // y = mx + b (y = 0)
    // -b = mx
    // x = -b / m
    pub fn x_intercept(&self) -> Option<T> {
        // vertical line does have an x intercept
        if self.n > 1 && self.sx2 == T::zero() {
            return Some(self.sx / self.n64());
        }
        // horizontal lines have no x intercepts
        if self.sy2 == T::zero() {
            return None;
        }
        Some(-self.intercept()? / self.slope()?)
    }

    /// returns the square of the correlation coefficent (aka the coefficient of determination)
    pub fn determination_coeff(&self) -> Option<T> {
        if self.n == 0 || self.sx2 == T::zero() {
            return None;
        }
        //horizontal lines return 1.0 error
        if self.sy2 == T::zero() {
            return Some(T::one());
        }
        Some(self.sxy * self.sxy / (self.sx2 * self.sy2))
    }

    ///returns the sample covariance: (sumxy()/n-1)
    ///```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = (2.0 * 1.0 + 4.0 * 2.0 + 6.0 * 3.0) - (2.0 + 4.0 + 6.0)*(1.0 + 2.0 + 3.0)/3.0;
    /// let s = s/2.0;
    /// assert_eq!(p.covar_samp().unwrap(), s);
    /// //empty StatsSummary2Ds return None
    /// assert!(StatsSummary2D::<f64>::new().covar_samp().is_none());
    /// ```
    pub fn covar_samp(&self) -> Option<T> {
        if self.n <= 1 {
            return None;
        }
        Some(self.sxy / (self.n64() - T::one()))
    }

    ///returns the population covariance: (sumxy()/n)
    ///```
    /// use stats_agg::stats2d::StatsSummary2D;
    /// use stats_agg::XYPair;
    /// let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = (2.0 * 1.0 + 4.0 * 2.0 + 6.0 * 3.0) - (2.0 + 4.0 + 6.0)*(1.0 + 2.0 + 3.0)/3.0;
    /// let s = s/3.0;
    /// assert_eq!(p.covar_pop().unwrap(), s);
    /// //empty StatsSummary2Ds return None
    /// assert!(StatsSummary2D::<f64>::new().covar_pop().is_none());
    /// ```
    pub fn covar_pop(&self) -> Option<T> {
        if self.n == 0 {
            return None;
        }
        Some(self.sxy / self.n64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tf(f: f64) -> TwoFloat {
        TwoFloat::new_add(f, 0.0)
    }

    #[test]
    fn test_linear() {
        let p = StatsSummary2D::new_from_vec(vec![
            XYPair { y: 2.0, x: 1.0 },
            XYPair { y: 4.0, x: 2.0 },
            XYPair { y: 6.0, x: 3.0 },
        ])
        .unwrap();
        assert_eq!(p.slope().unwrap(), 2.0);
        assert_eq!(p.intercept().unwrap(), 0.0);
        assert_eq!(p.x_intercept().unwrap(), 0.0);

        let p = StatsSummary2D::new_from_vec(vec![
            XYPair { y: 2.0, x: 2.0 },
            XYPair { y: 4.0, x: 3.0 },
            XYPair { y: 6.0, x: 4.0 },
        ])
        .unwrap();
        assert_eq!(p.slope().unwrap(), 2.0);
        assert_eq!(p.intercept().unwrap(), -2.0);
        assert_eq!(p.x_intercept().unwrap(), 1.0);

        // empty
        let p: StatsSummary2D<f64> = StatsSummary2D::new();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept(), None);
        // singleton
        let p = StatsSummary2D::new_from_vec(vec![XYPair { y: 2.0, x: 2.0 }]).unwrap();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept(), None);
        //vertical
        let p = StatsSummary2D::new_from_vec(vec![
            XYPair { y: 2.0, x: 2.0 },
            XYPair { y: 4.0, x: 2.0 },
        ])
        .unwrap();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept().unwrap(), 2.0);
        //horizontal
        let p = StatsSummary2D::new_from_vec(vec![
            XYPair { y: 2.0, x: 2.0 },
            XYPair { y: 2.0, x: 4.0 },
        ])
        .unwrap();
        assert_eq!(p.slope().unwrap(), 0.0);
        assert_eq!(p.intercept().unwrap(), 2.0);
        assert_eq!(p.x_intercept(), None);
    }

    #[test]
    fn test_linear_tf() {
        let p = StatsSummary2D::new_from_vec(vec![
            XYPair {
                y: tf(2.0),
                x: tf(1.0),
            },
            XYPair {
                y: tf(4.0),
                x: tf(2.0),
            },
            XYPair {
                y: tf(6.0),
                x: tf(3.0),
            },
        ])
        .unwrap();
        assert_eq!(p.slope().unwrap(), tf(2.0));
        assert_eq!(p.intercept().unwrap(), tf(0.0));
        assert_eq!(p.x_intercept().unwrap(), tf(0.0));

        let p = StatsSummary2D::new_from_vec(vec![
            XYPair {
                y: tf(2.0),
                x: tf(2.0),
            },
            XYPair {
                y: tf(4.0),
                x: tf(3.0),
            },
            XYPair {
                y: tf(6.0),
                x: tf(4.0),
            },
        ])
        .unwrap();
        assert_eq!(p.slope().unwrap(), tf(2.0));
        assert_eq!(p.intercept().unwrap().hi(), -2.0);
        assert!(p.intercept().unwrap().lo().abs() < f64::EPSILON);
        assert_eq!(p.x_intercept().unwrap().hi(), 1.0);
        assert!(p.x_intercept().unwrap().lo().abs() < f64::EPSILON);

        // empty
        let p: StatsSummary2D<TwoFloat> = StatsSummary2D::new();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept(), None);
        // singleton
        let p = StatsSummary2D::new_from_vec(vec![XYPair {
            y: tf(2.0),
            x: tf(2.0),
        }])
        .unwrap();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept(), None);
        //vertical
        let p = StatsSummary2D::new_from_vec(vec![
            XYPair {
                y: tf(2.0),
                x: tf(2.0),
            },
            XYPair {
                y: tf(4.0),
                x: tf(2.0),
            },
        ])
        .unwrap();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept().unwrap(), tf(2.0));
        //horizontal
        let p = StatsSummary2D::new_from_vec(vec![
            XYPair {
                y: tf(2.0),
                x: tf(2.0),
            },
            XYPair {
                y: tf(2.0),
                x: tf(4.0),
            },
        ])
        .unwrap();
        assert_eq!(p.slope().unwrap(), tf(0.0));
        assert_eq!(p.intercept().unwrap(), tf(2.0));
        assert_eq!(p.x_intercept(), None);
    }
}
