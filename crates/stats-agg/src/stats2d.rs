// 2D stats are based on the Youngs-Cramer implementation in PG here: 
// https://github.com/postgres/postgres/blob/472e518a44eacd9caac7d618f1b6451672ca4481/src/backend/utils/adt/float.c#L3260
use serde::{Deserialize, Serialize};
use crate::{StatsError, XYPair};


#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct StatsSummary2D {
    pub n: u64,   // count
    pub sx: f64,  // sum(x)
    pub sxx: f64, // sum((x-sx/n)^2) (sum of squares)
    pub sy: f64,  // sum(y)
    pub syy: f64, // sum((y-sy/n)^2) (sum of squares)
    pub sxy: f64, // sum((x-sx/n)*(y-sy/n)) (sum of products)
}


impl StatsSummary2D {
    pub fn new() -> Self {
        StatsSummary2D {
            n: 0,
            sx: 0.0,
            sxx: 0.0,
            sy: 0.0,
            syy: 0.0,
            sxy: 0.0,
        }
    }

    pub fn n64(&self) -> f64 {
        self.n as f64
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
    pub fn accum(&mut self, p: XYPair) -> Result<(), StatsError> {
        let old = StatsSummary2D {
            n: self.n,
            sx: self.sx,
            sxx: self.sxx,
            sy: self.sy,
            syy: self.syy,
            sxy: self.sxy,
        };
        self.n += 1;
        self.sx += p.x;
        self.sy += p.y;
        if old.n > 0 {
            let tmpx = p.x * self.n64() - self.sx;
            let tmpy = p.y * self.n64() - self.sy;
            let scale = 1.0 / (self.n64() * old.n64());
            self.sxx += tmpx * tmpx * scale;
            self.syy += tmpy * tmpy * scale;
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
                if self.sxx.is_infinite() {
                    self.sxx = f64::NAN;
                }
                if self.syy.is_infinite() {
                    self.syy = f64::NAN;
                }
                if self.sxy.is_infinite() {
                    self.sxy = f64::NAN;
                }
            }
        } else {
            // first input, leave sxx/syy/sxy alone unless we have infinite inputs
            if !p.x.is_finite() {
                self.sxx = f64::NAN;
                self.sxy = f64::NAN;
            }
            if !p.y.is_finite() {
                self.syy = f64::NAN;
                self.sxy = f64::NAN;
            }
        }
        Result::Ok(())
    }
    fn has_infinite(&self) -> bool {
        self.sx.is_infinite()
            || self.sxx.is_infinite()
            || self.sy.is_infinite()
            || self.syy.is_infinite()
            || self.sxy.is_infinite()
    }
    fn check_overflow(&self, old: &StatsSummary2D, p: XYPair) -> bool {
        //Only report overflow if we have finite inputs that lead to infinite results.
        ((self.sx.is_infinite() || self.sxx.is_infinite()) && old.sx.is_finite() && p.x.is_finite())
            || ((self.sy.is_infinite() || self.syy.is_infinite())
                && old.sy.is_finite()
                && p.y.is_finite())
            || (self.sxy.is_infinite()
                && old.sx.is_finite()
                && p.x.is_finite()
                && old.sy.is_finite()
                && p.y.is_finite())
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
    pub fn new_from_vec(v: Vec<XYPair>) -> Result<Self, StatsError> {
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
    //      sxx = sxx1 + sxx2 + n1 * n2 * (sx1/n1 - sx2/n)^2 / n
    //      sy / syy analogous
    //      sxy = sxy1 + sxy2 + n1 * n2 * (sx1/n1 - sx2/n2) * (sy1/n1 - sy2/n2) / n
    pub fn combine(&self, other: StatsSummary2D) -> Result<Self, StatsError> {
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
            n: n,
            sx: self.sx + other.sx,
            sxx: self.sxx + other.sxx + self.n64() * other.n64() * tmpx * tmpx / n as f64,
            sy: self.sy + other.sy,
            syy: self.syy + other.syy + self.n64() * other.n64() * tmpy * tmpy / n as f64,
            sxy: self.sxy + other.sxy + self.n64() * other.n64() * tmpx * tmpy / n as f64,
        };
        if r.has_infinite() && !self.has_infinite() && !other.has_infinite() {
            return Err(StatsError::DoubleOverflow);
        }
        Ok(r)
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
    pub fn offset(&mut self, offset: XYPair) -> Result<(), StatsError> {
        self.sx = self.sx + self.n64() * offset.x;
        self.sy = self.sy + self.n64() * offset.y;
        if self.has_infinite() && offset.x.is_finite() && offset.y.is_finite(){
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
    /// assert!(StatsSummary2D::new().sum_squares().is_none());
    /// ```
    pub fn sum_squares(&self) -> Option<XYPair> {
        if self.n == 0 {
            return None;
        }
        Some(XYPair {
            x: self.sxx,
            y: self.syy,
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
    /// assert!(StatsSummary2D::new().sumxy().is_none());
    /// ```
    pub fn sumxy(&self) -> Option<f64> {
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
    /// assert!(StatsSummary2D::new().avg().is_none());
    /// ```
    pub fn avg(&self) -> Option<XYPair> {
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
    /// assert_eq!(StatsSummary2D::new().count(), 0);
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
    /// assert!(StatsSummary2D::new().sum().is_none());
    /// ```
    pub fn sum(&self) -> Option<XYPair> {
        if self.n == 0 {
            return None;
        }
        Some(XYPair {
            x: self.sx,
            y: self.sy,
        })
    }

    pub fn var_pop(&self) -> Option<XYPair> {
        if self.n == 0 {
            return None;
        }
        Some(XYPair {
            x: self.sxx / self.n64(),
            y: self.syy / self.n64(),
        })
    }

    pub fn var_samp(&self) -> Option<XYPair> {
        if self.n <= 1 {
            return None;
        }
        Some(XYPair {
            x: self.sxx / (self.n64() - 1.0),
            y: self.syy / (self.n64() - 1.0),
        })
    }

    ///returns the population standard deviation of both the independent and dependent variables as an XYPair
    pub fn stddev_pop(&self) -> Option<XYPair> {
        let var = self.var_pop()?;
        Some(XYPair {
            x: var.x.sqrt(),
            y: var.y.sqrt(),
        })
    }

    ///returns the sample standard deviation of both the independent and dependent variables as an XYPair
    pub fn stddev_samp(&self) -> Option<XYPair> {
        let var = self.var_samp()?;
        Some(XYPair {
            x: var.x.sqrt(),
            y: var.y.sqrt(),
        })
    }

    /// returns the correlation coefficient, which is the covariance / (stddev(x) * stddev(y))
    /// Note that it makes no difference whether we choose the sample or
    /// population covariance and stddev, because we end up with a canceling n or n-1 term. This
    /// also allows us to reduce our calculation to the sumxy / sqrt(sum_squares(x)*sum_squares(y))
    pub fn corr(&self) -> Option<f64> {
        // empty StatsSummary2Ds, horizontal or vertical lines should return None
        if self.n == 0 || self.sxx == 0.0 || self.syy == 0.0 {
            return None;
        }
        Some(self.sxy / (self.sxx * self.syy).sqrt())
    }

    /// returns the slope of the least squares fit line
    pub fn slope(&self) -> Option<f64> {
        // the case of a single point will usually be triggered by the the second branch of this (which is also a test for a vertical line)
        //however, in cases where we had an infinite input, we will end up with NaN which is the expected behavior.
        if self.n == 0 || self.sxx == 0.0 {
            return None;
        }
        Some(self.sxy / self.sxx)
    }

    /// returns the intercept of the least squares fit line
    pub fn intercept(&self) -> Option<f64> {
        if self.n == 0 || self.sxx == 0.0 {
            return None;
        }
        Some((self.sy - self.sx * self.sxy / self.sxx) / self.n64())
    }

    /// returns the x intercept of the least squares fit line
    // y = mx + b (y = 0)
    // -b = mx
    // x = -b / m 
    pub fn x_intercept(&self) -> Option<f64> {
        // vertical line does have an x intercept
        if self.n > 1 && self.sxx == 0.0 {
            return Some(self.sx / self.n64())
        }
        // horizontal lines have no x intercepts
        if self.syy == 0.0 {
            return None;
        }
        Some(-1.0 * self.intercept()? / self.slope()?) 
    }
    
    /// returns the square of the correlation coefficent (aka the coefficient of determination)
    pub fn determination_coeff(&self) -> Option<f64> {
        if self.n == 0 || self.sxx == 0.0 {
            return None;
        }
        //horizontal lines return 1.0 error
        if self.syy == 0.0 {
            return Some(1.0);
        }
        Some(self.sxy * self.sxy / (self.sxx * self.syy))
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
    /// assert!(StatsSummary2D::new().covar_samp().is_none());
    /// ```
    pub fn covar_samp(&self) -> Option<f64> {
        if self.n <= 1 {
            return None;
        }
        Some(self.sxy / (self.n64() - 1.0))
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
    /// assert!(StatsSummary2D::new().covar_pop().is_none());
    /// ```
    pub fn covar_pop(&self) -> Option<f64> {
        if self.n == 0 {
            return None;
        }
        Some(self.sxy / self.n64())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_linear(){
        let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
        assert_eq!(p.slope().unwrap(), 2.0);
        assert_eq!(p.intercept().unwrap(), 0.0);
        assert_eq!(p.x_intercept().unwrap(), 0.0);

        let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:2.0,}, XYPair{y:4.0, x:3.0,}, XYPair{y:6.0, x:4.0,}]).unwrap();
        assert_eq!(p.slope().unwrap(), 2.0);
        assert_eq!(p.intercept().unwrap(), -2.0);
        assert_eq!(p.x_intercept().unwrap(), 1.0);

        // empty
        let p = StatsSummary2D::new();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept(), None);
        // singleton
        let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:2.0,}, ]).unwrap();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept(), None);
        //vertical
        let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:2.0,}, XYPair{y:4.0, x:2.0,},]).unwrap();
        assert_eq!(p.slope(), None);
        assert_eq!(p.intercept(), None);
        assert_eq!(p.x_intercept().unwrap(), 2.0);
        //horizontal
        let p = StatsSummary2D::new_from_vec(vec![XYPair{y:2.0, x:2.0,}, XYPair{y:2.0, x:4.0,},]).unwrap();
        assert_eq!(p.slope().unwrap(), 0.0);
        assert_eq!(p.intercept().unwrap(), 2.0);
        assert_eq!(p.x_intercept(), None);
    }
}