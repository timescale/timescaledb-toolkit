//regr implements the Youngs-Cramer algorithm and are based on the Postgres implementation
//here:
//https://github.com/postgres/postgres/blob/472e518a44eacd9caac7d618f1b6451672ca4481/src/backend/utils/adt/float.c#L3260
//

#[derive(Debug, PartialEq)]
pub enum RegrError {
    DoubleOverflow,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct XYPair {
    pub x: f64,
    pub y: f64,
}
//regression summary state
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct RegrSummary {
    n: f64,   // count (as f64 for convenience, should always be a non-negative integer)
    sx: f64,  // sum(x)
    sxx: f64, // sum((x-sx/n)^2) (sum of squares)
    sy: f64,  // sum(y)
    syy: f64, // sum((y-sy/n)^2) (sum of squares)
    sxy: f64, // sum((x-sx/n)*(y-sy/n)) (sum of products)
}
impl RegrSummary {
    pub fn new() -> Self {
        RegrSummary {
            n: 0.0,
            sx: 0.0,
            sxx: 0.0,
            sy: 0.0,
            syy: 0.0,
            sxy: 0.0,
        }
    }
    /// accumulate an XYPair into a RegrSummary
    /// ```
    /// use regr::*;
    /// let mut p = RegrSummary::new();
    /// p.accum(XYPair{x:1.0, y:1.0,}).unwrap();
    /// p.accum(XYPair{x:2.0, y:2.0,}).unwrap();
    /// //we can add in infinite values and it will handle it properly.
    /// p.accum(XYPair{x:f64::INFINITY, y:1.0}).unwrap();
    /// assert_eq!(p.sum().unwrap().x, f64::INFINITY);
    /// assert!(p.sum_squares().unwrap().x.is_nan()); // this is NaN because it involves multiplication of two infinite values
    ///
    /// assert_eq!(p.accum(XYPair{y:f64::MAX, x:1.0,}), Err(RegrError::DoubleOverflow)); // we do error if we actually overflow however
    ///
    ///```
    pub fn accum(&mut self, p: XYPair) -> Result<(), RegrError> {
        let old = *self;
        self.n += 1.0;
        self.sx += p.x;
        self.sy += p.y;
        if old.n > 0.0 {
            let tmpx = p.x * self.n - self.sx;
            let tmpy = p.y * self.n - self.sy;
            let scale = 1.0 / (self.n * old.n);
            self.sxx += tmpx * tmpx * scale;
            self.syy += tmpy * tmpy * scale;
            self.sxy += tmpx * tmpy * scale;
            // Overflow check.  Postgres only reports an overflow error when finite inputs
		    // lead to infinite results we follow the same pattern.  Note also that sxx,
		    // syy and sxy should be NaN if any of the relevant inputs are infinite, so we
		    // intentionally prevent them from becoming infinite.
            if self.has_infinite() {
                if self.check_overflow(&old, p) {
                    return Err(RegrError::DoubleOverflow);
                }
                // sxx, syy, and sxy should be set to NaN if any of their inputs are
                // infinite, so if they ended up as infinite and there wasn't an overflow,
                // we need to set them to NaN instead as this implies that there was an
                // infinite input (because they necessarily involve multiplications of
                // infinites, which are NaNs in Postgres land at least)
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
        Ok(())
    }
    fn has_infinite(&self) -> bool {
        self.n.is_infinite() // not sure if this is necessary, may not be possible?
                || self.sx.is_infinite()
                || self.sxx.is_infinite()
                || self.sy.is_infinite()
                || self.syy.is_infinite()
                || self.sxy.is_infinite()
    }
    fn check_overflow(&self, old: &RegrSummary, p: XYPair) -> bool {
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

    ///create a RegrSummary from a vector of XYPairs
    /// ```
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let mut p = RegrSummary::new();
    /// p.accum(XYPair{x:1.0, y:1.0,}).unwrap();
    /// p.accum(XYPair{x:2.0, y:2.0,}).unwrap();
    /// p.accum(XYPair{x:3.0, y:3.0,}).unwrap();
    /// let q = RegrSummary::new_from_vec(vec![XYPair{x:1.0, y:1.0,}, XYPair{x:2.0, y:2.0,}, XYPair{x:3.0, y:3.0,}]).unwrap();
    /// assert_eq!(p, q);
    ///```
    pub fn new_from_vec(v: impl IntoIterator<Item=XYPair>) -> Result<Self, RegrError> {
        let mut r = RegrSummary::new();
        for p in v {
            r.accum(p)?;
        }
        Ok(r)
    }
    /// combine two RegrSummarys
    /// ```
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let p = RegrSummary::new_from_vec(vec![XYPair{x:1.0, y:1.0,}, XYPair{x:2.0, y:2.0,}, XYPair{x:3.0, y:3.0,}, XYPair{x:4.0, y:4.0,}]).unwrap();
    /// let q = RegrSummary::new_from_vec(vec![XYPair{x:1.0, y:1.0,}, XYPair{x:2.0, y:2.0,},]).unwrap();
    /// let r = RegrSummary::new_from_vec(vec![XYPair{x:3.0, y:3.0,}, XYPair{x:4.0, y:4.0,},]).unwrap();
    /// let r = r.combine(q).unwrap();
    /// assert_eq!(r, p);
    /// ```
    // we combine two RegrSummarys via a generalization of the Youngs-Cramer algorithm, we follow what Postgres does here
    //      n = n1 + n2
    //      sx = sx1 + sx2
    //      sxx = sxx1 + sxx2 + n1 * n2 * (sx1/n1 - sx2/n)^2 / n
    //      sy / syy analogous
    //      sxy = sxy1 + sxy2 + n1 * n2 * (sx1/n1 - sx2/n2) * (sy1/n1 - sy2/n2) / n
    pub fn combine(&self, other: RegrSummary) -> Result<Self, RegrError> {
        // TODO: think about whether we want to just modify &self in place here for perf
        // reasons. This is also a set of weird questions around the Rust compiler, so
        // easier to just add the copy trait here, may need to adjust or may make things
        // harder if we do generics.
        if self.n < 1.0 && other.n < 1.0 {
            return Ok(RegrSummary::new());
        } else if self.n < 1.0 {
            // handle the trivial n = 0 cases here, and don't worry about divide by zero errors later.
            return Ok(other);
        } else if other.n < 1.0 {
            return Ok(*self);
        }
        let tmpx = self.sx / self.n - other.sx / other.n;
        let tmpy = self.sy / self.n - other.sy / other.n;
        let n = self.n + other.n;
        let r = RegrSummary {
            n: n,
            sx: self.sx + other.sx,
            sxx: self.sxx + other.sxx + self.n * other.n * tmpx * tmpx / n,
            sy: self.sy + other.sy,
            syy: self.syy + other.syy + self.n * other.n * tmpy * tmpy / n,
            sxy: self.sxy + other.sxy + self.n * other.n * tmpx * tmpy / n,
        };
        // TODO check x and y separately (probably doesn't matter in practice, but good to do)
        if r.has_infinite() && !self.has_infinite() && !other.has_infinite() {
            return Err(RegrError::DoubleOverflow);
        }
        Ok(r)
    }
    /// offsets all values accumulated in a RegrSummary by a given amount in X & Y. This
    /// only works if all values are offset by that amount. This is used for allowing
    /// relative calculations in a local region and then allowing them to be combined with
    /// other regions where all points are offset by the same amount. The main use case
    /// for now is in the counter case where, when partials are combined you can get a new
    /// offset for all points in the counter.
    // Add proof
    // TODO: Check for numerical stability (floating point arithmetic error)
    pub fn offset(&mut self, offset: XYPair) -> Result<(), RegrError> {
        let _ = offset;
        unimplemented!();
    }
    ///returns the sum of squares of both the independent (x) and dependent (y) variables
    ///as an XYPair, where the sum of squares is defined as: sum(x^2) - sum(x)^2 / n)
    ///```
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let p = RegrSummary::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let ssx = (1.0_f64.powi(2) + 2.0_f64.powi(2) + 3.0_f64.powi(2)) - (1.0+2.0+3.0_f64).powi(2)/3.0;
    /// let ssy = (2.0_f64.powi(2) + 4.0_f64.powi(2) + 6.0_f64.powi(2)) - (2.0+4.0+6.0_f64).powi(2)/3.0;
    /// let ssp = p.sum_squares().unwrap();
    /// assert_eq!(ssp.x, ssx);
    /// assert_eq!(ssp.y, ssy);
    /// //empty RegrSummarys return None
    /// assert!(RegrSummary::new().sum_squares().is_none());
    /// ```
    pub fn sum_squares(&self) -> Option<XYPair> {
        if self.n < 1.0 {
            return None;
        }
        Some(XYPair {
            x: self.sxx,
            y: self.syy,
        })
    }
    ///returns the "sum of products" of the dependent * independent variables sum(x * y) - sum(x) * sum(y) / n
    ///```
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let p = RegrSummary::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = (2.0 * 1.0 + 4.0 * 2.0 + 6.0 * 3.0) - (2.0 + 4.0 + 6.0)*(1.0 + 2.0 + 3.0)/3.0;
    /// assert_eq!(p.sumxy().unwrap(), s);
    /// //empty RegrSummarys return None
    /// assert!(RegrSummary::new().sumxy().is_none());
    /// ```
    pub fn sumxy(&self) -> Option<f64> {
        if self.n < 1.0 {
            return None;
        }
        Some(self.sxy)
    }
    ///returns the averages of the x and y variables
    ///```
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let p = RegrSummary::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let avgx = (1.0 + 2.0 + 3.0)/3.0;
    /// let avgy = (2.0 + 4.0 + 6.0)/3.0;
    /// let avgp = p.avg().unwrap();
    /// assert_eq!(avgp.x, avgx);
    /// assert_eq!(avgp.y, avgy);
    /// //empty RegrSummarys return None
    /// assert!(RegrSummary::new().avg().is_none());
    /// ```
    pub fn avg(&self) -> Option<XYPair> {
        if self.n < 1.0 {
            return None;
        }
        Some(XYPair {
            x: self.sx / self.n,
            y: self.sy / self.n,
        })
    }
    ///returns the count of inputs as an i64
    ///```
    /// use regr::RegrSummary;
    /// use regr::XYPair;

    /// let p = RegrSummary::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = 3;
    /// assert_eq!(p.count(), s);
    /// //empty RegrSummarys return 0 count
    /// assert_eq!(RegrSummary::new().count(), 0);
    /// ```
    pub fn count(&self) -> i64 {
        self.n as i64
    }
    ///returns the sums of x and y as an XYPair
    ///```
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let p = RegrSummary::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let sumx = (1.0 + 2.0 + 3.0);
    /// let sumy = (2.0 + 4.0 + 6.0);
    /// let sump = p.sum().unwrap();
    /// assert_eq!(sump.x, sumx);
    /// assert_eq!(sump.y, sumy);
    /// //empty RegrSummarys return None
    /// assert!(RegrSummary::new().sum().is_none());
    /// ```
    pub fn sum(&self) -> Option<XYPair> {
        if self.n < 1.0 {
            return None;
        }
        Some(XYPair {
            x: self.sx,
            y: self.sy,
        })
    }
    ///returns the population standard deviation of both the independent and dependent variables as an XYPair
    pub fn stddev_pop(&self) -> Option<XYPair> {
        if self.n < 1.0 {
            return None;
        }
        Some(XYPair {
            x: (self.sxx / self.n).sqrt(),
            y: (self.syy / self.n).sqrt(),
        })
    }

    ///returns the sample standard deviation of both the independent and dependent variables as an XYPair
    pub fn stddev_samp(&self) -> Option<XYPair> {
        if self.n < 2.0 {
            return None;
        }
        Some(XYPair {
            x: (self.sxx / self.n - 1.0).sqrt(),
            y: (self.syy / self.n - 1.0).sqrt(),
        })
    }
    /// returns the correlation coefficient, which is the covariance / (stddev(x) * stddev(y))
    /// Note that it makes no difference whether we choose the sample or
    /// population covariance and stddev, because we end up with a canceling n or n-1 term. This
    /// also allows us to reduce our calculation to the sumxy / sqrt(sum_squares(x)*sum_squares(y))
    pub fn corr(&self) -> Option<f64> {
        // empty RegrSummarys, horizontal or vertical lines should return None
        if self.n < 1.0 || self.sxx == 0.0 || self.syy == 0.0 {
            return None;
        }
        Some(self.sxy / (self.sxx * self.syy).sqrt())
    }
    /// returns the slope of the least squares fit line
    pub fn slope(&self) -> Option<f64> {
        // the case of a single point will usually be triggered by the the second branch of this (which is also a test for a vertical line)
        //however, in cases where we had an infinite input, we will end up with NaN which is the expected behavior.
        if self.n < 1.0 || self.sxx == 0.0 {
            return None;
        }
        Some(self.sxy / self.sxx)
    }
    /// returns the intercept of the least squares fit line
    pub fn intercept(&self) -> Option<f64> {
        if self.n < 1.0 || self.sxx == 0.0 {
            return None;
        }
        Some((self.sy - self.sx * self.sxy / self.sxx) / self.n)
    }
    /// returns the squared error of the least squares fit line
    pub fn square_error(&self) -> Option<f64> {
        if self.n < 1.0 || self.sxx == 0.0 {
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
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let p = RegrSummary::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = (2.0 * 1.0 + 4.0 * 2.0 + 6.0 * 3.0) - (2.0 + 4.0 + 6.0)*(1.0 + 2.0 + 3.0)/3.0;
    /// let s = s/2.0;
    /// assert_eq!(p.covar_samp().unwrap(), s);
    /// //empty RegrSummarys return None
    /// assert!(RegrSummary::new().covar_samp().is_none());
    /// ```
    pub fn covar_samp(&self) -> Option<f64> {
        if self.n < 2.0 {
            return None;
        }
        Some(self.sxy / (self.n - 1.0))
    }
    ///returns the population covariance: (sumxy()/n)
    ///```
    /// use regr::RegrSummary;
    /// use regr::XYPair;
    /// let p = RegrSummary::new_from_vec(vec![XYPair{y:2.0, x:1.0,}, XYPair{y:4.0, x:2.0,}, XYPair{y:6.0, x:3.0,}]).unwrap();
    /// let s = (2.0 * 1.0 + 4.0 * 2.0 + 6.0 * 3.0) - (2.0 + 4.0 + 6.0)*(1.0 + 2.0 + 3.0)/3.0;
    /// let s = s/3.0;
    /// assert_eq!(p.covar_pop().unwrap(), s);
    /// //empty RegrSummarys return None
    /// assert!(RegrSummary::new().covar_pop().is_none());
    /// ```
    pub fn covar_pop(&self) -> Option<f64> {
        if self.n < 1.0 {
            return None;
        }
        Some(self.sxy / self.n)
    }
}
