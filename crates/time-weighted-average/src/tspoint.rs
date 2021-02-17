
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct TSPoint {
    pub ts: i64,
    pub val: f64,
}

#[derive(Debug, PartialEq)]
pub enum TSPointError {
    TimesEqualInterpolate,
}

impl TSPoint {
    pub fn interpolate_linear(&self, p2: &TSPoint, ts: i64) -> Result<f64, TSPointError> {
        if self.ts == p2.ts {
            return Err(TSPointError::TimesEqualInterpolate);
        }

        // using point slope form of a line iteratively y = y2 - y1 / (x2 - x1) * (x - x1) + y1
        let duration = (p2.ts - self.ts) as f64; // x2 - x1
        let dinterp = (ts - self.ts) as f64; // x - x1
        Ok((p2.val - self.val) * dinterp / duration + self.val) 
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_linear_interpolate(){
        use crate::{TSPoint, TSPointError};
        let p1 = TSPoint{ts: 1, val: 1.0};
        let p2 = TSPoint{ts: 3, val: 3.0};
        assert_eq!(p1.interpolate_linear(&p2, 2).unwrap(), 2.0);
        assert_eq!(p1.interpolate_linear(&p2, 3).unwrap(), 3.0);
        assert_eq!(p1.interpolate_linear(&p2, 4).unwrap(), 4.0);
        assert_eq!(p1.interpolate_linear(&p2, 0).unwrap(), 0.0);
        assert_eq!(p1.interpolate_linear(&p1, 2).unwrap_err(), TSPointError::TimesEqualInterpolate);
    }
}