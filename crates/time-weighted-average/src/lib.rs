pub mod tspoint;
use tspoint::TSPoint;
#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum TimeWeightMethod {
    LOCF,
    Linear,
}

#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct TimeWeightSummary {
    pub method: TimeWeightMethod,
    pub first: TSPoint,
    pub last: TSPoint,
    pub w_sum: f64,
}

#[derive(PartialEq, Debug)]
pub enum TimeWeightError {
    OrderError,
    DoubleOverflow, // do we need to do this?
    MethodMismatch,
    InvalidParameter,
    ZeroDuration,
}

impl TimeWeightSummary {
    pub fn new(pt: TSPoint, method: TimeWeightMethod) -> Self {
        TimeWeightSummary { method: method, first: pt, last: pt, w_sum: 0.0 }
    }

    pub fn accum(&mut self, pt: TSPoint) -> Result<(), TimeWeightError> {
        if pt.ts < self.last.ts {
            return Err(TimeWeightError::OrderError);
        }
        if pt.ts == self.last.ts {
            return Ok(()); // if two points are equal we only use the first we see
        }
        self.w_sum += self.method.weighted_sum(self.last, pt);
        self.last = pt;
        Ok(())
    }

    pub fn combine(&self, next: &TimeWeightSummary) -> Result<TimeWeightSummary, TimeWeightError> {
        if self.method != next.method {
            return Err(TimeWeightError::MethodMismatch);
        }
        if self.last.ts >= next.first.ts {
            return Err(TimeWeightError::OrderError);
        }
        let new = TimeWeightSummary {
            method: self.method,
            first: self.first,
            last: next.last,
            w_sum: self.w_sum + next.w_sum + self.method.weighted_sum(self.last, next.first),
        };
        Ok(new)
    }

    /// evaluate the time_weighted_average based on the summary, along with several other
    /// parameters that determine how we extrapolate to the ends of time ranges. Notes on
    /// behaviour in several cases:
    /// 1. The start_prev parameter is optional, but if a start is provided a previous point must be
    /// provided (for both linear and locf weighting methods), if it is not provided the average
    /// will be evaluated from the *observed* start
    /// 2. The end_next parameter is also optional, if an end is provided and the locf weighting
    /// method is specified, a next parameter isn't needed, with the linear method, the next
    /// point is needed and we will error if it is not provided. If an end is not specified, the
    /// average will be evaluated to the *observed* end

    pub fn time_weighted_average(
        &self, start_prev: Option<(i64, TSPoint)>, end_next: Option<(i64, Option<TSPoint>)>,
    ) -> Result<f64, TimeWeightError> {
        let mut calc = *self;
        if let Some((start, prev)) = start_prev {
            calc = self.with_prev(start, prev)?
        }

        if let Some((end, next)) = end_next {
            calc = self.with_next(end, next)?
        }

        if calc.last.ts == calc.first.ts {
            return Err(TimeWeightError::ZeroDuration);
        }

        let duration = (calc.last.ts - calc.first.ts) as f64;
        Ok(calc.w_sum / duration)
    }

    fn with_prev(&self, start: i64, prev: TSPoint) -> Result<Self, TimeWeightError> {
        if prev.ts > self.first.ts || start < self.first.ts || prev.ts > start {
            return Err(TimeWeightError::OrderError); // should this be a different error?
        }

        let new_first = self.method.interpolate(prev, Some(self.first), start)?;
        let w_sum = self.w_sum + self.method.weighted_sum(new_first, self.first);

        Ok(TimeWeightSummary { first: new_first, w_sum, ..*self })
    }

    fn with_next(&self, end: i64, next: Option<TSPoint>) -> Result<Self, TimeWeightError> {
        if end < self.last.ts {
            // equal is okay, will just reduce to zero add in the sum, but not an error
            return Err(TimeWeightError::OrderError);
        }

        if let Some(next) = next {
            if next.ts < end {
                return Err(TimeWeightError::OrderError);
            }
        }

        let new_last = self.method.interpolate(self.last, next, end)?;
        let w_sum = self.w_sum + self.method.weighted_sum(self.last, new_last);

        Ok(TimeWeightSummary { last: new_last, w_sum, ..*self })
    }
}



impl TimeWeightMethod {
    fn interpolate(
        &self, first: TSPoint, second: Option<TSPoint>, target: i64,
    ) -> Result<TSPoint, TimeWeightError> {
        if let Some(second) = second {
            if second.ts <= first.ts {
                return Err(TimeWeightError::OrderError);
            }
        }
        let pt = TSPoint {
            ts: target,
            val: match (self, second) {
                (TimeWeightMethod::LOCF, _) => first.val,
                // TODO make this a method on TimeWeightMethod?
                (TimeWeightMethod::Linear, Some(second)) => {
                    first.interpolate_linear(&second, target).unwrap()
                }
                (TimeWeightMethod::Linear, None) => return Err(TimeWeightError::InvalidParameter),
            },
        };
        Ok(pt)
    }

    fn weighted_sum(&self, first: TSPoint, second: TSPoint) -> f64 {
        debug_assert!(second.ts > first.ts);
        let duration = (second.ts - first.ts) as f64;
        match self {
            TimeWeightMethod::LOCF => first.val * duration,
            //the weighting for a linear interpolation is equivalent to the midpoint
            //between the two values, this is because we're taking the area under the
            //curve, which is the sum of the smaller of the two values multiplied by
            //duration (a rectangle) + the triangle formed on top (abs diff between the
            //two / 2 * duration) this is equivalent to the rectangle formed by the
            //midpoint of the two.
            //TODO: Stable midpoint calc? http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0811r2.html
            TimeWeightMethod::Linear => (first.val + second.val) / 2.0 * duration,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    // Just creating and basic use works
    // Simple case gets correct results Done
    // errors for each of with_prev/with_next,
    // other error conditions:
    // weird cases:
    // NaN/Inf inputs -> should these error?
    // Overflow? -> Inf
    //

    fn new_from_iter(
        iter: impl IntoIterator<Item = TSPoint>, method: TimeWeightMethod,
    ) -> Result<TimeWeightSummary, TimeWeightError> {
        let mut t = iter.into_iter();
        let mut s = TimeWeightSummary::new(t.next().unwrap(), method);
        for p in t {
            s.accum(p)?;
        }
        Ok(s)
    }
    #[test]
    fn test_simple_accum_locf() {
        let mut s = TimeWeightSummary::new(TSPoint { ts: 0, val: 1.0 }, TimeWeightMethod::LOCF);
        assert_eq!(s.w_sum, 0.0);
        s.accum(TSPoint { ts: 10, val: 0.0 }).unwrap();
        assert_eq!(s.w_sum, 10.0);
        s.accum(TSPoint { ts: 20, val: 2.0 }).unwrap();
        assert_eq!(s.w_sum, 10.0);
        s.accum(TSPoint { ts: 30, val: 1.0 }).unwrap();
        assert_eq!(s.w_sum, 30.0);
        s.accum(TSPoint { ts: 40, val: -3.0 }).unwrap();
        assert_eq!(s.w_sum, 40.0);
        s.accum(TSPoint { ts: 50, val: -3.0 }).unwrap();
        assert_eq!(s.w_sum, 10.0);
    }
    #[test]
    fn test_simple_accum_linear() {
        let mut s = TimeWeightSummary::new(TSPoint { ts: 0, val: 1.0 }, TimeWeightMethod::Linear);
        assert_eq!(s.w_sum, 0.0);
        s.accum(TSPoint { ts: 10, val: 0.0 }).unwrap();
        assert_eq!(s.w_sum, 5.0);
        s.accum(TSPoint { ts: 20, val: 2.0 }).unwrap();
        assert_eq!(s.w_sum, 15.0);
        s.accum(TSPoint { ts: 30, val: 1.0 }).unwrap();
        assert_eq!(s.w_sum, 30.0);
        s.accum(TSPoint { ts: 40, val: -3.0 }).unwrap();
        assert_eq!(s.w_sum, 20.0);
        s.accum(TSPoint { ts: 50, val: -3.0 }).unwrap();
        assert_eq!(s.w_sum, -10.0);
    }

    fn combine_test(t: TimeWeightMethod) {
        let s = new_from_iter(
            vec![
                TSPoint { ts: 0, val: 1.0 },
                TSPoint { ts: 10, val: 0.0 },
                TSPoint { ts: 20, val: 2.0 },
                TSPoint { ts: 30, val: 1.0 },
            ],
            t,
        )
        .unwrap();
        let s1 = new_from_iter(vec![TSPoint { ts: 0, val: 1.0 }, TSPoint { ts: 10, val: 0.0 }], t)
            .unwrap();
        let s2 = new_from_iter(vec![TSPoint { ts: 20, val: 2.0 }, TSPoint { ts: 30, val: 1.0 }], t)
            .unwrap();
        let s_comb = s1.combine(&s2).unwrap();
        assert_eq!(s, s_comb);
        // test combine with single val as well as multiple
        let s21 = TimeWeightSummary::new(TSPoint { ts: 20, val: 2.0 }, t);
        let s22 = TimeWeightSummary::new(TSPoint { ts: 30, val: 1.0 }, t);
        assert_eq!(s1.combine(&s21).unwrap().combine(&s22).unwrap(), s);
    }
    #[test]
    fn test_combine() {
        combine_test(TimeWeightMethod::LOCF);
        combine_test(TimeWeightMethod::Linear);
    }

    fn order_accum_test(t: TimeWeightMethod) {
        let s = new_from_iter(vec![TSPoint { ts: 0, val: 1.0 }, TSPoint { ts: 10, val: 0.0 }], t)
            .unwrap();
        let mut o = s;
        // adding points at the same timestamp shouldn't affect the value (no matter whether the
        // value is larger or smaller than the original)
        o.accum(TSPoint { ts: 10, val: 2.0 }).unwrap();
        assert_eq!(s, o);
        o.accum(TSPoint { ts: 10, val: -1.0 }).unwrap();
        assert_eq!(s, o);
        assert_eq!(o.accum(TSPoint { ts: 5, val: -1.0 }), Err(TimeWeightError::OrderError));
    }
    fn order_combine_test(t: TimeWeightMethod) {
        let s = new_from_iter(vec![TSPoint { ts: 0, val: 1.0 }, TSPoint { ts: 10, val: 0.0 }], t)
            .unwrap();
        let smaller =
            new_from_iter(vec![TSPoint { ts: 5, val: 1.0 }, TSPoint { ts: 15, val: 0.0 }], t)
                .unwrap();
        let equal =
            new_from_iter(vec![TSPoint { ts: 10, val: 1.0 }, TSPoint { ts: 15, val: 0.0 }], t)
                .unwrap();

        assert_eq!(s.combine(&smaller), Err(TimeWeightError::OrderError));
        assert_eq!(s.combine(&equal), Err(TimeWeightError::OrderError));
    }
    #[test]
    fn test_order() {
        order_accum_test(TimeWeightMethod::LOCF);
        order_accum_test(TimeWeightMethod::Linear);
        order_combine_test(TimeWeightMethod::LOCF);
        order_combine_test(TimeWeightMethod::Linear);
    }

    #[test]
    fn test_mismatch_combine() {
        let s1 = new_from_iter(
            vec![TSPoint { ts: 0, val: 1.0 }, TSPoint { ts: 10, val: 0.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        let s2 = new_from_iter(
            vec![TSPoint { ts: 20, val: 2.0 }, TSPoint { ts: 30, val: 1.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        assert_eq!(s1.combine(&s2), Err(TimeWeightError::MethodMismatch));

        let s1 = new_from_iter(
            vec![TSPoint { ts: 0, val: 1.0 }, TSPoint { ts: 10, val: 0.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        let s2 = new_from_iter(
            vec![TSPoint { ts: 20, val: 2.0 }, TSPoint { ts: 30, val: 1.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        assert_eq!(s1.combine(&s2), Err(TimeWeightError::MethodMismatch));
    }


}
