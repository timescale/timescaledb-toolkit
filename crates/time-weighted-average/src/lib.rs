pub mod tspoint;
#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};
use tspoint::TSPoint;
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum TimeWeightMethod {
    LOCF,
    Linear,
}

#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct TimeWeightSummaryInternal {
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
    EmptyIterator,
}

impl TimeWeightSummaryInternal {
    pub fn new(pt: TSPoint, method: TimeWeightMethod) -> Self {
        TimeWeightSummaryInternal {
            method: method,
            first: pt,
            last: pt,
            w_sum: 0.0,
        }
    }

    pub fn accum(&mut self, pt: TSPoint) -> Result<(), TimeWeightError> {
        if pt.ts < self.last.ts {
            return Err(TimeWeightError::OrderError);
        }
        if pt.ts == self.last.ts {
            // if two points are equal we only use the first we see
            // see discussion at https://github.com/timescale/timescale-analytics/discussions/65
            return Ok(());
        }
        self.w_sum += self.method.weighted_sum(self.last, pt);
        self.last = pt;
        Ok(())
    }

    // This combine function is different than some other combine functions as it requires disjoint time ranges in order to work
    // correctly. The aggregate will never be parallel safe in the Postgres formulation because of this. However in the continuous
    // aggregate context (and potentially in a multinode context) where we can be sure of disjoint time ranges, this will work.
    // If there are space partitions, the space partition keys should be included in the group bys in order to be sure of this, otherwise
    // overlapping ranges will be created.
    pub fn combine(&self, next: &TimeWeightSummaryInternal) -> Result<TimeWeightSummaryInternal, TimeWeightError> {
        if self.method != next.method {
            return Err(TimeWeightError::MethodMismatch);
        }
        if self.last.ts >= next.first.ts {
            // this combine function should always be pulling from disjoint sets, so duplicate values do not need to be handled
            // as we do in accum() (where duplicates are ignored) here we throw an error, because duplicate values should
            // always have been sorted into one or another bucket, and it means that the bounds of our buckets were wrong.
            return Err(TimeWeightError::OrderError);
        }
        let new = TimeWeightSummaryInternal {
            method: self.method,
            first: self.first,
            last: next.last,
            w_sum: self.w_sum + next.w_sum + self.method.weighted_sum(self.last, next.first),
        };
        Ok(new)
    }

    pub fn new_from_sorted_iter<'a>(
        iter: impl IntoIterator<Item = &'a TSPoint>,
        method: TimeWeightMethod,
    ) -> Result<TimeWeightSummaryInternal, TimeWeightError> {
        let mut t = iter.into_iter();
        let mut s = match t.next() {
            None => {
                return Err(TimeWeightError::EmptyIterator);
            }
            Some(val) => TimeWeightSummaryInternal::new(*val, method),
        };
        for p in t {
            s.accum(*p)?;
        }
        Ok(s)
    }

    pub fn combine_sorted_iter<'a>(
        iter: impl IntoIterator<Item = &'a TimeWeightSummaryInternal>,
    ) -> Result<TimeWeightSummaryInternal, TimeWeightError> {
        let mut t = iter.into_iter();
        let mut s = match t.next() {
            None => {
                return Err(TimeWeightError::EmptyIterator);
            }
            Some(val) => *val,
        };
        for p in t {
            s = s.combine(p)?;
        }
        Ok(s)
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
        &self,
        start_prev: Option<(i64, TSPoint)>,
        end_next: Option<(i64, Option<TSPoint>)>,
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

        Ok(TimeWeightSummaryInternal {
            first: new_first,
            w_sum,
            ..*self
        })
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

        Ok(TimeWeightSummaryInternal {
            last: new_last,
            w_sum,
            ..*self
        })
    }
}

impl TimeWeightMethod {
    fn interpolate(
        &self,
        first: TSPoint,
        second: Option<TSPoint>,
        target: i64,
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

    #[test]
    fn test_simple_accum_locf() {
        let mut s = TimeWeightSummaryInternal::new(TSPoint { ts: 0, val: 1.0 }, TimeWeightMethod::LOCF);
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
        let mut s = TimeWeightSummaryInternal::new(TSPoint { ts: 0, val: 1.0 }, TimeWeightMethod::Linear);
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

    fn new_from_sorted_iter_test(t: TimeWeightMethod) {
        // simple test
        let mut s = TimeWeightSummaryInternal::new(TSPoint { ts: 0, val: 1.0 }, t);
        s.accum(TSPoint { ts: 10, val: 0.0 }).unwrap();
        s.accum(TSPoint { ts: 20, val: 2.0 }).unwrap();
        s.accum(TSPoint { ts: 30, val: 1.0 }).unwrap();

        let n = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 10, val: 0.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 1.0 },
            ],
            t,
        )
        .unwrap();
        assert_eq!(s, n);

        //single value
        let s = TimeWeightSummaryInternal::new(TSPoint { ts: 0, val: 1.0 }, t);
        let n =
            TimeWeightSummaryInternal::new_from_sorted_iter(vec![&TSPoint { ts: 0, val: 1.0 }], t).unwrap();
        assert_eq!(s, n);

        //no values should error
        let n = TimeWeightSummaryInternal::new_from_sorted_iter(vec![], t);
        assert_eq!(n, Err(TimeWeightError::EmptyIterator));

    }

    #[test]
    fn test_new_from_sorted_iter(){
        new_from_sorted_iter_test(TimeWeightMethod::LOCF);
        new_from_sorted_iter_test(TimeWeightMethod::Linear);
    }

    fn combine_test(t: TimeWeightMethod) {
        let s = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 10, val: 0.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 1.0 },
            ],
            t,
        )
        .unwrap();
        let s1 = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            t,
        )
        .unwrap();
        let s2 = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 20, val: 2.0 }, &TSPoint { ts: 30, val: 1.0 }],
            t,
        )
        .unwrap();
        let s_comb = s1.combine(&s2).unwrap();
        assert_eq!(s, s_comb);
        // test combine with single val as well as multiple
        let s21 = TimeWeightSummaryInternal::new(TSPoint { ts: 20, val: 2.0 }, t);
        let s22 = TimeWeightSummaryInternal::new(TSPoint { ts: 30, val: 1.0 }, t);
        assert_eq!(s1.combine(&s21).unwrap().combine(&s22).unwrap(), s);
    }
    #[test]
    fn test_combine() {
        combine_test(TimeWeightMethod::LOCF);
        combine_test(TimeWeightMethod::Linear);
    }


    fn order_accum_test(t: TimeWeightMethod) {
        let s = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            t,
        )
        .unwrap();
        let mut o = s;
        // adding points at the same timestamp shouldn't affect the value (no matter whether the
        // value is larger or smaller than the original)
        o.accum(TSPoint { ts: 10, val: 2.0 }).unwrap();
        assert_eq!(s, o);
        o.accum(TSPoint { ts: 10, val: -1.0 }).unwrap();
        assert_eq!(s, o);

        //but adding out of order points doesn't work
        assert_eq!(
            o.accum(TSPoint { ts: 5, val: -1.0 }),
            Err(TimeWeightError::OrderError)
        );
        

        //same for new_from_sorted_iter - test that multiple values only the first is taken
        let n = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 4.0 },
            ],
            t,
        ).unwrap();

        let m = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 20, val: 0.0 },
                &TSPoint { ts: 30, val: 4.0 },
            ],
            t,
        ).unwrap();
        assert_eq!(m, n);
        
        // but out of order inputs correctly error
        let n = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 10, val: 0.0 },
            ],
            t,
        );
        assert_eq!(n,  Err(TimeWeightError::OrderError));
    }
    #[test]
    fn test_order_accum() {
        order_accum_test(TimeWeightMethod::LOCF);
        order_accum_test(TimeWeightMethod::Linear);
    }

    fn order_combine_test(t: TimeWeightMethod) {
        let s = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            t,
        )
        .unwrap();
        let smaller = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 5, val: 1.0 }, &TSPoint { ts: 15, val: 0.0 }],
            t,
        )
        .unwrap();
        // see note above, but 
        let equal = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 15, val: 0.0 }],
            t,
        )
        .unwrap();

        assert_eq!(s.combine(&smaller), Err(TimeWeightError::OrderError));
        assert_eq!(s.combine(&equal), Err(TimeWeightError::OrderError));
    }
    #[test]
    fn test_order_combine(){
        order_combine_test(TimeWeightMethod::LOCF);
        order_combine_test(TimeWeightMethod::Linear);
    }

    fn combine_sorted_iter_test(t:TimeWeightMethod){
        //simple case
        let m = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 0.0 },
                &TSPoint { ts: 40, val: 4.0 },
            ],
            t,
        ).unwrap();
        let a = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
            ],
            t,
        ).unwrap();
        let b = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 30, val: 0.0 },
                &TSPoint { ts: 40, val: 4.0 },
            ],
            t,
        ).unwrap();
        let n = TimeWeightSummaryInternal::combine_sorted_iter(vec![&a, &b]).unwrap();
        assert_eq!(m, n);

        //single values are no problem
        let n = TimeWeightSummaryInternal::combine_sorted_iter(vec![&m]).unwrap();
        assert_eq!(m, n);

        //single values in TimeWeightSummaries are no problem
        let c = TimeWeightSummaryInternal::new(TSPoint { ts: 0, val: 1.0 }, t);
        let d = TimeWeightSummaryInternal::new(TSPoint { ts: 20, val: 2.0 }, t);
        let n = TimeWeightSummaryInternal::combine_sorted_iter(vec![&c, &d, &b]).unwrap();
        assert_eq!(m, n);
        // whether single values come first or later
        let e = TimeWeightSummaryInternal::new(TSPoint { ts: 30, val: 0.0 }, t);
        let f = TimeWeightSummaryInternal::new(TSPoint { ts: 40, val: 4.0 }, t);
        let n = TimeWeightSummaryInternal::combine_sorted_iter(vec![&a, &e, &f]).unwrap();
        assert_eq!(m, n);

        // empty iterators error
        assert_eq!(TimeWeightSummaryInternal::combine_sorted_iter(vec![]), Err(TimeWeightError::EmptyIterator));

        // out of order values error
        let n = TimeWeightSummaryInternal::combine_sorted_iter(vec![&c, &d, &f, &e]);
        assert_eq!(n, Err(TimeWeightError::OrderError));

        // even with two values
        let n = TimeWeightSummaryInternal::combine_sorted_iter(vec![&b, &a]);
        assert_eq!(n, Err(TimeWeightError::OrderError));
    }
    #[test]
    fn test_combine_sorted_iter(){
        combine_sorted_iter_test(TimeWeightMethod::LOCF);
        combine_sorted_iter_test(TimeWeightMethod::Linear);        
    }

    #[test]
    fn test_mismatch_combine() {
        let s1 = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        let s2 = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 20, val: 2.0 }, &TSPoint { ts: 30, val: 1.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        assert_eq!(s1.combine(&s2), Err(TimeWeightError::MethodMismatch));

        let s1 = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        let s2 = TimeWeightSummaryInternal::new_from_sorted_iter(
            vec![&TSPoint { ts: 20, val: 2.0 }, &TSPoint { ts: 30, val: 1.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        assert_eq!(s1.combine(&s2), Err(TimeWeightError::MethodMismatch));
    }

    //next steps:
    // split with_prev / with_next and test
    // add average tests
}
