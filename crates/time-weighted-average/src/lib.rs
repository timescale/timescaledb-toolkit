use serde::{Deserialize, Serialize};
use tspoint::TSPoint;

use flat_serialize_macro::FlatSerializable;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, FlatSerializable)]
#[repr(u8)]
pub enum TimeWeightMethod {
    LOCF = 0,
    Linear,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub struct TimeWeightSummary {
    pub method: TimeWeightMethod,
    pub first: TSPoint,
    pub last: TSPoint,
    pub w_sum: f64,
}

#[derive(PartialEq, Eq, Debug)]
pub enum TimeWeightError {
    OrderError,
    DoubleOverflow, // do we need to do this?
    MethodMismatch,
    InterpolateMissingPoint,
    ZeroDuration,
    EmptyIterator,
}

impl TimeWeightSummary {
    pub fn new(pt: TSPoint, method: TimeWeightMethod) -> Self {
        TimeWeightSummary {
            method,
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
            // see discussion at https://github.com/timescale/timescaledb-toolkit/discussions/65
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
    pub fn combine(&self, next: &TimeWeightSummary) -> Result<TimeWeightSummary, TimeWeightError> {
        if self.method != next.method {
            return Err(TimeWeightError::MethodMismatch);
        }
        if self.last.ts >= next.first.ts {
            // this combine function should always be pulling from disjoint sets, so duplicate values do not need to be handled
            // as we do in accum() (where duplicates are ignored) here we throw an error, because duplicate values should
            // always have been sorted into one or another bucket, and it means that the bounds of our buckets were wrong.
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

    pub fn new_from_sorted_iter<'a>(
        iter: impl IntoIterator<Item = &'a TSPoint>,
        method: TimeWeightMethod,
    ) -> Result<TimeWeightSummary, TimeWeightError> {
        let mut t = iter.into_iter();
        let mut s = match t.next() {
            None => {
                return Err(TimeWeightError::EmptyIterator);
            }
            Some(val) => TimeWeightSummary::new(*val, method),
        };
        for p in t {
            s.accum(*p)?;
        }
        Ok(s)
    }

    pub fn combine_sorted_iter<'a>(
        iter: impl IntoIterator<Item = &'a TimeWeightSummary>,
    ) -> Result<TimeWeightSummary, TimeWeightError> {
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

    /// Extrapolate a TimeWeightSummary to bounds using the method and provided points outside the bounds of the original summary.
    /// This is especially useful for cases where you want to get an average for, say, a time_bucket, using points outside of that time_bucket.
    /// The initial aggregate will only have points within the time bucket, but outside of it, you will either have a point that you select
    /// or a TimeWeightSummary where the first or last point can be used depending on which bound you are extrapolating to.
    /// 1. The start_prev parameter is optional, but if a start is provided a previous point must be
    /// provided (for both linear and locf weighting methods).
    /// 2. The end_next parameter is also optional, if an end is provided and the locf weighting
    /// method is specified, a next parameter isn't needed, with the linear method, the next
    /// point is needed and we will error if it is not provided.
    pub fn with_bounds(
        &self,
        start_prev: Option<(i64, TSPoint)>,
        end_next: Option<(i64, Option<TSPoint>)>,
    ) -> Result<Self, TimeWeightError> {
        let mut calc = *self;
        if let Some((start, prev)) = start_prev {
            calc = self.with_prev(start, prev)?
        }

        if let Some((end, next)) = end_next {
            calc = self.with_next(end, next)?
        }
        Ok(calc)
    }

    fn with_prev(&self, target_start: i64, prev: TSPoint) -> Result<Self, TimeWeightError> {
        // target_start must always be between [prev.ts, self.first.ts]
        if prev.ts >= self.first.ts || target_start > self.first.ts || prev.ts > target_start {
            return Err(TimeWeightError::OrderError); // should this be a different error?
        }
        if target_start == self.first.ts {
            return Ok(*self);
        }

        let new_first = self
            .method
            .interpolate(prev, Some(self.first), target_start)?;
        let w_sum = self.w_sum + self.method.weighted_sum(new_first, self.first);

        Ok(TimeWeightSummary {
            first: new_first,
            w_sum,
            ..*self
        })
    }

    fn with_next(&self, target_end: i64, next: Option<TSPoint>) -> Result<Self, TimeWeightError> {
        if target_end < self.last.ts {
            // equal is okay, will just reduce to zero add in the sum, but not an error
            return Err(TimeWeightError::OrderError);
        }
        // if our target matches last, there's no work to do, we're already there.
        if target_end == self.last.ts {
            return Ok(*self);
        }

        if let Some(next) = next {
            if next.ts < target_end {
                return Err(TimeWeightError::OrderError);
            }
        }

        let new_last = self.method.interpolate(self.last, next, target_end)?;
        let w_sum = self.w_sum + self.method.weighted_sum(self.last, new_last);

        Ok(TimeWeightSummary {
            last: new_last,
            w_sum,
            ..*self
        })
    }

    ///Evaluate the time_weighted_average from the summary.
    pub fn time_weighted_average(&self) -> Result<f64, TimeWeightError> {
        if self.last.ts == self.first.ts {
            return Err(TimeWeightError::ZeroDuration);
        }
        let duration = (self.last.ts - self.first.ts) as f64;
        Ok(self.w_sum / duration)
    }

    /// Evaluate the integral in microseconds.
    pub fn time_weighted_integral(&self) -> f64 {
        if self.last.ts == self.first.ts {
            // the integral of a duration of zero width is zero
            0.0
        } else {
            self.w_sum
        }
    }
}

impl TimeWeightMethod {
    pub fn interpolate(
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
                (TimeWeightMethod::Linear, None) => {
                    return Err(TimeWeightError::InterpolateMissingPoint)
                }
            },
        };
        Ok(pt)
    }

    pub fn weighted_sum(&self, first: TSPoint, second: TSPoint) -> f64 {
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

    fn new_from_sorted_iter_test(t: TimeWeightMethod) {
        // simple test
        let mut s = TimeWeightSummary::new(TSPoint { ts: 0, val: 1.0 }, t);
        s.accum(TSPoint { ts: 10, val: 0.0 }).unwrap();
        s.accum(TSPoint { ts: 20, val: 2.0 }).unwrap();
        s.accum(TSPoint { ts: 30, val: 1.0 }).unwrap();

        let n = TimeWeightSummary::new_from_sorted_iter(
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
        let s = TimeWeightSummary::new(TSPoint { ts: 0, val: 1.0 }, t);
        let n =
            TimeWeightSummary::new_from_sorted_iter(vec![&TSPoint { ts: 0, val: 1.0 }], t).unwrap();
        assert_eq!(s, n);

        //no values should error
        let n = TimeWeightSummary::new_from_sorted_iter(vec![], t);
        assert_eq!(n, Err(TimeWeightError::EmptyIterator));
    }

    #[test]
    fn test_new_from_sorted_iter() {
        new_from_sorted_iter_test(TimeWeightMethod::LOCF);
        new_from_sorted_iter_test(TimeWeightMethod::Linear);
    }

    fn combine_test(t: TimeWeightMethod) {
        let s = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 10, val: 0.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 1.0 },
            ],
            t,
        )
        .unwrap();
        let s1 = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            t,
        )
        .unwrap();
        let s2 = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 20, val: 2.0 }, &TSPoint { ts: 30, val: 1.0 }],
            t,
        )
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
        let s = TimeWeightSummary::new_from_sorted_iter(
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
        let n = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 4.0 },
            ],
            t,
        )
        .unwrap();

        let m = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 20, val: 0.0 },
                &TSPoint { ts: 30, val: 4.0 },
            ],
            t,
        )
        .unwrap();
        assert_eq!(m, n);

        // but out of order inputs correctly error
        let n = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 10, val: 0.0 },
            ],
            t,
        );
        assert_eq!(n, Err(TimeWeightError::OrderError));
    }
    #[test]
    fn test_order_accum() {
        order_accum_test(TimeWeightMethod::LOCF);
        order_accum_test(TimeWeightMethod::Linear);
    }

    fn order_combine_test(t: TimeWeightMethod) {
        let s = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            t,
        )
        .unwrap();
        let smaller = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 5, val: 1.0 }, &TSPoint { ts: 15, val: 0.0 }],
            t,
        )
        .unwrap();
        // see note above, but
        let equal = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 15, val: 0.0 }],
            t,
        )
        .unwrap();

        assert_eq!(s.combine(&smaller), Err(TimeWeightError::OrderError));
        assert_eq!(s.combine(&equal), Err(TimeWeightError::OrderError));
    }
    #[test]
    fn test_order_combine() {
        order_combine_test(TimeWeightMethod::LOCF);
        order_combine_test(TimeWeightMethod::Linear);
    }

    fn combine_sorted_iter_test(t: TimeWeightMethod) {
        //simple case
        let m = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 0, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 0.0 },
                &TSPoint { ts: 40, val: 4.0 },
            ],
            t,
        )
        .unwrap();
        let a = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 20, val: 2.0 }],
            t,
        )
        .unwrap();
        let b = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 30, val: 0.0 }, &TSPoint { ts: 40, val: 4.0 }],
            t,
        )
        .unwrap();
        let n = TimeWeightSummary::combine_sorted_iter(vec![&a, &b]).unwrap();
        assert_eq!(m, n);

        //single values are no problem
        let n = TimeWeightSummary::combine_sorted_iter(vec![&m]).unwrap();
        assert_eq!(m, n);

        //single values in TimeWeightSummaries are no problem
        let c = TimeWeightSummary::new(TSPoint { ts: 0, val: 1.0 }, t);
        let d = TimeWeightSummary::new(TSPoint { ts: 20, val: 2.0 }, t);
        let n = TimeWeightSummary::combine_sorted_iter(vec![&c, &d, &b]).unwrap();
        assert_eq!(m, n);
        // whether single values come first or later
        let e = TimeWeightSummary::new(TSPoint { ts: 30, val: 0.0 }, t);
        let f = TimeWeightSummary::new(TSPoint { ts: 40, val: 4.0 }, t);
        let n = TimeWeightSummary::combine_sorted_iter(vec![&a, &e, &f]).unwrap();
        assert_eq!(m, n);

        // empty iterators error
        assert_eq!(
            TimeWeightSummary::combine_sorted_iter(vec![]),
            Err(TimeWeightError::EmptyIterator)
        );

        // out of order values error
        let n = TimeWeightSummary::combine_sorted_iter(vec![&c, &d, &f, &e]);
        assert_eq!(n, Err(TimeWeightError::OrderError));

        // even with two values
        let n = TimeWeightSummary::combine_sorted_iter(vec![&b, &a]);
        assert_eq!(n, Err(TimeWeightError::OrderError));
    }
    #[test]
    fn test_combine_sorted_iter() {
        combine_sorted_iter_test(TimeWeightMethod::LOCF);
        combine_sorted_iter_test(TimeWeightMethod::Linear);
    }

    #[test]
    fn test_mismatch_combine() {
        let s1 = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        let s2 = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 20, val: 2.0 }, &TSPoint { ts: 30, val: 1.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        assert_eq!(s1.combine(&s2), Err(TimeWeightError::MethodMismatch));

        let s1 = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 0, val: 1.0 }, &TSPoint { ts: 10, val: 0.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        let s2 = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 20, val: 2.0 }, &TSPoint { ts: 30, val: 1.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        assert_eq!(s1.combine(&s2), Err(TimeWeightError::MethodMismatch));
    }

    #[test]
    fn test_weighted_sum() {
        let pt1 = TSPoint { ts: 10, val: 20.0 };
        let pt2 = TSPoint { ts: 20, val: 40.0 };

        let locf = TimeWeightMethod::LOCF.weighted_sum(pt1, pt2);
        assert_eq!(locf, 200.0);

        let linear = TimeWeightMethod::Linear.weighted_sum(pt1, pt2);
        assert_eq!(linear, 300.0);

        let pt2 = TSPoint { ts: 20, val: -40.0 };

        let locf = TimeWeightMethod::LOCF.weighted_sum(pt1, pt2);
        assert_eq!(locf, 200.0);

        let linear = TimeWeightMethod::Linear.weighted_sum(pt1, pt2);
        assert_eq!(linear, -100.0);
    }

    fn with_prev_common_test(t: TimeWeightMethod) {
        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 20, val: 0.0 }],
            t,
        )
        .unwrap();
        // target = starting point should produce itself no matter the method
        let prev = TSPoint { ts: 5, val: 5.0 };
        let target: i64 = 10;
        assert_eq!(test.with_prev(target, prev).unwrap(), test);

        // target = prev should always produce the same as if we made a new one with prev as the starting point, no matter the extrapolation method, though technically, this shouldn't come up in real world data, because you'd never target a place you had real data for, but that's fine, it's a useful reductive case for testing
        let prev = TSPoint { ts: 5, val: 5.0 };
        let target: i64 = 5;
        let expected = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &prev,
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 0.0 },
            ],
            t,
        )
        .unwrap();
        assert_eq!(test.with_prev(target, prev).unwrap(), expected);

        // prev >= first should produce an order error
        let prev = TSPoint { ts: 10, val: 5.0 };
        let target: i64 = 10;
        assert_eq!(
            test.with_prev(target, prev).unwrap_err(),
            TimeWeightError::OrderError
        );

        // target okay, but prev not less than it
        let prev = TSPoint { ts: 5, val: 5.0 };
        let target: i64 = 2;
        assert_eq!(
            test.with_prev(target, prev).unwrap_err(),
            TimeWeightError::OrderError
        );

        // prev okay, but target > start
        let prev = TSPoint { ts: 5, val: 5.0 };
        let target: i64 = 15;
        assert_eq!(
            test.with_prev(target, prev).unwrap_err(),
            TimeWeightError::OrderError
        );
    }

    #[test]
    fn test_with_prev() {
        // adding a previous point is the same as a TimeWeightSummary constructed from the properly extrapolated previous point and the original
        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 20, val: 0.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        let prev = TSPoint { ts: 0, val: 5.0 };
        let target: i64 = 5;
        let expected_origin = TSPoint { ts: 5, val: 5.0 };
        let expected = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &expected_origin,
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 0.0 },
            ],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        assert_eq!(test.with_prev(target, prev).unwrap(), expected);

        // if the Summary uses a linear method, the extrapolation should be linear as well
        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 20, val: 0.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        let prev = TSPoint { ts: 0, val: 5.0 };
        let target: i64 = 5;
        let expected_origin = TSPoint { ts: 5, val: 3.0 };
        let expected = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &expected_origin,
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 0.0 },
            ],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        assert_eq!(test.with_prev(target, prev).unwrap(), expected);

        // now some common tests:
        with_prev_common_test(TimeWeightMethod::Linear);
        with_prev_common_test(TimeWeightMethod::LOCF);
    }

    fn with_next_common_test(t: TimeWeightMethod) {
        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 20, val: 0.0 }],
            t,
        )
        .unwrap();
        // target = end point should produce itself no matter the method
        let next = TSPoint { ts: 25, val: 5.0 };
        let target: i64 = 20;
        assert_eq!(test.with_next(target, Some(next)).unwrap(), test);

        // target = next should always produce the same as if we added the next point for linear,  and will produce the same w_sum, though not the same final point for LOCF, here' we'll test the w_sum. Though technically, this shouldn't come up in real world data, because you'd never target a place you had real data for, but that's fine, it's a useful reductive case for testing
        let next = TSPoint { ts: 25, val: 5.0 };
        let target: i64 = 25;
        let expected = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 0.0 },
                &next,
            ],
            t,
        )
        .unwrap();
        assert_eq!(
            test.with_next(target, Some(next)).unwrap().w_sum,
            expected.w_sum
        );

        // next <= last should produce an order error
        let next = TSPoint { ts: 20, val: 5.0 };
        let target: i64 = 22;
        assert_eq!(
            test.with_next(target, Some(next)).unwrap_err(),
            TimeWeightError::OrderError
        );

        // target okay, but next not greater than it
        let next = TSPoint { ts: 22, val: 5.0 };
        let target: i64 = 25;
        assert_eq!(
            test.with_next(target, Some(next)).unwrap_err(),
            TimeWeightError::OrderError
        );

        // next okay, but target < last
        let next = TSPoint { ts: 25, val: 5.0 };
        let target: i64 = 15;
        assert_eq!(
            test.with_next(target, Some(next)).unwrap_err(),
            TimeWeightError::OrderError
        );
    }

    #[test]
    fn test_with_next() {
        // adding a target_next point is the same as a TimeWeightSummary constructed from the properly extrapolated next point and the original
        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 20, val: 2.0 }],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        let next = TSPoint { ts: 30, val: 3.0 };
        let target: i64 = 25;
        let expected_next = TSPoint { ts: 25, val: 2.0 };
        let expected = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &expected_next,
            ],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        assert_eq!(test.with_next(target, Some(next)).unwrap(), expected);
        // For LOCF it doesn't matter if next is provided, only the target is required
        assert_eq!(test.with_next(target, None).unwrap(), expected);

        // if the Summary uses a linear method, the extrapolation should be linear as well
        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![&TSPoint { ts: 10, val: 1.0 }, &TSPoint { ts: 20, val: 2.0 }],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        let next = TSPoint { ts: 30, val: 3.0 };
        let target: i64 = 25;
        let expected_next = TSPoint { ts: 25, val: 2.5 };
        let expected = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &expected_next,
            ],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        assert_eq!(test.with_next(target, Some(next)).unwrap(), expected);
        // For Linear method, we need the second point, and not providing a next will error:
        assert_eq!(
            test.with_next(target, None).unwrap_err(),
            TimeWeightError::InterpolateMissingPoint
        );

        // now some common tests:
        with_next_common_test(TimeWeightMethod::Linear);
        with_next_common_test(TimeWeightMethod::LOCF);
    }

    // add average tests
    fn average_common_tests(t: TimeWeightMethod) {
        let single = TimeWeightSummary::new(TSPoint { ts: 20, val: 2.0 }, t);
        assert_eq!(
            single.time_weighted_average().unwrap_err(),
            TimeWeightError::ZeroDuration
        );
    }
    #[test]
    fn test_average() {
        average_common_tests(TimeWeightMethod::Linear);
        average_common_tests(TimeWeightMethod::LOCF);

        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 3.0 },
            ],
            TimeWeightMethod::LOCF,
        )
        .unwrap();
        let expected = (10.0 * 1.0 + 10.0 * 2.0) / (30.0 - 10.0);
        assert_eq!(test.time_weighted_average().unwrap(), expected);
        let test = TimeWeightSummary::new_from_sorted_iter(
            vec![
                &TSPoint { ts: 10, val: 1.0 },
                &TSPoint { ts: 20, val: 2.0 },
                &TSPoint { ts: 30, val: 3.0 },
            ],
            TimeWeightMethod::Linear,
        )
        .unwrap();
        let expected = (10.0 * 1.5 + 10.0 * 2.5) / (30.0 - 10.0);
        assert_eq!(test.time_weighted_average().unwrap(), expected);
    }
}
