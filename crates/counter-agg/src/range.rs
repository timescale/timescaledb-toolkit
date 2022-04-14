use std::cmp::{min, max};
use serde::{Deserialize, Serialize};


// we always store ranges as half open, inclusive on left, exclusive on right, 
// we are a discrete type so translating is simple [), this enforces equality
// between ranges like [0, 10) and [0, 9]
// None values denote infinite bounds on that side
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[repr(C)]
pub struct I64Range {
    left: Option<i64>,
    right: Option<i64>
}

impl I64Range {
    /// Panics if `left` > `right`.
    pub fn new(left: Option<i64>, right: Option<i64>) -> Self {
        let range = Self { left, right };
        assert!(range.is_valid());
        range
    }

    pub fn infinite() -> Self {
        Self {
            left: None,
            right: None,
        }
    }

    /// Return `Some([left]))` when it is finite, else `None`.
    #[inline]
    pub fn left(&self) -> Option<i64> {
        self.left
    }

    /// Return `Some([right]))` when it is finite, else `None`.
    #[inline]
    pub fn right(&self) -> Option<i64> {
        self.right
    }

    /// Return `Some(([left], [right]))` when both are finite, else `None`.
    pub fn both(&self) -> Option<(i64, i64)> {
        match (self.left, self.right) {
            (Some(left), Some(right)) => Some((left, right)),
            _ => None,
        }
    }

    pub fn is_infinite_either(&self) -> bool {
        self.is_infinite_left() || self.is_infinite_right()
    }

    pub fn is_infinite(&self) -> bool {
        self.is_infinite_left() && self.is_infinite_right()
    }

    pub fn is_infinite_left(&self) -> bool {
        self.left.is_none()
    }

    pub fn is_infinite_right(&self) -> bool {
        self.right.is_none()
    }

    // TODO See TODO below about range validity.
    fn is_valid(&self) -> bool {
        self.both()
            .map(|(left, right)| left <= right)
            .unwrap_or(true)
    }

    pub fn is_singleton(&self) -> bool {
        self.both()
            .map(|(left, right)| left == right)
            .unwrap_or(false)
    }

    pub fn extend(&mut self, other: &Self) {
        // TODO: What should extend do with invalid ranges on either side? right now it treats them as if they are real...
        self.left = match (self.left, other.left) {
            (None, _) => None,
            (_, None) => None,
            (Some(a), Some(b)) => Some(min(a, b))
        };
        self.right = match (self.right, other.right) {
            (None, _) => None,
            (_, None) => None,
            (Some(a), Some(b)) => Some(max(a, b))
        };
    }

    pub fn contains(&self, pt: i64) -> bool {
        match (self.left, self.right) {
            (Some(l), Some(r)) => pt >= l && pt < r, 
            (Some(l), None) => pt >= l,
            (None, Some(r)) => pt < r, 
            (None, None) => true,
        }
    }

    /// Panics if either `left` or `right` is infinite.
    pub fn duration(&self) -> i64 {
        let (left, right) = self.both().expect("infinite duration");
        right - left
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_extend(){
        let mut a = I64Range{left:Some(4), right:Some(5)};
        let b = I64Range{left:Some(3), right:Some(6)};
        a.extend(&b);
        // b completely covers a
        assert_eq!(a, b);
        // extend to left
        let c = I64Range{left:Some(2), right:Some(5)};
        a.extend(&c);
        assert_eq!(a, I64Range{left:Some(2), right:Some(6)});
        // extend to right
        let d = I64Range{left:Some(6), right:Some(9)};
        a.extend(&d);
        assert_eq!(a, I64Range{left:Some(2), right:Some(9)});
        // infinites
        let e = I64Range{left:Some(10), right:None};
        a.extend(&e);
        assert_eq!(a, I64Range{left:Some(2), right:None});
        let f = I64Range{left:None, right:Some(5)};
        a.extend(&f);
        assert_eq!(a, I64Range{left:None, right:None});
        // if a range contains another, it's unaffected
        a.extend(&c);
        assert_eq!(a, I64Range{left:None, right:None});
        // whether infinite or not
        let mut a = I64Range{left:Some(2), right:Some(9)};
        a.extend(&b);
        assert_eq!(a, I64Range{left:Some(2), right:Some(9)});

        // right now invalid ranges are are extended as normal though they can only ever extend in a single direction
        let weird = I64Range{left:Some(-2), right:Some(-9)};
        a.extend(&weird);
        assert_eq!(a,I64Range{left:Some(-2), right:Some(9)} );
        let weird = I64Range{left:Some(20), right:Some(10)};
        a.extend(&weird);
        assert_eq!(a,I64Range{left:Some(-2), right:Some(10)} );

        //same if we extend a weird one, we can make a valid, or invalid one...
        let mut weird = I64Range{left:Some(-2), right:Some(-9)};

        let weird2 = I64Range{left:Some(-6), right:Some(-10)};
        weird.extend(&weird2);
        assert_eq!(weird, I64Range{left:Some(-6), right:Some(-9)});
        // it is also possible to get a valid range from two weirds
        let weird3 = I64Range{left:Some(6), right:Some(3)};
        weird.extend(&weird3);
        assert_eq!(weird, I64Range{left:Some(-6), right:Some(3)});
        assert!(weird.is_valid());

        // extending with a valid should always produce a valid and will work as usual
        let mut weird = I64Range{left:Some(-6), right:Some(-9)};
        let normal = I64Range{left:Some(2), right:Some(9)};
        weird.extend(&normal);
        assert_eq!(weird, I64Range{left:Some(-6), right:Some(9)});
    }

    #[test]
    fn test_contains(){
        let a = I64Range{left:Some(2), right:Some(5)};
        assert!(a.contains(2));
        assert!(a.contains(4));
        assert!(!a.contains(5));
        assert!(!a.contains(6));

        let a = I64Range{left:None, right:Some(-5)};
        assert!(a.contains(-100));
        assert!(!a.contains(0));
        assert!(!a.contains(6));

        let a = I64Range{left:Some(-10), right:None};
        assert!(a.contains(-10));
        assert!(a.contains(0));
        assert!(a.contains(1000));
        assert!(!a.contains(-20));

        //invalid ranges contain no points 
        let a = I64Range{left:Some(0), right:Some(-5)};
        assert!(!a.contains(-4));
        assert!(!a.contains(1));
        assert!(!a.contains(-6));
    }

    #[test]
    fn test_duration(){
        let a = I64Range{left:Some(3), right:Some(7)};
        assert_eq!(a.duration(), 4);
        let a = I64Range{left:Some(-3), right:Some(7)};
        assert_eq!(a.duration(), 10);
    }

    #[test]
    #[should_panic(expected = "infinite duration")]
    fn duration_infinite_left() {
        I64Range{left:None, right:Some(7)}
        .duration();
    }

    #[test]
    #[should_panic(expected = "infinite duration")]
    fn duration_infinite_right() {
        I64Range{left:Some(-1), right:None}
        .duration();
    }

    #[test]
    #[should_panic(expected = "infinite duration")]
    fn duration_infinite_both() {
        I64Range::infinite().duration();
    }

    #[test]
    fn test_checks() {
        let a = I64Range{left:Some(2), right:Some(5)};
        assert!(a.is_valid());
        assert!(!a.is_singleton());
        let a = I64Range{left:None, right:Some(-5)};
        assert!(a.is_valid());
        assert!(!a.is_singleton());
        let a = I64Range{left:Some(-10), right:None};
        assert!(a.is_valid());
        assert!(!a.is_singleton());
        let a = I64Range{left:Some(2), right:Some(2)};
        assert!(a.is_valid());
        assert!(a.is_singleton());
        assert_eq!(a.duration(), 0);
        let a = I64Range{left:Some(0), right:Some(-10)};
        assert!(!a.is_valid());
        assert!(!a.is_singleton());
    }

    #[test]
    fn infinite() {
        let range = I64Range { left: None, right: None };
        assert!(range.contains(i64::MIN));
        assert!(range.contains(i64::MIN + 1));
        assert!(range.contains(i64::MAX));
        assert!(range.contains(i64::MAX - 1));
    }

    #[test]
    fn exclude_i64_max() {
        let range = I64Range { left: Some(i64::MIN), right: Some(i64::MAX) };
        assert!(range.contains(i64::MIN));
        // TODO If we don't need to exclude i64::MAX, we can simplify even
        //  further and make right non-Option (left already doesn't need to be
        //  Option as None and Some(i64::MIN) are handled the same way.
        //  How important is it that we draw the line at
        //  9,223,372,036,854,775,807 rather than 9,223,372,036,854,775,806?
        assert!(!range.contains(i64::MAX));
    }
}
