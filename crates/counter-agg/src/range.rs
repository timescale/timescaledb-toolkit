use std::cmp::{min, max};
use serde::{Deserialize, Serialize};


// we always store ranges as half open, inclusive on left, exclusive on right, 
// we are a discrete type so translating is simple [), this enforces equality
// between ranges like [0, 10) and [0, 9]
// None values denote infinite bounds on that side
#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct I64Range {
    pub left: Option<i64>,
    pub right: Option<i64>
}


impl I64Range {
    pub fn has_infinite(&self)-> bool{
        self.left.is_none() || self.right.is_none()
    }

    // TODO See TODO below about range validity.  Right now we don't care
    //  much.  If we start to care, move the caring to `new` and `extend`
    //  methods.  That will allow this crate to protect the integrity of
    //  MetricSummary and I64Range in the face of the extension needing to be
    //  able to construct them from raw (and therefore potentially
    //  corrupt) inputs.
    fn is_valid(&self) -> bool {
        match (self.left, self.right) {
            (Some(a), Some(b)) => a <= b, 
            _ => true,
        }
    }

    pub fn is_singleton(&self) -> bool{
        match (self.left, self.right) {
            (Some(a), Some(b)) => a == b, 
            _ => false,
        }
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
    
    // pub fn contains(&self, other: I64Range) -> bool{
    //     unimplemented!()
    // }
    pub fn duration(&self) -> Option<i64> {
        if self.has_infinite() || !self.is_valid() {
            return None
        }
        Some(self.right.unwrap() - self.left.unwrap())
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
        assert_eq!(a.duration().unwrap(), 4);
        let a = I64Range{left:Some(-3), right:Some(7)};
        assert_eq!(a.duration().unwrap(), 10);
        let a = I64Range{left:None, right:Some(7)};
        assert_eq!(a.duration(), None);
        let a = I64Range{left:Some(3), right:None};
        assert_eq!(a.duration(), None);
        //invalid ranges return None durations as well
        let a = I64Range{left:Some(3), right:Some(0)};
        assert_eq!(a.duration(), None);
    }

    #[test]
    fn test_checks(){

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
        assert_eq!(a.duration().unwrap(), 0);
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
        assert!(!range.contains(i64::MAX));
    }
}
