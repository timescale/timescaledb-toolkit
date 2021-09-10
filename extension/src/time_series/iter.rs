use time_series::TSPoint;

use Iter::*;

pub enum Iter<'a> {
    Slice {
        iter: flat_serialize::Iter<'a, 'a, TSPoint>
    },
    Normal {
        idx: u64,
        start: i64,
        step: i64,
        vals: flat_serialize::Iter<'a, 'a, f64>,
    },
    GappyNormal {
        idx: u64,
        count: u64,
        start: i64,
        step: i64,
        present: &'a [u64],
        vals: flat_serialize::Iter<'a, 'a, f64>,
    },
}

impl<'a> Iterator for Iter<'a> {
    type Item = TSPoint;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Slice{iter} => {
                match iter.next() {
                    None => None,
                    Some(point) => Some(point)
                }
            },
            Normal{idx, start, step, vals} => {
                let val = vals.next();
                if val.is_none() {
                    return None;
                }
                let val = val.unwrap();
                let ts = *start + *idx as i64 * *step;
                *idx += 1;
                Some(TSPoint{ts, val})
            }
            GappyNormal{idx, count, start, step, present, vals} => {
                if idx >= count {
                    return None;
                }
                while present[(*idx/64) as usize] & (1 << (*idx % 64)) == 0 {
                    *idx += 1;
                }
                let ts = *start + *idx as i64 * *step;
                let val = vals.next().unwrap();  // last entry of gappy series is required to be a value, so this must not return None here
                *idx += 1;
                Some(TSPoint{ts, val})
            }
        }
    }

    // XXX the functions below, `last()` and `count()` in particular rely on
    //     this being precise and accurate, with both elements of the tuple
    //     being the same as the actual yielded number of elements, if this
    //     changes those will also nee to change
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Slice { iter } => (iter.len(), Some(iter.len())),
            Normal { idx: _, start: _, step: _, vals } => (vals.len(), Some(vals.len())),
            GappyNormal { idx: _, count, start: _, step: _, present: _, vals: _ } =>
                (*count as _, Some(*count as _)),
        }
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.size_hint().0
    }
}