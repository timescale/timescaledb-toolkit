use tspoint::TSPoint;

use Iter::*;

pub enum Iter<'a> {
    Slice {
        iter: flat_serialize::Iter<'a, 'a, TSPoint>
    },
}

impl<'a> Iterator for Iter<'a> {
    type Item = TSPoint;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Slice{iter} => iter.next(),
        }
    }

    // XXX the functions below, `last()` and `count()` in particular rely on
    //     this being precise and accurate, with both elements of the tuple
    //     being the same as the actual yielded number of elements, if this
    //     changes those will also nee to change
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Slice { iter } => (iter.len(), Some(iter.len())),
        }
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.size_hint().0
    }
}