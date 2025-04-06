use crate::SketchHashKey::Invalid;
use crate::{SketchHashEntry, SketchHashKey, SketchHashMap};
use std::mem::MaybeUninit;

type T = (SketchHashKey, u64);
pub struct ArrayCompactor<const N: usize> {}

impl<const N: usize> ArrayCompactor<N> {
    const INIT: [T; N] = [(SketchHashKey::Invalid, 0); N]; // important for optimization of `new`
    pub fn compact(map: &mut SketchHashMap) {
        let mut len = map.map.len();
        assert!(len <= N);
        let mut buffer = Self::INIT;

        for (idx, bucket) in map.map.drain().enumerate() {
            buffer[idx] = (bucket.0, bucket.1.count);
        }

        // We need to sort as we want to recreate the linked list style
        // in the Hash Map
        // We use the `unstable` variant, as it does not allocate, and its
        // properties are fine for our use-case, and should perform
        // better than the non-stable variant.
        // > This sort is unstable (i.e., may reorder equal elements),
        // > in-place (i.e., does not allocate), and O(n * log(n)) worst-case.
        buffer[0..=len].sort_unstable_by_key(|b| b.0);

        let mut swap_iter = buffer.into_iter();
        compact_from_iter(&mut swap_iter, map)
    }
}

#[inline]
pub fn compact_from_iter(swap_iter: &mut impl Iterator<Item = T>, map: &mut SketchHashMap) {
    let Some(mut current) = swap_iter.next() else {
        return;
    };

    for next in swap_iter {
        if next.0 == Invalid {
            break;
        }

        // This combines those buckets that compact into the same one
        // For example, Positive(9) and Positive(8) both
        // compact into Positive(4)
        if current.0 == next.0 {
            current.1 += next.1;
        } else {
            map.map.insert(
                current.0,
                SketchHashEntry {
                    count: current.1,
                    next: next.0,
                },
            );
            current = next;
        }
    }

    // And the final one ...
    map.map.insert(
        current.0,
        SketchHashEntry {
            count: current.1,
            next: Invalid,
        },
    );
    map.head = map.head.compact_key();
}
