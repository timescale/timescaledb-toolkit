use crate::SketchHashKey::Invalid;
use crate::{SketchHashEntry, SketchHashKey, SketchHashMap};

type T = (SketchHashKey, u64);

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
