use crate::swap::SwapBucket;
use crate::SketchHashKey::Invalid;
use crate::{SketchHashEntry, SketchHashKey, SketchHashMap};

#[inline]
pub fn compact_from_iter(
    swap_iter: &mut impl Iterator<Item = SwapBucket>,
    map: &mut SketchHashMap,
) {
    let Some(mut current) = swap_iter.next() else {
        return;
    };

    for next in swap_iter {
        if next.key == Invalid {
            break;
        }

        // This combines those buckets that compact into the same one
        // For example, Positive(9) and Positive(8) both
        // compact into Positive(4)
        if current.key == next.key {
            current.count += next.count;
        } else {
            map.map.insert(
                current.key,
                SketchHashEntry {
                    count: current.count,
                    next: next.key,
                },
            );
            current = next;
        }
    }

    // And the final one ...
    map.map.insert(
        current.key,
        SketchHashEntry {
            count: current.count,
            next: Invalid,
        },
    );
    map.head = map.head.compact_key();
}
