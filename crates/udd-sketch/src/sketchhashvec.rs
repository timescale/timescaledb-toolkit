use std::collections::VecDeque;
use crate::{SketchHashEntry, SketchHashKey, SketchHashMap};
use crate::SketchHashKey::Invalid;

pub enum SketchHashContainer {
    Map(SketchHashMap),
    Vec(SketchHashVec)
}

impl SketchHashMap {
    pub fn compact_into(&mut self, vec: &mut SketchHashVec, compactions: u32) {
        assert!(vec.inner.is_empty());
        vec.inner.reserve(self.map.len());

        for (k, v) in self.map.drain() {
            let k = k.compact_key_multiple(compactions);
            vec.inner.push(SketchBucket::new(k, v.count));
        }
    }
}

#[derive(Copy, Clone)]
struct SketchBucket {
    bucket: SketchHashKey,
    count: u64
}

impl SketchBucket {
    const fn new(bucket: SketchHashKey, count: u64) -> Self {
        Self {
            bucket,
            count
        }
    }
}


/// A `SketchHashVec` contains the buckets of a `UDDSketch`.
/// It may contain duplicate buckets, for example, after compacting or merging.
/// Therefore, it is not usable by itself by an end-user, but it is an intermediate representation
/// to speed up compactions and merges.
pub struct SketchHashVec {
    inner: Vec<SketchBucket>,
}

impl SketchHashVec {

    /// Compact the values in this `SketchHashVec`.
    pub fn compact(&mut self, compactions: u32) {
        for entry in self.inner.iter_mut() {
            entry.bucket = entry.bucket.compact_key_multiple(compactions);
        }
    }

    /// `reduce` does an in-place reduction in number of elements
    /// in this Vec. It combines adjacent values into 1 aggregated value.
    /// It does not allocate.
    pub fn reduce(&mut self) {
        if self.inner.len() < 2 {
            return;
        }

        self.inner.sort_unstable_by_key(|k| k.bucket);

        let mut new_idx = 0;
        let mut current = self.inner[0];

        // SAFETY: While we loop over the Vec, we also modify it.
        // we know this is safe, as we old_idx > new_idx always
        unsafe {
            for old_idx in 1..self.inner.len() {
                debug_assert!(old_idx > new_idx);

                let next = self.inner[old_idx];
                if next.bucket != current.bucket {
                    self.inner[new_idx] = current;
                    current = next;
                    new_idx += 1;
                } else {
                    current.count += next.count;
                }
            }

            // Final one
            self.inner[new_idx] = current;
            self.inner.set_len(new_idx + 1);
        }
    }

    /// Populate the linked-list style of the Map  using this Vec.
    /// The caller should ensure that the provided `map` is actually empty.
    pub fn drain_into(&mut self, map: &mut SketchHashMap) {
        assert!(map.map.is_empty());

        // We need to sort as we want to recreate the linked list style
        // in the Hash Map
        // We use the `unstable` variant, as it does not allocate, and its
        // properties are fine for our use-case, and should perform
        // better than the non-stable variant.
        // > This sort is unstable (i.e., may reorder equal elements),
        // > in-place (i.e., does not allocate), and O(n * log(n)) worst-case.
        self.inner.sort_unstable_by_key(|k| k.bucket);

        let mut swap_iter = self.inner.drain(..);
        let Some(mut current) = swap_iter.next() else {
            map.head = Invalid;
            return;
        };

        for next in swap_iter {
            if next.bucket == Invalid {
                break;
            }

            // This combines those buckets that compact into the same one
            // For example, Positive(9) and Positive(8) both
            // compact into Positive(4)
            if current.bucket == next.bucket {
                current.count += next.count;
            } else {
                map.map.insert(
                    current.bucket,
                    SketchHashEntry {
                        count: current.count,
                        next: next.bucket,
                    },
                );
                current = next;
            }
        }

        // And the final one ...
        map.map.insert(
            current.bucket,
            SketchHashEntry {
                count: current.count,
                next: Invalid,
            },
        );
    }
}