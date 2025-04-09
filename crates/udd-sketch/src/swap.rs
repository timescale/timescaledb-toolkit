use crate::swap::Kind::Aggregated;
use crate::SketchHashKey::Invalid;
use crate::{SketchHashEntry, SketchHashKey, SketchHashMap};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SwapBucket {
    pub key: SketchHashKey,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum Kind {
    Ordered,
    Unordered,
    Aggregated,
}

/// `Swap` is used for merging sketches. By making it part of the `UDDSketch`, we prevent
/// repeated allocations if it is needed.
/// If it is never used (no aggregations are done) it doesn't heap allocate.
#[derive(Debug, Clone, PartialEq)]
pub struct Swap {
    kind: Kind,
    pub buckets: Vec<SwapBucket>,
}

impl Default for Swap {
    fn default() -> Self {
        Self {
            kind: Aggregated,
            buckets: Vec::new(),
        }
    }
}


impl Swap {
    /// Populates the `Swap` from the given `SketchHashMap`. Will empty the `SketchHashMap`
    pub fn populate_from_map(&mut self, map: &mut SketchHashMap, additional_compactions: u32, capacity: usize) {
        assert!(self.buckets.is_empty());
        self.buckets.reserve(map.len());

        for (mut key, entry) in map.map.drain() {
            for _ in 0..additional_compactions {
                key = key.compact_key();
            }
            self.buckets.push(SwapBucket {
                key,
                count: entry.count,
            });
        }
    }

    /// Ensure the number of distinct buckets in this swap is below this value
    /// Returns the number of compactions executed in order to do this.
    /// The caller is expected to update certain
    /// values in response to the
    /// number of compactions.
    #[must_use]
    pub fn reduce_buckets(&mut self, max_buckets: usize) -> u32 {
        let mut compactions = 0;
        while self.buckets.len() >= max_buckets {
            self.aggregate();
            if self.buckets.len() <= max_buckets {
                break;
            }
            self.compact(1);
            compactions += 1;
        }

        compactions
    }

    /// Append the keys and counts to the Swap
    /// Returns the number of compactions that were required to be able
    /// to stay below `max_buckets`
    pub fn append(
        &mut self,
        keys: impl Iterator<Item = SketchHashKey>,
        counts: impl Iterator<Item = u64>,
        compactions_to_apply_to_keys: u32,
    ) {
        let mut iter = keys.zip(counts);

        let Some(mut current) = iter.next() else {
            return;
        };

        // As we're adding items to our Vec, our ordering is chaos.
        self.kind = Kind::Unordered;

        for _ in 0..compactions_to_apply_to_keys {
            current.0 = current.0.compact_key();
        }

        while let Some(mut next) = iter.next() {
            for _ in 0..compactions_to_apply_to_keys {
                next.0 = next.0.compact_key();
            }

            if current.0 != next.0 {
                self.buckets.push(SwapBucket {
                    key: current.0,
                    count: current.1,
                });
                current = next;
            } else {
                current.1 += next.1;
            }
        }

        // Final one
        self.buckets.push(SwapBucket {
            key: current.0,
            count: current.1,
        });
    }

    /// Populate the `SketchHashMap` with the values from the Swap
    /// Drains the `Swap`
    pub fn populate(&mut self, map: &mut SketchHashMap) {
        debug_assert!(map.map.is_empty());

        self.aggregate();

        let mut iter = self.buckets.drain(..).peekable();

        map.head = iter.peek().map(|i| i.key).unwrap_or(Invalid);

        while let Some(i) = iter.next() {
            map.map.insert(
                i.key,
                SketchHashEntry {
                    count: i.count,
                    next: iter.peek().map(|i| i.key).unwrap_or(Invalid),
                },
            );
        }
    }

    /// Sort the values
    fn sort(&mut self) {
        if matches!(self.kind, Kind::Unordered) {
            self.buckets.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            self.kind = Kind::Ordered;
        }
    }

    /// Run a number of compactions of the `sketch`
    pub fn compact(&mut self, compactions: u32) {
        for bucket in self.buckets.iter_mut() {
            for _ in 0..compactions {
                bucket.key = bucket.key.compact_key();
            }
        }

        self.kind = Kind::Unordered;
    }

    /// Aggregate the values in the `Vec` so that all elements are unique
    /// and are in ascending order
    pub fn aggregate(&mut self) {
        match &self.kind {
            Kind::Ordered => (),
            Kind::Unordered => self.sort(),
            Kind::Aggregated => return,
        };
        debug_assert!(matches!(self.kind, Kind::Ordered));

        let Some(current) = self.buckets.first() else {
            return;
        };
        let mut current = *current;

        let mut new_idx = 0;

        // We're both taking values from the same Vec and populating
        // the Vec. That could be frowned upon, however, we ensure that
        // - new_idx < old_idx all the time
        // - number of new elements <= number of old elements
        for old_idx in 1..self.buckets.len() {
            let next = self.buckets[old_idx];
            if next.key != current.key {
                self.buckets[new_idx] = current;
                current = next;
                new_idx += 1;
            } else {
                current.count += next.count;
            }
        }

        // Final one
        self.buckets[new_idx] = current;
        self.buckets.truncate(new_idx + 1);
        self.kind = Kind::Aggregated;
    }
}
