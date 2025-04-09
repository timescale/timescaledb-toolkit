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
    pub fn from_map(map: &mut SketchHashMap, additional_compactions: u32, capacity: usize) -> Self {
        let mut buckets = Vec::with_capacity(capacity);
        for (mut key, entry) in map.map.drain() {
            for _ in 0..additional_compactions {
                key = key.compact_key();
            }
            buckets.push(SwapBucket {
                key,
                count: entry.count,
            });
        }

        Self {
            kind: Kind::Unordered,
            buckets,
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
    #[must_use]
    pub fn append(
        &mut self,
        keys: impl Iterator<Item = SketchHashKey>,
        counts: impl Iterator<Item = u64>,
        compactions_to_apply_to_keys: u32,
        max_buckets: usize,
    ) -> u32 {
        let mut iter = keys.zip(counts);

        let Some(mut current) = iter.next() else {
            return 0;
        };

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
        self.kind = Kind::Unordered;

        self.reduce_buckets(max_buckets)
    }

    /// Populate the `SketchHashMap` with the values from the Swap
    /// Returns the amount of compactions required in order to make it fit
    #[must_use]
    pub fn populate(&mut self, map: &mut SketchHashMap, max_buckets: usize) -> u32 {
        debug_assert!(map.map.is_empty());

        let compactions = self.reduce_buckets(max_buckets);
        self.aggregate();

        debug_assert!(matches!(self.kind, Kind::Aggregated));

        let mut iter = self.buckets.iter().peekable();

        map.head = iter.peek().map(|i| i.key).unwrap_or(Invalid);

        for i in &self.buckets {
            map.map.insert(
                i.key,
                SketchHashEntry {
                    count: i.count,
                    next: iter.peek().map(|i| i.key).unwrap_or(Invalid),
                },
            );
        }

        compactions
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
