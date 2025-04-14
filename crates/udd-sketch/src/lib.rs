//! UDDSketch implementation in rust.
//! Based on the paper: https://arxiv.org/abs/2004.08604

use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

#[cfg(test)]
use ordered_float::OrderedFloat;
#[cfg(test)]
use std::collections::HashSet;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

// This is used to index the buckets of the UddSketch.  In particular, because UddSketch stores values
// based on a logarithmic scale, we need to track negative values separately from positive values, and
// zero also needs special casing.
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Copy, Clone, Debug)]
pub enum SketchHashKey {
    Negative(i64),
    Zero,
    Positive(i64),
    Invalid,
}

// Invalid is treated as greater than valid values (making it a nice boundary value for list end)
impl std::cmp::PartialOrd for SketchHashKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use self::SketchHashKey::*;
        use std::cmp::Ordering::*;
        match (self, other) {
            (Invalid, Invalid) => Equal,
            (Invalid, _) => Greater,
            (_, Invalid) => Less,
            (Zero, Zero) => Equal,
            (Positive(a), Positive(b)) => a.cmp(b),
            (Negative(a), Negative(b)) => a.cmp(b).reverse(),
            (_, Positive(_)) => Less,
            (Positive(_), _) => Greater,
            (_, Negative(_)) => Greater,
            (Negative(_), _) => Less,
        }
        .into()
    }
}

/// `UDDSketchMetadata` was created to avoid passing along many parameters
/// to function calls.
pub struct UDDSketchMetadata {
    pub max_buckets: u32,
    pub current_error: f64,
    pub compactions: u32,
    pub values: u64,
    pub sum: f64,
}
impl SketchHashKey {
    /// This is the key corresponding to the current key after the SketchHashMap it refers to has gone through one compaction.
    /// Note that odd buckets get combined with the bucket after them (i.e. old buckets -3 and -2 become new bucket -1, {-1, 0} -> 0, {1, 2} -> 1)
    fn compact_key(&self) -> SketchHashKey {
        use SketchHashKey::*;

        match *self {
            Negative(i64::MAX) => *self, // Infinite buckets remain infinite
            Positive(i64::MAX) => *self,
            Negative(x) => Negative(if x > 0 { x + 1 } else { x } / 2),
            Positive(x) => Positive(if x > 0 { x + 1 } else { x } / 2),
            x => x, // Zero and Invalid don't compact
        }
    }
}

// Entries in the SketchHashMap contain a count and the next valid index of the map.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct SketchHashEntry {
    count: u64,
    next: SketchHashKey,
}

// SketchHashMap is a special hash map of SketchHashKey->count that also keeps the equivalent of a linked list of the entries by increasing key value.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct SketchHashMap {
    map: HashMap<SketchHashKey, SketchHashEntry>,
    head: SketchHashKey,
}

impl std::ops::Index<SketchHashKey> for SketchHashMap {
    type Output = u64;

    fn index(&self, id: SketchHashKey) -> &Self::Output {
        &self.map[&id].count
    }
}

// Iterator for a SketchHashMap will travel through the map in order of increasing key value and return the (key, count) pairs
#[derive(Clone)]
pub struct SketchHashIterator<'a> {
    container: &'a SketchHashMap,
    next_key: SketchHashKey,
}

impl<'a> Iterator for SketchHashIterator<'a> {
    type Item = (SketchHashKey, u64);

    fn next(&mut self) -> Option<(SketchHashKey, u64)> {
        if self.next_key == SketchHashKey::Invalid {
            None
        } else {
            let key = self.next_key;
            self.next_key = self.container.map[&self.next_key].next;
            Some((key, self.container[key]))
        }
    }
}

impl SketchHashMap {
    fn new() -> SketchHashMap {
        SketchHashMap {
            map: HashMap::new(),
            head: SketchHashKey::Invalid,
        }
    }

    fn with_capacity(capacity: usize) -> SketchHashMap {
        SketchHashMap {
            map: HashMap::with_capacity(capacity),
            head: SketchHashKey::Invalid,
        }
    }

    /// Increment the count at a key, creating the entry if needed.
    fn increment(&mut self, key: SketchHashKey) {
        self.entry_upsert(key, 1);
    }

    fn iter(&self) -> SketchHashIterator {
        SketchHashIterator {
            container: self,
            next_key: self.head,
        }
    }

    /// Splits an entry if `key` is supposed to come right after it
    /// Returns the key *after* the one that was split.
    #[inline]
    #[must_use] // The caller should really do something with this information.
    fn entry_split(&mut self, key: SketchHashKey) -> SketchHashKey {
        debug_assert_ne!(
            key,
            SketchHashKey::Invalid,
            "Invalid should never be used as a key into the SketchHashMap"
        );

        let next: SketchHashKey;

        // Special case, if we're actually in front of the Head,
        // we're not really splitting the linked list, but prepending.
        if key < self.head {
            next = self.head;
            self.head = key;
            return next;
        }

        // Unfortunately, we'll now have to walk the whole map in order
        // to find the location where we should be inserted
        // into the single-linked list
        for (k, e) in self.map.iter_mut() {
            if *k < key && e.next > key {
                next = e.next;
                e.next = key;
                return next;
            }
        }

        unreachable!("Invalid key found");
    }

    /// Upsert the given key/count into our map. This function
    /// ensures the Linked List is in good shape afterwards.
    #[inline]
    fn entry_upsert(&mut self, key: SketchHashKey, count: u64) {
        match self.map.entry(key) {
            Entry::Occupied(mut o) => {
                o.get_mut().count += count;
                // Great, we don't have to update the Linked List
                return;
            }
            Entry::Vacant(v) if self.head > key => {
                v.insert(SketchHashEntry {
                    count,
                    next: self.head,
                });
                self.head = key;
                // Great, we don't have to update the Linked List
                return;
            }
            Entry::Vacant(_) => (), // We need to release our &mut map here, as we need to update 2 entries
        };

        // We've just inserted a new value, but need to ensure we fix the linked list again.
        let new_next = self.entry_split(key);
        self.map.insert(
            key,
            SketchHashEntry {
                count,
                next: new_next,
            },
        );
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    // Combine adjacent buckets
    fn compact(&mut self) {
        let mut target = self.head;
        // TODO can we do without this additional map?
        let old_map = std::mem::take(&mut self.map);

        self.head = self.head.compact_key();

        while target != SketchHashKey::Invalid {
            let old_entry = &old_map[&target];
            let new_key = target.compact_key();
            // it doesn't matter where buckets are absolutely, their relative
            // positions will remain unchanged unless two buckets are compacted
            // together
            let new_next = if old_entry.next.compact_key() == new_key {
                // the old `next` bucket is going to be compacted into the same
                // one as `target`
                old_map[&old_entry.next].next.compact_key()
            } else {
                old_entry.next.compact_key()
            };
            self.map
                .entry(new_key)
                .or_insert(SketchHashEntry {
                    count: 0,
                    next: new_next,
                })
                .count += old_entry.count;
            target = old_map[&target].next;
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UDDSketch {
    buckets: SketchHashMap,
    alpha: f64,
    gamma: f64,
    compactions: u32, // should always be smaller than 64
    max_buckets: u64,
    num_values: u64,
    values_sum: f64,
}

impl UDDSketch {
    pub fn new(max_buckets: u64, initial_error: f64) -> Self {
        assert!((1e-12..1.0).contains(&initial_error));
        UDDSketch {
            buckets: SketchHashMap::new(),
            alpha: initial_error,
            gamma: (1.0 + initial_error) / (1.0 - initial_error),
            compactions: 0,
            max_buckets,
            num_values: 0,
            values_sum: 0.0,
        }
    }

    // This constructor is used to recreate a UddSketch from its component data
    pub fn new_from_data(
        metadata: &UDDSketchMetadata,
        mut keys: impl Iterator<Item = SketchHashKey>,
        mut counts: impl Iterator<Item = u64>,
    ) -> Self {
        let mut sketch = UDDSketch {
            buckets: SketchHashMap::with_capacity(metadata.values as usize),
            alpha: metadata.current_error,
            gamma: gamma(metadata.current_error),
            compactions: metadata.compactions,
            max_buckets: metadata.max_buckets as u64,
            num_values: metadata.values,
            values_sum: metadata.sum,
        };

        while let (Some(key), Some(count)) = (keys.next(), counts.next()) {
            sketch.buckets.entry_upsert(key, count);
        }

        sketch
    }
}

impl UDDSketch {
    // For a given value return the index of it's bucket in the current sketch.
    fn key(&self, value: f64) -> SketchHashKey {
        key(value, self.gamma)
    }

    pub fn compact_buckets(&mut self) {
        self.buckets.compact();

        self.compactions += 1;
        self.gamma *= self.gamma; // See https://arxiv.org/pdf/2004.08604.pdf Equation 3
        self.alpha = 2.0 * self.alpha / (1.0 + self.alpha.powi(2)); // See https://arxiv.org/pdf/2004.08604.pdf Equation 4
    }

    pub fn bucket_iter(&self) -> SketchHashIterator {
        self.buckets.iter()
    }
}

impl UDDSketch {
    pub fn add_value(&mut self, value: f64) {
        self.buckets.increment(self.key(value));

        while self.buckets.len() > self.max_buckets as usize {
            self.compact_buckets();
        }

        self.num_values += 1;
        self.values_sum += value;
    }

    /// `merge_items` will merge these values into the current sketch
    /// it requires less memory than `merge_sketch`, as that needs a fully serialized
    /// `UDDSketch`, whereas this function relies on iterators to do its job.
    pub fn merge_items(
        &mut self,
        other: &UDDSketchMetadata,
        mut keys: impl Iterator<Item = SketchHashKey>,
        mut counts: impl Iterator<Item = u64>,
    ) {
        let other_gamma = gamma(other.current_error);
        // Require matching initial parameters
        debug_assert!(
            (self
                .gamma
                .powf(1.0 / f64::powi(2.0, self.compactions as i32))
                - other_gamma.powf(1.0 / f64::powi(2.0, other.compactions as i32)))
            .abs()
                < 1e-9 // f64::EPSILON too small, see issue #396
        );
        debug_assert_eq!(self.max_buckets, other.max_buckets as u64);

        if other.values == 0 {
            return;
        }

        while self.compactions < other.compactions {
            self.compact_buckets();
        }

        let extra_compactions = self.compactions - other.compactions;
        while let (Some(mut key), Some(count)) = (keys.next(), counts.next()) {
            for _ in 0..extra_compactions {
                key = key.compact_key();
            }

            self.buckets.entry_upsert(key, count);
        }

        while self.buckets.len() > self.max_buckets as usize {
            self.compact_buckets();
        }

        self.num_values += other.values;
        self.values_sum += other.sum;
    }

    pub fn merge_sketch(&mut self, other: &UDDSketch) {
        // Require matching initial parameters
        assert!(
            (self
                .gamma
                .powf(1.0 / f64::powi(2.0, self.compactions as i32))
                - other
                    .gamma
                    .powf(1.0 / f64::powi(2.0, other.compactions as i32)))
            .abs()
                < 1e-9 // f64::EPSILON too small, see issue #396
        );
        assert!(self.max_buckets == other.max_buckets);

        if other.num_values == 0 {
            return;
        }
        if self.num_values == 0 {
            *self = other.clone();
            return;
        }

        let mut other = other.clone();

        while self.compactions > other.compactions {
            other.compact_buckets();
        }
        while other.compactions > self.compactions {
            self.compact_buckets();
        }

        for entry in other.buckets.iter() {
            let (key, value) = entry;
            self.buckets.entry_upsert(key, value);
        }

        while self.buckets.len() > self.max_buckets as usize {
            self.compact_buckets();
        }

        self.num_values += other.num_values;
        self.values_sum += other.values_sum;
    }

    pub fn max_allowed_buckets(&self) -> u64 {
        self.max_buckets
    }

    pub fn times_compacted(&self) -> u32 {
        self.compactions
    }

    pub fn current_buckets_count(&self) -> usize {
        self.buckets.map.len()
    }
}

impl UDDSketch {
    #[inline]
    pub fn mean(&self) -> f64 {
        if self.num_values == 0 {
            0.0
        } else {
            self.values_sum / self.num_values as f64
        }
    }

    #[inline]
    pub fn sum(&self) -> f64 {
        self.values_sum
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.num_values
    }

    #[inline]
    pub fn max_error(&self) -> f64 {
        self.alpha
    }

    pub fn estimate_quantile(&self, quantile: f64) -> f64 {
        estimate_quantile(
            quantile,
            self.alpha,
            self.gamma,
            self.num_values,
            self.buckets.iter(),
        )
    }

    pub fn estimate_quantile_at_value(&self, value: f64) -> f64 {
        estimate_quantile_at_value(value, self.gamma, self.num_values, self.buckets.iter())
    }
}

pub fn estimate_quantile(
    quantile: f64,
    alpha: f64,
    gamma: f64,
    num_values: u64,
    buckets: impl Iterator<Item = (SketchHashKey, u64)>,
) -> f64 {
    assert!((0.0..=1.0).contains(&quantile));

    let mut remaining = (num_values as f64 * quantile) as u64 + 1;
    if remaining >= num_values {
        return last_bucket_value(alpha, gamma, buckets);
    }

    for entry in buckets {
        let (key, count) = entry;
        if remaining <= count {
            return bucket_to_value(alpha, gamma, key);
        } else {
            remaining -= count;
        }
    }
    unreachable!();
}

// Look up the value of the last bucket
// This is not an efficient operation
fn last_bucket_value(
    alpha: f64,
    gamma: f64,
    buckets: impl Iterator<Item = (SketchHashKey, u64)>,
) -> f64 {
    let (key, _) = buckets.last().unwrap();
    bucket_to_value(alpha, gamma, key)
}

/// inverse of `key()` within alpha
fn bucket_to_value(alpha: f64, gamma: f64, bucket: SketchHashKey) -> f64 {
    // When taking gamma ^ i below we have to use powf as powi only takes a u32, and i can exceed 2^32 for small alphas
    match bucket {
        SketchHashKey::Zero => 0.0,
        SketchHashKey::Positive(i) => gamma.powf(i as f64 - 1.0) * (1.0 + alpha),
        SketchHashKey::Negative(i) => -(gamma.powf(i as f64 - 1.0) * (1.0 + alpha)),
        SketchHashKey::Invalid => panic!("Unable to convert invalid bucket id to value"),
    }
}

pub fn estimate_quantile_at_value(
    value: f64,
    gamma: f64,
    num_values: u64,
    buckets: impl Iterator<Item = (SketchHashKey, u64)>,
) -> f64 {
    let mut count = 0.0;
    let target = key(value, gamma);

    for entry in buckets {
        let (key, value) = entry;
        if target > key {
            count += value as f64;
        } else {
            if target == key {
                // If the value falls in the target bucket, assume it's greater than half the other values
                count += value as f64 / 2.0;
            }
            return count / num_values as f64;
        }
    }

    1.0 // Greater than anything in the sketch
}

fn key(value: f64, gamma: f64) -> SketchHashKey {
    let negative = value < 0.0;
    let value = value.abs();

    if value == 0.0 {
        SketchHashKey::Zero
    } else if negative {
        SketchHashKey::Negative(value.log(gamma).ceil() as i64)
    } else {
        SketchHashKey::Positive(value.log(gamma).ceil() as i64)
    }
}

pub fn gamma(alpha: f64) -> f64 {
    (1.0 + alpha) / (1.0 - alpha)
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    fn build_and_add_values() {
        let mut sketch = UDDSketch::new(20, 0.1);
        sketch.add_value(1.0);
        sketch.add_value(3.0);
        sketch.add_value(0.5);

        assert_eq!(sketch.count(), 3);
        assert_eq!(sketch.mean(), 1.5);
        assert_eq!(sketch.max_error(), 0.1);
    }

    #[test]
    fn exceed_buckets() {
        let mut sketch = UDDSketch::new(20, 0.1);
        sketch.add_value(1.1); // Bucket #1
        sketch.add_value(400.0); // Bucket #30
        let a2 = 0.2 / 1.01;

        assert_eq!(sketch.count(), 2);
        assert_eq!(sketch.max_error(), 0.1);
        for i in 2..20 {
            sketch.add_value(1000.0 * 1.23_f64.powi(i));
        }

        assert_eq!(sketch.count(), 20);
        assert_eq!(sketch.max_error(), 0.1);

        for i in 20..30 {
            sketch.add_value(1000.0 * 1.23_f64.powi(i));
        }

        assert_eq!(sketch.count(), 30);
        assert_eq!(sketch.max_error(), a2);
    }

    /// We create this `merge_verifier` so that every test we run also tests
    /// the multiple implementations we have for merging sketches.
    /// It is a drop-in replacement for `merge_sketches`, with additional asserts.
    fn merge_verifier(sketch: &mut UDDSketch, other: &UDDSketch) {
        let mut second = sketch.clone();

        sketch.merge_sketch(other);

        let mut keys = Vec::with_capacity(other.num_values as usize);
        let mut counts = Vec::with_capacity(other.num_values as usize);
        for (key, count) in other.buckets.iter() {
            keys.push(key);
            counts.push(count);
        }

        let metadata = UDDSketchMetadata {
            max_buckets: other.max_buckets as u32,
            current_error: other.alpha,
            compactions: other.compactions,
            values: other.num_values,
            sum: other.values_sum,
        };

        second.merge_items(&metadata, keys.into_iter(), counts.into_iter());

        // Both methods should result in the same end result.
        assert_eq!(*sketch, second);
    }

    #[test]
    fn merge_sketches() {
        let a1 = 0.1; // alpha for up to 20 buckets
        let a2 = 0.2 / 1.01; // alpha for 1 compaction
        let a3 = 2.0 * a2 / (1.0 + f64::powi(a2, 2)); // alpha for 2 compactions
        let a4 = 2.0 * a3 / (1.0 + f64::powi(a3, 2)); // alpha for 3 compactions
        let a5 = 2.0 * a4 / (1.0 + f64::powi(a4, 2)); // alpha for 4 compactions

        let mut sketch1 = UDDSketch::new(20, 0.1);
        sketch1.add_value(1.1); // Bucket #1
        sketch1.add_value(1.5); // Bucket #3
        sketch1.add_value(1.6); // Bucket #3
        sketch1.add_value(1.3); // Bucket #2
        sketch1.add_value(4.2); // Bucket #8

        assert_eq!(sketch1.count(), 5);
        assert_eq!(sketch1.max_error(), a1);

        let mut sketch2 = UDDSketch::new(20, 0.1);
        sketch2.add_value(5.1); // Bucket #9
        sketch2.add_value(7.5); // Bucket #11
        sketch2.add_value(10.6); // Bucket #12
        sketch2.add_value(9.3); // Bucket #12
        sketch2.add_value(11.2); // Bucket #13

        assert_eq!(sketch2.max_error(), a1);

        merge_verifier(&mut sketch1, &sketch2);
        assert_eq!(sketch1.count(), 10);
        assert_eq!(sketch1.max_error(), a1);

        let mut sketch3 = UDDSketch::new(20, 0.1);
        sketch3.add_value(0.8); // Bucket #-1
        sketch3.add_value(3.7); // Bucket #7
        sketch3.add_value(15.2); // Bucket #14
        sketch3.add_value(3.4); // Bucket #7
        sketch3.add_value(0.6); // Bucket #-2

        assert_eq!(sketch3.max_error(), a1);

        merge_verifier(&mut sketch1, &sketch3);
        assert_eq!(sketch1.count(), 15);
        assert_eq!(sketch1.max_error(), a1);

        let mut sketch4 = UDDSketch::new(20, 0.1);
        sketch4.add_value(400.0); // Bucket #30
        sketch4.add_value(0.004); // Bucket #-27
        sketch4.add_value(0.0); // Zero Bucket
        sketch4.add_value(-400.0); // Neg. Bucket #30
        sketch4.add_value(-0.004); // Neg. Bucket #-27
        sketch4.add_value(400000000000.0); // Some arbitrary large bucket
        sketch4.add_value(0.00000005); // Some arbitrary small bucket
        sketch4.add_value(-400000000000.0); // Some arbitrary large neg. bucket
        sketch4.add_value(-0.00000005); // Some arbitrary small neg. bucket

        assert_eq!(sketch4.max_error(), a1);

        merge_verifier(&mut sketch1, &sketch4);

        assert_eq!(sketch1.count(), 24);
        assert_eq!(sketch1.max_error(), a2);

        let mut sketch5 = UDDSketch::new(20, 0.1);
        for i in 100..220 {
            sketch5.add_value(1.23_f64.powi(i));
        }

        assert_eq!(sketch5.max_error(), a4);

        merge_verifier(&mut sketch1, &sketch5);

        assert_eq!(sketch1.count(), 144);
        assert_eq!(sketch1.max_error(), a5); // Note that each compaction doesn't always result in half the numbers of buckets, hence a5 here instead of a4
    }

    #[test]
    fn test_quantile_and_value_estimates() {
        let mut sketch = UDDSketch::new(50, 0.1);
        for v in 1..=10000 {
            sketch.add_value(v as f64 / 100.0);
        }
        assert_eq!(sketch.count(), 10000);
        assert_eq!(sketch.max_error(), 0.1);

        for i in 1..=100 {
            let value = i as f64;
            let quantile = value / 100.0;
            let quantile_value = value + 0.01; // correct value for quantile should be next number > value

            let test_value = sketch.estimate_quantile(quantile);
            let test_quant = sketch.estimate_quantile_at_value(value);

            let percentage = (test_value - quantile_value).abs() / quantile_value;
            assert!(
                percentage <= 0.1,
                "Exceeded 10% error on quantile {}: expected {}, received {} (error% {})",
                quantile,
                quantile_value,
                test_value,
                (test_value - quantile_value).abs() / quantile_value
            );
            let percentage = (test_quant - quantile).abs() / quantile;
            assert!(
                percentage < 0.2,
                "Exceeded 20% error on quantile at value {}: expected {}, received {} (error% {})",
                value,
                quantile,
                test_quant,
                (test_quant - quantile).abs() / quantile
            );
        }

        assert!((sketch.mean() - 50.005).abs() < 0.001);
    }

    #[test]
    fn test_extreme_quantile_at_value() {
        let mut sketch = UDDSketch::new(50, 0.1);
        for v in 1..=10000 {
            sketch.add_value(v as f64 / 100.0);
        }

        assert_eq!(sketch.estimate_quantile_at_value(-100.0), 0.0);
        assert_eq!(sketch.estimate_quantile_at_value(0.0), 0.0);
        assert_eq!(sketch.estimate_quantile_at_value(0.0001), 0.0);
        assert_eq!(sketch.estimate_quantile_at_value(1000.0), 1.0);
        assert!(sketch.estimate_quantile_at_value(0.01) < 0.0001);
        assert!(sketch.estimate_quantile_at_value(100.0) > 0.9);
    }

    #[test]
    fn random_stress() {
        let mut sketch = UDDSketch::new(1000, 0.01);
        let seed = rand::thread_rng().gen();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut bounds = Vec::new();
        for _ in 0..100 {
            let v = rng.gen_range(-1000000.0..1000000.0);
            sketch.add_value(v);
            bounds.push(v);
        }
        bounds.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mut prev = -2000000.0;
        for f in bounds.iter() {
            for _ in 0..10000 {
                sketch.add_value(rng.gen_range(prev..*f));
            }
            prev = *f;
        }

        for i in 0..100 {
            assert!(((sketch.estimate_quantile((i as f64 + 1.0) / 100.0) / bounds[i]) - 1.0).abs() < sketch.max_error() * bounds[i].abs(),
            "Failed to correct match {} quantile with seed {}.  Received: {}, Expected: {}, Error: {}, Expected error bound: {}",
            (i as f64 + 1.0) / 100.0,
            seed,
            sketch.estimate_quantile((i as f64 + 1.0) / 100.0),
            bounds[i],
            ((sketch.estimate_quantile((i as f64 + 1.0) / 100.0) / bounds[i]) - 1.0).abs() / bounds[i].abs(),
            sketch.max_error());
        }
    }

    use crate::SketchHashKey::Invalid;
    use quickcheck::*;

    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Debug)]
    struct OrderedF64(OrderedFloat<f64>);

    impl Arbitrary for OrderedF64 {
        fn arbitrary(g: &mut Gen) -> Self {
            OrderedF64(f64::arbitrary(g).into())
        }
    }

    #[test]
    #[should_panic]
    fn test_entry_invalid_hashmap_key() {
        let mut map = SketchHashMap {
            map: HashMap::new(),
            head: Invalid,
        };

        map.entry_upsert(Invalid, 0);
    }

    #[test]
    fn test_entry_insertion_order() {
        let mut map = SketchHashMap {
            map: HashMap::new(),
            head: Invalid,
        };

        map.entry_upsert(SketchHashKey::Negative(i64::MIN), 5);
        map.entry_upsert(SketchHashKey::Negative(10), 1);
        map.entry_upsert(SketchHashKey::Positive(i64::MAX - 100), 17);
        map.entry_upsert(SketchHashKey::Zero, 7);
        map.entry_upsert(SketchHashKey::Positive(-10), 11);
        map.entry_upsert(SketchHashKey::Negative(-10), 3);
        map.entry_upsert(SketchHashKey::Positive(10), 13);

        let keys: Vec<_> = map.iter().collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                (SketchHashKey::Negative(10), 1),
                (SketchHashKey::Negative(-10), 3),
                (SketchHashKey::Negative(i64::MIN), 5),
                (SketchHashKey::Zero, 7),
                (SketchHashKey::Positive(-10), 11),
                (SketchHashKey::Positive(10), 13),
                (SketchHashKey::Positive(i64::MAX - 100), 17),
            ]
        );

        // We add some things before the current head, insert some new ones,
        // add some to the end, and again inbetween some others
        map.entry_upsert(SketchHashKey::Negative(i64::MAX), 3);
        map.entry_upsert(SketchHashKey::Negative(-10), 23);
        map.entry_upsert(SketchHashKey::Positive(9), 29);
        map.entry_upsert(SketchHashKey::Positive(i64::MAX), 8);
        map.entry_upsert(SketchHashKey::Positive(10), 123);
        map.entry_upsert(SketchHashKey::Positive(11), 31);

        let keys: Vec<_> = map.iter().collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                (SketchHashKey::Negative(i64::MAX), 3),
                (SketchHashKey::Negative(10), 1),
                (SketchHashKey::Negative(-10), 26), // 3 + 23
                (SketchHashKey::Negative(i64::MIN), 5),
                (SketchHashKey::Zero, 7),
                (SketchHashKey::Positive(-10), 11),
                (SketchHashKey::Positive(9), 29),
                (SketchHashKey::Positive(10), 136), // 13 + 123
                (SketchHashKey::Positive(11), 31),
                (SketchHashKey::Positive(i64::MAX - 100), 17),
                (SketchHashKey::Positive(i64::MAX), 8),
            ]
        );
    }

    #[quickcheck]
    // Use multiple hashsets as input to allow a small number of duplicate values without getting ridiculous levels of duplication (as quickcheck is inclined to create)
    fn fuzzing_test(
        batch1: HashSet<OrderedF64>,
        batch2: HashSet<OrderedF64>,
        batch3: HashSet<OrderedF64>,
        batch4: HashSet<OrderedF64>,
    ) -> TestResult {
        let mut master: Vec<f64> = batch1
            .into_iter()
            .chain(batch2.into_iter())
            .chain(batch3.into_iter())
            .chain(batch4.into_iter())
            .map(|x| x.0.into())
            .filter(|x: &f64| !x.is_nan())
            .collect();

        if master.len() < 100 {
            return TestResult::discard();
        }
        let mut sketch = UDDSketch::new(100, 0.000001);
        for value in &master {
            sketch.add_value(*value);
        }

        let quantile_tests = [0.01, 0.1, 0.25, 0.5, 0.6, 0.8, 0.95];

        master.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for i in 0..quantile_tests.len() {
            let quantile = quantile_tests[i];

            let mut test_val = sketch.estimate_quantile(quantile);

            // If test_val is infinite, use the most extreme finite value to test relative error
            if test_val.is_infinite() {
                if test_val.is_sign_negative() {
                    test_val = f64::MIN;
                } else {
                    test_val = f64::MAX;
                }
            }

            // Compute target quantile using nearest rank definition
            let master_idx = (quantile * master.len() as f64).floor() as usize;
            let target = master[master_idx];
            if target.is_infinite() {
                continue; // trivially correct...or NaN, depending how you define it
            }
            let error = if target == 0.0 {
                test_val
            } else {
                (test_val - target).abs() / target.abs()
            };

            assert!(error <= sketch.max_error(), "sketch with error {} estimated {} quantile as {}, true value is {} resulting in relative error {}
            values: {:?}", sketch.max_error(), quantile, test_val, target, error, master);
        }

        TestResult::passed()
    }
}
