//! UDDSketch implementation in rust.
//! Based on the paper: https://arxiv.org/abs/2004.08604

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

// This is used to index the buckets of the UddSketch.  In particular, because UddSketch stores values
// based on a logarithmic scale, we need to track negative values separately from positive values, and
// zero also needs special casing.
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Copy, Clone, Ord)]
#[repr(C, u64)]
pub enum SketchHashKey {
    Negative(i64),
    Zero,
    Positive(i64),
    Invalid,
}

// we're going to write SketchHashKey's to disk, so ensure they have no padding/are the correct size
const _:()=[()][(std::mem::size_of::<SketchHashKey>() != 16) as u8 as usize];

// Invalid is treated as greater than valid values (making it a nice boundary value for list end)
impl std::cmp::PartialOrd for SketchHashKey{
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
        }.into()
    }
}

impl SketchHashKey {
    /// This is the key corresponding to the current key after the SketchHashMap it refers to has gone through one compaction.
    /// compact bucket index towards the smaller bucket size,
    /// aka 0 on the number line, -INF on the bucket indicies
    fn compact_key(&self) -> SketchHashKey {
        use SketchHashKey::*;

        match *self {
            Negative(x) => Negative(if x < 0 {x-1} else {x} /2),
            Positive(x) => Positive(if x < 0 {x-1} else {x} /2),
            x => x.clone(),  // Zero and Invalid don't compact
        }
    }
}

// Entries in the SketchHashMap contain a count and the next valid index of the map.
#[derive(Serialize, Deserialize)]
#[derive(Clone)]
struct SketchHashEntry {
    count: u64,
    next: SketchHashKey,
}

// SketchHashMap is a special hash map of SketchHashKey->count that also keeps the equivalent of a linked list of the entries by increasing key value.
#[derive(Serialize, Deserialize)]
#[derive(Clone)]
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

    // Increment the count at a key, creating the entry if needed.
    fn increment(&mut self, key: SketchHashKey) {
        self.entry(key).count += 1;
    }

    fn iter(&self) -> SketchHashIterator {
        SketchHashIterator{container: &self, next_key: self.head}
    }

    // Returns the entry for a given key.
    // If the entry doesn't yet exist, this function will create it
    // with 0 count and ensure the list of keys is correctly updated.
    fn entry(&mut self, key: SketchHashKey) -> &mut SketchHashEntry {
        let mut next = self.head;
        if !self.map.contains_key(&key) {
            if key < self.head {
                self.head = key;
            } else {
                let mut prev = SketchHashKey::Invalid;
                while key > next {
                    prev = next;
                    next = self.map[&next].next;
                }
                self.map.get_mut(&prev).expect("Invalid key found").next = key;
            }
        }
        self.map.entry(key).or_insert(SketchHashEntry{count: 0, next})
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    // Combine adjacent buckets
    fn compact(&mut self) {
        let mut target = self.head;
        // TODO can we do without this additional map?
        let old_map = std::mem::replace(&mut self.map, HashMap::new());

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
            self.map.entry(new_key).or_insert(SketchHashEntry{count: 0, next: new_next}).count += old_entry.count;
            target = old_map[&target].next;
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UDDSketch {
    buckets: SketchHashMap,
    alpha: f64,
    gamma: f64,
    compactions: u32,// should always be smaller than 64
    max_buckets: u64,
    num_values: u64,
    min_value: f64,
    max_value: f64,
    values_sum: f64,
}

impl UDDSketch {
    pub fn new(max_buckets: u64, initial_error: f64) -> Self {
        assert!(initial_error > 0.0);
        UDDSketch {
            buckets: SketchHashMap::new(),
            alpha: initial_error,
            gamma: (1.0 + initial_error) / (1.0 - initial_error),
            compactions: 0,
            max_buckets: max_buckets,
            num_values: 0,
            min_value: f64::INFINITY,
            max_value: f64::NEG_INFINITY,
            values_sum: 0.0,
        }
    }

    // This constructor is used to recreate a UddSketch from it's component data
    pub fn new_from_data(max_buckets: u64, current_error: f64,
    compactions: u64, values: u64, sum: f64, min: f64, max: f64, keys: Vec<SketchHashKey>,
    counts: Vec<u64>) -> Self {
        let mut sketch =UDDSketch {
            buckets: SketchHashMap::new(),
            alpha: current_error,
            gamma: (1.0 + current_error) / (1.0 - current_error),
            compactions: compactions as u32,
            max_buckets: max_buckets,
            num_values: values,
            min_value: min,
            max_value: max,
            values_sum: sum,
        };
        assert_eq!(keys.len(), counts.len());
        // assert!(keys.is_sorted());
        for i in 0..keys.len() {
            sketch.buckets.map.entry(keys[i]).or_insert(
                SketchHashEntry{count: 0, next: if i == keys.len() - 1 {SketchHashKey::Invalid} else {keys[i+1]}}
            ).count = counts[i];
        }
        sketch.buckets.head = keys[0];

        sketch
    }
}

impl UDDSketch {
    // For a given value return the index of it's bucket in the current sketch.
    fn key(&self, value: f64) -> SketchHashKey {
        let negative = value < 0.0;
        let value = value.abs();

        if value == 0.0 {
            SketchHashKey::Zero
        } else if negative {
            SketchHashKey::Negative(value.log(self.gamma).ceil() as i64)
        } else {
            SketchHashKey::Positive(value.log(self.gamma).ceil() as i64)
        }
    }

    /// inverse of `key()` within alpha
    fn bucket_to_value(&self, bucket: SketchHashKey) -> f64 {
        match bucket {
            SketchHashKey::Zero => 0.0,
            SketchHashKey::Positive(i) => self.gamma.powi(i as i32 - 1) * (1.0 + self.alpha),
            SketchHashKey::Negative(i) => -(self.gamma.powi(i as i32 - 1) * (1.0 + self.alpha)),
            SketchHashKey::Invalid => panic!("Unable to convert invalid bucket id to value"),
        }
    }

    pub fn compact_buckets(&mut self) {
        self.buckets.compact();

        self.compactions += 1;
        self.gamma *= self.gamma; // See https://arxiv.org/pdf/2004.08604.pdf Equation 3
        self.alpha = 2.0 * self.alpha / (1.0 + self.alpha.powi(2)); // See https://arxiv.org/pdf/2004.08604.pdf Equation 4
    }

    pub fn bucket_iter(&mut self) -> SketchHashIterator {
        self.buckets.iter()
    }
}

impl UDDSketch {
    pub fn add_value(&mut self, value: f64) {
        self.buckets.increment(self.key(value));

        while self.buckets.len() > self.max_buckets as usize {
            self.compact_buckets();
        }

        if value < self.min_value {
            self.min_value = value;
        }
        if value > self.max_value {
            self.max_value = value;
        }
        self.num_values += 1;
        self.values_sum += value;
    }

    pub fn merge_sketch(&mut self, other: &UDDSketch) {
        // Require matching initial parameters
        assert!(
            self.gamma.powf(1.0 / f64::powi(2.0, self.compactions as i32))
                == other.gamma.powf(1.0 / f64::powi(2.0, other.compactions as i32))
        );
        assert!(self.max_buckets == other.max_buckets);

        if other.num_values == 0 {
            return;
        }
        if self.num_values == 0 {
            *self = other.clone();
            return;
        }

        let mut target = other.clone();

        while self.compactions > target.compactions {
            target.compact_buckets();
        }
        while target.compactions > self.compactions {
            self.compact_buckets();
        }

        for entry in other.buckets.iter() {
            let (key, value) = entry;
            self.buckets.entry(key).count += value;
        }

        while self.buckets.len() > self.max_buckets as usize {
            self.compact_buckets();
        }

        self.min_value = f64::min(self.min_value, target.min_value);
        self.max_value = f64::max(self.max_value, target.max_value);
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
    pub fn max(&self) -> f64 {
        self.max_value
    }

    #[inline]
    pub fn min(&self) -> f64 {
        self.min_value
    }

    #[inline]
    pub fn max_error(&self) -> f64 {
        self.alpha
    }

    pub fn estimate_quantile(&self, quantile: f64) -> f64 {
        assert!(quantile >= 0.0 && quantile <= 1.0);
        if quantile == 1.0 {
            return self.max_value;
        }
        let mut remaining = (self.num_values as f64 * quantile) as u64;
        for entry in self.buckets.iter() {
            let (key, count) = entry;
            if remaining <= count {
                return self.bucket_to_value(key);
            } else {
                remaining -= count;
            }
        }
        unreachable!();
    }

    // This relative error isn't bounded by alpha, what is the bound?
    pub fn estimate_quantile_at_value(&self, value: f64) -> f64 {
        if value < self.min_value {
            return 0.0;
        } else if value > self.max_value {
            return 1.0;
        }

        let mut count = 0;
        let target = self.key(value);

        for entry in self.buckets.iter() {
            let (key, value) = entry;
            if target > key {
                count += value;
            } else {
                return (count as f64 + value as f64 / 2.0) / self.num_values as f64;
            }
        }

        unreachable!();
    }
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
        assert_eq!(sketch.min(), 0.5);
        assert_eq!(sketch.max(), 3.0);
        assert_eq!(sketch.sum(), 4.5);
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
            sketch.add_value(1000.0 * (1.23 as f64).powi(i));
        }

        assert_eq!(sketch.count(), 20);
        assert_eq!(sketch.max_error(), 0.1);

        for i in 20..30 {
            sketch.add_value(1000.0 * (1.23 as f64).powi(i));
        }

        assert_eq!(sketch.count(), 30);
        assert_eq!(sketch.max_error(), a2);
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
        assert_eq!(sketch1.min(), 1.1);
        assert_eq!(sketch1.max(), 4.2);
        assert_eq!(sketch1.max_error(), a1);

        let mut sketch2 = UDDSketch::new(20, 0.1);
        sketch2.add_value(5.1); // Bucket #9
        sketch2.add_value(7.5); // Bucket #11
        sketch2.add_value(10.6); // Bucket #12
        sketch2.add_value(9.3); // Bucket #12
        sketch2.add_value(11.2); // Bucket #13

        assert_eq!(sketch2.max_error(), a1);

        sketch1.merge_sketch(&sketch2);
        assert_eq!(sketch1.count(), 10);
        assert_eq!(sketch1.min(), 1.1);
        assert_eq!(sketch1.max(), 11.2);
        assert_eq!(sketch1.max_error(), a1);

        let mut sketch3 = UDDSketch::new(20, 0.1);
        sketch3.add_value(0.8); // Bucket #-1
        sketch3.add_value(3.7); // Bucket #7
        sketch3.add_value(15.2); // Bucket #14
        sketch3.add_value(3.4); // Bucket #7
        sketch3.add_value(0.6); // Bucket #-2

        assert_eq!(sketch3.max_error(), a1);

        sketch1.merge_sketch(&sketch3);
        assert_eq!(sketch1.count(), 15);
        assert_eq!(sketch1.min(), 0.6);
        assert_eq!(sketch1.max(), 15.2);
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

        sketch1.merge_sketch(&sketch4);
        assert_eq!(sketch1.count(), 24);
        assert_eq!(sketch1.min(), -400000000000.0);
        assert_eq!(sketch1.max(), 400000000000.0);
        assert_eq!(sketch1.max_error(), a2);

        let mut sketch5 = UDDSketch::new(20, 0.1);
        for i in 100..220 {
            sketch5.add_value((1.23 as f64).powi(i));
        }

        assert_eq!(sketch5.max_error(), a4);

        sketch1.merge_sketch(&sketch5);
        assert_eq!(sketch1.count(), 144);
        assert_eq!(sketch1.max_error(), a5);  // Note that each compaction doesn't always result in half the numbers of buckets, hence a5 here instead of a4
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

            let test_value = sketch.estimate_quantile(quantile);
            let test_quant = sketch.estimate_quantile_at_value(value);

            let percentage = (test_value - value).abs() / value;
            assert!(
                percentage <= 0.1,
                "Exceeded 10% error on quantile {}: expected {}, received {} (error% {})",
                quantile,
                value,
                test_value,
                (test_value - value).abs() / value
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
}
