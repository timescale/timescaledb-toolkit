//! Count-Min Sketch implementation in Rust
//!
//! Based on the paper:
//! <http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf>

use std::{
    fmt,
    hash::{Hash, Hasher},
};

#[allow(deprecated)]
use std::hash::SipHasher;

use serde::{Deserialize, Serialize};

/// The CountMinHashFn is a data structure used to hash items that are being
/// added to a Count-Min Sketch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[repr(C)]
pub struct CountMinHashFn {
    key: u64,
}

const SEED: u64 = 0x517cc1b727220a95; // from FxHash

impl CountMinHashFn {
    /// Creates a new CountMinHashFn whose hash function key is equal to `key`.
    pub fn with_key(key: u64) -> Self {
        Self { key }
    }

    /// Computes the hash of `item` according to the hash function and returns
    /// the bucket index corresponding to the hashed value.
    ///
    /// The returned value will be between 0 and (`nbuckets` - 1).
    #[allow(deprecated)]
    pub fn hash_into_buckets<T: Hash>(&self, item: &T, nbuckets: usize) -> usize {
        let (key1, key2) = (self.key, SEED);
        let mut hasher = SipHasher::new_with_keys(key1, key2);
        item.hash(&mut hasher);
        let hash_val = hasher.finish();
        (hash_val % (nbuckets as u64)) as usize
    }

    /// Returns the key for the hash function.
    pub(crate) fn key(&self) -> u64 {
        self.key
    }
}

/// The Count-Min Sketch is a compact summary data structure capable of
/// representing a high-dimensional vector and answering queries on this vector,
/// in particular point queries and dot product queries, with strong accuracy
/// guarantees. Such queries are at the core of many computations, so the
/// structure can be used in order to answer a variety of other queries, such as
/// frequent items (heavy hitters), quantile finding, join size estimation, and
/// more. Since the data structure can easily process updates in the form of
/// additions or subtractions to dimensions of the vector (which may correspond
/// to insertions or deletions, or other transactions), it is capable of working
/// over streams of updates, at high rates.[1]
///
/// [1]: <http://dimacs.rutgers.edu/~graham/pubs/papers/cmencyc.pdf>
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountMinSketch {
    width: usize,
    depth: usize,
    // hashfuncs must be at least `depth` in length
    hashfuncs: Vec<CountMinHashFn>,
    // The outer and inner `Vec`s must be `depth` and `width` long, respectively
    counters: Vec<Vec<i64>>,
}

impl CountMinSketch {
    /// Constructs a new Count-Min Sketch with the specified dimensions, using
    /// `hashfuncs` to construct the underlying hash functions and `counters` to
    /// populate the sketch with any data.
    pub fn new(
        width: usize,
        depth: usize,
        hashfuncs: Vec<CountMinHashFn>,
        counters: Vec<Vec<i64>>,
    ) -> Self {
        assert_eq!(hashfuncs.len(), depth);
        assert_eq!(counters.len(), depth);
        assert_eq!(counters[0].len(), width);
        Self {
            width,
            depth,
            hashfuncs,
            counters,
        }
    }

    /// Constructs a new, empty Count-Min Sketch with the specified dimensions,
    /// using `keys` to seed the underlying hash functions.
    pub fn with_dims_and_hashfn_keys(width: usize, depth: usize, keys: Vec<u64>) -> Self {
        assert_eq!(keys.len(), depth);
        Self {
            width,
            depth,
            hashfuncs: keys
                .iter()
                .map(|key| CountMinHashFn::with_key(*key))
                .collect(),
            counters: vec![vec![0; width]; depth],
        }
    }

    /// Constructs a new, empty Count-Min Sketch with the specified dimensions.
    pub fn with_dim(width: usize, depth: usize) -> Self {
        let keys = (1..=depth).map(|k| k as u64).collect::<Vec<_>>();
        CountMinSketch::with_dims_and_hashfn_keys(width, depth, keys)
    }

    /// Constructs a new, empty Count-Min Sketch whose dimensions will be
    /// derived from the parameters.
    ///
    /// Then for any element *i*, an estimate of its count, âᵢ, will have the
    /// guarantee:
    ///         aᵢ ≤ âᵢ ≤ aᵢ + ϵN    with probability 1-δ
    /// where aᵢ is the true count of element *i*
    ///
    /// Thus `epsilon` controls the error of the estimated count, relative to
    /// the total number of items seen, and `delta` determines the probability
    /// that the estimate will exceed the true count beyond the epsilon error
    /// term.
    ///
    /// To accommodate this result, the sketch will have a width of ⌈e/ε⌉ and a
    /// depth of ⌈ln(1/δ)⌉.
    pub fn with_prob(epsilon: f64, delta: f64) -> Self {
        assert!(0.0 < epsilon && epsilon < 1.0);
        assert!(0.0 < delta && delta < 1.0);
        let width = (1f64.exp() / epsilon).ceil() as usize;
        let depth = (1f64 / delta).ln().ceil() as usize;
        CountMinSketch::with_dim(width, depth)
    }

    /// Returns the width of the sketch.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Returns the depth of the sketch.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Returns a vector containing the keys of the hash functions used with the
    /// sketch.
    pub fn hash_keys(&self) -> Vec<u64> {
        self.hashfuncs.iter().map(|f| f.key()).collect()
    }

    /// Returns a nested vector representing the sketch's counter table. Each
    /// element in the outer vector corresponds to a row of the counter table,
    /// and each element of the inner vector corresponds to the tally in that
    /// bucket for a given row.
    pub fn counters(&self) -> &Vec<Vec<i64>> {
        &self.counters
    }

    /// Returns an estimate of the number of times `item` has been seen by the
    /// sketch.
    pub fn estimate<T: Hash>(&self, item: T) -> i64 {
        let buckets = self
            .hashfuncs
            .iter()
            .map(|h| h.hash_into_buckets(&item, self.width));

        self.counters
            .iter()
            .zip(buckets)
            .map(|(counter, bucket)| counter[bucket])
            .min()
            .unwrap()
    }

    /// Returns a vector of the indices for the buckets into which `item` hashes.
    ///
    /// The vector will have `self.depth` elements, each in the range
    /// [0, self.width-1].
    pub fn get_bucket_indices<T: Hash>(&self, item: T) -> Vec<usize> {
        self.hashfuncs
            .iter()
            .map(|h| h.hash_into_buckets(&item, self.width))
            .collect()
    }

    /// Adds the given `item` to the sketch.
    pub fn add_value<T: Hash>(&mut self, item: T) {
        for i in 0..self.depth {
            let bucket = self.hashfuncs[i].hash_into_buckets(&item, self.width);
            self.counters[i][bucket] += 1;
        }
    }

    /// Subtract the given `item` from the sketch.
    pub fn subtract_value<T: Hash>(&mut self, item: T) {
        for i in 0..self.depth {
            let bucket = self.hashfuncs[i].hash_into_buckets(&item, self.width);
            self.counters[i][bucket] -= 1;
        }
    }

    /// Includes the counts from `other` into `self` via elementwise addition of
    /// the counter vectors.
    ///
    /// The underlying `CountMinHashFn`s in each sketch must have the same keys.
    pub fn combine(&mut self, other: CountMinSketch) {
        assert_eq!(self.width, other.width);
        assert_eq!(self.depth, other.depth);
        assert_eq!(self.hashfuncs, other.hashfuncs);
        for (counter1, counter2) in self.counters.iter_mut().zip(other.counters) {
            for (val1, val2) in counter1.iter_mut().zip(counter2) {
                *val1 += val2;
            }
        }
    }
}

impl fmt::Display for CountMinSketch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Count-Min Sketch:")?;
        write!(f, "+------++")?;
        for _ in 0..self.width {
            write!(f, "--------+")?;
        }
        writeln!(f)?;

        write!(f, "|      ||")?;
        for b in 0..self.width {
            write!(f, "    {:>3} |", b)?;
        }
        writeln!(f)?;

        write!(f, "+======++")?;
        for _ in 0..self.width {
            write!(f, "========+")?;
        }
        writeln!(f)?;

        for n in 0..self.depth {
            write!(f, "|  {:>3} ||", n)?;
            for x in &self.counters[n] {
                write!(f, " {:>6} |", x)?;
            }
            writeln!(f)?;
        }

        write!(f, "+------++")?;
        for _ in 0..self.width {
            write!(f, "--------+")?;
        }
        writeln!(f)
    }
}
