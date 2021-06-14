// Based on https://github.com/MnO2/t-digest/blob/master/src/lib.rs
// as of commit 66d7c19d32c1547daa628f1d9f12178a686ba022

//! T-Digest algorithm in rust
//!
//! ## Installation
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! tdigest = "0.2"
//! ```
//!
//! then you are good to go. If you are using Rust 2015 you have to ``extern crate tdigest`` to your crate root as well.
//!
//! ## Example
//!
//! ```rust
//! use tdigest::TDigest;
//!
//! let t = TDigest::new_with_size(100);
//! let values: Vec<f64> = (1..=1_000_000).map(f64::from).collect();
//!
//! let t = t.merge_sorted(values);
//!
//! let ans = t.estimate_quantile(0.99);
//! let expected: f64 = 990_000.0;
//!
//! let percentage: f64 = (expected - ans).abs() / expected;
//! assert!(percentage < 0.01);
//! ```

use ordered_float::OrderedFloat;
use std::cmp::Ordering;
#[cfg(test)]
use std::collections::HashSet;

use flat_serialize_macro::FlatSerializable;

use serde::{Deserialize, Serialize};

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

/// Centroid implementation to the cluster mentioned in the paper.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, FlatSerializable)]
#[repr(C)]
pub struct Centroid {
    mean: OrderedFloat<f64>,
    weight: u64,
}

impl PartialOrd for Centroid {
    fn partial_cmp(&self, other: &Centroid) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Centroid {
    fn cmp(&self, other: &Centroid) -> Ordering {
        self.mean.cmp(&other.mean)
    }
}

impl Centroid {
    pub fn new(mean: f64, weight: u64) -> Self {
        Centroid {
            mean: OrderedFloat::from(mean),
            weight,
        }
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        self.mean.into_inner()
    }

    #[inline]
    pub fn weight(&self) -> u64 {
        self.weight
    }

    pub fn add(&mut self, sum: f64, weight: u64) -> f64 {
        let weight_: u64 = self.weight;
        let mean_: f64 = self.mean.into_inner();

        let new_sum: f64 = sum + weight_ as f64 * mean_;
        let new_weight: u64 = weight_ + weight;
        self.weight = new_weight;
        self.mean = OrderedFloat::from(new_sum / new_weight as f64);
        new_sum
    }
}

impl Default for Centroid {
    fn default() -> Self {
        Centroid {
            mean: OrderedFloat::from(0.0),
            weight: 1,
        }
    }
}

/// T-Digest to be operated on.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    max_size: usize,
    sum: OrderedFloat<f64>,
    count: u64,
    max: OrderedFloat<f64>,
    min: OrderedFloat<f64>,
}

impl TDigest {
    pub fn new_with_size(max_size: usize) -> Self {
        TDigest {
            centroids: Vec::new(),
            max_size,
            sum: OrderedFloat::from(0.0),
            count: 0,
            max: OrderedFloat::from(std::f64::NAN),
            min: OrderedFloat::from(std::f64::NAN),
        }
    }

    pub fn new(
        centroids: Vec<Centroid>,
        sum: f64,
        count: u64,
        max: f64,
        min: f64,
        max_size: usize,
    ) -> Self {
        if centroids.len() <= max_size {
            TDigest {
                centroids,
                max_size,
                sum: OrderedFloat::from(sum),
                count,
                max: OrderedFloat::from(max),
                min: OrderedFloat::from(min),
            }
        } else {
            let sz = centroids.len();
            let digests: Vec<TDigest> = vec![
                TDigest::new_with_size(max_size),
                TDigest::new(centroids, sum, count, max, min, sz),
            ];

            Self::merge_digests(digests)
        }
    }

    pub fn raw_centroids(&self) -> &[Centroid] {
        &self.centroids
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        let sum_: f64 = self.sum.into_inner();

        if self.count > 0 {
            sum_ / self.count as f64
        } else {
            0.0
        }
    }

    #[inline]
    pub fn sum(&self) -> f64 {
        self.sum.into_inner()
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    #[inline]
    pub fn max(&self) -> f64 {
        self.max.into_inner()
    }

    #[inline]
    pub fn min(&self) -> f64 {
        self.min.into_inner()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.centroids.is_empty()
    }

    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    #[inline]
    pub fn num_buckets(&self) -> usize {
        self.centroids.len()
    }
}

impl Default for TDigest {
    fn default() -> Self {
        TDigest {
            centroids: Vec::new(),
            max_size: 100,
            sum: OrderedFloat::from(0.0),
            count: 0,
            max: OrderedFloat::from(std::f64::NAN),
            min: OrderedFloat::from(std::f64::NAN),
        }
    }
}

impl TDigest {
    fn k_to_q(k: f64, d: f64) -> f64 {
        let k_div_d = k / d;
        if k_div_d >= 0.5 {
            let base = 1.0 - k_div_d;
            1.0 - 2.0 * base * base
        } else {
            2.0 * k_div_d * k_div_d
        }
    }

    pub fn merge_unsorted(&self, unsorted_values: Vec<f64>) -> TDigest {
        let mut sorted_values: Vec<OrderedFloat<f64>> = unsorted_values
            .into_iter()
            .map(OrderedFloat::from)
            .collect();
        sorted_values.sort();
        let sorted_values = sorted_values.into_iter().map(|f| f.into_inner()).collect();

        self.merge_sorted(sorted_values)
    }

    // Allow f64 overflow to create centroids with infinite mean, but make sure our min/max are updated
    fn update_bounds_on_overflow(
        value: OrderedFloat<f64>,
        lower_bound: &mut OrderedFloat<f64>,
        upper_bound: &mut OrderedFloat<f64>,
    ) {
        if value < *lower_bound {
            *lower_bound = value;
        }
        if value > *upper_bound {
            *upper_bound = value;
        }
    }

    pub fn merge_sorted(&self, sorted_values: Vec<f64>) -> TDigest {
        if sorted_values.is_empty() {
            return self.clone();
        }

        let mut result = TDigest::new_with_size(self.max_size());
        result.count = self.count() + (sorted_values.len() as u64);

        let maybe_min = OrderedFloat::from(*sorted_values.first().unwrap());
        let maybe_max = OrderedFloat::from(*sorted_values.last().unwrap());

        if self.count() > 0 {
            result.min = std::cmp::min(self.min, maybe_min);
            result.max = std::cmp::max(self.max, maybe_max);
        } else {
            result.min = maybe_min;
            result.max = maybe_max;
        }

        let mut compressed: Vec<Centroid> = Vec::with_capacity(self.max_size);

        let mut k_limit: f64 = 1.0;
        let mut q_limit_times_count: f64 =
            Self::k_to_q(k_limit, self.max_size as f64) * result.count as f64;
        k_limit += 1.0;

        let mut iter_centroids = self.centroids.iter().peekable();
        let mut iter_sorted_values = sorted_values.iter().peekable();

        let mut curr: Centroid = if let Some(c) = iter_centroids.peek() {
            let curr = **iter_sorted_values.peek().unwrap();
            if c.mean() < curr {
                iter_centroids.next().unwrap().clone()
            } else {
                Centroid::new(*iter_sorted_values.next().unwrap(), 1)
            }
        } else {
            Centroid::new(*iter_sorted_values.next().unwrap(), 1)
        };

        let mut weight_so_far: u64 = curr.weight();

        let mut sums_to_merge: f64 = 0.0;
        let mut weights_to_merge: u64 = 0;

        while iter_centroids.peek().is_some() || iter_sorted_values.peek().is_some() {
            let next: Centroid = if let Some(c) = iter_centroids.peek() {
                if iter_sorted_values.peek().is_none()
                    || c.mean() < **iter_sorted_values.peek().unwrap()
                {
                    iter_centroids.next().unwrap().clone()
                } else {
                    Centroid::new(*iter_sorted_values.next().unwrap(), 1)
                }
            } else {
                Centroid::new(*iter_sorted_values.next().unwrap(), 1)
            };

            let next_sum: f64 = next.mean() * next.weight() as f64;
            weight_so_far += next.weight();

            if weight_so_far as f64 <= q_limit_times_count {
                sums_to_merge += next_sum;
                weights_to_merge += next.weight();
            } else {
                result.sum = OrderedFloat::from(
                    result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge),
                );
                sums_to_merge = 0.0;
                weights_to_merge = 0;
                TDigest::update_bounds_on_overflow(curr.mean, &mut result.min, &mut result.max);

                compressed.push(curr.clone());
                q_limit_times_count =
                    Self::k_to_q(k_limit, self.max_size as f64) * result.count() as f64;
                k_limit += 1.0;
                curr = next;
            }
        }

        result.sum =
            OrderedFloat::from(result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge));
        TDigest::update_bounds_on_overflow(curr.mean, &mut result.min, &mut result.max);
        compressed.push(curr);
        compressed.shrink_to_fit();
        compressed.sort();

        result.centroids = compressed;
        result
    }

    fn external_merge(centroids: &mut Vec<Centroid>, first: usize, middle: usize, last: usize) {
        let mut result: Vec<Centroid> = Vec::with_capacity(centroids.len());

        let mut i = first;
        let mut j = middle;

        while i < middle && j < last {
            match centroids[i].cmp(&centroids[j]) {
                Ordering::Less => {
                    result.push(centroids[i].clone());
                    i += 1;
                }
                Ordering::Greater => {
                    result.push(centroids[j].clone());
                    j += 1;
                }
                Ordering::Equal => {
                    result.push(centroids[i].clone());
                    i += 1;
                }
            }
        }

        while i < middle {
            result.push(centroids[i].clone());
            i += 1;
        }

        while j < last {
            result.push(centroids[j].clone());
            j += 1;
        }

        i = first;
        for centroid in result.into_iter() {
            centroids[i] = centroid;
            i += 1;
        }
    }

    // Merge multiple T-Digests
    pub fn merge_digests(digests: Vec<TDigest>) -> TDigest {
        let n_centroids: usize = digests.iter().map(|d| d.centroids.len()).sum();
        if n_centroids == 0 {
            return TDigest::default();
        }

        // TODO should this be the smaller of the sizes?
        let max_size = digests.first().unwrap().max_size;
        let mut centroids: Vec<Centroid> = Vec::with_capacity(n_centroids);
        let mut starts: Vec<usize> = Vec::with_capacity(digests.len());

        let mut count: u64 = 0;
        let mut min = OrderedFloat::from(std::f64::INFINITY);
        let mut max = OrderedFloat::from(std::f64::NEG_INFINITY);

        let mut start: usize = 0;
        for digest in digests.into_iter() {
            starts.push(start);

            let curr_count: u64 = digest.count();
            if curr_count > 0 {
                min = std::cmp::min(min, digest.min);
                max = std::cmp::max(max, digest.max);
                count += curr_count;
                for centroid in digest.centroids {
                    centroids.push(centroid);
                    start += 1;
                }
            }
        }

        let mut digests_per_block: usize = 1;
        while digests_per_block < starts.len() {
            for i in (0..starts.len()).step_by(digests_per_block * 2) {
                if i + digests_per_block < starts.len() {
                    let first = starts[i];
                    let middle = starts[i + digests_per_block];
                    let last = if i + 2 * digests_per_block < starts.len() {
                        starts[i + 2 * digests_per_block]
                    } else {
                        centroids.len()
                    };

                    debug_assert!(first <= middle && middle <= last);
                    Self::external_merge(&mut centroids, first, middle, last);
                }
            }

            digests_per_block *= 2;
        }

        let mut result = TDigest::new_with_size(max_size);
        let mut compressed: Vec<Centroid> = Vec::with_capacity(max_size);

        let mut k_limit: f64 = 1.0;
        let mut q_limit_times_count: f64 = Self::k_to_q(k_limit, max_size as f64) * (count as f64);

        let mut iter_centroids = centroids.iter_mut();
        let mut curr = iter_centroids.next().unwrap();
        let mut weight_so_far: u64 = curr.weight();
        let mut sums_to_merge: f64 = 0.0;
        let mut weights_to_merge: u64 = 0;

        for centroid in iter_centroids {
            weight_so_far += centroid.weight();

            if weight_so_far as f64 <= q_limit_times_count {
                sums_to_merge += centroid.mean() * centroid.weight() as f64;
                weights_to_merge += centroid.weight();
            } else {
                result.sum = OrderedFloat::from(
                    result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge),
                );
                sums_to_merge = 0.0;
                weights_to_merge = 0;
                TDigest::update_bounds_on_overflow(curr.mean, &mut min, &mut max);
                compressed.push(curr.clone());
                q_limit_times_count = Self::k_to_q(k_limit, max_size as f64) * (count as f64);
                k_limit += 1.0;
                curr = centroid;
            }
        }

        result.sum =
            OrderedFloat::from(result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge));
        TDigest::update_bounds_on_overflow(curr.mean, &mut min, &mut max);
        compressed.push(curr.clone());
        compressed.shrink_to_fit();
        compressed.sort();

        result.count = count;
        result.min = min;
        result.max = max;
        result.centroids = compressed;
        result
    }

    /// Given a value estimate the corresponding quantile in a digest
    pub fn estimate_quantile_at_value(&self, v: f64) -> f64 {
        if self.centroids.is_empty() {
            return 0.0;
        }

        if v < self.min.into_inner() {
            return 0.0;
        }

        if v > self.max.into_inner() {
            return 1.0;
        }

        let mut low_bound = self.min.into_inner();
        let mut low_weight = 0;
        let mut hi_bound = self.max.into_inner();
        let mut hi_weight = 0;
        let mut accum_weight = 0;

        for cent in &self.centroids {
            if v < cent.mean.into_inner() {
                hi_bound = cent.mean.into_inner();
                hi_weight = cent.weight;
                break;
            }
            low_bound = cent.mean.into_inner();
            low_weight = cent.weight;
            accum_weight += low_weight;
        }

        let weighted_midpoint = low_bound
            + (hi_bound - low_bound) * low_weight as f64 / (low_weight + hi_weight) as f64;
        if v > weighted_midpoint {
            (accum_weight as f64
                + (v - weighted_midpoint) / (hi_bound - weighted_midpoint) * hi_weight as f64 / 2.0)
                / self.count as f64
        } else {
            (accum_weight as f64
                - (weighted_midpoint - v) / (weighted_midpoint - low_bound) * low_weight as f64
                    / 2.0)
                / self.count as f64
        }
    }

    /// To estimate the value located at `q` quantile
    pub fn estimate_quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return 0.0;
        }

        let rank: f64 = q * self.count as f64;

        let mut pos: usize;
        let mut t: u64;
        if q > 0.5 {
            if q >= 1.0 {
                return self.max();
            }

            pos = 0;
            t = self.count;

            for (k, centroid) in self.centroids.iter().enumerate().rev() {
                t -= centroid.weight();

                if rank >= t as f64 {
                    pos = k;
                    break;
                }
            }
        } else {
            if q <= 0.0 || rank <= 1.0 {
                return self.min();
            }

            pos = self.centroids.len() - 1;
            t = 0;

            for (k, centroid) in self.centroids.iter().enumerate() {
                if rank < (t + centroid.weight()) as f64 {
                    pos = k;
                    break;
                }

                t += centroid.weight();
            }
        }

        // At this point pos indexes the centroid containing the desired rank and t is the combined weight of all buckets < pos
        // With this we can determine the location of our target rank within the range covered by centroid 'pos'
        let centroid_weight = (rank - t as f64) / self.centroids[pos].weight() as f64;

        // Now we use that location to interpolate the desired value between the centroid mean and the weighted midpoint between the next centroid in the direction of the target rank.
        return if centroid_weight == 0.5 {
            self.centroids[pos].mean()
        } else if centroid_weight < 0.5 {
            let weighted_lower_bound = if pos == 0 {
                weighted_average(
                    self.min(),
                    0,
                    self.centroids[pos].mean(),
                    self.centroids[pos].weight(),
                )
            } else {
                weighted_average(
                    self.centroids[pos - 1].mean(),
                    self.centroids[pos - 1].weight(),
                    self.centroids[pos].mean(),
                    self.centroids[pos].weight(),
                )
            };

            interpolate(
                weighted_lower_bound,
                self.centroids[pos].mean(),
                centroid_weight * 2.0,
            )
        } else {
            let weighted_upper_bound = if pos == self.centroids.len() - 1 {
                weighted_average(
                    self.centroids[pos].mean(),
                    self.centroids[pos].weight(),
                    self.max(),
                    0,
                )
            } else {
                weighted_average(
                    self.centroids[pos].mean(),
                    self.centroids[pos].weight(),
                    self.centroids[pos + 1].mean(),
                    self.centroids[pos + 1].weight(),
                )
            };
            interpolate(
                self.centroids[pos].mean(),
                weighted_upper_bound,
                (centroid_weight - 0.5) * 2.0,
            )
        };

        // Helper functions for quantile calculation

        // Given two points and their relative weights, return the weight midpoint (i.e. if p2 is twice the weight of p1, the midpoint will be twice as close to p2 as to p1)
        fn weighted_average(p1: f64, p1_weight: u64, p2: f64, p2_weight: u64) -> f64 {
            interpolate(p1, p2, p1_weight as f64 / (p1_weight + p2_weight) as f64)
        }

        // Given two points and a weight in the range [0,1], return p1 + weight * (p2-p1)
        fn interpolate(p1: f64, p2: f64, weight: f64) -> f64 {
            // We always call this with p2 >= p1 and ensuring this reduces the cases we have to match on
            debug_assert!(OrderedFloat::from(p2) >= OrderedFloat::from(p1));
            // Not being able to match on floats makes this match much uglier than it should be
            match (p1.is_infinite(), p2.is_infinite(), p1.is_sign_positive(), !p2.is_sign_negative()) {
                (true, true, false, true) /* (f64::NEG_INFINITY, f64::INFINITY) */ => f64::NAN, // This is a stupid case, and the only time we'll see quantile return a NaN
                (true, _, false, _) /* (f64::NEG_INFINITY, _) */ => f64::NEG_INFINITY,
                (_, true, _, true) /* (_, f64::INFINITY) */ => f64::INFINITY,
                _ => p1 + (p2 - p1) * weight
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_centroid_addition_regression() {
        //https://github.com/MnO2/t-digest/pull/1

        let vals = vec![1.0, 1.0, 1.0, 2.0, 1.0, 1.0];
        let mut t = TDigest::new_with_size(10);

        for v in vals {
            t = t.merge_unsorted(vec![v]);
        }

        let ans = t.estimate_quantile(0.5);
        let expected: f64 = 1.0;
        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.95);
        let expected: f64 = 2.0;
        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);
    }

    #[test]
    fn test_merge_sorted_against_uniform_distro() {
        let t = TDigest::new_with_size(100);
        let values: Vec<f64> = (1..=1_000_000).map(f64::from).collect();

        let t = t.merge_sorted(values);

        let ans = t.estimate_quantile(1.0);
        let expected: f64 = 1_000_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.99);
        let expected: f64 = 990_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.01);
        let expected: f64 = 10_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.0);
        let expected: f64 = 1.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.5);
        let expected: f64 = 500_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);
    }

    #[test]
    fn test_merge_unsorted_against_uniform_distro() {
        let t = TDigest::new_with_size(100);
        let values: Vec<f64> = (1..=1_000_000).map(f64::from).collect();

        let t = t.merge_unsorted(values);

        let ans = t.estimate_quantile(1.0);
        let expected: f64 = 1_000_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.99);
        let expected: f64 = 990_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.01);
        let expected: f64 = 10_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.0);
        let expected: f64 = 1.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.5);
        let expected: f64 = 500_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);
    }

    #[test]
    fn test_merge_sorted_against_skewed_distro() {
        let t = TDigest::new_with_size(100);
        let mut values: Vec<f64> = (1..=600_000).map(f64::from).collect();
        for _ in 0..400_000 {
            values.push(1_000_000.0);
        }

        let t = t.merge_sorted(values);

        let ans = t.estimate_quantile(0.99);
        let expected: f64 = 1_000_000.0;
        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.01);
        let expected: f64 = 10_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.5);
        let expected: f64 = 500_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);
    }

    #[test]
    fn test_merge_unsorted_against_skewed_distro() {
        let t = TDigest::new_with_size(100);
        let mut values: Vec<f64> = (1..=600_000).map(f64::from).collect();
        for _ in 0..400_000 {
            values.push(1_000_000.0);
        }

        let t = t.merge_unsorted(values);

        let ans = t.estimate_quantile(0.99);
        let expected: f64 = 1_000_000.0;
        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.01);
        let expected: f64 = 10_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.5);
        let expected: f64 = 500_000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);
    }

    #[test]
    fn test_merge_digests() {
        let mut digests: Vec<TDigest> = Vec::new();

        for _ in 1..=100 {
            let t = TDigest::new_with_size(100);
            let values: Vec<f64> = (1..=1_000).map(f64::from).collect();
            let t = t.merge_sorted(values);
            digests.push(t)
        }

        let t = TDigest::merge_digests(digests);

        let ans = t.estimate_quantile(1.0);
        let expected: f64 = 1000.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.99);
        let expected: f64 = 990.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.01);
        let expected: f64 = 10.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.2);

        let ans = t.estimate_quantile(0.0);
        let expected: f64 = 1.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);

        let ans = t.estimate_quantile(0.5);
        let expected: f64 = 500.0;

        let percentage: f64 = (expected - ans).abs() / expected;
        assert!(percentage < 0.01);
    }

    #[test]
    fn test_quantile_and_value_estimates() {
        let t = TDigest::new_with_size(100);
        let values: Vec<f64> = (1..=10000).map(|v| f64::from(v) / 100.0).collect();

        let t = t.merge_sorted(values);

        for i in 1..=100 {
            let value = i as f64;
            let quantile = value / 100.0;

            let test_value = t.estimate_quantile(quantile);
            let test_quant = t.estimate_quantile_at_value(value);

            let percentage = (test_value - value).abs() / value;
            assert!(
                percentage < 0.01,
                "Exceeded 1% error on quantile {}: expected {}, received {} (error% {})",
                quantile,
                value,
                test_value,
                (test_value - value).abs() / value * 100.0
            );
            let percentage = (test_quant - quantile).abs() / quantile;
            assert!(
                percentage < 0.01,
                "Exceeded 1% error on quantile at value {}: expected {}, received {} (error% {})",
                value,
                quantile,
                test_quant,
                (test_quant - quantile).abs() / quantile * 100.0
            );

            let test = t.estimate_quantile_at_value(t.estimate_quantile(quantile));
            let percentage = (test - quantile).abs() / quantile;
            assert!(percentage < 0.001);
        }
    }

    #[test]
    fn test_buffered_merge() {
        let mut digested = TDigest::new_with_size(100);
        let mut buffer = vec![];
        for i in 1..=100 {
            buffer.push(i as f64);
            if buffer.len() >= digested.max_size() {
                let new = std::mem::replace(&mut buffer, vec![]);
                digested = digested.merge_unsorted(new)
            }
        }
        if !buffer.is_empty() {
            digested = digested.merge_unsorted(buffer)
        }
        let estimate = digested.estimate_quantile(0.99);
        assert_eq!(estimate, 99.5);
    }

    use quickcheck::*;

    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Debug)]
    struct OrderedF64(OrderedFloat<f64>);

    impl Arbitrary for OrderedF64 {
        fn arbitrary(g: &mut Gen) -> Self {
            OrderedF64(f64::arbitrary(g).into())
        }
    }

    #[quickcheck]
    fn fuzzing_test(
        batch1: HashSet<OrderedF64>,
        batch2: HashSet<OrderedF64>,
        batch3: HashSet<OrderedF64>,
        batch4: HashSet<OrderedF64>,
    ) -> TestResult {
        let batch1: Vec<f64> = batch1
            .into_iter()
            .map(|x| x.0.into())
            .filter(|x: &f64| !x.is_nan())
            .collect();
        let batch2: Vec<f64> = batch2
            .into_iter()
            .map(|x| x.0.into())
            .filter(|x: &f64| !x.is_nan())
            .collect();
        let batch3: Vec<f64> = batch3
            .into_iter()
            .map(|x| x.0.into())
            .filter(|x: &f64| !x.is_nan())
            .collect();
        let batch4: Vec<f64> = batch4
            .into_iter()
            .map(|x| x.0.into())
            .filter(|x: &f64| !x.is_nan())
            .collect();
        let digest1 = TDigest::new_with_size(20).merge_unsorted(batch1.clone());
        let digest1 = digest1.merge_unsorted(batch2.clone());
        let digest2 = TDigest::new_with_size(20).merge_unsorted(batch3.clone());
        let digest2 = digest2.merge_unsorted(batch4.clone());

        let digest = TDigest::merge_digests(vec![digest1, digest2]);

        let quantile_tests = vec![0.01, 0.1, 0.25, 0.5, 0.6, 0.8, 0.95];
        let tolerated_percentile_error =
            vec![0.010001, 0.100001, 0.2, 0.30, 0.275, 0.1725, 0.050001]; // .000001 cases are to handle rounding errors on cases that might return infinities

        let mut master: Vec<f64> = batch1
            .iter()
            .chain(batch2.iter())
            .chain(batch3.iter())
            .chain(batch4.iter())
            .map(|x| *x)
            .collect();
        master.sort_by(|a, b| a.partial_cmp(b).unwrap());

        if master.len() < 100 {
            return TestResult::discard();
        }

        for i in 0..quantile_tests.len() {
            let quantile = quantile_tests[i];
            let error_bound = tolerated_percentile_error[i];

            let test_val = digest.estimate_quantile(quantile);
            let target_idx = quantile * master.len() as f64;
            let target_allowed_error = master.len() as f64 * error_bound;
            let mut test_idx = 0;
            if test_val != f64::INFINITY {
                while test_idx < master.len() && master[test_idx] < test_val {
                    test_idx += 1;
                }
            } else {
                // inequality checking against infinity is wonky
                test_idx = master.len();
            }
            // test idx is now the idx of the smallest element >= test_val (and yes, this could be done faster with binary search)

            assert!((test_idx as f64) >= target_idx - target_allowed_error && (test_idx as f64) <= target_idx + target_allowed_error, "testing {} quantile returned {}, there are {} values lower than this, target range {} +/- {}", quantile, test_val, test_idx, target_idx, target_allowed_error);
        }

        TestResult::passed()
    }
}
