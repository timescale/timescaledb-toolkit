//! SpaceSaving implementation in rust.
//! Based on the paper: https://cs.ucsb.edu/sites/default/files/documents/2005-23.pdf

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Clone, Copy, Serialize, Deserialize)]
struct SSHashEntry<T: Copy> {
    value: T,
    count: u64,
    overcount: u64,
}


#[derive(Clone, Serialize, Deserialize)]
pub struct SpaceSaving<T: Eq + Hash + Copy> {
    entries: Vec<SSHashEntry<T>>,
    value_idx_map: HashMap<T, u32>,
    replacement_idx: u32,
    maximum_entries: u32,
    total_vals: u64,
}

impl <T: Eq + Hash + Copy> SpaceSaving<T> {
    pub fn new(epsilon: f64) -> SpaceSaving<T> {
        let maximum_entries = (1. / epsilon) as u32;
        SpaceSaving {
            entries: Vec::with_capacity(maximum_entries as usize),
            value_idx_map: HashMap::new(),
            replacement_idx: maximum_entries,
            maximum_entries,
            total_vals: 0,
        }
    }

    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }

    pub fn max_entries(&self) -> u32 {
        self.maximum_entries
    }

    pub fn total_values(&self) -> u64 {
        self.total_vals
    }

    // Arrays are assumed to match and be in decreasing order of counts
    pub fn new_from_components(epsilon: f64, values: &[T], counts: &[u64], overcounts: &[u64], total_vals: u64) -> SpaceSaving<T> {
        let mut result = SpaceSaving::new(epsilon);
        for i in 0..values.len() {
            result.value_idx_map.insert(values[i], result.entries.len() as _);
            result.entries.push(
                SSHashEntry {
                    value: values[i],
                    count: counts[i],
                    overcount: overcounts[i],
                }
            )
        }
        result.total_vals = total_vals;

        result
    }

    // Fills passed vectors with entry data
    pub fn generate_component_data(&self, values: &mut Vec<T>, counts: &mut Vec<u64>, overcounts: &mut Vec<u64>) {
        for entry in &self.entries {
            values.push(entry.value);
            counts.push(entry.count);
            overcounts.push(entry.overcount);
        }
    }

    fn swap_entries(&mut self, idx1: usize, idx2 : usize) {
        *self.value_idx_map.get_mut(&self.entries[idx1].value).unwrap() = idx2 as u32;
        *self.value_idx_map.get_mut(&self.entries[idx2].value).unwrap() = idx1 as u32;
        self.entries.swap(idx1, idx2);
    }

    pub fn add(&mut self, val: T) {
        self.total_vals += 1;

        if self.value_idx_map.contains_key(&val) {
            let idx = self.value_idx_map[&val] as usize;
            let mut entry = self.entries.get_mut(idx).unwrap();
            entry.count += 1;
            let count = entry.count;
            let mut target_idx = idx;
            while target_idx > 0 && self.entries[target_idx - 1].count < count {
                target_idx -= 1;
            }
            if target_idx != idx {
                self.swap_entries(idx, target_idx);
            }
            if target_idx == self.replacement_idx as _ {
                self.replacement_idx += 1;
            }
        } else if self.entries.len() < self.maximum_entries as _ {
                self.value_idx_map.insert(val, self.entries.len() as _);
                self.entries.push(SSHashEntry {value: val, count: 1, overcount: 0});
        } else {
            // Lazily update replacement index if it's fallen off the back of the array
            if self.replacement_idx == self.maximum_entries {
                self.replacement_idx -= 1;
                while self.replacement_idx > 0 && self.entries[self.replacement_idx as usize - 1].count == self.entries[self.replacement_idx as usize].count {
                    self.replacement_idx -= 1;
                }
            }

            let mut entry = self.entries.get_mut(self.replacement_idx as usize).unwrap();
            self.value_idx_map.remove(&entry.value);
            self.value_idx_map.insert(val, self.replacement_idx);
            entry.value = val;
            entry.overcount = entry.count;
            entry.count += 1;  // will not require a resort
            self.replacement_idx += 1;
        }
    }

    fn low_value(&self) -> u64 {
        if self.entries.is_empty() {
            0
        } else {
            self.entries.last().unwrap().count
        }
    }

    // Create a new object containing the combination of two SpaceSaving objects
    // The incoming objects don't have to be the same size, but the returned object will match the size of the larger one
    pub fn combine(first: &SpaceSaving<T>, second: &SpaceSaving<T>) -> SpaceSaving<T> {
        fn next_candidate<T: Eq + Hash + Copy>(it: &mut impl Iterator<Item = T>, first: &SpaceSaving<T>, second: &SpaceSaving<T>, result: &SpaceSaving<T>) -> Option<SSHashEntry<T>> {
            let mut value = it.next();
            while value.is_some() && result.value_idx_map.contains_key(&value.unwrap()) {
                value = it.next();
            }

            value.map(|value| {
                let ent1 = first.value_idx_map.get(&value).map(|idx| first.entries.get(*idx as usize).unwrap());
                let ent2 = second.value_idx_map.get(&value).map(|idx| second.entries.get(*idx as usize).unwrap());

                if let Some(ent1) = ent1 {
                    if let Some(ent2) = ent2 {
                        SSHashEntry {
                            value,
                            count: ent1.count + ent2.count,
                            overcount: ent1.overcount + ent2.overcount,
                        }
                    } else {
                        let mut result = *ent1;
                        result.count += second.low_value();
                        result.overcount += second.low_value();
                        result
                    }
                } else {
                    let mut result = *ent2.unwrap();
                    result.count += first.low_value();
                    result.overcount += first.low_value();
                    result
                }
            })
        }

        let maximum_entries = std::cmp::max(first.maximum_entries, second.maximum_entries) as u32;

        let mut result = SpaceSaving {
            entries: Vec::with_capacity(maximum_entries as usize),
            value_idx_map: HashMap::new(),
            replacement_idx: maximum_entries,
            maximum_entries,
            total_vals: first.total_vals + second.total_vals,
        };
        
        let mut it1 = first.iter();
        let mut it2 = second.iter();
        let mut candidate1 = next_candidate(&mut it1, first, second, &result);
        let mut candidate2 = next_candidate(&mut it2, first, second, &result);

        while candidate1.is_some() && candidate2.is_some() && result.entries.len() < maximum_entries as _ {
            let cand1 = candidate1.unwrap();
            let cand2 = candidate2.unwrap();

            if cand1.count >= cand2.count {
                result.value_idx_map.insert(cand1.value, result.entries.len() as _);
                result.entries.push(cand1);

                candidate1 = next_candidate(&mut it1, first, second, &result);
            } else {
                result.value_idx_map.insert(cand2.value, result.entries.len() as _);
                result.entries.push(cand2);

                candidate2 = next_candidate(&mut it2, first, second, &result);
            }

            // If the candidates were the same we also need to update candidate2
            if cand1.value == cand2.value {
                candidate2 = next_candidate(&mut it2, first, second, &result);
            }
        }

        if result.entries.len() != maximum_entries as _ {
            // Ran out of values from one of the stucts before filling the result
            while candidate1.is_some() && result.entries.len() < maximum_entries as _ {
                let cand1 = candidate1.unwrap();
                result.value_idx_map.insert(cand1.value, result.entries.len() as _);
                result.entries.push(cand1);

                candidate1 = next_candidate(&mut it1, first, second, &result);
            }
            while candidate2.is_some() && result.entries.len() < maximum_entries as _ {
                let cand2 = candidate2.unwrap();
                result.value_idx_map.insert(cand2.value, result.entries.len() as _);
                result.entries.push(cand2);

                candidate2 = next_candidate(&mut it2, first, second, &result);
            }
        }

        result
    }

    // returns true if this stuctured is able to guarantee a TopN result for a given 'n'
    pub fn guaranteed_n(&self, n: usize) -> bool {
        if n + 1 > self.entries.len() {
            return false;
        }

        // since n is 1-based, entries[n] is the n+1 element
        let ncount = self.entries[n].count;
        for i in 0..n {
            if self.entries[i].count - self.entries[i].overcount < ncount {
                return false;
            }
        }

        true
    }

    // returns the highest 'n' such that the first 'n' values are guanteed to be a correct ordered TopN result
    pub fn guaranteed_order_count(&self) -> usize {
        for i in 0..self.entries.len() - 1 {
            if self.entries[i].count - self.entries[i].overcount < self.entries[i+1].count {
                return i;
            }
        }

        self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.entries.iter().map(move |e| e.value)
    }

    // Is this useful outside testing?
    #[cfg(test)]
    fn raw_iter(&self) -> std::slice::Iter<SSHashEntry<T>> {
        self.entries.iter()
    }

    // Returns the first n values, if they're guaranteed to be in order (else returns an empty iterator)
    pub fn guaranteed_topn_iter(&self, n: usize) -> impl Iterator<Item = T> + '_ {
        let mut count = if !self.guaranteed_n(n) {
            n
        } else { 
            0
        };

        self.entries.iter().filter(move |_| {count += 1; count < n}).map(move |e| e.value)
    }

    // Returns an iterator over (Item, min_frequency, max_frequency)
    pub fn freq_iter(&self) -> impl Iterator<Item = (T, f64, f64)> + '_ {
        let val_count = self.total_vals as f64;
        self.entries.iter().map(move |e| (e.value, (e.count - e.overcount) as f64 / val_count, e.count as f64 / val_count))
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng};
    use rand::rngs::SmallRng;

    use super::*;

    struct WeightedGen {
        gen: SmallRng,
        min: i32,
        max: i32,
        weight: f64,
    }

    impl WeightedGen {
        // Gen will random numbers over the given 'num_vals', with 'pct_from_hot' chance of picking a value from the top 'pct_vals_hot' range.
        pub fn new(num_vals: usize, pct_vals_hot: f64, pct_from_hot: f64, seed: Option<u64>) -> WeightedGen {
            let seed = match seed {
                Some(s) => s,
                None => SmallRng::from_entropy().gen()
            };

            let max = (num_vals as f64 * pct_vals_hot) as i32;
            let min = max - num_vals as i32;

            WeightedGen {
                gen: SmallRng::seed_from_u64(seed),
                min,
                max,
                weight: pct_from_hot,
            }
        }

        pub fn val(&mut self) -> i32 {
            if self.gen.gen::<f64>() < self.weight {
                self.gen.gen_range(0..self.max) + 1
            } else {
                self.gen.gen_range(self.min..0)
            }
        }

        pub fn vals(&mut self, num: usize) -> Vec<i32> {
            let mut result = Vec::with_capacity(num);
            for _ in 0..num {
                result.push(self.val());
            }
            result
        }
    }

    #[test]
    fn create_populate_and_retrieve() {
        let mut ss = SpaceSaving::<i32>::new(0.1);

        ss.add(5);
        ss.add(6);
        ss.add(6);
        ss.add(5);
        ss.add(7);
        ss.add(5);
        ss.add(8);
        ss.add(5);
        ss.add(7);
        ss.add(6);

        let mut it = ss.raw_iter();

        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (5, 4));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (6, 3));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (7, 2));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (8, 1));
        assert!(it.next().is_none());
    }

    #[test]
    fn exceed_capacity() {
        let mut ss = SpaceSaving::<i32>::new(0.25); // 4 values

        ss.add(5);
        ss.add(6);
        ss.add(7);
        ss.add(8);
        ss.add(9);

        let mut it = ss.raw_iter();

        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (9, 2));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (6, 1));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (7, 1));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count), (8, 1));
        assert!(it.next().is_none());
    }

    #[test]
    fn combine() {
        let mut ss = SpaceSaving::<i32>::new(0.25);

        ss.add(5);
        ss.add(6);
        ss.add(6);
        ss.add(5);
        ss.add(7);
        ss.add(5);
        ss.add(8);
        ss.add(5);
        ss.add(7);
        ss.add(6);

        let mut ss2 = SpaceSaving::<i32>::new(0.25);

        ss2.add(5);
        ss2.add(6);
        ss2.add(7);
        ss2.add(8);

        let ss = SpaceSaving::combine(&ss, &ss2);

        let mut it = ss.raw_iter();

        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (5, 5, 0));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (6, 4, 0));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (7, 3, 0));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (8, 2, 0));
        assert!(it.next().is_none());
        assert_eq!(ss.total_vals, 14);

        let mut ss2 = SpaceSaving::<i32>::new(0.25);
        ss2.add(9);
        ss2.add(9);
        ss2.add(5);
        let ss = SpaceSaving::combine(&ss, &ss2);

        let mut it = ss.raw_iter();
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (5, 6, 0));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (6, 5, 1));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (7, 4, 1));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (9, 4, 2));
        assert!(it.next().is_none());
        assert_eq!(ss.total_vals, 17);

        let mut ss2 = SpaceSaving::<i32>::new(0.2);
        ss2.add(10);
        let ss = SpaceSaving::combine(&ss, &ss2);

        let mut it = ss.raw_iter();
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (5, 7, 1));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (6, 6, 2));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (7, 5, 2));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (9, 5, 3));
        let test = it.next().unwrap();
        assert_eq!((test.value, test.count, test.overcount), (10, 5, 4));
        assert!(it.next().is_none());
        assert_eq!(ss.total_vals, 18);
    }

    #[test]
    fn test90_10() {
        let mut gen = WeightedGen::new(100, 0.10, 0.90, None);
        let values = gen.vals(10000);

        let mut ss = SpaceSaving::new(1. / 15.);
        for v in values {
            ss.add(v);
        }

        let mut test = ss.raw_iter();

        for _ in 0..10 {
            let tval = test.next().unwrap().value;
            assert!(tval > 0);
        }
    }

    #[test]
    fn test80_20() {
        let mut gen = WeightedGen::new(100, 0.20, 0.80, None);
        let values = gen.vals(10000);

        let mut ss = SpaceSaving::new(1. / 30.);
        for v in values {
            ss.add(v);
        }

        let mut test = ss.raw_iter();

        for _ in 0..20 {
            let tval = test.next().unwrap().value;
            assert!(tval > 0);
        }
    }

    #[test]
    fn zipfian() {
        test_zipfian(1000000000, 100000, 50);
        test_zipfian(1000000000, 100000, 20);
        test_zipfian(1000000000, 100000, 10);
        test_zipfian(1000000000, 100000, 5);
    }

    fn test_zipfian(population: usize, samples: usize, ss_size: usize) {
        use rand::distributions::Distribution;

        let expected_error = 1. / ss_size as f64;

        let mut rng = rand::thread_rng();
        let zipf = zipf::ZipfDistribution::new(population, 1.).unwrap();

        let mut ss = SpaceSaving::new(expected_error);
        let mut abs = HashMap::<usize, usize>::new();

        for _ in 0..samples {
            let v: usize = zipf.sample(&mut rng) - 1;
            ss.add(v);
            *abs.entry(v).or_insert(0) += 1;
        }

        let mut test = ss.raw_iter();
        for i in 0..population {
            let tval = test.next().unwrap().value;
            if tval != i {
                let missed_val_pct = abs[&i] as f64 / samples as f64;
                assert!(missed_val_pct < expected_error);
                return;
            }
        }
    }
}
