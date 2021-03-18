pub mod regr;
use time_weighted_average::tspoint::TSPoint;
use serde::{Deserialize, Serialize};


#[derive(Debug, PartialEq)]
pub enum CounterError{
    OrderError,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CounterSummary {
    pub first: TSPoint,
    pub second: TSPoint,
    pub penultimate: TSPoint,
    pub last: TSPoint,
    pub reset_sum: f64,
    pub num_resets: u64,
    pub num_changes: u64,
}


impl CounterSummary {
    pub fn new(pt: &TSPoint) -> CounterSummary {
        CounterSummary{
            first: *pt,
            second: *pt,
            penultimate: *pt,
            last: *pt,
            reset_sum: 0.0,
            num_resets: 0,
            num_changes: 0,
        }
    }



    // expects time-ordered input 
    pub fn add_point(&mut self, incoming: &TSPoint) -> Result<(), CounterError>{

        if incoming.ts < self.last.ts {
            return Err(CounterError::OrderError);
        }
        
        if incoming.val < self.last.val {
            self.reset_sum += self.last.val;
            self.num_resets+= 1;
        }
        if incoming.val != self.last.val{
            self.num_changes += 1;
        }
        if self.first == self.second {
            self.second = *incoming;
        }
        self.penultimate = self.last;
        self.last = *incoming;
        Ok(())
    }

    fn single_value(&self) -> bool {
        self.last == self.first
    }

    

    // combining can only happen for disjoint time ranges 
    pub fn combine(&mut self, incoming: &CounterSummary) -> Result<(), CounterError> {
        // this requires that self comes before incoming in time order
        if self.last.ts >= incoming.first.ts {
            return Err(CounterError::OrderError);
        }
        if self.last.val != incoming.first.val{
            self.num_changes += 1;
            if  incoming.first.val < self.last.val {
                self.reset_sum += self.last.val;
                self.num_resets += 1;
            }
        }
        
        if incoming.single_value() {
            self.penultimate = self.last;
        } else {
            self.penultimate = incoming.penultimate;
        }
        if self.single_value() {
            self.second = incoming.first;
        }
        self.last = incoming.last;
        self.reset_sum += incoming.reset_sum;
        self.num_resets += incoming.num_resets;
        self.num_changes += incoming.num_changes;
        Ok(())
    }

    pub fn delta(&self) -> f64 {
        self.last.val - self.first.val + self.reset_sum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn create() {
        let testpt = TSPoint{ts: 0, val:0.0};
        let test = CounterSummary::new(&testpt);
        assert_eq!(test.first, testpt);
        assert_eq!(test.second, testpt);
        assert_eq!(test.penultimate, testpt);
        assert_eq!(test.last, testpt);
        assert_eq!(test.reset_sum, 0.0);
    }
    #[test]
    fn adding_point() {
        let mut test = CounterSummary::new( &TSPoint{ts: 0, val:0.0});
        let testpt = TSPoint{ts:5, val:10.0};

        test.add_point(&testpt).unwrap();
        
        assert_eq!(test.first, TSPoint{ts: 0, val:0.0});
        assert_eq!(test.second, testpt);
        assert_eq!(test.penultimate, TSPoint{ts: 0, val:0.0});
        assert_eq!(test.last, testpt);
        assert_eq!(test.reset_sum, 0.0);
        assert_eq!(test.num_resets, 0);
        assert_eq!(test.num_changes, 1);
    }

    
    #[test]
    fn adding_points_to_counter() {
        let startpt = TSPoint{ts: 0, val:0.0};
        let mut summary = CounterSummary::new( &startpt);
        
        summary.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        summary.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();
        

        assert_eq!(summary.first, startpt);
        assert_eq!(summary.second, TSPoint{ts: 5, val:10.0});
        assert_eq!(summary.penultimate, TSPoint{ts: 20, val:50.0});
        assert_eq!(summary.last, TSPoint{ts: 25, val:10.0});
        assert_eq!(summary.reset_sum, 50.0);
        assert_eq!(summary.num_resets, 1);
        assert_eq!(summary.num_changes, 4);
    }

    #[test]
    fn adding_out_of_order_counter(){
        let startpt = TSPoint{ts: 0, val:0.0};
        let mut summary = CounterSummary::new( &startpt);

        summary.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        assert_eq!(CounterError::OrderError, summary.add_point(&TSPoint{ts: 2, val:9.0}).unwrap_err());
    } 


    #[test]
    fn test_counter_delta(){
        let startpt = &TSPoint{ts: 0, val:10.0};
        let mut summary = CounterSummary::new(&startpt);

        // with one point
        assert_eq!(summary.delta(), 0.0);

        // simple case
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        assert_eq!(summary.delta(), 10.0);

        //now with a reset
        summary.add_point(&TSPoint{ts: 20, val:10.0}).unwrap();
        assert_eq!(summary.delta(), 20.0);
    }

    #[test]
    fn test_combine(){
        let mut summary = CounterSummary::new( &TSPoint{ts: 0, val:0.0});
        summary.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val:30.0}).unwrap();
        summary.add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        summary.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();

        let mut part1 = CounterSummary::new(&TSPoint{ts: 0, val:0.0});
        part1.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        part1.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();

        let mut part2 = CounterSummary::new(&TSPoint{ts: 15, val:30.0});
        part2.add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        part2.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();

        let mut combined = part1.clone();
        combined.combine(&part2).unwrap();
        assert_eq!(summary, combined);

        // test error in wrong direction
        assert_eq!(part2.combine(&part1).unwrap_err(), CounterError::OrderError);
    }

    #[test]
    fn test_combine_with_small_summary(){
        let mut summary = CounterSummary::new( &TSPoint{ts: 0, val:50.0});
        summary.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();

        // also tests that a reset at the boundary works correctly
        let part1 = CounterSummary::new( &TSPoint{ts: 0, val:50.0});
        let part2 = CounterSummary::new( &TSPoint{ts: 25, val:10.0});

        let mut combined = part1.clone();
        combined.combine(&part2).unwrap();
        assert_eq!(summary, combined);

        // test error in wrong direction
        combined = part2.clone();
        assert_eq!(combined.combine(&part1).unwrap_err(), CounterError::OrderError);
    }
}



