pub mod regr;
use time_weighted_average::tspoint::TSPoint;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum MetricType {
    Counter,
//    Gauge,
}

#[derive(Debug, PartialEq)]
pub enum MetricError{
    CounterOrderError,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MetricSummary {
    pub kind: MetricType,
    pub first: TSPoint,
    pub second: TSPoint,
    pub penultimate: TSPoint,
    pub last: TSPoint,
    pub reset_sum: f64
}


impl MetricSummary {
    pub fn new(kind: MetricType, pt: &TSPoint) -> MetricSummary {
        MetricSummary{
            kind,
            first: *pt,
            second: *pt,
            penultimate: *pt,
            last: *pt,
            reset_sum: 0.0,
        }
    }

    // pub fn add_point(&mut self, incoming: &TSPoint) -> Result<(), MetricError>{
    //     match self.kind{
    //         MetricType
    //     }
    //     Ok(())
    // }


    // expects time-ordered input 
    pub fn counter_add_point(&mut self, incoming: &TSPoint) -> Result<(), MetricError>{
        debug_assert!(self.kind == MetricType::Counter);

        if incoming.ts < self.last.ts {
            return Err(MetricError::CounterOrderError);
        }
        
        if incoming.val < self.last.val {
            self.reset_sum += self.last.val;
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
    pub fn counter_combine(&mut self, incoming: &MetricSummary) -> Result<(), MetricError> {
        debug_assert!(self.kind == MetricType::Counter);
        debug_assert!(incoming.kind == MetricType::Counter);
        // this requires that self comes before incoming in time order
        if self.last.ts >= incoming.first.ts {
            return Err(MetricError::CounterOrderError);
        }
        if self.last.val > incoming.first.val {
            self.reset_sum += self.last.val;
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
        let test = MetricSummary::new(MetricType::Counter, &testpt);
        assert_eq!(test.kind , MetricType::Counter);
        assert_eq!(test.first, testpt);
        assert_eq!(test.second, testpt);
        assert_eq!(test.penultimate, testpt);
        assert_eq!(test.last, testpt);
        assert_eq!(test.reset_sum, 0.0);
    }

    #[test]
    fn adding_point() {
        let mut test = MetricSummary::new(MetricType::Counter, &TSPoint{ts: 0, val:0.0});
        let testpt = TSPoint{ts:5, val:10.0};

        test.counter_add_point(&testpt).unwrap();
        
        assert_eq!(test.kind, MetricType::Counter);
        assert_eq!(test.first, TSPoint{ts: 0, val:0.0});
        assert_eq!(test.second, testpt);
        assert_eq!(test.penultimate, TSPoint{ts: 0, val:0.0});
        assert_eq!(test.last, testpt);
        assert_eq!(test.reset_sum, 0.0);
    }

    
    #[test]
    fn adding_points_to_counter() {
        let startpt = TSPoint{ts: 0, val:0.0};
        let mut summary = MetricSummary::new(MetricType::Counter, &startpt);
        
        assert_eq!(summary.kind, MetricType::Counter);

        summary.counter_add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 15, val:30.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 25, val:10.0}).unwrap();
        

        assert_eq!(summary.first, startpt);
        assert_eq!(summary.second, TSPoint{ts: 5, val:10.0});
        assert_eq!(summary.penultimate, TSPoint{ts: 20, val:50.0});
        assert_eq!(summary.last, TSPoint{ts: 25, val:10.0});
        assert_eq!(summary.reset_sum, 50.0);
    }

    // #[test]
    // #[should_panic]
    // fn add_counter_called_on_gauge(){
    //     let startpt = TSPoint{ts: 0, val:0.0};
    //     let mut summary = MetricSummary::new(MetricType::Gauge, &startpt);

    //     assert_eq!(summary.kind, MetricType::Gauge);
    //     summary.counter_add_point(&TSPoint{ts: 5, val:10.0});
    // } 

    #[test]
    fn adding_out_of_order_counter(){
        let startpt = TSPoint{ts: 0, val:0.0};
        let mut summary = MetricSummary::new(MetricType::Counter, &startpt);

        assert_eq!(summary.kind, MetricType::Counter);
        summary.counter_add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        assert_eq!(MetricError::CounterOrderError, summary.counter_add_point(&TSPoint{ts: 2, val:9.0}).unwrap_err());
    } 

    // #[test]
    // fn adding_points_to_gauge() {
    //     let startpt = TSPoint{ts: 0, val:0.0};
    //     let mut summary = MetricSummary::new(MetricType::Gauge, &startpt);
        
    //     assert_eq!(summary.kind, MetricType::Gauge);

    //     summary.add_point(&TSPoint{ts: 15, val:30.0});
    //     summary.add_point(&TSPoint{ts: 5, val:10.0});
    //     summary.add_point(&TSPoint{ts: 20, val:50.0});
    //     summary.add_point(&TSPoint{ts: 25, val:10.0});
    //     summary.add_point(&TSPoint{ts: 10, val:20.0});        

    //     assert_eq!(summary.first, startpt);
    //     assert_eq!(summary.second, TSPoint{ts: 5, val:10.0});
    //     assert_eq!(summary.penultimate, TSPoint{ts: 20, val:50.0});
    //     assert_eq!(summary.last, TSPoint{ts: 25, val:10.0});
    //     assert_eq!(summary.reset_sum, 0.0);
    // }
    
    #[test]
    fn test_counter_delta(){
        let startpt = &TSPoint{ts: 0, val:10.0};
        let mut summary = MetricSummary::new(MetricType::Counter, &startpt);

        // with one point
        assert_eq!(summary.delta(), 0.0);

        // simple case
        summary.counter_add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        assert_eq!(summary.delta(), 10.0);

        //now with a reset
        summary.counter_add_point(&TSPoint{ts: 20, val:10.0}).unwrap();
        assert_eq!(summary.delta(), 20.0);
    }

    #[test]
    fn test_counter_combine(){
        let mut summary = MetricSummary::new(MetricType::Counter, &TSPoint{ts: 0, val:0.0});
        summary.counter_add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 15, val:30.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        summary.counter_add_point(&TSPoint{ts: 25, val:10.0}).unwrap();

        let mut part1 = MetricSummary::new(MetricType::Counter, &TSPoint{ts: 0, val:0.0});
        part1.counter_add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        part1.counter_add_point(&TSPoint{ts: 10, val:20.0}).unwrap();

        let mut part2 = MetricSummary::new(MetricType::Counter, &TSPoint{ts: 15, val:30.0});
        part2.counter_add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        part2.counter_add_point(&TSPoint{ts: 25, val:10.0}).unwrap();

        let mut combined = part1.clone();
        combined.counter_combine(&part2).unwrap();
        assert_eq!(summary, combined);

        // test error in wrong direction
        assert_eq!(part2.counter_combine(&part1).unwrap_err(), MetricError::CounterOrderError);
    }

    #[test]
    fn test_counter_combine_with_small_summary(){
        let mut summary = MetricSummary::new(MetricType::Counter, &TSPoint{ts: 0, val:50.0});
        summary.counter_add_point(&TSPoint{ts: 25, val:10.0}).unwrap();

        // also tests that a reset at the boundary works correctly
        let part1 = MetricSummary::new(MetricType::Counter, &TSPoint{ts: 0, val:50.0});
        let part2 = MetricSummary::new(MetricType::Counter, &TSPoint{ts: 25, val:10.0});

        let mut combined = part1.clone();
        combined.counter_combine(&part2).unwrap();
        assert_eq!(summary, combined);

        // test error in wrong direction
        combined = part2.clone();
        assert_eq!(combined.counter_combine(&part1).unwrap_err(), MetricError::CounterOrderError);
    }
}



