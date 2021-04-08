#[cfg(test)]
pub mod tests {
    use approx::assert_relative_eq;
    use crate::range::I64Range;
    use crate::*;
    fn to_micro(t: f64) -> f64{
        t * 1_000_000.0
    }
    //do proper numerical comparisons on the values where that matters, use exact where it should be exact.
    #[track_caller]
    pub fn assert_close_enough(p1:&CounterSummary, p2:&CounterSummary) {
        assert_eq!(p1.first, p2.first, "first");
        assert_eq!(p1.second, p2.second, "second");
        assert_eq!(p1.penultimate, p2.penultimate, "penultimate");
        assert_eq!(p1.last, p2.last, "last");
        assert_eq!(p1.num_changes, p2.num_changes, "num_changes");
        assert_eq!(p1.num_resets, p2.num_resets, "num_resets");
        assert_eq!(p1.regress.n, p2.regress.n, "n");
        assert_relative_eq!(p1.regress.sx, p2.regress.sx);
        assert_relative_eq!(p1.regress.sxx, p2.regress.sxx);
        assert_relative_eq!(p1.regress.sy, p2.regress.sy);
        assert_relative_eq!(p1.regress.syy, p2.regress.syy);
        assert_relative_eq!(p1.regress.sxy, p2.regress.sxy);
    }
    #[test]
    fn create() {
        let testpt = TSPoint{ts: 0, val:0.0};
        let test = CounterSummary::new(&testpt, None);
        assert_eq!(test.first, testpt);
        assert_eq!(test.second, testpt);
        assert_eq!(test.penultimate, testpt);
        assert_eq!(test.last, testpt);
        assert_eq!(test.reset_sum, 0.0);
    }
    #[test]
    fn adding_point() {
        let mut test = CounterSummary::new( &TSPoint{ts: 0, val:0.0}, None);
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
        let mut summary = CounterSummary::new( &startpt, None);
        
        summary.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        summary.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();
        

        assert_eq!(summary.first, startpt);
        assert_eq!(summary.second, TSPoint{ts: 5, val:10.0});
        assert_eq!(summary.penultimate, TSPoint{ts: 20, val:50.0});
        assert_eq!(summary.last, TSPoint{ts: 25, val:10.0});
        assert_relative_eq!(summary.reset_sum, 50.0);
        assert_eq!(summary.num_resets, 1);
        assert_eq!(summary.num_changes, 4);
        assert_eq!(summary.regress.count(), 6);
        assert_relative_eq!(summary.regress.sum().unwrap().x, to_seconds(75.0));
        // non obvious one here, sumy should be the sum of all values including the resets at the time.
        assert_relative_eq!(summary.regress.sum().unwrap().y, 0.0 + 10.0 + 20.0 + 20.0 + 50.0 + 60.0);
    }
    

    #[test]
    fn adding_out_of_order_counter(){
        let startpt = TSPoint{ts: 0, val:0.0};
        let mut summary = CounterSummary::new( &startpt, None);

        summary.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        assert_eq!(CounterError::OrderError, summary.add_point(&TSPoint{ts: 2, val:9.0}).unwrap_err());
    } 


    #[test]
    fn test_counter_delta(){
        let startpt = &TSPoint{ts: 0, val:10.0};
        let mut summary = CounterSummary::new(&startpt, None);

        // with one point
        assert_relative_eq!(summary.delta(), 0.0);

        // simple case
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        assert_relative_eq!(summary.delta(), 10.0);

        //now with a reset
        summary.add_point(&TSPoint{ts: 20, val:10.0}).unwrap();
        assert_relative_eq!(summary.delta(), 20.0);
    }

    #[test]
    fn test_combine(){
        let mut summary = CounterSummary::new( &TSPoint{ts: 0, val:0.0}, None);
        summary.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val:30.0}).unwrap();
        summary.add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        summary.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();
        summary.add_point(&TSPoint{ts: 30, val:40.0}).unwrap();


        let mut part1 = CounterSummary::new(&TSPoint{ts: 0, val:0.0}, None);
        part1.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        part1.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();

        let mut part2 = CounterSummary::new(&TSPoint{ts: 15, val:30.0}, None);
        part2.add_point(&TSPoint{ts: 20, val:50.0}).unwrap();
        part2.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();
        part2.add_point(&TSPoint{ts: 30, val:40.0}).unwrap();


        let mut combined = part1.clone();
        combined.combine(&part2).unwrap();
        assert_close_enough(&summary, &combined);

        // test error in wrong direction
        assert_eq!(part2.combine(&part1).unwrap_err(), CounterError::OrderError);
    }

    #[test]
    fn test_combine_with_small_summary(){
        let mut summary = CounterSummary::new( &TSPoint{ts: 0, val:50.0}, None);
        summary.add_point(&TSPoint{ts: 25, val:10.0}).unwrap();


        // also tests that a reset at the boundary works correctly
        let part1 = CounterSummary::new( &TSPoint{ts: 0, val:50.0}, None);
        let part2 = CounterSummary::new( &TSPoint{ts: 25, val:10.0}, None);

        let mut combined = part1.clone();
        combined.combine(&part2).unwrap();
        assert_close_enough(&summary, &combined);

        // test error in wrong direction
        combined = part2.clone();
        assert_eq!(combined.combine(&part1).unwrap_err(), CounterError::OrderError);
    }
    #[test]
    fn test_multiple_resets() {
        let startpt = TSPoint{ts: 0, val:0.0};
        let mut summary = CounterSummary::new( &startpt, None);
        
        summary.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val:10.0}).unwrap();
        summary.add_point(&TSPoint{ts: 20, val:40.0}).unwrap();
        summary.add_point(&TSPoint{ts: 25, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 30, val:40.0}).unwrap();

        

        assert_eq!(summary.first, startpt);
        assert_eq!(summary.second, TSPoint{ts: 5, val:10.0});
        assert_eq!(summary.penultimate, TSPoint{ts: 25, val:20.0});
        assert_eq!(summary.last, TSPoint{ts: 30, val:40.0});
        assert_relative_eq!(summary.reset_sum, 60.0);
        assert_eq!(summary.num_resets, 2);
        assert_eq!(summary.num_changes, 6);
        assert_eq!(summary.regress.count(), 7);
        assert_relative_eq!(summary.regress.sum().unwrap().x, to_seconds(105.0));
        // non obvious one here, sy should be the sum of all values including the resets at the time they were added. 
        assert_relative_eq!(summary.regress.sum().unwrap().y, 0.0 + 10.0 + 20.0 + 30.0 + 60.0 + 80.0 + 100.0);

        let mut part1 = CounterSummary::new(&TSPoint{ts: 0, val:0.0}, None);
        part1.add_point(&TSPoint{ts: 5, val:10.0}).unwrap();
        part1.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();

        let mut part2 = CounterSummary::new(&TSPoint{ts: 15, val:10.0}, None);
        part2.add_point(&TSPoint{ts: 20, val:40.0}).unwrap();
        part2.add_point(&TSPoint{ts: 25, val:20.0}).unwrap();
        part2.add_point(&TSPoint{ts: 30, val:40.0}).unwrap();


        let mut combined = part1.clone();
        combined.combine(&part2).unwrap();
        assert_close_enough(&summary, &combined);

        // test error in wrong direction
        assert_eq!(part2.combine(&part1).unwrap_err(), CounterError::OrderError);
    }
    
    #[test]
    fn test_extraction_single_point() {
        let startpt = TSPoint{ts: 20, val:10.0};
        let summary = CounterSummary::new( &startpt, None);
        assert_relative_eq!(summary.delta(), 0.0);
        assert_eq!(summary.rate(), None);
        assert_relative_eq!(summary.idelta_left(), 0.0);
        assert_relative_eq!(summary.idelta_right(), 0.0);
        assert_eq!(summary.irate_left(), None);
        assert_eq!(summary.irate_right(), None);
        assert_eq!(summary.num_changes, 0);
        assert_eq!(summary.num_resets, 0);
    }

    #[test]
    fn test_extraction_simple(){
        let mut summary = CounterSummary::new(&TSPoint{ts: 0, val:0.0}, None);
        summary.add_point(&TSPoint{ts: 5, val:5.0}).unwrap();
        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val: 30.0}).unwrap();

        assert_relative_eq!(summary.delta(), 30.0);
        assert_relative_eq!(summary.rate().unwrap(), to_micro(2.0));
        assert_relative_eq!(summary.idelta_left(), 5.0);
        assert_relative_eq!(summary.idelta_right(), 10.0);
        assert_relative_eq!(summary.irate_left().unwrap(), to_micro(1.0));
        assert_relative_eq!(summary.irate_right().unwrap(), to_micro(2.0));
        assert_eq!(summary.num_changes, 3);
        assert_eq!(summary.num_resets, 0);
    }

    #[test]
    fn test_extraction_with_resets(){
        let mut summary = CounterSummary::new(&TSPoint{ts: 0, val: 10.0}, None);
        summary.add_point(&TSPoint{ts: 5, val:5.0}).unwrap();
        summary.add_point(&TSPoint{ts: 10, val:30.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val: 15.0}).unwrap();

        assert_relative_eq!(summary.delta(), 45.0);
        assert_relative_eq!(summary.rate().unwrap(),to_micro(3.0));
        assert_relative_eq!(summary.idelta_left(), 5.0);
        assert_relative_eq!(summary.idelta_right(), 15.0);
        assert_relative_eq!(summary.irate_left().unwrap(), to_micro(1.0));
        assert_relative_eq!(summary.irate_right().unwrap(), to_micro(3.0));
        assert_eq!(summary.num_changes, 3);
        assert_eq!(summary.num_resets, 2);
    }

    #[test]
    fn test_bounds(){
        let summary = CounterSummary::new(&TSPoint{ts: 0, val: 10.0}, None);
        assert!(summary.bounds_valid()); // no bound is fine.

        let summary = CounterSummary::new(&TSPoint{ts: 0, val: 10.0}, Some(I64Range{left:Some(5), right:Some(10)}));
        assert!(!summary.bounds_valid()); // wrong bound not

        // left bound inclusive
        let mut summary = CounterSummary::new(&TSPoint{ts: 0, val: 10.0}, Some(I64Range{left:Some(0), right:Some(10)}));
        assert!(summary.bounds_valid());
        summary.add_point(&TSPoint{ts: 5, val:5.0}).unwrap();
        assert!(summary.bounds_valid());

        // adding points past our bounds is okay, but the bounds will be invalid when we check, this will happen in the final function not on every point addition for efficiency
        // note the right bound is exclusive
        summary.add_point(&TSPoint{ts: 10, val:10.0}).unwrap();
        assert!(!summary.bounds_valid());

        // slightly weird case here... two invalid bounds can produce a validly bounded object once the bounds are combined, this is a bit weird, but seems like it's the correct behavior
        let summary2 = CounterSummary::new(&TSPoint{ts: 15, val: 10.0}, Some(I64Range{left:Some(20), right:Some(30)}));
        summary.combine(&summary2).unwrap();
        assert!(summary.bounds_valid());
        assert_eq!(summary.bounds.unwrap(), I64Range{left:Some(0), right:Some(30)});

        // two of the same valid bounds remain the same and valid
        let summary2 = CounterSummary::new(&TSPoint{ts: 20, val: 10.0}, Some(I64Range{left:Some(0), right:Some(30)}));
        summary.combine(&summary2).unwrap();
        assert!(summary.bounds_valid());
        assert_eq!(summary.bounds.unwrap(), I64Range{left:Some(0), right:Some(30)});

        // combining with unbounded ones is fine, but the bounds survive
        let summary2 = CounterSummary::new(&TSPoint{ts: 25, val: 10.0}, None);
        summary.combine(&summary2).unwrap();
        assert!(summary.bounds_valid());
        assert_eq!(summary.bounds.unwrap(), I64Range{left:Some(0), right:Some(30)});

        // and combining bounds that do not span are still invalid
        let summary2 = CounterSummary::new(&TSPoint{ts: 35, val: 10.0}, Some(I64Range{left:Some(0), right:Some(32)}));
        summary.combine(&summary2).unwrap();
        assert!(!summary.bounds_valid());
        assert_eq!(summary.bounds.unwrap(), I64Range{left:Some(0), right:Some(32)});

        // combining unbounded with bounded ones is fine, but the bounds survive
        let mut summary = CounterSummary::new(&TSPoint{ts: 0, val: 10.0}, None);
        let summary2 = CounterSummary::new(&TSPoint{ts: 25, val: 10.0}, Some(I64Range{left:Some(0), right:Some(30)}));
        summary.combine(&summary2).unwrap();
        assert!(summary.bounds_valid());
        assert_eq!(summary.bounds.unwrap(), I64Range{left:Some(0), right:Some(30)});
    }

    #[test]
    fn test_prometheus_extrapolation_simple(){
        //error on lack of bounds provided
        let summary = CounterSummary::new(&TSPoint{ts: 5, val:15.0}, None);
        assert_eq!(summary.prometheus_delta().unwrap_err(), CounterError::BoundsInvalid);
        assert_eq!(summary.prometheus_rate().unwrap_err(), CounterError::BoundsInvalid);
        
        //error on infinite bounds
        let summary = CounterSummary::new(&TSPoint{ts: 5, val:15.0}, Some(I64Range{left:None, right:Some(20)}));
        assert_eq!(summary.prometheus_delta().unwrap_err(), CounterError::BoundsInvalid);
        assert_eq!(summary.prometheus_rate().unwrap_err(), CounterError::BoundsInvalid);


        let mut summary = CounterSummary::new(&TSPoint{ts: 5, val:15.0}, Some(I64Range{left:Some(0), right:Some(20)}));
        // singletons should return none
        assert_eq!(summary.prometheus_delta().unwrap(), None);
        assert_eq!(summary.prometheus_rate().unwrap(), None);

        summary.add_point(&TSPoint{ts: 10, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 15, val: 25.0}).unwrap();

        assert_relative_eq!(summary.delta(), 10.0);
        assert_relative_eq!(summary.rate().unwrap(),to_micro(1.0));
        assert_relative_eq!(summary.prometheus_delta().unwrap().unwrap(), 20.0);
        // linear cases like this should be equal
        assert_relative_eq!(summary.prometheus_rate().unwrap().unwrap(), summary.rate().unwrap());
        
        // add a point outside our bounds and make sure we error correctly
        summary.add_point(&TSPoint{ts: 25, val: 35.0}).unwrap();
        assert_eq!(summary.prometheus_delta().unwrap_err(), CounterError::BoundsInvalid);
        assert_eq!(summary.prometheus_rate().unwrap_err(), CounterError::BoundsInvalid);

    }

    #[test]
    fn test_prometheus_extrapolation_bound_size(){
        let mut summary = CounterSummary::new(&TSPoint{ts: 20, val:40.0}, Some(I64Range{left:Some(10), right:Some(50)}));
        summary.add_point(&TSPoint{ts: 30, val:20.0}).unwrap();
        summary.add_point(&TSPoint{ts: 40, val: 40.0}).unwrap();
        assert_relative_eq!(summary.delta(), 40.0);
        assert_relative_eq!(summary.rate().unwrap(),to_micro(2.0));
        //we go all the way to the edge of the bounds here because it's within 1.1 average steps
        assert_relative_eq!(summary.prometheus_delta().unwrap().unwrap(), 80.0);
        // linear cases like this should be equal
        assert_relative_eq!(summary.prometheus_rate().unwrap().unwrap(), summary.rate().unwrap());
        
        // now lets push the bounds to be a bit bigger
        summary.bounds = Some(I64Range{left:Some(8), right:Some(52)});
        // now because we're further than 1.1 out on each side, we end projecting out to half the avg distance on each side
        assert_relative_eq!(summary.prometheus_delta().unwrap().unwrap(), 60.0);
        // but the rate is still divided by the full bound duration
        assert_relative_eq!(summary.prometheus_rate().unwrap().unwrap(), to_micro(60.0 / 44.0));

        //this should all be the same as the last one in the first part. 
        // The change occurs because we hit the zero boundary condition 
        // so things change on the second bit because of where resets occur and our starting value
        let mut summary = CounterSummary::new(&TSPoint{ts: 20, val:20.0}, Some(I64Range{left:Some(10), right:Some(50)}));
        summary.add_point(&TSPoint{ts: 30, val:40.0}).unwrap();
        summary.add_point(&TSPoint{ts: 40, val: 20.0}).unwrap();
        assert_relative_eq!(summary.delta(), 40.0);
        assert_relative_eq!(summary.rate().unwrap(),to_micro(2.0));
        //we go all the way to the edge of the bounds here because it's within 1.1 average steps
        assert_relative_eq!(summary.prometheus_delta().unwrap().unwrap(), 80.0);
        // linear cases like this should be equal
        assert_relative_eq!(summary.prometheus_rate().unwrap().unwrap(), summary.rate().unwrap());
        
        // now lets push the bounds to be a bit bigger
        summary.bounds = Some(I64Range{left:Some(8), right:Some(52)});
        // now because we're further than 1.1 out on the right side, 
        // we end projecting out to half the avg distance on that side, 
        // but because we hit the inferred zero point  on the left (0 in this case)
        // we use zero as the bound on the left side
        assert_relative_eq!(summary.prometheus_delta().unwrap().unwrap(), 70.0);
        // but the rate is still divided by the full bound duration
        assert_relative_eq!(summary.prometheus_rate().unwrap().unwrap(), to_micro(70.0 / 44.0));
        
    }   

}



