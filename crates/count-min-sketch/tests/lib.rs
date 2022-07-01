use countminsketch::CountMinSketch;

#[test]
fn empty_sketch() {
    let cms = CountMinSketch::with_dim(1, 1);
    assert_eq!(cms.estimate("foo"), 0);
}

#[test]
fn add_once() {
    let mut cms = CountMinSketch::with_dim(2, 2);
    cms.add_value("foo");
    assert_eq!(cms.estimate("foo"), 1);
}

#[test]
fn subtract_is_inverse_of_add() {
    let mut cms = CountMinSketch::with_dim(2, 2);
    cms.add_value("foo");
    cms.subtract_value("foo");
    assert_eq!(cms.estimate("foo"), 0);
}

#[test]
fn add_repeated() {
    let mut cms = CountMinSketch::with_dim(2, 2);
    for _ in 0..100_000 {
        cms.add_value("foo");
    }
    assert_eq!(cms.estimate("foo"), 100_000);
}

#[test]
fn add_repeated_with_collisions() {
    // if sketch has width = 2 and we add 3 items, then we
    // are guaranteed that we will have at least one hash
    // collision in every row
    let mut cms = CountMinSketch::with_dim(2, 5);

    for _ in 0..100_000 {
        cms.add_value("foo")
    }

    for _ in 0..1_000 {
        cms.add_value("bar")
    }

    for _ in 0..1_000_000 {
        cms.add_value("baz")
    }

    let foo_est = cms.estimate("foo");
    let bar_est = cms.estimate("bar");
    let baz_est = cms.estimate("baz");

    let err_margin = (0.01 * (100_000f64 + 1_000f64 + 1_000_000f64)) as i64;
    assert!(100_000 <= foo_est && foo_est < (100_000 + err_margin));
    assert!(1_000 <= bar_est && bar_est < (1_000 + err_margin));
    assert!(1_000_000 <= baz_est && baz_est < (1_000_000 + err_margin));
}
