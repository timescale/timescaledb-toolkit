use pgx::*;

/// Saturating Math for Integers

/// Computes x+y, saturating at the numeric bounds instead of overflowing
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn saturating_add(x: i32, y: i32) -> i32 {
    x.saturating_add(y)
}

/// Computes x+y, saturating at 0 for the minimum bound instead of i32::MIN
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn saturating_add_pos(x: i32, y: i32) -> i32 {
    // check to see if abs of y is greater than the abs of x?
    let result = x.saturating_add(y);
    if result > 0 {
        result
    } else {
        0
    }
}

/// Computes x-y, saturating at the numeric bounds instead of overflowing.
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn saturating_sub(x: i32, y: i32) -> i32 {
    x.saturating_sub(y)
}

/// Computes x-y, saturating at 0 for the minimum bound instead of i32::MIN
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn saturating_sub_pos(x: i32, y: i32) -> i32 {
    if y > x {
        0
    } else {
        x.saturating_sub(y)
    }
}

/// Computes x*y, saturating at the numeric bounds instead of overflowing
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn saturating_mul(x: i32, y: i32) -> i32 {
    x.saturating_mul(y)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_add_max() {
        assert_eq!(i32::MAX, saturating_add(i32::MAX, 100));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_add_min() {
        assert_eq!(i32::MIN, saturating_add(i32::MIN, -100));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_add_pos() {
        assert_eq!(0, saturating_add_pos(200, -350));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_sub_max() {
        assert_eq!(i32::MAX, saturating_sub(i32::MAX, -10));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_sub_min() {
        assert_eq!(i32::MIN, saturating_sub(i32::MIN, 10));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_sub_pos() {
        assert_eq!(0, saturating_sub_pos(i32::MIN, 10));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_mul_max() {
        assert_eq!(i32::MAX, saturating_mul(i32::MAX, 2));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_saturating_mul_min() {
        assert_eq!(i32::MIN, saturating_mul(i32::MAX, -2));
    }
}
