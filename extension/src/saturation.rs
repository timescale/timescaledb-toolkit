use pgx::*;

/// Saturating Math for Integers

/// Computes x+y, saturating at the numeric bounds instead of overflowing
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn addition_saturate_to_min(x: i32, y: i32) -> i32 {
    x.saturating_add(y)
}

/// Computes x+y, saturating at 0 for the minimum bound instead of i32::MIN
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn addition_saturate_to_zero(x: i32, y: i32) -> i32 {
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
fn subtraction_saturate_to_min(x: i32, y: i32) -> i32 {
    x.saturating_sub(y)
}

/// Computes x-y, saturating at 0 for the minimum bound instead of i32::MIN
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn subtraction_saturate_to_zero(x: i32, y: i32) -> i32 {
    if y > x {
        0
    } else {
        x.saturating_sub(y)
    }
}

/// Computes x*y, saturating at the numeric bounds instead of overflowing
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn multiplication_saturate_to_min(x: i32, y: i32) -> i32 {
    x.saturating_mul(y)
}

/// Computes x*y, saturating at 0 for the minimum bound instead of i32::MIN
#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
fn multiplication_saturate_to_zero(x: i32, y: i32) -> i32 {
    if x <= 0 || y <= 0 {
        0
    } else {
        x.saturating_mul(y)
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use pgx_macros::pg_test;

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_addition_saturate_max() {
        assert_eq!(i32::MAX, addition_saturate_to_min(i32::MAX, 100));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_addition_saturate_min() {
        assert_eq!(i32::MIN, addition_saturate_to_min(i32::MIN, -100));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_addition_saturate_to_zero() {
        assert_eq!(0, addition_saturate_to_zero(200, -350));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_subtraction_saturate_to_max() {
        assert_eq!(i32::MAX, subtraction_saturate_to_min(i32::MAX, -10));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_subtraction_saturate_to_min() {
        assert_eq!(i32::MIN, subtraction_saturate_to_min(i32::MIN, 10));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_subtraction_saturate_to_zero() {
        assert_eq!(0, subtraction_saturate_to_zero(i32::MIN, 10));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_multiplication_saturate_to_max() {
        assert_eq!(i32::MAX, multiplication_saturate_to_min(i32::MAX, 2));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_multiplication_saturate_to_min() {
        assert_eq!(i32::MIN, multiplication_saturate_to_min(i32::MAX, -2));
    }

    #[pg_test]
    #[allow(arithmetic_overflow)]
    fn test_multiplication_saturate_to_zero() {
        assert_eq!(0, multiplication_saturate_to_zero(i32::MAX, -2));
    }
}
