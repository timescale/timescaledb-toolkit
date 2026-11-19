// PostgreSQL considers all NaN values equal, unlike Rust's f64 equality.
pub fn float8_eq(left: f64, right: f64) -> bool {
    left == right || (left.is_nan() && right.is_nan())
}
