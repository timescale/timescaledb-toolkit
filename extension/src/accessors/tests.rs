use pgrx::*;

use super::accessor;

//use crate::{accessor, build};

// TODO don't require that trailing comma
accessor! { one_field(value: f64,) }
accessor! { two_fields(a: f64, b: i64,) }

#[test]
fn one_field_works() {
    let d: AccessorOneField = accessor_one_field(1.0);
    assert_eq!(1.0, d.value);
}

#[test]
fn two_field_works() {
    let d: AccessorTwoFields = accessor_two_fields(1.0, 2);
    assert_eq!((1.0, 2), (d.a, d.b));
}
