use std::convert::TryInto;
use std::slice;
use serde::{Serialize, Deserialize};
use pgx::{extension_sql, pg_sys};
use counter_agg::range::I64Range;

use flat_serialize_macro::flat_serialize;

#[allow(non_camel_case_types)]
pub type tstzrange = *mut pg_sys::varlena;

// Derived from Postgres' range_deserialize: https://github.com/postgres/postgres/blob/27e1f14563cf982f1f4d71e21ef247866662a052/src/backend/utils/adt/rangetypes.c#L1779
// but we modify because we only allow specific types of ranges, namely [) inclusive on left and exclusive on right, as this makes a lot of logic simpler, and allows for a standard way to represent a range.
#[allow(clippy::missing_safety_doc)]
pub unsafe fn get_range(range: tstzrange) -> Option<I64Range> {
    let range_bytes = get_toasted_bytes(&*range);
    let mut range_bytes = &range_bytes[8..]; // don't care about the Header and Oid
    let flags = *range_bytes.last().unwrap();
    let mut range = I64Range{
        left: None,
        right: None,
    };
    if flags & RANGE_EMPTY != 0{
        return None
    }
    if range_has_lbound(flags) {
        let bytes = range_bytes[..8].try_into().unwrap();
        range_bytes = &range_bytes[8..];
        let mut left = i64::from_ne_bytes(bytes);
        if !lbound_inclusive(flags) {
            left += 1;
        }
        range.left = Some(left);
    }
    if range_has_rbound(flags){
        let bytes = range_bytes[..8].try_into().unwrap();
        let mut right = i64::from_ne_bytes(bytes);
        if rbound_inclusive(flags) {
            right += 1;
        }
        range.right = Some(right);
    }
    Some(range)

}

unsafe fn get_toasted_bytes(ptr: &pg_sys::varlena) -> &[u8] {
    let mut ptr = pg_sys::pg_detoast_datum_packed(ptr as *const _ as *mut _);
    if pgx::varatt_is_1b(ptr) {
        ptr = pg_sys::pg_detoast_datum_copy(ptr as *const _ as *mut _);
    }
    let data_len = pgx::varsize_any(ptr);
    slice::from_raw_parts(ptr as *mut u8, data_len)
}

const RANGE_EMPTY: u8 = 0x01;
const RANGE_LB_INC: u8 = 0x02;
const RANGE_UB_INC: u8 = 0x04;
const RANGE_LB_INF: u8 = 0x08;
const RANGE_UB_INF: u8 = 0x10;
const RANGE_LB_NULL: u8 = 0x20; // should never be used, but why not.
const RANGE_UB_NULL: u8 = 0x40; // should never be used, but why not.

fn range_has_lbound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF) == 0
}

fn lbound_inclusive(flags: u8) -> bool {
    flags & RANGE_LB_INC != 0
}

fn range_has_rbound(flags: u8) -> bool {
    (flags) & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF) == 0
}
fn rbound_inclusive(flags: u8) -> bool {
    flags & RANGE_UB_INC != 0
}

// `Option<...>` is not suitable for disk storage. `Option<...>` does not have a
// well-defined layout, for instance, an `Option<u32>` takes 8 bytes while an
// `Option<NonZeroU32>` only takes up 4 bytes, despite them both representing
// 32-bit numbers. Worse from our perspective is that the layouts of these are
// not stable and they can change arbitrarily in the future, so an `Option<u32>`
// as created by rust 1.50 may not have the same bytes as one created by rust
// 1.51. `DiskOption<...>` is `repr(C, u64)` and thus does have a well-defined
// layout: 8 bytes for the tag-bit determining if it's `None` or `Some` followed
// by `size_of::<T>()` bytes in which the type can be stored.
// Before stabalization we should probably change the layout to
// ```
// flat_serialize! {
//     is_some: bool,
//     value: [T; self.is_some as u8],
// }
// ```
flat_serialize! {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct I64RangeWrapper {
        is_present: u8,
        has_left: u8,
        has_right: u8,
        padding: [u8; 5],
        left: i64 if self.is_present == 1 && self.has_left == 1,
        right: i64 if self.is_present == 1 && self.has_right == 1,
    }
}
impl I64RangeWrapper {
    pub fn to_i64range(&self) -> Option<I64Range> {
        if self.is_present == 0 {
            return None
        }
        Some(I64Range{
            left: self.left,
            right: self.right,
        })
    }

    pub fn from_i64range(b: Option<I64Range>) -> Self {
        match b {
            Some(range) => Self {
                is_present: 1,
                has_left: range.left.is_some().into(),
                has_right:  range.right.is_some().into(),
                padding: [0; 5],
                left: range.left,
                right: range.right,
            },
            None => Self {
                is_present: 0,
                has_left: 0,
                has_right: 0,
                padding: [0; 5],
                left: None,
                right: None,
            },
        }
    }
}

// this introduces a timescaledb dependency, but only kind of,
extension_sql!("\n\
CREATE OR REPLACE FUNCTION toolkit_experimental.time_bucket_range( bucket_width interval, ts timestamptz)\n\
RETURNS tstzrange as $$\n\
SELECT tstzrange(time_bucket(bucket_width, ts), time_bucket(bucket_width, ts + bucket_width), '[)');\n\
$$\n\
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;\n\
",
name = "time_bucket_range",
);
