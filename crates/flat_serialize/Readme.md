# Flat Serialize #

A cannonicalization of write-to-pointer style serialization. You write a
definition describing the layout the data should have when serialized, and the
macro will generate code that reads and writes each field in order. It also
supports variable-length fields where the length is stored in an earlier field.

## Examples ##

### Basic ###

```rust
/// This will define a struct like
/// ```
/// struct Basic<'a> {
///     header: u32,
///     data_len: usize,
///     array: [u16; 3],
///     data: &'a [u8],
///     data2: &'a [u8],
/// }
/// ```
/// along with various functions to read and write this data to byte buffers
/// (see below)
flat_serialize!{
    struct Basic<'a> {
        header: u32,
        data_len: usize,
        array: [u16; 3],
        data: [u8; self.data_len],
        data2: [u8; self.data_len / 2],
    }
}


#[test]
fn basic() {
    let basic = Basic{
        header: 33,
        array: [202, 404, 555],
        data: &[1, 3, 5, 7, 9, 11],
        data2: &[4, 4, 4],
    };

    // The generated struct can be used to serialize data to a byte vector
    let &mut serialized = Vec::with_capacity(basic.len());
    basic.fill_vec(&mut serialized)

    // or deserialize data from a vector so written
    let (deserialized, remaining_bytes) = unsafe {
        Basic::try_ref(&bytes).unwrap()
    };
    assert_eq!(deserialized.header, &33);
    assert_eq!(deserialized.array, &[202, 404, 555]);
    assert_eq!(deserialized.data, &[1, 3, 5, 7, 9, 11][..]);
    assert_eq!(deserialized.data2, &[4, 4, 4][..]);
    assert_eq!(remaining_bytes, &[][..]);

    // For serialization, the generated code will simply write each field, one
    // after another. (It is currently the programmer's responsibility to ensure
    // that the fields will be aligned correctly for deserialization)
    let mut expected = Vec::new();
    bytes.extend_from_slice(&33u32.to_ne_bytes());
    bytes.extend_from_slice(&6usize.to_ne_bytes());
    bytes.extend_from_slice(&202u16.to_ne_bytes());
    bytes.extend_from_slice(&404u16.to_ne_bytes());
    bytes.extend_from_slice(&555u16.to_ne_bytes());
    bytes.extend_from_slice(&[1, 3, 5, 7, 9, 11]);
    bytes.extend_from_slice(&[4, 4, 4]);
    assert_eq!(serialized, expected);
}
```

### Advanced ###

```rust
/// flat-serializable values can be nested, a field marked with
/// `flat_serialize::flatten` will be read and written using `FlattenableRef`.
//  The data layout is equivalent to just inlining all the fields.
/// ```
/// struct Nested<'a> {
///     prefix: u64,
///     basic: Basic<'a>,
/// }
/// ```
flat_serialize!{
    struct Nested<'a> {
        prefix: u64,
        #[flat_serialize::flatten]
        basic: Basic<'a>,
    }
}

/// Enum-like values are also supported. The enum tag is stored immediately
/// before the enum fields.
flat_serialize!{
    enum Enum<'a> {
        k: u64,
        First: 2 {
            data_len: u32,
            data: [u8; self.data_len],
        },
        Fixed: 3 {
            array: [u16; 3],
        },
    }
}

fn enum_example(e: Enum) {
    match e {
        Enum::First{ data_len, data } => todo!(),
        Enum::Fixed{ array } => todo!(),
    }
}
```
