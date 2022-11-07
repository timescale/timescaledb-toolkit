use super::*;

// expanded from FlatSerializable derive macro and made to work right with generic arg
#[allow(warnings, clippy::all)]
unsafe impl<'a> flat_serialize::FlatSerializable<'a> for StatsSummary2D<f64> {
    const REQUIRED_ALIGNMENT: usize = {
        use std::mem::align_of;
        let mut required_alignment = 1;
        let alignment = <u64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        let alignment = <f64 as flat_serialize::FlatSerializable>::REQUIRED_ALIGNMENT;
        if alignment > required_alignment {
            required_alignment = alignment;
        }
        required_alignment
    };
    const MAX_PROVIDED_ALIGNMENT: Option<usize> = {
        use std::mem::align_of;
        let mut min_align: Option<usize> = None;
        let ty_align = <u64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        let ty_align = <f64 as flat_serialize::FlatSerializable>::MAX_PROVIDED_ALIGNMENT;
        match (ty_align, min_align) {
            (None, _) => {}
            (Some(align), None) => min_align = Some(align),
            (Some(align), Some(min)) if align < min => min_align = Some(align),
            _ => {}
        }
        match min_align {
            None => None,
            Some(min_align) => {
                let min_size = Self::MIN_LEN;
                if min_size % 8 == 0 && min_align >= 8 {
                    Some(8)
                } else if min_size % 4 == 0 && min_align >= 4 {
                    Some(4)
                } else if min_size % 2 == 0 && min_align >= 2 {
                    Some(2)
                } else {
                    Some(1)
                }
            }
        }
    };
    const MIN_LEN: usize = {
        use std::mem::size_of;
        let mut size = 0;
        size += <u64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size += <f64 as flat_serialize::FlatSerializable>::MIN_LEN;
        size
    };
    const TRIVIAL_COPY: bool = true;
    type SLICE = flat_serialize::Slice<'a, StatsSummary2D<f64>>;
    type OWNED = Self;
    #[allow(unused_assignments, unused_variables)]
    #[inline(always)]
    unsafe fn try_ref(mut input: &[u8]) -> Result<(Self, &[u8]), flat_serialize::WrapErr> {
        if input.len() < Self::MIN_LEN {
            return Err(flat_serialize::WrapErr::NotEnoughBytes(Self::MIN_LEN));
        }
        let __packet_macro_read_len = 0usize;
        let mut n: Option<u64> = None;
        let mut sx: Option<f64> = None;
        let mut sx2: Option<f64> = None;
        let mut sx3: Option<f64> = None;
        let mut sx4: Option<f64> = None;
        let mut sy: Option<f64> = None;
        let mut sy2: Option<f64> = None;
        let mut sy3: Option<f64> = None;
        let mut sy4: Option<f64> = None;
        let mut sxy: Option<f64> = None;
        'tryref: loop {
            {
                let (field, rem) = match <u64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                n = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sx = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sx2 = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sx3 = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sx4 = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sy = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sy2 = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sy3 = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sy4 = Some(field);
            }
            {
                let (field, rem) = match <f64>::try_ref(input) {
                    Ok((f, b)) => (f, b),
                    Err(flat_serialize::WrapErr::InvalidTag(offset)) => {
                        return Err(flat_serialize::WrapErr::InvalidTag(
                            __packet_macro_read_len + offset,
                        ));
                    }
                    Err(..) => break 'tryref,
                };
                input = rem;
                sxy = Some(field);
            }
            let _ref = StatsSummary2D {
                n: n.unwrap(),
                sx: sx.unwrap(),
                sx2: sx2.unwrap(),
                sx3: sx3.unwrap(),
                sx4: sx4.unwrap(),
                sy: sy.unwrap(),
                sy2: sy2.unwrap(),
                sy3: sy3.unwrap(),
                sy4: sy4.unwrap(),
                sxy: sxy.unwrap(),
            };
            return Ok((_ref, input));
        }
        Err(flat_serialize::WrapErr::NotEnoughBytes(
            0 + <u64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN
                + <f64>::MIN_LEN,
        ))
    }
    #[allow(unused_assignments, unused_variables)]
    #[inline(always)]
    unsafe fn fill_slice<'out>(
        &self,
        input: &'out mut [std::mem::MaybeUninit<u8>],
    ) -> &'out mut [std::mem::MaybeUninit<u8>] {
        let total_len = self.num_bytes();
        let (mut input, rem) = input.split_at_mut(total_len);
        let StatsSummary2D {
            n,
            sx,
            sx2,
            sx3,
            sx4,
            sy,
            sy2,
            sy3,
            sy4,
            sxy,
        } = self;
        unsafe {
            input = n.fill_slice(input);
        };
        unsafe {
            input = sx.fill_slice(input);
        };
        unsafe {
            input = sx2.fill_slice(input);
        };
        unsafe {
            input = sx3.fill_slice(input);
        };
        unsafe {
            input = sx4.fill_slice(input);
        };
        unsafe {
            input = sy.fill_slice(input);
        };
        unsafe {
            input = sy2.fill_slice(input);
        };
        unsafe {
            input = sy3.fill_slice(input);
        };
        unsafe {
            input = sy4.fill_slice(input);
        };
        unsafe {
            input = sxy.fill_slice(input);
        }
        if true {
            match (&input.len(), &0) {
                (left_val, right_val) => {
                    debug_assert_eq!(input.len(), 0);
                }
            };
        }
        rem
    }
    #[allow(unused_assignments, unused_variables)]
    #[inline(always)]
    fn num_bytes(&self) -> usize {
        let StatsSummary2D {
            n,
            sx,
            sx2,
            sx3,
            sx4,
            sy,
            sy2,
            sy3,
            sy4,
            sxy,
        } = self;
        0usize
            + <u64 as flat_serialize::FlatSerializable>::num_bytes(n)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sx)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sx2)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sx3)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sx4)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sy)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sy2)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sy3)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sy4)
            + <f64 as flat_serialize::FlatSerializable>::num_bytes(sxy)
    }
    #[inline(always)]
    fn make_owned(&mut self) {}
    #[inline(always)]
    fn into_owned(self) -> Self::OWNED {
        self
    }
}
