#[derive(Copy, Clone, Debug, serde::Serialize)]
pub enum CachedDatum<'r> {
    None,
    FromInput(&'r [u8]),
    Flattened(&'r [u8]),
}

impl PartialEq for CachedDatum<'_> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

// XXX Required by [`pgrx::PostgresType`] for default [`pgrx::FromDatum`]
// implementation but isn't used since we implement [`pgrx::FromDatum`]
// ourselves. We need a custom implementation because with the default one the
// compiler complains that `'input` and `'de` lifetimes are incompatible.
impl<'de> serde::Deserialize<'de> for CachedDatum<'_> {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        unimplemented!();
    }
}

#[macro_export]
macro_rules! pg_type {
    // base case, all fields are collected into $vals
    (
        $(#[$attrs: meta])*
        struct $name: ident $(<$inlife: lifetime>)? {
            $(,)?
        }

        $(%($($vals:tt)*))?
    ) => {
        $crate::pg_type_impl!{
            'input
            $(#[$attrs])* 
            struct $name $(<$inlife>)?
            {
                $($($vals)*)?
            }
        }
    };
    // eat a struct field and add it to $vals
    (
        $(#[$attrs: meta])*
        struct $name: ident $(<$inlife: lifetime>)? {
            $(#[$fattrs: meta])* $field:ident : $typ: tt $(<$life:lifetime>)?,
            $($tail: tt)*
        }

        $(%($($vals:tt)*))?
    ) => {
        $crate::pg_type!{
            $(#[$attrs])*
            struct $name $(<$inlife>)? {
                $($tail)*
            }

            %( $($($vals)*)?
                $(#[$fattrs])* $field : $typ $(<$life>)? ,
            )
        }
    };
    // eat an enum field, define the enum, and add the equivalent struct field to $vals
    (
        $(#[$attrs: meta])*
        struct $name: ident $(<$inlife: lifetime>)? {
            $(#[$fattrs: meta])* $estructfield: ident : $(#[$enumattrs: meta])* enum $ename:ident $(<$elife: lifetime>)? {
                $($enum_def:tt)*
            },
            $($tail: tt)*
        }

        $(%($($vals:tt)*))?
    ) => {
        flat_serialize_macro::flat_serialize! {
            $(#[$attrs])*
            $(#[$enumattrs])*
            #[derive(serde::Serialize, serde::Deserialize)]
            enum $ename $(<$elife>)? {
                $($enum_def)*
            }
        }
        $crate::pg_type!{
            $(#[$attrs])*
            #[derive(serde::Serialize, serde::Deserialize)]
            struct $name $(<$inlife>)? {
                $($tail)*
            }

            %( $($($vals)*)?
                $(#[$fattrs])*
                #[flat_serialize::flatten]
                $estructfield : $ename $(<$elife>)?,
            )
        }
    }
}

#[macro_export]
macro_rules! pg_type_impl {
    (
        $lifetemplate: lifetime
        $(#[$attrs: meta])*
        struct $name: ident $(<$inlife: lifetime>)? {
            $($(#[$fattrs: meta])* $field:ident : $typ: tt $(<$life:lifetime>)?),*
            $(,)?
        }
    ) => {
        ::paste::paste! {
            $(#[$attrs])*
            #[derive(pgrx::PostgresType, Clone, serde::Serialize, serde::Deserialize)]
            #[bikeshed_postgres_type_manually_impl_from_into_datum]
            pub struct $name<$lifetemplate>(pub [<$name Data>] $(<$inlife>)?, $crate::type_builder::CachedDatum<$lifetemplate>);

            flat_serialize_macro::flat_serialize! {
                $(#[$attrs])*
                #[derive(serde::Serialize, serde::Deserialize)]
                struct [<$name Data>] $(<$inlife>)? {
                    #[serde(skip, default="crate::serialization::serde_reference_adaptor::default_header")]
                    header: u32,
                    version: u8,
                    #[serde(skip, default="crate::serialization::serde_reference_adaptor::default_padding")]
                    padding: [u8; 3],
                    $($(#[$fattrs])* $field: $typ $(<$life>)?),*
                }
            }

            #[::pgrx::pgrx_macros::pg_extern(immutable,parallel_safe)]
            pub fn [<$name:lower _in>](input: Option<&::core::ffi::CStr>) -> Option<$name<'static>> {
                input.map_or_else(|| {
                    while let Some(m) = <$name as ::pgrx::inoutfuncs::InOutFuncs>::NULL_ERROR_MESSAGE {
                        ::pgrx::pg_sys::error!("{m}");
                    }
                    None
                }, |i| Some(<$name as ::pgrx::inoutfuncs::InOutFuncs>::input(i)))
            }

            #[::pgrx::pgrx_macros::pg_extern(immutable,parallel_safe)]
            pub fn [<$name:lower _out>](input: $name) -> ::pgrx::ffi::CString {
                let mut buffer = ::pgrx::stringinfo::StringInfo::new();
                ::pgrx::inoutfuncs::InOutFuncs::output(&input, &mut buffer);
                // SAFETY: We just constructed this StringInfo ourselves
                unsafe { buffer.leak_cstr().to_owned() }
            }

            impl<'input> $name<'input> {
                pub fn in_current_context<'foo>(&self) -> $name<'foo> {
                    unsafe { self.0.flatten() }
                }

                #[allow(clippy::missing_safety_doc)]
                pub unsafe fn cached_datum_or_flatten(&mut self) -> pgrx::pg_sys::Datum {
                    use $crate::type_builder::CachedDatum::*;
                    match self.1 {
                        None => {
                            *self = self.0.flatten();
                            self.cached_datum_or_flatten()
                        },
                        FromInput(bytes) | Flattened(bytes) => pg_sys::Datum::from(bytes.as_ptr()),
                    }
                }
            }

            impl<$lifetemplate> [<$name Data>] $(<$inlife>)? {
                #[allow(clippy::missing_safety_doc)]
                pub unsafe fn flatten<'any>(&self) -> $name<'any> {
                    use flat_serialize::FlatSerializable as _;
                    use $crate::type_builder::CachedDatum::Flattened;
                    // if we already have a CachedDatum::Flattened can just
                    // return it without re-flattening?
                    // TODO this needs extensive testing before we enable it
                    //  XXX this will not work if the lifetime of the memory
                    //      context the value was previously flattened into is
                    //      wrong; this may be bad enough that we should never
                    //      enable it by default...
                    // if let Flattened(bytes) = self.1 {
                    //     let bytes = extend_lifetime(bytes);
                    //     let wrapped = [<$name Data>]::try_ref(bytes).unwrap().0;
                    //     $name(wrapped, Flattened(bytes))
                    //     return self
                    // }
                    let bytes: &'static [u8] = self.to_pg_bytes();
                    let wrapped = [<$name Data>]::try_ref(bytes).unwrap().0;
                    $name(wrapped, Flattened(bytes))
                }

                pub fn to_pg_bytes(&self) -> &'static [u8] {
                    use std::{mem::MaybeUninit, slice};
                    use flat_serialize::FlatSerializable as _;
                    unsafe {
                        let len = self.num_bytes();
                        // valena types have a maximum size
                        if len > 0x3FFFFFFF {
                            pgrx::error!("size {} bytes is to large", len)
                        }
                        let memory: *mut MaybeUninit<u8> = pg_sys::palloc0(len).cast();
                        let slice = slice::from_raw_parts_mut(memory, len);
                        let rem = self.fill_slice(slice);
                        debug_assert_eq!(rem.len(), 0);

                        ::pgrx::set_varsize_4b(memory.cast(), len as i32);
                        slice::from_raw_parts(memory.cast(), len)
                    }
                }
            }

            impl<$lifetemplate> pgrx::FromDatum for $name<$lifetemplate> {
                unsafe fn from_polymorphic_datum(datum: pgrx::pg_sys::Datum, is_null: bool, _: pg_sys::Oid) -> Option<Self>
                where
                    Self: Sized,
                {
                    use flat_serialize::FlatSerializable as _;
                    if is_null {
                        return None;
                    }

                    let mut ptr = pg_sys::pg_detoast_datum_packed(datum.cast_mut_ptr());
                    //TODO is there a better way to do this?
                    if pgrx::varatt_is_1b(ptr) {
                        ptr = pg_sys::pg_detoast_datum_copy(ptr);
                    }
                    let data_len = pgrx::varsize_any(ptr);
                    let bytes = std::slice::from_raw_parts(ptr as *mut u8, data_len);
                    let (data, _) = match [<$name Data>]::try_ref(bytes) {
                        Ok(wrapped) => wrapped,
                        Err(e) => error!(concat!("invalid ", stringify!($name), " {:?}, got len {}"), e, bytes.len()),
                    };

                    $name(data, $crate::type_builder::CachedDatum::FromInput(bytes)).into()
                }
            }

            impl<$lifetemplate> pgrx::IntoDatum for $name<$lifetemplate> {
                fn into_datum(self) -> Option<pgrx::pg_sys::Datum> {
                    use $crate::type_builder::CachedDatum::*;
                    let datum = match self.1 {
                        Flattened(bytes) => pg_sys::Datum::from(bytes.as_ptr()),
                        FromInput(..) | None => pg_sys::Datum::from(self.0.to_pg_bytes().as_ptr()),
                    };
                    Some(datum)
                }

                fn type_oid() -> pg_sys::Oid {
                    rust_regtypein::<Self>()
                }
            }

            unsafe impl<$lifetemplate> ::pgrx::callconv::BoxRet for $name<$lifetemplate> {
                unsafe fn box_into<'fcx>(
                    self,
                    fcinfo: &mut ::pgrx::callconv::FcInfo<'fcx>,
                ) -> ::pgrx::datum::Datum<'fcx> {
                    match ::pgrx::datum::IntoDatum::into_datum(self) {
                        None => fcinfo.return_null(),
                        Some(datum) => unsafe { fcinfo.return_raw_datum(datum) }
                    }
                }
            }

            unsafe impl<'fcx, $lifetemplate> callconv::ArgAbi<'fcx> for $name<$lifetemplate>
            where
                Self: 'fcx,
            {
                unsafe fn unbox_arg_unchecked(arg: callconv::Arg<'_, 'fcx>) -> Self {
                    let index = arg.index();
                    unsafe { arg.unbox_arg_using_from_datum().unwrap_or_else(|| panic!("argument {index} must not be null")) }
                }

                unsafe fn unbox_nullable_arg(arg: callconv::Arg<'_, 'fcx>) -> nullable::Nullable<Self> {
                    unsafe { arg.unbox_arg_using_from_datum().into() }
                }
            }

            impl<$lifetemplate> ::std::ops::Deref for $name <$lifetemplate> {
                type Target=[<$name Data>] $(<$inlife>)?;
                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

            impl<$lifetemplate> ::std::ops::DerefMut for $name <$lifetemplate> {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    self.1 = $crate::type_builder::CachedDatum::None;
                    &mut self.0
                }
            }

            impl<$lifetemplate> From<[<$name Data>]$(<$inlife>)?> for $name<$lifetemplate> {
                fn from(inner: [<$name Data>]$(<$inlife>)?) -> Self {
                    Self(inner, $crate::type_builder::CachedDatum::None)
                }
            }

            impl<$lifetemplate> From<[<$name Data>]$(<$inlife>)?> for Option<$name<$lifetemplate>> {
                fn from(inner: [<$name Data>]$(<$inlife>)?) -> Self {
                    Some($name(inner, $crate::type_builder::CachedDatum::None))
                }
            }
        }
    }
}

#[macro_export]
macro_rules! ron_inout_funcs {
    ($name:ident) => {
        impl<'input> InOutFuncs for $name<'input> {
            fn output(&self, buffer: &mut StringInfo) {
                use $crate::serialization::{str_to_db_encoding, EncodedStr::*};

                let stringified = ron::to_string(&**self).unwrap();
                match str_to_db_encoding(&stringified) {
                    Utf8(s) => buffer.push_str(s),
                    Other(s) => buffer.push_bytes(s.to_bytes()),
                }
            }

            fn input(input: &std::ffi::CStr) -> $name<'input>
            where
                Self: Sized,
            {
                use $crate::serialization::str_from_db_encoding;

                let input = str_from_db_encoding(input);
                let val = ron::from_str(input).unwrap();
                unsafe { Self(val, $crate::type_builder::CachedDatum::None).flatten() }
            }
        }
    };
}

#[macro_export]
macro_rules! flatten {
    ($typ:ident { $($field:ident$(: $value:expr)?),* $(,)? }) => {
        {
            let data = ::paste::paste! {
                [<$typ Data>] {
                    header: 0,
                    version: 1,
                    padding: [0; 3],
                    $(
                        $field$(: $value)?
                    ),*
                }
            };
            data.flatten()
        }
    }
}

#[macro_export]
macro_rules! build {
    ($typ:ident { $($field:ident$(: $value:expr)?),* $(,)? }) => {
        {
            <$typ>::from(::paste::paste! {
                [<$typ Data>] {
                    header: 0,
                    version: 1,
                    padding: [0; 3],
                    $(
                        $field$(: $value)?
                    ),*
                }
            })
        }
    }
}

#[repr(u8)]
pub enum SerializationType {
    Default = 1,
}

#[macro_export]
macro_rules! do_serialize {
    ($state: ident) => {
        {
            $crate::do_serialize!($state, version: 1)
        }
    };
    ($state: ident, version: $version: expr) => {
        {
            use $crate::type_builder::SerializationType;
            use std::io::{Cursor, Write};
            use std::convert::TryInto;

            let state = &*$state;
            let serialized_size = bincode::serialized_size(state)
                .unwrap_or_else(|e| pgrx::error!("serialization error {}", e));
            let our_size = serialized_size + 2; // size of serialized data + our version flags
            let allocated_size = our_size + 4; // size of our data + the varlena header
            let allocated_size = allocated_size.try_into()
                .unwrap_or_else(|e| pgrx::error!("serialization error {}", e));
            // valena types have a maximum size
            if allocated_size > 0x3FFFFFFF {
                pgrx::error!("size {} bytes is to large", allocated_size)
            }

            let bytes: &mut [u8] = unsafe {
                let bytes = pgrx::pg_sys::palloc0(allocated_size);
                std::slice::from_raw_parts_mut(bytes.cast(), allocated_size)
            };
            let mut writer = Cursor::new(bytes);
            // varlena header space
            let varsize = [0; 4];
            writer.write_all(&varsize)
                .unwrap_or_else(|e| pgrx::error!("serialization error {}", e));
            // type version
            writer.write_all(&[$version])
                .unwrap_or_else(|e| pgrx::error!("serialization error {}", e));
            // serialization version; 1 for bincode is currently the only option
            writer.write_all(&[SerializationType::Default as u8])
                .unwrap_or_else(|e| pgrx::error!("serialization error {}", e));
            bincode::serialize_into(&mut writer, state)
                .unwrap_or_else(|e| pgrx::error!("serialization error {}", e));
            unsafe {
                let len = writer.position().try_into().expect("serialized size too large");
                ::pgrx::set_varsize_4b(writer.get_mut().as_mut_ptr() as *mut _, len);
            }
            $crate::raw::bytea::from(pg_sys::Datum::from(writer.into_inner().as_mut_ptr()))
        }
    };
}

#[macro_export]
macro_rules! do_deserialize {
    ($bytes: expr, $t: ty) => {{
        use $crate::type_builder::SerializationType;

        let state: $t = unsafe {
            let input: $crate::raw::bytea = $bytes;
            let input: pgrx::pg_sys::Datum = input.into();
            let detoasted = pg_sys::pg_detoast_datum_packed(input.cast_mut_ptr());
            let len = pgrx::varsize_any_exhdr(detoasted);
            let data = pgrx::vardata_any(detoasted);
            let bytes = std::slice::from_raw_parts(data as *mut u8, len);
            if bytes.len() < 1 {
                pgrx::error!("deserialization error, no bytes")
            }
            if bytes[0] != 1 {
                pgrx::error!(
                    "deserialization error, invalid serialization version {}",
                    bytes[0]
                )
            }
            if bytes[1] != SerializationType::Default as u8 {
                pgrx::error!(
                    "deserialization error, invalid serialization type {}",
                    bytes[1]
                )
            }
            bincode::deserialize(&bytes[2..])
                .unwrap_or_else(|e| pgrx::error!("deserialization error {}", e))
        };
        state.into()
    }};
}
