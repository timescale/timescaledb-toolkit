
#[derive(Copy, Clone, Debug)]
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
            struct $name $(<$inlife>)? {
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
            #[derive(pgx::PostgresType, Clone)]
            #[inoutfuncs]
            pub struct $name<$lifetemplate>([<$name Data>] $(<$inlife>)?, $crate::type_builder::CachedDatum<$lifetemplate>);

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

            impl<'input> $name<'input> {
                pub fn in_current_context<'foo>(&self) -> $name<'foo> {
                    unsafe { self.0.flatten() }
                }

                pub unsafe fn cached_datum_or_flatten(&mut self) -> pgx::pg_sys::Datum {
                    use $crate::type_builder::CachedDatum::*;
                    match self.1 {
                        None => {
                            *self = self.0.flatten();
                            self.cached_datum_or_flatten()
                        },
                        FromInput(bytes) | Flattened(bytes) => bytes.as_ptr() as _,
                    }
                }
            }

            impl<$lifetemplate> [<$name Data>] $(<$inlife>)? {
                pub unsafe fn flatten<'any>(&self) -> $name<'any> {
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
                    unsafe {
                        let len = self.num_bytes();
                        // valena tyes have a maximum size
                        if len > 0x3FFFFFFF {
                            pgx::error!("size {} bytes is to large", len)
                        }
                        let memory: *mut MaybeUninit<u8> = pg_sys::palloc0(len).cast();
                        let slice = slice::from_raw_parts_mut(memory, len);
                        let rem = self.fill_slice(slice);
                        debug_assert_eq!(rem.len(), 0);

                        ::pgx::set_varsize(memory.cast(), len as i32);
                        slice::from_raw_parts(memory.cast(), len)
                    }
                }
            }

            impl<$lifetemplate> pgx::FromDatum for $name<$lifetemplate> {
                unsafe fn from_datum(datum: pgx::pg_sys::Datum, is_null: bool, _: pg_sys::Oid) -> Option<Self>
                where
                    Self: Sized,
                {
                    if is_null {
                        return None;
                    }

                    let mut ptr = pg_sys::pg_detoast_datum_packed(datum as *mut pg_sys::varlena);
                    //TODO is there a better way to do this?
                    if pgx::varatt_is_1b(ptr) {
                        ptr = pg_sys::pg_detoast_datum_copy(ptr);
                    }
                    let data_len = pgx::varsize_any(ptr);
                    let bytes = std::slice::from_raw_parts(ptr as *mut u8, data_len);
                    let (data, _) = match [<$name Data>]::try_ref(bytes) {
                        Ok(wrapped) => wrapped,
                        Err(e) => error!(concat!("invalid ", stringify!($name), " {:?}, got len {}"), e, bytes.len()),
                    };

                    $name(data, $crate::type_builder::CachedDatum::FromInput(bytes)).into()
                }
            }

            impl<$lifetemplate> pgx::IntoDatum for $name<$lifetemplate> {
                fn into_datum(self) -> Option<pgx::pg_sys::Datum> {
                    use $crate::type_builder::CachedDatum::*;
                    let datum = match self.1 {
                        Flattened(bytes) => bytes.as_ptr() as pgx::pg_sys::Datum,
                        FromInput(..) | None => self.0.to_pg_bytes().as_ptr() as pgx::pg_sys::Datum,
                    };
                    Some(datum)
                }

                fn type_oid() -> pg_sys::Oid {
                    rust_regtypein::<Self>()
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
                use $crate::serialization::{EncodedStr::*, str_to_db_encoding};

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

                // SAFETY our serde shims will allocate and leak copies of all
                // the data, so the lifetimes of the borrows aren't actually
                // relevant to the output lifetime
                let val = unsafe {
                    unsafe fn extend_lifetime(s: &str) -> &'static str {
                        std::mem::transmute(s)
                    }
                    let input = extend_lifetime(str_from_db_encoding(input));
                    ron::from_str(input).unwrap()
                };
                unsafe { Self(val, $crate::type_builder::CachedDatum::None).flatten() }
            }
        }
    };
}

#[macro_export]
macro_rules! flatten {
    ($typ:ident { $($field:ident: $value:expr),* $(,)? }) => {
        {
            let data = ::paste::paste! {
                [<$typ Data>] {
                    header: 0,
                    version: 1,
                    padding: [0; 3],
                    $(
                        $field: $value
                    ),*
                }
            };
            data.flatten()
        }
    }
}

#[macro_export]
macro_rules! build {
    ($typ:ident { $($field:ident: $value:expr),* $(,)? }) => {
        {
            <$typ>::from(::paste::paste! {
                [<$typ Data>] {
                    header: 0,
                    version: 1,
                    padding: [0; 3],
                    $(
                        $field: $value
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
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            let our_size = serialized_size + 2; // size of serialized data + our version flags
            let allocated_size = our_size + 4; // size of our data + the varlena header
            let allocated_size = allocated_size.try_into()
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            // valena tyes have a maximum size
            if allocated_size > 0x3FFFFFFF {
                pgx::error!("size {} bytes is to large", allocated_size)
            }

            let bytes: &mut [u8] = unsafe {
                let bytes = pgx::pg_sys::palloc0(allocated_size);
                std::slice::from_raw_parts_mut(bytes.cast(), allocated_size)
            };
            let mut writer = Cursor::new(bytes);
            // varlena header space
            let varsize = [0; 4];
            writer.write_all(&varsize)
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            // type version
            writer.write_all(&[$version])
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            // serialization version; 1 for bincode is currently the only option
            writer.write_all(&[SerializationType::Default as u8])
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            bincode::serialize_into(&mut writer, state)
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            unsafe {
                let len = writer.position().try_into().expect("serialized size too large");
                ::pgx::set_varsize(writer.get_mut().as_mut_ptr() as *mut _, len);
            }
            writer.into_inner().as_mut_ptr() as pg_sys::Datum
        }
    };
}
#[macro_export]
macro_rules! do_deserialize {
    ($bytes: ident, $t: ty) => {
        {
            use $crate::type_builder::SerializationType;

            let state: $t = unsafe {
                let detoasted = pg_sys::pg_detoast_datum_packed($bytes as *mut _);
                let len = pgx::varsize_any_exhdr(detoasted);
                let data = pgx::vardata_any(detoasted);
                let bytes = slice::from_raw_parts(data as *mut u8, len);
                if bytes.len() < 1 {
                    pgx::error!("deserialization error, no bytes")
                }
                if bytes[0] != 1 {
                    pgx::error!("deserialization error, invalid serialization version {}", bytes[0])
                }
                if bytes[1] != SerializationType::Default as u8 {
                    pgx::error!("deserialization error, invalid serialization type {}", bytes[1])
                }
                bincode::deserialize(&bytes[2..]).unwrap_or_else(|e|
                    pgx::error!("deserialization error {}", e))
            };
            state.into()
        }
    };
}
