#[macro_export]
macro_rules! pg_type {
    (
        $(#[$attrs: meta])?
        struct $name: ident {
            $($(#[$fattrs: meta])* $field:ident : $typ: tt$(<$life:lifetime>)?),*
            $(,)?
        }
    ) => {
        ::paste::paste! {
            $(#[$attrs])?
            #[derive(pgx::PostgresType, Copy, Clone)]
            #[inoutfuncs]
            pub struct $name<'input>([<$name Data>]<'input>, Option<&'input [u8]>);

            flat_serialize_macro::flat_serialize! {
                $(#[$attrs])?
                #[derive(serde::Serialize, serde::Deserialize)]
                #[flat_serialize::field_attr(
                    fixed = r##"#[serde(deserialize_with = "crate::serialization::serde_reference_adaptor::deserialize")]"##,
                    variable = r##"#[serde(deserialize_with = "crate::serialization::serde_reference_adaptor::deserialize_slice")]"##,
                )]
                struct [<$name Data>] {
                    #[serde(skip, default="crate::serialization::serde_reference_adaptor::default_header")]
                    header: u32,
                    version: u8,
                    #[serde(skip, default="crate::serialization::serde_reference_adaptor::default_padding")]
                    padding: [u8; 3],
                    $($(#[$fattrs])* $field: $typ $(<$life>)?),*
                }
            }

            impl<'input> [<$name Data>]<'input> {
                pub unsafe fn flatten(&self) -> $name<'static> {
                    let bytes = self.to_pg_bytes();
                    let wrapped = [<$name Data>]::try_ref(bytes).unwrap().0;
                    (wrapped, bytes).into()
                }

                pub fn to_pg_bytes(&self) -> &'static [u8] {
                    let mut output = vec![];
                    self.fill_vec(&mut output);
                    unsafe {
                        ::pgx::set_varsize(output.as_mut_ptr() as *mut _, output.len() as i32);
                    }
                    &*output.leak()
                }
            }

            impl<'input> pgx::FromDatum for $name<'input> {
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

                    $name(data, Some(bytes)).into()
                }
            }

            impl<'input> pgx::IntoDatum for $name<'input> {
                fn into_datum(self) -> Option<pgx::pg_sys::Datum> {
                    let datum = match self.1 {
                        Some(bytes) => bytes.as_ptr() as pgx::pg_sys::Datum,
                        None => self.0.to_pg_bytes().as_ptr() as pgx::pg_sys::Datum,
                    };
                    Some(datum)
                }

                fn type_oid() -> pg_sys::Oid {
                    rust_regtypein::<Self>()
                }
            }

            impl<'input> ::std::ops::Deref for $name<'input> {
                type Target=[<$name Data>]<'input>;
                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

            impl<'input> From<[<$name Data>]<'input>> for $name<'input> {
                fn from(inner: [<$name Data>]<'input>) -> Self {
                    Self(inner, None)
                }
            }

            impl<'input> From<[<$name Data>]<'input>> for Option<$name<'input>> {
                fn from(inner: [<$name Data>]<'input>) -> Self {
                    Some($name(inner, None))
                }
            }

            impl<'input> From<([<$name Data>]<'input>, &'input [u8])> for $name<'input> {
                fn from((inner, bytes): ([<$name Data>]<'input>, &'input [u8])) -> Self {
                    Self(inner, Some(bytes))
                }
            }
        }
    }
}

#[macro_export]
macro_rules! json_inout_funcs {
    ($name:ident) => {
        impl<'input> InOutFuncs for $name<'input> {
            fn output(&self, buffer: &mut StringInfo) {
                use $crate::serialization::{EncodedStr::*, str_to_db_encoding};

                let stringified = serde_json::to_string(&**self).unwrap();
                match str_to_db_encoding(&stringified) {
                    Utf8(s) => buffer.push_str(s),
                    Other(s) => buffer.push_bytes(s.to_bytes()),
                }
            }

            fn input(input: &std::ffi::CStr) -> Self
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
                    serde_json::from_str(input).unwrap()
                };
                unsafe { Self(val, None).flatten() }
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
                    header: &0,
                    version: &1,
                    padding: &[0; 3],
                    $(
                        $field: $value
                    ),*
                }
            };
            data.flatten()
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

            let state = &*$state;
            let serialized_size = bincode::serialized_size(state)
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            let size = serialized_size + 2; // size of serialized data + our version flags
            let mut bytes = Vec::with_capacity(size as usize + 4);
            let varsize = [0; 4];
            bytes.extend_from_slice(&varsize);
            // type version
            bytes.push($version);
            // serialization version; 1 for bincode is currently the only option
            bytes.push(SerializationType::Default as u8);
            bincode::serialize_into(&mut bytes, state)
                .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
            unsafe {
                ::pgx::set_varsize(bytes.as_mut_ptr() as *mut _, bytes.len() as i32);
            }
            bytes.leak().as_mut_ptr() as pg_sys::Datum
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
