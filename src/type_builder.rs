#[macro_export]
macro_rules! pg_type {
    (
        $(#[$attrs: meta])?
        struct $name: ident: $inner_name:ident {
            $($field:ident : $typ: tt),*
            $(,)?
        }
    ) => {
        use pgx::PostgresType;

        $(#[$attrs])?
        #[derive(PostgresType, Copy, Clone)]
        #[inoutfuncs]
        pub struct $name<'input>($inner_name<'input>);

        $(#[$attrs])?
        flat_serialize_macro::flat_serialize! {
            struct $inner_name {
                header: u32,
                $($field: $typ),*
            }
        }

        impl<'input> pgx::FromDatum for $name<'input> {
            unsafe fn from_datum(datum: Datum, is_null: bool, _: pg_sys::Oid) -> Option<Self>
            where
                Self: Sized,
            {
                if is_null {
                    return None;
                }

                let ptr = pg_sys::pg_detoast_datum_packed(datum as *mut pg_sys::varlena);
                let data_len = varsize_any(ptr);
                let bytes = slice::from_raw_parts(ptr as *mut u8, data_len);

                let (data, _) = match $inner_name::try_ref(bytes) {
                    Ok(wrapped) => wrapped,
                    Err(e) => error!(concat!("invalid ", stringify!($name), " {:?}, got len {}"), e, bytes.len()),
                };

                $name(data).into()
            }
        }

        impl<'input> pgx::IntoDatum for $name<'input> {
            fn into_datum(self) -> Option<Datum> {
                // to convert to a datum just get a pointer to the start of the buffer
                // _technically_ this is only safe if we're sure that the data is laid
                // out contiguously, which we have no way to guarantee except by
                // allocation a new buffer, or storing some additional metadata.
                Some(self.0.header as *const u32 as Datum)
            }

            fn type_oid() -> pg_sys::Oid {
                rust_regtypein::<Self>()
            }
        }

        impl<'input> ::std::ops::Deref for $name<'input> {
            type Target=$inner_name<'input>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    }
}

#[macro_export]
macro_rules! flatten {
    ($typ:ident { $($field:ident: $value:expr),* $(,)? }) => {
        {
            let data = $typ {
                $(
                    $field: $value
                ),*
            };
            let mut output = vec![];
            data.fill_vec(&mut output);
            set_varsize(output.as_mut_ptr() as *mut _, output.len() as i32);

            $typ::try_ref(output.leak()).unwrap().0
        }
    }
}
