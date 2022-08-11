
pub use stabilization_info::*;

#[path = "../../../../extension/src/stabilization_info.rs"]
mod stabilization_info;

#[macro_export]
macro_rules! functions_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($fn_name: ident ( $( $($fn_type: ident)+ ),* ) ),* $(,)?
            }
        )*
    ) => {
        pub static $export_symbol: &[&str] = &[
            $(
                $(stringify!( $fn_name( $( $($fn_type)+  ),* ) ),)*
            )*
        ];
    };
}

#[macro_export]
macro_rules! types_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($type_name: ident),* $(,)?
            }
        )*
    ) => {
        pub static $export_symbol: &[&str] = &[
            $(
                $(stringify!($type_name),)*
            )*
        ];
    };
}

#[macro_export]
macro_rules! operators_stabilized_at {
    (
        $export_symbol: ident
        $(
            $version: literal => {
                $($operator_name: literal ( $( $($fn_type: ident)+ ),* ) ),* $(,)?
            }
        )*
    ) => {
        #[allow(non_snake_case)]
        pub fn $export_symbol() -> std::collections::HashSet<String> {
            static OPERATORS: &[(&str, &[&str])] = &[
                $(
                    $(
                        (
                            $operator_name,
                            &[
                                $( stringify!($($fn_type)+) ),*
                            ]
                        ),
                    )*
                )*
            ];
            OPERATORS.iter().map(|(name, types)| {
                format!("{}({})", name, types.join(","))
            }).collect()
        }
    };
}