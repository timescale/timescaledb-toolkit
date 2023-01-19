use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser as _;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Token,
};

// TODO move to crate rather than duplicating
macro_rules! error {
    ($span: expr, $fmt: literal, $($arg:expr),* $(,)?) => {
        return Err(syn::Error::new($span, format!($fmt, $($arg),*)))
    };
    ($span: expr, $msg: literal) => {
        return Err(syn::Error::new($span, $msg))
    };
}

/// Parsed representation of the source function we generate from.
#[derive(Debug)]
pub struct SourceFunction {
    ident: syn::Ident,
    state_parameter: crate::AggregateArg,
    extra_parameters: Vec<crate::AggregateArg>,
    return_type: syn::ReturnType,
    body: syn::Block,
}
impl Parse for SourceFunction {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let crate::AggregateFn {
            ident,
            parens,
            args,
            ret: return_type,
            body,
            ..
        } = input.parse()?;
        let mut iter = args.iter();
        let state_parameter = iter
            .next()
            .ok_or_else(|| syn::Error::new(parens.span, "state parameter required"))?
            .clone();
        let extra_parameters = iter.map(|p| p.clone()).collect();
        Ok(Self {
            ident,
            state_parameter,
            extra_parameters,
            return_type,
            body,
        })
    }
}

#[derive(Debug)]
pub struct Attributes {
    name: syn::Ident,
    schema: Option<syn::Ident>,
    immutable: bool,
    parallel: Parallel,
    strict: bool,

    finalfunc: Option<Func>,
    combinefunc: Option<Func>,
    serialfunc: Option<Func>,
    deserialfunc: Option<Func>,
}

impl Attributes {
    pub fn parse(input: TokenStream) -> syn::Result<Self> {
        let mut aggregate_name = None;
        let mut schema = None;
        let mut immutable = false;
        let mut parallel = Parallel::default();
        let mut strict = false;
        let mut finalfunc = None;
        let mut combinefunc = None;
        let mut serialfunc = None;
        let mut deserialfunc = None;

        let parser = Punctuated::<Attr, Token![,]>::parse_terminated;
        for attr in parser.parse2(input.into())?.iter_mut() {
            assert!(
                !attr.value.is_empty(),
                "Attr::Parse should not allow empty attribute value"
            );
            let name = attr.name.to_string();
            match name.as_str() {
                "name" | "schema" | "immutable" | "parallel" | "strict" => {
                    if attr.value.len() > 1 {
                        error!(attr.name.span(), "{} requires simple identifier", name);
                    }
                    let value = attr.value.pop().ok_or_else(|| {
                        syn::Error::new(
                            attr.name.span(),
                            format!("{} requires simple identifier", name),
                        )
                    })?;
                    match name.as_str() {
                        "name" => aggregate_name = Some(value),
                        "schema" => schema = Some(value),
                        "parallel" => {
                            parallel = match value.to_string().as_str() {
                                "restricted" => Parallel::Restricted,
                                "safe" => Parallel::Safe,
                                "unsafe" => Parallel::Unsafe,
                                _ => error!(value.span(), "illegal parallel"),
                            }
                        }
                        "immutable" | "strict" => {
                            let value = match value.to_string().as_str() {
                                "true" => true,
                                "false" => false,
                                _ => {
                                    error!(attr.value[0].span(), "{} requires true or false", name)
                                }
                            };
                            match name.as_str() {
                                "immutable" => immutable = value,
                                "strict" => strict = value,
                                _ => unreachable!("processing subset here"),
                            }
                        }
                        _ => unreachable!("processing subset here"),
                    }
                }

                "finalfunc" | "combinefunc" | "serialfunc" | "deserialfunc" => {
                    if attr.value.len() > 2 {
                        error!(
                            attr.name.span(),
                            "{} requires one or two path segments only (`foo` or `foo::bar`)", name
                        );
                    }
                    let func = {
                        let name = attr.value.pop().ok_or_else(||syn::Error::new(
                            attr.name.span(),
                            format!("{} requires one or two path segments only (`foo` or `foo::bar`)", name)
                        ))?;
                        match attr.value.pop() {
                            None => Func { name, schema: None },
                            schema => Func { name, schema },
                        }
                    };
                    match name.as_str() {
                        "finalfunc" => finalfunc = Some(func),
                        "combinefunc" => combinefunc = Some(func),
                        "serialfunc" => serialfunc = Some(func),
                        "deserialfunc" => deserialfunc = Some(func),
                        _ => unreachable!("processing subset here"),
                    }
                }
                _ => error!(attr.name.span(), "unexpected"),
            };
        }
        let name = aggregate_name
            .ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "name required"))?;
        Ok(Self {
            name,
            schema,
            immutable,
            parallel,
            strict,
            finalfunc,
            combinefunc,
            serialfunc,
            deserialfunc,
        })
    }
}

#[derive(Debug)]
pub struct Generator {
    attributes: Attributes,
    schema: Option<syn::Ident>,
    function: SourceFunction,
}

impl Generator {
    pub(crate) fn new(attributes: Attributes, function: SourceFunction) -> syn::Result<Self> {
        // TODO Default None but `schema=` attribute overrides; or just don't
        // support `schema=` and instead require using pg_extern's treating
        // enclosing mod as schema.  Why have more than one way to do things?
        let schema = match &attributes.schema {
            Some(schema) => Some(schema.clone()),
            None => None,
        };
        Ok(Self {
            attributes,
            schema,
            function,
        })
    }

    pub fn generate(self) -> proc_macro2::TokenStream {
        let Self {
            attributes,
            schema,
            function,
        } = self;

        let name = attributes.name.to_string();

        let transition_fn_name = function.ident;

        // TODO It's redundant to require us to mark every type with its sql
        // type.  We should do that just once and derive it here.
        let mut sql_args = vec![];
        let state_signature = function.state_parameter.rust;
        let mut all_arg_signatures = vec![&state_signature];
        let mut extra_arg_signatures = vec![];
        for arg in function.extra_parameters.iter() {
            let super::AggregateArg { rust, sql } = arg;
            sql_args.push({
                let name = match rust.pat.as_ref() {
                    syn::Pat::Ident(syn::PatIdent { ident, .. }) => ident,
                    _ => unreachable!("parsing made this name available"),
                };
                format!(
                    "{} {}",
                    name,
                    match sql {
                        None => unreachable!("parsing made this sql type available"),
                        Some(sql) => sql.value(),
                    }
                )
            });
            extra_arg_signatures.push(rust);
            all_arg_signatures.push(rust);
        }

        let ret = function.return_type;
        let body = function.body;

        let (sql_schema, pg_extern_schema) = match schema.as_ref() {
            None => (String::new(), None),
            Some(schema) => {
                let schema = schema.to_string();
                (format!("{schema}."), Some(quote!(, schema = #schema)))
            }
        };

        let impl_fn_name = syn::Ident::new(
            &format!("{}__impl", transition_fn_name),
            proc_macro2::Span::call_site(),
        );

        let mut create = format!(
            r#"CREATE AGGREGATE {}{}(
    {})
(
    stype = internal,
    sfunc = {}{},
"#,
            sql_schema,
            name,
            sql_args.join(",\n    "),
            sql_schema,
            transition_fn_name,
        );
        let final_fn_name = attributes
            .finalfunc
            .map(|func| fmt_agg_func(&mut create, "final", &func));
        let combine_fn_name = attributes
            .combinefunc
            .map(|func| fmt_agg_func(&mut create, "combine", &func));
        let serial_fn_name = attributes
            .serialfunc
            .map(|func| fmt_agg_func(&mut create, "serial", &func));
        let deserial_fn_name = attributes
            .deserialfunc
            .map(|func| fmt_agg_func(&mut create, "deserial", &func));
        let create = format!(
            r#"{}
    immutable = {},
    parallel = {},
    strict = {});"#,
            create, attributes.immutable, attributes.parallel, attributes.strict
        );

        let extension_sql_name = format!("{}_extension_sql", name);

        let name = format!("{}", transition_fn_name);
        let name = quote! { name = #name };

        quote! {
            // TODO type checks

            fn #transition_fn_name(
                #(#all_arg_signatures,)*
            ) #ret {
                #body
            }

            // TODO derive immutable and parallel_safe from above
            #[pgx::pg_extern(#name, immutable, parallel_safe #pg_extern_schema)]
            fn #impl_fn_name(
                state: crate::palloc::Internal,
                #(#extra_arg_signatures,)*
                fcinfo: pgx::pg_sys::FunctionCallInfo,
            ) -> Option<crate::palloc::Internal> {
                // TODO Extract extra_arg_NAMES so we can call directly into transition_fn above rather than duplicate.
                let f = |#state_signature| #body;
                unsafe { crate::aggregate_utils::transition(state, fcinfo, f) }
            }

            pgx::extension_sql!(
                #create,
                name=#extension_sql_name,
                requires = [
                    #impl_fn_name,
                    #final_fn_name
                    #combine_fn_name
                    #serial_fn_name
                    #deserial_fn_name
                ],
            );
        }
    }
}

fn fmt_agg_func(create: &mut String, funcprefix: &str, func: &Func) -> proc_macro2::TokenStream {
    create.push_str(&format!("    {}func = ", funcprefix));
    if let Some(schema) = func.schema.as_ref() {
        create.push_str(&format!("{}.", schema));
    }
    create.push_str(&format!("{},\n", func.name));
    let name = &func.name;
    quote! { #name, }
}

#[derive(Debug)]
enum Parallel {
    Unsafe,
    Restricted,
    Safe,
}
impl Default for Parallel {
    fn default() -> Self {
        Self::Unsafe
    }
}
impl std::fmt::Display for Parallel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unsafe => "unsafe",
            Self::Restricted => "restricted",
            Self::Safe => "safe",
        })
    }
}

#[derive(Debug)]
struct Attr {
    name: syn::Ident,
    value: Vec<syn::Ident>,
}
impl Parse for Attr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name = input.parse()?;
        let _: Token![=] = input.parse()?;
        let path: syn::Path = input.parse()?;
        let value;
        match path.segments.iter().collect::<Vec<_>>().as_slice() {
            [syn::PathSegment { ident, .. }] => value = vec![ident.clone()],
            [schema, ident] => {
                value = vec![schema.ident.clone(), ident.ident.clone()];
            }
            what => todo!("hmm got {:?}", what),
        }
        Ok(Self { name, value })
    }
}

#[derive(Debug)]
struct Func {
    name: syn::Ident,
    schema: Option<syn::Ident>,
}
