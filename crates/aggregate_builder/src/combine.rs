use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned as _;

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
    parameters: Vec<crate::AggregateArg>,
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

        if args.len() != 2 {
            error!(
                parens.span,
                "combine function must take exactly two parameters of type `Option<&T>`"
            )
        }
        let state_type = get_state_type(&args[0])?;
        let state_type2 = get_state_type(&args[1])?;
        if state_type2 != state_type {
            error!(
                args[1].rust.span(),
                "mismatched state types {} vs. {}", state_type, state_type2
            )
        }

        let parameters = args.iter().map(|p| p.clone()).collect();

        Ok(Self {
            ident,
            parameters,
            return_type,
            body,
        })
    }
}

pub struct Generator {
    schema: Option<syn::Ident>,
    function: SourceFunction,
}

impl Generator {
    pub(crate) fn new(
        _attributes: syn::AttributeArgs,
        function: SourceFunction,
    ) -> syn::Result<Self> {
        // TODO Default None but `schema=` attribute overrides; or just don't
        // support `schema=` and instead require using pg_extern's treating
        // enclosing mod as schema.  Why have more than one way to do things?
        let schema = Some(syn::Ident::new(
            "toolkit_experimental",
            function.ident.span(),
        ));

        Ok(Self { schema, function })
    }

    pub fn generate(self) -> proc_macro2::TokenStream {
        let Self { schema, function } = self;

        let fn_name = function.ident;

        let impl_fn_name = syn::Ident::new(
            &format!("{}__impl", fn_name),
            proc_macro2::Span::call_site(),
        );

        let inner_arg_signatures = function.parameters.iter().map(|arg| &arg.rust);

        let ret = function.return_type;
        let body = function.body;

        // TODO default to this but `name=` attribute overrides
        let name = format!("{}", fn_name);
        let name = quote!(, name = #name);

        let schema = schema.as_ref().map(|s| {
            let s = format!("{}", s);
            quote!(, schema = #s)
        });

        quote! {
            fn #impl_fn_name(
                #(#inner_arg_signatures,)*
            ) #ret {
                #body
            }

            #[::pgx::pg_extern(immutable, parallel_safe #name #schema)]
            fn #fn_name(
                state1: crate::palloc::Internal,
                state2: crate::palloc::Internal,
                fcinfo: pgx::pg_sys::FunctionCallInfo
            ) -> Option<crate::palloc::Internal> {
                unsafe {
                    crate::aggregate_utils::combine(
                        state1,
                        state2,
                        fcinfo,
                        #impl_fn_name,
                    )
                }
            }
        }
    }
}

fn get_state_type(arg: &crate::AggregateArg) -> syn::Result<&syn::Ident> {
    match arg.rust.ty.as_ref() {
        syn::Type::Path(path) => {
            // TODO want `match path.path.segments.as_slice() { [segment] => ...` but they don't have as_slice :(
            match path.path.segments.iter().collect::<Vec<_>>().as_slice() {
                [segment] => {
                    // TODO This erroneously accepts local types also named Option.
                    if segment.ident.to_string() == "Option" {
                        match &segment.arguments {
                            syn::PathArguments::AngleBracketed(arguments) => {
                                match arguments.args.iter().collect::<Vec<_>>().as_slice() {
                                    [syn::GenericArgument::Type(syn::Type::Reference(
                                        syn::TypeReference { elem, .. },
                                    ))] => match elem.as_ref() {
                                        syn::Type::Path(path) => {
                                            match path
                                                .path
                                                .segments
                                                .iter()
                                                .collect::<Vec<_>>()
                                                .as_slice()
                                            {
                                                [segment] => return Ok(&segment.ident),
                                                _ => {}
                                            }
                                        }
                                        _ => {}
                                    },
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    error!(arg.rust.span(), "parameters must be Option<&T>")
}
