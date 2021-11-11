
use std::borrow::Cow;

use proc_macro::{TokenStream};

use proc_macro2::{Span, TokenStream as TokenStream2};

use quote::{quote, quote_spanned};

use syn::{Token, parse::{Parse, ParseStream}, parse_macro_input, punctuated::Punctuated, spanned::Spanned, token::Comma, parse_quote};

#[proc_macro_attribute]
pub fn aggregate(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(item as Aggregate);
    let expanded = expand(input);
    if cfg!(feature = "print-generated") {
        println!("{}", expanded.to_string());
    }
    expanded.into()
}

// like ItemImpl except that we allow `name: Type "SqlType"` for `fn transition`
struct Aggregate {
    schema: Option<syn::Ident>,
    name: syn::Ident,

    state_ty: AggregateTy,

    parallel_safe: Option<syn::LitBool>,

    transition_fn: AggregateFn,
    final_fn: AggregateFn,

    serialize_fn: Option<AggregateFn>,
    deserialize_fn: Option<AggregateFn>,
    combine_fn: Option<AggregateFn>,
}

enum AggregateItem {
    State(AggregateTy),
    Fn(AggregateFn),
    ParallelSafe(AggregateParallelSafe),
}

struct AggregateTy {
    ident: syn::Ident,
    ty: Box<syn::Type>,
}

struct AggregateParallelSafe {
    value: syn::LitBool,
}

struct AggregateFn {
    ident: syn::Ident,
    sql_name: Option<syn::LitStr>,
    parens: syn::token::Paren,
    args: Punctuated<AggregateArg, Comma>,
    ret: syn::ReturnType,
    body: syn::Block,
}

struct AggregateArg {
    rust: syn::PatType,
    sql: Option<syn::LitStr>,
}

macro_rules! error {
    ($span: expr, $fmt: literal, $($arg:expr),* $(,)?) => {
        return Err(syn::Error::new($span, format!($fmt, $($arg),*)))
    };
    ($span: expr, $msg: literal) => {
        return Err(syn::Error::new($span, $msg))
    };
}

macro_rules! check_duplicate {
    ($val: expr, $span:expr, $name: expr) => {
        if $val.is_some() {
            error!($span, "duplicate {}")
        }
    };
}

impl Parse for Aggregate {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _: Token![impl] = input.parse()?;

        let first_path_segment = input.parse()?;
        let (schema, name): (_, syn::Ident) =
            if input.peek(Token![::]) {
                let _: Token![::] = input.parse()?;
                (Some(first_path_segment), input.parse()?)
            } else {
                (None, first_path_segment)
            };

        let body;
        let _brace_token = syn::braced!(body in input);
        let mut state_ty = None;

        let mut parallel_safe = None;

        let mut fns: Vec<AggregateFn> = vec![];
        while !body.is_empty() {
            use AggregateItem::*;
            let item = body.parse()?;
            match item {
                State(ty) => {
                    if ty.ident != "State" {
                        error!(ty.ident.span(), "unexpected `type {}`, expected `State`", ty.ident)
                    }
                    if state_ty.is_some() {
                        error!(ty.ident.span(), "duplicate `type State`")
                    }
                    state_ty = Some(ty);
                },
                ParallelSafe(safe) => parallel_safe = Some(safe.value),
                Fn(f) => {
                    fns.push(f);
                },
            }
        }

        let mut transition_fn = None;
        let mut final_fn = None;
        let mut serialize_fn = None;
        let mut deserialize_fn = None;
        let mut combine_fn = None;
        for f in fns {
            if f.ident == "transition" {
                check_duplicate!(transition_fn, f.ident.span(), "`fn transition`");
                if f.args.is_empty() {
                    error!(f.parens.span, "transition function must have at least one argument")
                }
                for arg in f.args.iter().skip(1) {
                    if arg.sql.is_none() {
                        error!(arg.rust.span(), "missing SQL type")
                    }
                }
                transition_fn = Some(f);
            } else if f.ident == "finally" {
                check_duplicate!(final_fn, f.ident.span(), "`fn finally`");
                if f.args.len() != 1 {
                    error!(f.parens.span, "final function must have at one argument of type `Option<Inner<State>>`")
                }
                if f.args[0].sql.is_some() {
                    error!(f.args[0].sql.span(), "should not have SQL type, will be inferred")
                }
                final_fn = Some(f);
            } else if f.ident == "serialize" {
                check_duplicate!(serialize_fn, f.ident.span(), "`fn serialize`");
                if f.args.len() != 1 {
                    error!(f.parens.span, "serialize function must have at one argument of type `Inner<State>`")
                }
                if f.args[0].sql.is_some() {
                    error!(f.args[0].sql.span(), "should not have SQL type, will be inferred")
                }
                serialize_fn = Some(f);
            } else if f.ident == "deserialize" {
                check_duplicate!(deserialize_fn, f.ident.span(), "`fn deserialize`");
                if f.args.len() != 1 {
                    error!(f.parens.span, "deserialize function must have at one argument of type `bytea`")
                }
                if f.args[0].sql.is_some() {
                    error!(f.args[0].sql.span(), "should not have SQL type, will be inferred")
                }
                deserialize_fn = Some(f);
            } else if f.ident == "combine" {
                check_duplicate!(combine_fn, f.ident.span(), "`fn combine`");
                if f.args.len() != 2 {
                    error!(f.parens.span, "deserialize function must have at one argument of type `Option<Inner<State>>`")
                }
                for arg in &f.args {
                    if arg.sql.is_some() {
                        error!(arg.sql.span(), "should not have SQL type, will be inferred")
                    }
                }
                combine_fn = Some(f)
            } else {
                error!(
                    f.ident.span(),
                    "unexpected `fn {}`, expected one of `transition`, `final`, `serialize`, or `deserialize`",
                    f.ident
                )
            }
        }


        let state_ty = match state_ty  {
            Some(state_ty) => state_ty,
            None => error!(name.span(), "missing `type State = ...;`"),
        };

        let transition_fn = match transition_fn  {
            Some(transition_fn) => transition_fn,
            None => error!(name.span(), "missing `fn transition`"),
        };

        let final_fn = match final_fn  {
            Some(final_fn) => final_fn,
            None => error!(name.span(), "missing `fn final`"),
        };

        Ok(Aggregate {
            schema,
            name,
            state_ty,
            parallel_safe,
            transition_fn,
            final_fn,
            serialize_fn,
            deserialize_fn,
            combine_fn,
        })
    }
}

impl Parse for AggregateItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ahead = input.fork();
        let _ = ahead.call(syn::Attribute::parse_outer)?;
        let lookahead = ahead.lookahead1();
        if lookahead.peek(Token![fn]) {
            input.parse().map(AggregateItem::Fn)
        } else if lookahead.peek(Token![type]) {
            input.parse().map(AggregateItem::State)
        } else if lookahead.peek(Token![const]) {
            input.parse().map(AggregateItem::ParallelSafe)
        } else {
            Err(lookahead.error())
        }
    }
}

impl Parse for AggregateTy {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _: Token![type] = input.parse()?;
        let ident = input.parse()?;
        let _: Token![=] = input.parse()?;
        let ty = Box::new(input.parse()?);
        let _: Token![;] = input.parse()?;
        Ok(Self { ident, ty })
    }
}

impl Parse for AggregateParallelSafe {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _: Token![const] = input.parse()?;
        let name: syn::Ident = input.parse()?;
        if name != "PARALLEL_SAFE" {
            error!(name.span(), "unexpected const `{}` expected `PARALLEL_SAFE`", name)
        }
        let _: Token![:] = input.parse()?;
        let ty: syn::Ident = input.parse()?;
        if ty != "bool" {
            error!(ty.span(), "unexpected type `{}` expected `bool`", ty)
        }
        let _: Token![=] = input.parse()?;
        let value = input.parse()?;
        let _: Token![;] = input.parse()?;
        Ok(Self { value })
    }
}

impl Parse for AggregateFn {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut attributes = input.call(syn::Attribute::parse_outer)?;
        let _: Token![fn] = input.parse()?;
        let ident = input.parse()?;

        let contents;
        let parens = syn::parenthesized!(contents in input);

        let mut args = Punctuated::new();
        while !contents.is_empty() {
            let arg = contents.parse()?;
            args.push(arg);
            if contents.is_empty() {
                break
            }
            let comma: Token![,] = contents.parse()?;
            args.push_punct(comma);
        };

        let ret = input.parse()?;
        let body = input.parse()?;

        let expected_path = parse_quote!(sql_name);
        let sql_name = match take_attr(&mut attributes, &expected_path) {
            None => None,
            Some(attribute) => attribute.parse_args()?
        };
        if !attributes.is_empty() {
            error!(attributes[0].span(), "unexpected attribute")
        }
        Ok(Self {
            ident,
            sql_name,
            parens,
            args,
            ret,
            body,
        })
    }
}

impl Parse for AggregateArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let arg: syn::FnArg = input.parse()?;
        let mut rust = match arg {
            syn::FnArg::Typed(pat) => pat,
            _ => error!(arg.span(), "`self` is not a valid aggregate argument"),
        };
        let sql = {
            let expected_path = parse_quote!(sql_type);
            let attribute = take_attr(&mut rust.attrs, &expected_path);
            match attribute {
                None => None,
                Some(attribute) => attribute.parse_args()?
            }
        };
        Ok(Self {
            rust,
            sql,
        })
    }
}

fn take_attr(attrs: &mut Vec<syn::Attribute>, path: &syn::Path)
-> Option<syn::Attribute> {
    let idx = attrs.iter().enumerate().find(|(_, a)| &a.path == path);
    match idx {
        None => None,
        Some((idx, _)) => {
            let attribute = attrs.remove(idx);
            Some(attribute)
        }
    }
}

//
//
//
//

fn expand(agg: Aggregate) -> TokenStream2 {
    use std::fmt::Write;
    let Aggregate {
        schema,
        name,
        state_ty,
        parallel_safe,
        transition_fn,
        final_fn,
        serialize_fn,
        deserialize_fn,
        combine_fn,
    } = agg;

    let state_ty = state_ty.ty;

    let transition_fns = transition_fn.transition_fn_tokens(&schema, &name);
    let final_fns = final_fn.final_fn_tokens(&schema, &name);

    let mut extension_sql_reqs = vec![
        transition_fn.outer_ident(&name),
        final_fn.outer_ident(&name),
    ];

    let schema_qualifier = match &schema {
        Some(schema) => format!("{}.", schema),
        None => String::new(),
    };
    let mut create = format!("\nCREATE AGGREGATE {}{} (", schema_qualifier, name);
    for (i, (name, arg)) in transition_fn.sql_args().enumerate() {
        if i != 0 {
            let _ = write!(&mut create, ", ");
        }
        if let Some(name) = name {
            let _ = write!(&mut create, "{} ", name);
        }
        let _ = write!(&mut create, "{}", arg);
    }
    let _ = write!(
        &mut create,
        ") (\n    \
            stype = internal,\n    \
            sfunc = {}{},\n    \
            finalfunc = {}{}",
        schema_qualifier, transition_fn.outer_ident(&name),
        schema_qualifier, final_fn.outer_ident(&name),
    );

    let parallel_safe = parallel_safe.map(|p| {
        let value = p.value();
        let _ = write!(&mut create, ",\n    parallel = {}",
            if value {
                "safe"
            } else {
                "unsafe"
            });
        quote!(pub const PARALLEL_SAFE: bool = #value;)
    });

    let mut add_function =
        |f: AggregateFn,
        field: &str,
        make_tokens: fn(&AggregateFn, &Option<syn::Ident>, &syn::Ident) -> TokenStream2| {
            extension_sql_reqs.push(f.outer_ident(&name));
            let _ = write!(
                &mut create,
                ",\n    {} = {}{}",
                field,
                schema_qualifier,
                f.outer_ident(&name)
            );
            make_tokens(&f, &schema, &name)
    };
    let serialize_fns = serialize_fn.map(|f| add_function(f, "serialfunc", AggregateFn::serialize_fn_tokens));
    let deserialize_fns = deserialize_fn.map(|f| add_function(f, "deserialfunc", AggregateFn::deserialize_fn_tokens));
    let combine_fns = combine_fn.map(|f| add_function(f, "combinefunc", AggregateFn::combine_fn_tokens));

    let _ = write!(&mut create, "\n);\n");

    let extension_sql_name = format!("{}_extension_sql", name);

    quote! {
        pub mod #name {
            use super::*;

            pub type State = #state_ty;

            #parallel_safe

            #transition_fns

            #final_fns
            #serialize_fns
            #deserialize_fns
            #combine_fns

            pgx::extension_sql!(
                #create,
                name=#extension_sql_name,
                requires=[#(#extension_sql_reqs),*],
            );
        }
    }
}

impl AggregateFn {
    fn transition_fn_tokens(&self, schema: &Option<syn::Ident>, aggregate_name: &syn::Ident) -> TokenStream2 {
        let outer_ident = self.outer_ident(aggregate_name);
        let Self {
            ident,
            args,
            body,
            ret,
            ..
        } = self;

        let schema = schema.as_ref().map(|s| {
            let s = format!("{}", s);
            quote!(, schema = #s)
        });

        let state_type_check = state_type_check_tokens(&*args[0].rust.ty, Some(()));

        let arg_signatures = args.iter()
            .skip(1)
            .map(|arg| &arg.rust);
        let arg_vals: Punctuated<syn::Pat, Comma> = args.iter()
            .skip(1)
            .map(arg_ident)
            .collect();

        let inner_arg_signatures = args.iter().map(|arg| &arg.rust);

        let return_type_check = state_type_check_tokens(&*ret_type(ret), Some(()));

        quote! {

            #state_type_check

            #return_type_check

            #[pgx::pg_extern(immutable, parallel_safe #schema)]
            pub fn #outer_ident(
                __internal: pgx::Internal,
                #(#arg_signatures,)*
                __fcinfo: pg_sys::FunctionCallInfo
            ) -> Internal {
                use crate::palloc::{InternalAsValue, ToInternal};
                unsafe {
                    crate::aggregate_utils::in_aggregate_context(__fcinfo, ||
                        #ident(__internal.to_inner(), #arg_vals).internal()
                    )
                }
            }

            pub fn #ident(#(#inner_arg_signatures),*) #ret
                #body
        }
    }

    fn final_fn_tokens(&self, schema: &Option<syn::Ident>, aggregate_name: &syn::Ident) -> TokenStream2 {
        let outer_ident = self.outer_ident(aggregate_name);
        let Self {
            ident,
            args,
            ret,
            body,
            ..
        } = self;

        let schema = schema.as_ref().map(|s| {
            let s = format!("{}", s);
            quote!(, schema = #s)
        });

        let state_type_check = state_type_check_tokens(&*args[0].rust.ty, Some(()));
        let arg_vals: Punctuated<syn::Pat, Comma> = args.iter()
            .skip(1)
            .map(arg_ident)
            .collect();

        let inner_arg_signatures = args.iter().map(|arg| &arg.rust);

        quote! {
            #state_type_check

            #[pgx::pg_extern(immutable, parallel_safe #schema)]
            pub fn #outer_ident(
                __internal: pgx::Internal,
                __fcinfo: pg_sys::FunctionCallInfo
            ) #ret {
                use crate::palloc::{InternalAsValue, ToInternal};
                unsafe {
                    #ident(__internal.to_inner(), #arg_vals)
                }
            }

            pub fn #ident(#(#inner_arg_signatures,)*) #ret
                #body
        }
    }

    fn serialize_fn_tokens(&self, schema: &Option<syn::Ident>, aggregate_name: &syn::Ident) -> TokenStream2 {
        let outer_ident = self.outer_ident(aggregate_name);
        let Self {
            ident,
            args,
            ret,
            body,
            ..
        } = self;

        let schema = schema.as_ref().map(|s| {
            let s = format!("{}", s);
            quote!(, schema = #s)
        });

        let state_type_check = state_type_check_tokens(&*args[0].rust.ty, None);

        let return_type_check = bytea_type_check_tokens(&*ret_type(ret));

        let inner_arg_signatures = args.iter().map(|arg| &arg.rust);

        quote! {
            #state_type_check

            #return_type_check

            #[pgx::pg_extern(strict, immutable, parallel_safe #schema)]
            pub fn #outer_ident(
                __internal: pgx::Internal,
            ) -> bytea {
                use crate::palloc::InternalAsValue;
                unsafe {
                    #ident(__internal.to_inner().unwrap())
                }
            }

            pub fn #ident(#(#inner_arg_signatures,)*)
            -> bytea
                #body
        }
    }

    fn deserialize_fn_tokens(&self, schema: &Option<syn::Ident>, aggregate_name: &syn::Ident) -> TokenStream2 {
        let outer_ident = self.outer_ident(aggregate_name);
        let Self {
            ident,
            args,
            ret,
            body,
            ..
        } = self;

        let schema = schema.as_ref().map(|s| {
            let s = format!("{}", s);
            quote!(, schema = #s)
        });

        let state_name = arg_ident(&args[0]);

        let state_type_check = bytea_type_check_tokens(&*args[0].rust.ty);

        let return_type_check = state_type_check_tokens(&*ret_type(ret), None);

        // int8_avg_deserialize allocates in CurrentMemoryContext, so we do the same
        // https://github.com/postgres/postgres/blob/f920f7e799c587228227ec94356c760e3f3d5f2b/src/backend/utils/adt/numeric.c#L5728-L5770
        quote! {
            #state_type_check

            #return_type_check

            #[pgx::pg_extern(strict, immutable, parallel_safe #schema)]
            pub fn #outer_ident(
                bytes: crate::raw::bytea,
                _internal: Internal
            ) -> Internal {
                use crate::palloc::ToInternal;
                unsafe {
                    #ident(bytes).internal()
                }
            }

            pub fn #ident(#state_name: crate::raw::bytea) #ret
                #body
        }
    }

    fn combine_fn_tokens(&self, schema: &Option<syn::Ident>, aggregate_name: &syn::Ident) -> TokenStream2 {
        let outer_ident = self.outer_ident(aggregate_name);
        let Self {
            ident,
            args,
            ret,
            body,
            ..
        } = self;

        let schema = schema.as_ref().map(|s| {
            let s = format!("{}", s);
            quote!(, schema = #s)
        });

        let a_name = arg_ident(&args[0]);
        let b_name = arg_ident(&args[1]);

        let state_type_check_a = state_type_check_tokens(&*args[0].rust.ty, Some(()));
        let state_type_check_b = state_type_check_tokens(&*args[1].rust.ty, Some(()));

        let return_type_check = state_type_check_tokens(&*ret_type(ret), Some(()));

        let inner_arg_signatures = args.iter().map(|arg| &arg.rust);

        quote! {
            #state_type_check_a
            #state_type_check_b
            #return_type_check

            #[pgx::pg_extern(immutable, parallel_safe #schema)]
            pub fn #outer_ident(
                #a_name: Internal,
                #b_name: Internal,
                __fcinfo: pg_sys::FunctionCallInfo
            ) -> Internal {
                use crate::palloc::{InternalAsValue, ToInternal};
                unsafe {
                    crate::aggregate_utils::in_aggregate_context(__fcinfo, ||
                        #ident(#a_name.to_inner(), #b_name.to_inner()).internal()
                    )
                }
            }

            pub fn #ident(#(#inner_arg_signatures,)*) #ret
                #body
        }
    }

    fn outer_ident(&self, aggregate_name: &syn::Ident) -> syn::Ident {
        let name = match &self.sql_name {
            Some(name) => name.value(),
            None => format!("{}_{}_fn_outer", aggregate_name, self.ident),
        };
        syn::Ident::new(&name, Span::call_site())
    }

    fn sql_args(&self) -> impl Iterator<Item=(Option<&syn::Ident>, String)> {
        self.args.iter().skip(1).map(|arg| {
            let ident = match &*arg.rust.pat {
                syn::Pat::Ident(id) => Some(&id.ident),
                _ => None,
            };
            (ident, arg.sql.as_ref().expect("missing sql arg").value())
        })
    }
}

fn arg_ident(arg: &AggregateArg) -> syn::Pat {
    syn::Pat::clone(&*arg.rust.pat)
}

fn ret_type(ret: &syn::ReturnType) -> Cow<'_, syn::Type> {
    match ret {
        syn::ReturnType::Default => Cow::Owned(parse_quote!(())),
        syn::ReturnType::Type(_, ty) => Cow::Borrowed(ty),
    }
}

fn state_type_check_tokens(ty: &syn::Type, optional: Option<()>) -> TokenStream2 {
    match optional {
        Some(..) => {
            type_check_tokens(
                ty,
                parse_quote!(Option<crate::palloc::Inner<State>>),
                parse_quote!(None)
            )
        },
        None => {
            type_check_tokens(
                ty,
                parse_quote!(crate::palloc::Inner<State>),
                parse_quote!(crate::palloc::Inner(std::ptr::NonNull::dangling()))
            )
        },
    }
}

fn bytea_type_check_tokens(ty: &syn::Type) -> TokenStream2 {
    type_check_tokens(ty, parse_quote!(bytea), parse_quote!(crate::raw::bytea(0)))
}

fn type_check_tokens(
    user_ty: &syn::Type,
    expected_type: syn::Type,
    initializer: syn::Expr,
) -> TokenStream2 {
    quote_spanned!{user_ty.span()=>
        const _: () = {
            let _user: #user_ty = #initializer;
            let _expected: #expected_type = _user;
        };
    }
}
