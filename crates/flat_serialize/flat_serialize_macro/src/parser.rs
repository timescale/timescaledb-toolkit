
use std::{collections::HashSet, ops::Deref};

use proc_macro2::TokenStream as TokenStream2;

use syn::{Attribute, Expr, Field, Ident, Result, Token, Type, braced, parse::{Parse, ParseStream}, spanned::Spanned, token, visit::Visit};

use crate::{
    VariableLenFieldInfo, FlatSerialize, FlatSerializeEnum, FlatSerializeField,
    FlatSerializeStruct, FlatSerializeVariant, PerFieldsAttr,
};

use quote::{quote, quote_spanned};

const LIBRARY_MARKER: &str = "flat_serialize";

fn flat_serialize_attr_path(att_name: &str) -> syn::Path {
    let crate_name = quote::format_ident!("{}", LIBRARY_MARKER);
    let att_name = quote::format_ident!("{}", att_name);
    syn::parse_quote! { #crate_name :: #att_name }
}

impl Parse for FlatSerialize {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let field_attr_path = flat_serialize_attr_path("field_attr");
        let (per_field_attrs, attrs): (Vec<_>, _) = attrs
            .into_iter()
            .partition(|attr| attr.path == field_attr_path);
        let per_field_attrs: Result<_> = per_field_attrs
            .into_iter()
            .map(|a| a.parse_args_with(PerFieldsAttr::parse))
            .collect();
        let per_field_attrs = per_field_attrs?;
        let lookahead = input.lookahead1();
        //TODO Visibility
        if lookahead.peek(Token![struct]) {
            input.parse().map(|mut s: FlatSerializeStruct| {
                s.per_field_attrs = per_field_attrs;
                s.attrs = attrs;
                FlatSerialize::Struct(s)
            })
        } else if lookahead.peek(Token![enum]) {
            input.parse().map(|mut e: FlatSerializeEnum| {
                e.per_field_attrs = per_field_attrs;
                e.attrs = attrs;
                FlatSerialize::Enum(e)
            })
        } else {
            Err(lookahead.error())
        }
    }
}

impl Parse for FlatSerializeStruct {
    fn parse(input: ParseStream) -> Result<Self> {
        let content;
        let _struct_token: Token![struct] = input.parse()?;
        let ident = input.parse()?;
        let mut lifetime = None;
        if input.peek(Token![<]) {
            let _: Token![<] = input.parse()?;
            lifetime = Some(input.parse()?);
            let _: Token![>] = input.parse()?;
        }
        let _brace_token: token::Brace = braced!(content in input);
        let mut fields = content.parse_terminated(FlatSerializeField::parse)?;
        validate_self_fields(fields.iter_mut());
        Ok(Self {
            per_field_attrs: vec![],
            attrs: vec![],
            ident,
            lifetime,
            fields,
        })
    }
}

impl Parse for FlatSerializeEnum {
    fn parse(input: ParseStream) -> Result<Self> {
        let content;
        let _enum_token: Token![enum] = input.parse()?;
        let ident = input.parse()?;
        let mut lifetime = None;
        if input.peek(Token![<]) {
            let _: Token![<] = input.parse()?;
            lifetime = Some(input.parse()?);
            let _: Token![>] = input.parse()?;
        }
        let _brace_token: token::Brace = braced!(content in input);
        let tag = Field::parse_named(&content)?;
        let _comma_token: Token![,] = content.parse()?;
        let variants = content.parse_terminated(FlatSerializeVariant::parse)?;
        Ok(Self {
            per_field_attrs: vec![],
            attrs: vec![],
            ident,
            lifetime,
            tag: FlatSerializeField {
                field: tag,
                // TODO can we allow these?
                ty_without_lifetime: None,
                length_info: None,
            },
            variants,
        })
    }
}

impl Parse for FlatSerializeVariant {
    fn parse(input: ParseStream) -> Result<Self> {
        let content;
        let ident = input.parse()?;
        let _colon_token: Token![:] = input.parse()?;
        let tag_val = input.parse()?;
        let _brace_token: token::Brace = braced!(content in input);
        let mut fields = content.parse_terminated(FlatSerializeField::parse)?;
        validate_self_fields(fields.iter_mut());
        Ok(Self {
            tag_val,
            body: FlatSerializeStruct {
                per_field_attrs: vec![],
                attrs: vec![],
                ident,
                lifetime: None,
                fields,
            },
        })
    }
}

impl Parse for FlatSerializeField {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut field = Field::parse_named(input)?;
        // TODO switch to `drain_filter()` once stable
        let path = flat_serialize_attr_path("flatten");
        let mut use_trait = false;
        field.attrs = field
            .attrs
            .into_iter()
            .filter(|attr| {
                let is_flatten = &attr.path == &path;
                if is_flatten {
                    use_trait = true;
                    return false;
                }
                true
            })
            .collect();
        let mut length_info = None;
        if input.peek(Token![if]) {
            let _: Token![if] = input.parse()?;
            let expr = input.parse()?;
            length_info = Some(VariableLenFieldInfo {
                ty: field.ty.clone(),
                ty_without_lifetime: None,
                len_expr: expr,
                is_optional: true,
            });
        } else if let syn::Type::Array(array) = &field.ty {
            let has_self = has_self_field(&array.len);
            if has_self {
                // let self_fields_are_valid = validate_self_field(&array.len, &seen_fields);
                length_info = Some(VariableLenFieldInfo {
                    ty: (*array.elem).clone(),
                    ty_without_lifetime: None,
                    len_expr: array.len.clone(),
                    is_optional: false,
                });
            }
        }

        let mut ty_without_lifetime = None;
        if has_lifetime(&field.ty) {
            match &mut length_info {
                None => ty_without_lifetime = Some(as_turbofish(&field.ty)),
                Some(info) => {
                    info.ty_without_lifetime = Some(as_turbofish(&info.ty));
                }
            }
        }
        Ok(Self {
            field,
            ty_without_lifetime,
            length_info,
        })
    }
}

// TODO should we leave this in?
impl Deref for FlatSerializeField {
    type Target = Field;

    fn deref(&self) -> &Self::Target {
        &self.field
    }
}

impl Parse for PerFieldsAttr {
    fn parse(input: ParseStream) -> Result<Self> {
        let fixed: syn::MetaNameValue = input.parse()?;
        let mut variable: Option<syn::MetaNameValue> = None;
        if !input.is_empty() {
            let _comma_token: Token![,] = input.parse()?;
            if !input.is_empty() {
                variable = Some(input.parse()?)
            }
            if !input.is_empty() {
                let _comma_token: Token![,] = input.parse()?;
            }
        }

        if !fixed.path.is_ident("fixed") {
            return Err(syn::Error::new(fixed.path.span(), "expected `fixed`"));
        }
        if !variable
            .as_ref()
            .map(|v| v.path.is_ident("variable"))
            .unwrap_or(true)
        {
            return Err(syn::Error::new(
                variable.unwrap().path.span(),
                "expected `variable`",
            ));
        }
        let fixed = match &fixed.lit {
            syn::Lit::Str(fixed) => {
                let mut fixed_attrs = fixed.parse_with(Attribute::parse_outer)?;
                if fixed_attrs.len() != 1 {
                    return Err(syn::Error::new(
                        fixed.span(),
                        "must contain exactly one attribute",
                    ));
                }
                fixed_attrs.pop().unwrap()
            }

            _ => {
                return Err(syn::Error::new(
                    fixed.lit.span(),
                    "must contain exactly one attribute",
                ))
            }
        };

        let variable = match variable {
            None => None,
            Some(variable) => match &variable.lit {
                syn::Lit::Str(variable) => {
                    let mut variable_attrs = variable.parse_with(Attribute::parse_outer)?;
                    if variable_attrs.len() != 1 {
                        return Err(syn::Error::new(
                            variable.span(),
                            "must contain exactly one attribute",
                        ));
                    }
                    Some(variable_attrs.pop().unwrap())
                }

                _ => {
                    return Err(syn::Error::new(
                        variable.lit.span(),
                        "must contain exactly one attribute",
                    ))
                }
            },
        };

        Ok(Self { fixed, variable })
    }
}

fn has_self_field(expr: &Expr) -> bool {
    let mut has_self = FindSelf(false);
    has_self.visit_expr(&expr);
    has_self.0
}

struct FindSelf(bool);

impl<'ast> Visit<'ast> for FindSelf {
    fn visit_path_segment(&mut self, i: &'ast syn::PathSegment) {
        if self.0 {
            return;
        }
        self.0 |= i.ident == "self"
    }
}

/// validate that all references to a field in the struct (e.g. `len` in
/// `[u8; self.len + 1]`) contained in expression refers to already defined
/// fields. Otherwise output a "attempting to use field before definition"
/// compile error. This is used to ensure that wse don't generate structs that
/// are impossible to deserialize because fields are in ambiguous positions such
/// as
/// ```skip
/// struct {
///     variable: [u8; self.len],
///     len: u32,
/// }
/// ```
/// where the position of `len` depends on the value of `len`.
fn validate_self_fields<'a>(fields: impl Iterator<Item = &'a mut FlatSerializeField>) {
    let mut seen_fields = HashSet::new();

    for f in fields {
        if let Some(length_info) = &mut f.length_info {
            if let Err(error) = validate_self_field(&length_info.len_expr, &seen_fields) {
                length_info.len_expr = syn::parse2(error).unwrap()
            }
        }
        seen_fields.insert(f.ident.as_ref().unwrap());
    }
}

fn validate_self_field<'a>(
    expr: &Expr,
    seen_fields: &HashSet<&'a Ident>,
) -> std::result::Result<(), TokenStream2> {
    let mut validate_fields = ValidateLenFields(None, &seen_fields);
    validate_fields.visit_expr(&expr);
    match validate_fields.0 {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

struct ValidateLenFields<'a, 'b>(Option<TokenStream2>, &'b HashSet<&'a Ident>);

impl<'a, 'b, 'ast> Visit<'ast> for ValidateLenFields<'a, 'b> {
    fn visit_expr(&mut self, expr: &'ast syn::Expr) {
        if self.0.is_some() {
            return;
        }
        match expr {
            syn::Expr::Field(field) => {
                if let syn::Expr::Path(path) = &*field.base {
                    if path.path.segments[0].ident == "self" {
                        let name = match &field.member {
                            syn::Member::Named(name) => name.clone(),
                            syn::Member::Unnamed(_) => panic!("unnamed fields not supported"),
                        };
                        if !self.1.contains(&name) {
                            self.0 = Some(quote_spanned! {name.span()=>
                                compile_error!("attempting to use field before definition")
                            })
                        }
                    }
                }
            }
            _ => syn::visit::visit_expr(self, expr),
        }
    }
}


pub fn as_turbofish(ty: &Type) -> TokenStream2 {
    let path = match &ty {
        Type::Path(path) => path,
        _ => return quote_spanned! {ty.span()=>
            compile_error!("can only flatten path-based types")
        },
    };
    if path.qself.is_some() {
        return quote_spanned! {ty.span()=>
            compile_error!("cannot use `<Foo as Bar>` in flatten")
        }
    }
    let path = &path.path;
    let leading_colon = &path.leading_colon;
    let mut output = quote!{};
    let mut error = None;
    for segment in &path.segments {
        match &segment.arguments {
            syn::PathArguments::Parenthesized(args) => {
                error = Some(quote_spanned! {args.span()=>
                    compile_error!("cannot use `()` in flatten")
                });
            },
            syn::PathArguments::None => {
                if output.is_empty() {
                    output = quote!{ #leading_colon #segment };
                } else {
                    output = quote!{ #output::#segment};
                }
            },
            syn::PathArguments::AngleBracketed(_) => {
                let ident = &segment.ident;
                if output.is_empty() {
                    // TODO leave in args?
                    // output = quote!{ #leading_colon #ident::#args };
                    output = quote!{ #leading_colon #ident };
                } else {
                    // TODO leave in args?
                    // output = quote!{ #output::#ident::#args };
                    output = quote!{ #output::#ident };
                }
            },
        }
    }
    if let Some(error) = error {
        return error
    }

    output
}

pub fn has_lifetime(ty: &Type) -> bool {
    struct Visitor(bool);
    impl<'ast> Visit<'ast> for Visitor {
        fn visit_lifetime(&mut self, _: &'ast syn::Lifetime) {
            self.0 = true
        }
    }
    let mut visit = Visitor(false);
    syn::visit::visit_type(&mut visit, ty);
    visit.0
}