//! Attribute authoring for statically routed Lenso HTTP Endpoint providers.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Error, ImplItem, ItemImpl, LitStr, Result, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
};

/// Generates one HTTP Endpoint provider from handler route attributes.
///
/// Supported handler attributes are `get`, `post`, `put`, `patch`, `delete`,
/// `head`, and `options`. Each accepts a stable route ID and path.
#[proc_macro_attribute]
pub fn endpoint(arguments: TokenStream, input: TokenStream) -> TokenStream {
    if !arguments.is_empty() {
        return Error::new(
            proc_macro2::Span::call_site(),
            "endpoint does not accept arguments",
        )
        .into_compile_error()
        .into();
    }

    let implementation = parse_macro_input!(input as ItemImpl);
    expand_endpoint(implementation)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_endpoint(mut implementation: ItemImpl) -> Result<proc_macro2::TokenStream> {
    if implementation.trait_.is_some() {
        return Err(Error::new_spanned(
            implementation.impl_token,
            "endpoint can only be applied to an inherent impl block",
        ));
    }
    if !implementation.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &implementation.generics,
            "endpoint does not support generic impl blocks",
        ));
    }

    let provider = implementation.self_ty.clone();
    let mut routes = Vec::new();
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let route = take_route(&mut method.attrs)?;
        if let Some(route) = route {
            routes.push((route, method.sig.ident.clone()));
        }
    }
    if routes.is_empty() {
        return Err(Error::new_spanned(
            &implementation.self_ty,
            "endpoint impl must declare at least one HTTP handler attribute",
        ));
    }

    let route_ids = routes.iter().map(|(route, _)| &route.id);
    let methods = routes.iter().map(|(route, _)| &route.method);
    let paths = routes.iter().map(|(route, _)| &route.path);
    let handlers = routes.iter().map(|(_, handler)| handler);

    Ok(quote! {
        #implementation

        ::lenso_capability_http_endpoint::http_endpoint! {
            impl #provider {
                #(
                    #route_ids => (#methods, #paths) => #handlers,
                )*
            }
        }
    })
}

fn take_route(attributes: &mut Vec<Attribute>) -> Result<Option<Route>> {
    let mut route = None;
    let mut retained = Vec::with_capacity(attributes.len());
    for attribute in attributes.drain(..) {
        let Some(method) = http_method(&attribute) else {
            retained.push(attribute);
            continue;
        };
        if route.is_some() {
            return Err(Error::new_spanned(
                attribute,
                "an endpoint handler may declare only one HTTP method",
            ));
        }
        let arguments = attribute.parse_args::<RouteArguments>()?;
        route = Some(Route {
            method: LitStr::new(method, attribute.path().span()),
            id: arguments.route_id,
            path: arguments.path,
        });
    }
    *attributes = retained;
    Ok(route)
}

fn http_method(attribute: &Attribute) -> Option<&'static str> {
    let path = attribute.path();
    [
        ("get", "GET"),
        ("post", "POST"),
        ("put", "PUT"),
        ("patch", "PATCH"),
        ("delete", "DELETE"),
        ("head", "HEAD"),
        ("options", "OPTIONS"),
    ]
    .into_iter()
    .find_map(|(attribute, method)| path.is_ident(attribute).then_some(method))
}

struct RouteArguments {
    route_id: LitStr,
    path: LitStr,
}

impl Parse for RouteArguments {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let arguments = Punctuated::<LitStr, Token![,]>::parse_terminated(input)?;
        if arguments.len() != 2 {
            return Err(Error::new(
                input.span(),
                "HTTP handler attributes require a route ID and path",
            ));
        }
        let mut arguments = arguments.into_iter();
        Ok(Self {
            route_id: arguments.next().expect("length was checked"),
            path: arguments.next().expect("length was checked"),
        })
    }
}

struct Route {
    method: LitStr,
    id: LitStr,
    path: LitStr,
}

#[cfg(test)]
mod tests {
    use super::expand_endpoint;
    use syn::parse_quote;

    #[test]
    fn expands_handler_attributes_into_the_static_route_table() {
        let expanded = expand_endpoint(parse_quote! {
            impl OrdersHttp {
                #[get("orders.read", "/orders/{order_id}")]
                async fn read(&self) {}
            }
        })
        .unwrap()
        .to_string();

        assert!(expanded.contains("http_endpoint"));
        assert!(expanded.contains("orders.read"));
        assert!(expanded.contains("GET"));
        assert!(expanded.contains("/orders/{order_id}"));
        assert!(!expanded.contains("# [get"));
    }

    #[test]
    fn rejects_handlers_with_multiple_http_methods() {
        let error = expand_endpoint(parse_quote! {
            impl OrdersHttp {
                #[get("orders.read", "/orders/{order_id}")]
                #[post("orders.read", "/orders/{order_id}")]
                async fn read(&self) {}
            }
        })
        .unwrap_err();

        assert!(error.to_string().contains("only one HTTP method"));
    }

    #[test]
    fn rejects_impls_without_handlers() {
        let error = expand_endpoint(parse_quote! {
            impl OrdersHttp {
                fn helper(&self) {}
            }
        })
        .unwrap_err();

        assert!(error.to_string().contains("at least one HTTP handler"));
    }
}
