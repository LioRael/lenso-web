//! Attribute authoring for statically routed Lenso HTTP Endpoint providers.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Error, FnArg, Ident, ImplItem, ItemImpl, LitStr, Result, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
};

/// Generates one HTTP Endpoint provider from handler route attributes.
///
/// Supported handler attributes are `get`, `post`, `put`, `patch`, `delete`,
/// `head`, and `options`. Each accepts a stable route ID and path. One or more
/// `middleware` attributes may name async provider methods that run before the
/// handler and its typed request extractors.
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
        let metadata = take_handler_metadata(&mut method.attrs)?;
        if let Some(route) = metadata.route {
            routes.push(Handler {
                route,
                middlewares: metadata.middlewares,
                method: method.sig.ident.clone(),
                arguments: handler_arguments(method)?,
            });
        } else if !metadata.middlewares.is_empty() {
            return Err(Error::new_spanned(
                &method.sig.ident,
                "endpoint middleware can only be attached to an HTTP handler",
            ));
        }
    }
    if routes.is_empty() {
        return Err(Error::new_spanned(
            &implementation.self_ty,
            "endpoint impl must declare at least one HTTP handler attribute",
        ));
    }

    let const_routes = routes.iter().map(|handler| {
        let route_id = &handler.route.id;
        let method = &handler.route.method;
        let path = &handler.route.path;
        quote! {
            ::lenso_capability_http_endpoint::EndpointRoute::new(
                #route_id,
                #method,
                #path,
            ),
        }
    });
    let implementation_routes = routes.iter().map(|handler| {
        let route_id = &handler.route.id;
        let method = &handler.route.method;
        let path = &handler.route.path;
        quote! {
            ::lenso_capability_http_endpoint::EndpointRoute::new(
                #route_id,
                #method,
                #path,
            ),
        }
    });
    let dispatch_arms = routes.iter().map(dispatch_arm);

    Ok(quote! {
        #implementation

        const _: () = {
            const ROUTES: &[::lenso_capability_http_endpoint::EndpointRoute] = &[
                #(#const_routes)*
            ];
            ::lenso_capability_http_endpoint::__private::validate_endpoint_routes(ROUTES);
        };

        impl ::lenso_capability_http_endpoint::HttpEndpoint for #provider {
            const ROUTES: &'static [::lenso_capability_http_endpoint::EndpointRoute] = &[
                #(#implementation_routes)*
            ];

            fn dispatch(
                &self,
                context: ::lenso_capability_http_endpoint::__private::InvocationContext,
                request: ::lenso_capability_http_endpoint::HandleRequest,
            ) -> ::lenso_capability_http_endpoint::EndpointFuture {
                let provider = self.clone();
                Box::pin(async move {
                    let route_id = request.route_id.clone();
                    match route_id.as_str() {
                        #(#dispatch_arms,)*
                        _ => Ok(Err(::lenso_capability_http_endpoint::HandleError::Rejected)),
                    }
                })
            }
        }
    })
}

fn take_handler_metadata(attributes: &mut Vec<Attribute>) -> Result<HandlerMetadata> {
    let mut route = None;
    let mut middlewares = Vec::new();
    let mut retained = Vec::with_capacity(attributes.len());
    for attribute in attributes.drain(..) {
        if attribute.path().is_ident("middleware") {
            let arguments =
                attribute.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
            if arguments.is_empty() {
                return Err(Error::new_spanned(
                    attribute,
                    "middleware requires at least one provider method",
                ));
            }
            middlewares.extend(arguments);
            continue;
        }
        let Some(http_method) = http_method(&attribute) else {
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
            method: LitStr::new(http_method, attribute.path().span()),
            id: arguments.route_id,
            path: arguments.path,
        });
    }
    *attributes = retained;
    Ok(HandlerMetadata { route, middlewares })
}

fn handler_arguments(method: &syn::ImplItemFn) -> Result<Vec<HandlerArgument>> {
    let mut arguments = Vec::new();
    let mut context_count = 0;
    let mut request_count = 0;
    for (index, input) in method.sig.inputs.iter().enumerate() {
        let FnArg::Typed(argument) = input else {
            continue;
        };
        let ty = (*argument.ty).clone();
        let kind = match final_type_ident(&ty).map(Ident::to_string).as_deref() {
            Some("InvocationContext") => {
                context_count += 1;
                ArgumentKind::Context
            }
            Some("HandleRequest") => {
                request_count += 1;
                ArgumentKind::Request
            }
            _ => ArgumentKind::Extractor(Ident::new(
                &format!("__lenso_extracted_{index}"),
                argument.span(),
            )),
        };
        arguments.push(HandlerArgument { ty, kind });
    }
    if context_count > 1 || request_count > 1 {
        return Err(Error::new_spanned(
            &method.sig.inputs,
            "an endpoint handler accepts at most one InvocationContext and one HandleRequest",
        ));
    }
    Ok(arguments)
}

fn final_type_ident(ty: &Type) -> Option<&Ident> {
    let Type::Path(path) = ty else {
        return None;
    };
    path.path.segments.last().map(|segment| &segment.ident)
}

fn dispatch_arm(handler: &Handler) -> proc_macro2::TokenStream {
    let route_id = &handler.route.id;
    let method = &handler.method;
    let middleware_steps = handler.middlewares.iter().map(|middleware| {
        quote! {
            let (context, request) = match provider.#middleware(context, request).await {
                Ok(outcome) => match outcome.into_result() {
                    Ok(next) => next,
                    Err(response) => return Ok(Ok(response)),
                },
                Err(::lenso_capability_http_endpoint::EndpointHandleInvocationError::Domain(
                    error,
                )) => return Ok(Err(error)),
                Err(::lenso_capability_http_endpoint::EndpointHandleInvocationError::Runtime(
                    error,
                )) => return Err(error),
            };
        }
    });
    let extractor_steps = handler.arguments.iter().filter_map(|argument| {
        let ArgumentKind::Extractor(binding) = &argument.kind else {
            return None;
        };
        let ty = &argument.ty;
        Some(quote! {
            let #binding: #ty = match <#ty as
                ::lenso_capability_http_endpoint::__private::FromRequest<Self>
            >::from_request(&provider, &mut context, &request).await {
                Ok(value) => value,
                Err(::lenso_capability_http_endpoint::__private::ExtractorRejection::Response(
                    response,
                )) => return Ok(Ok(response)),
                Err(::lenso_capability_http_endpoint::__private::ExtractorRejection::Invocation(
                    ::lenso_capability_http_endpoint::EndpointHandleInvocationError::Domain(
                        error,
                    ),
                )) => return Ok(Err(error)),
                Err(::lenso_capability_http_endpoint::__private::ExtractorRejection::Invocation(
                    ::lenso_capability_http_endpoint::EndpointHandleInvocationError::Runtime(
                        error,
                    ),
                )) => return Err(error),
            };
        })
    });
    let mutable_context = handler
        .arguments
        .iter()
        .any(|argument| matches!(&argument.kind, ArgumentKind::Extractor(_)))
        .then(|| quote!(let mut context = context;));
    let arguments = handler
        .arguments
        .iter()
        .map(|argument| match &argument.kind {
            ArgumentKind::Context => quote!(context),
            ArgumentKind::Request => quote!(request),
            ArgumentKind::Extractor(binding) => quote!(#binding),
        });

    quote! {
        #route_id => {
            #(#middleware_steps)*
            #mutable_context
            #(#extractor_steps)*
            let _ = (&context, &request);
            match provider.#method(#(#arguments),*).await {
                Ok(response) => Ok(Ok(response)),
                Err(::lenso_capability_http_endpoint::EndpointHandleInvocationError::Domain(
                    error,
                )) => Ok(Err(error)),
                Err(::lenso_capability_http_endpoint::EndpointHandleInvocationError::Runtime(
                    error,
                )) => Err(error),
            }
        }
    }
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

struct HandlerMetadata {
    route: Option<Route>,
    middlewares: Vec<Ident>,
}

struct Handler {
    route: Route,
    middlewares: Vec<Ident>,
    method: Ident,
    arguments: Vec<HandlerArgument>,
}

struct HandlerArgument {
    ty: Type,
    kind: ArgumentKind,
}

enum ArgumentKind {
    Context,
    Request,
    Extractor(Ident),
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

        assert!(expanded.contains("HttpEndpoint"));
        assert!(expanded.contains("orders.read"));
        assert!(expanded.contains("GET"));
        assert!(expanded.contains("/orders/{order_id}"));
        assert!(!expanded.contains("# [get"));
    }

    #[test]
    fn expands_middleware_and_typed_extractors_before_the_handler() {
        let expanded = expand_endpoint(parse_quote! {
            impl OrdersHttp {
                #[middleware(authenticate)]
                #[get("orders.read", "/orders/{order_id}")]
                async fn read(
                    &self,
                    context: InvocationContext,
                    Path(path): Path<OrderPath>,
                ) {}
            }
        })
        .unwrap()
        .to_string();

        assert!(expanded.contains("provider . authenticate"));
        assert!(expanded.contains("into_result"));
        assert!(expanded.contains("FromRequest"));
        assert!(expanded.contains("provider . read (context , __lenso_extracted_2)"));
        assert!(!expanded.contains("# [middleware"));
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
