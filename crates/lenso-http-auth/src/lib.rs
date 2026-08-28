//! Auth-aware actor extraction for authored HTTP Endpoint providers.
//!
//! This crate owns ingress authentication glue only. Extracted actors identify
//! evidence accepted by the bound Auth Plugin; target Plugins must still verify
//! the attached assertion and make the final business authorization decision.

use std::rc::Rc;

use lenso_auth_sdk::{
    ActorAssertion, AuthOutcome, CredentialEvidence, authenticate_request, decode_auth_response,
};
use lenso_capability_auth::{AuthClient, AuthInvocationError, AuthenticateError};
use lenso_capability_http_endpoint::{
    EndpointHandleInvocationError, ExtractorFuture, HandleRequest, HandleResponse,
    response::{self, HeaderValue, StatusCode, header},
};
use lenso_kernel::{InvocationContext, RuntimeFailure};

/// Supplies the activation-time Auth client used during request extraction.
pub trait AuthClientSource {
    /// Returns the client bound to this HTTP Endpoint provider.
    fn auth_client(&self) -> Result<Rc<AuthClient>, EndpointHandleInvocationError>;
}

/// An application-owned actor projection at the HTTP authentication boundary.
///
/// `KIND` distinguishes credential actor kinds such as `user` or `admin`.
/// Roles, permissions, tenant access, and resource ownership remain target-owned
/// authorization concerns and must not be decided by this projection.
/// This is deliberately separate from `lenso_auth_sdk::TypedActor`, which is
/// projected only after the target verifies assertion provenance, audience,
/// validity, and proof.
pub trait AuthenticatedHttpActor: Sized {
    /// Auth assertion actor kind accepted by this extractor.
    const KIND: &'static str;

    /// Builds the application's ingress actor after the kind has matched.
    fn from_assertion(assertion: &ActorAssertion) -> Self;
}

/// Authenticates request evidence, attaches the assertion, and projects the
/// HTTP-edge identity `A`.
///
/// An application implements [`AuthClientSource`] for its Endpoint provider and
/// delegates its `FromRequest` implementation to this function. Authentication
/// failures become intentional HTTP responses; runtime failures keep their
/// generated Endpoint invocation semantics.
pub fn extract_authenticated_actor<'a, P, A>(
    provider: &'a P,
    context: &'a mut InvocationContext,
    request: &'a HandleRequest,
) -> ExtractorFuture<'a, A>
where
    P: AuthClientSource + ?Sized,
    A: AuthenticatedHttpActor + 'a,
{
    Box::pin(async move {
        let auth = provider.auth_client()?;
        let evidence = request.credential.as_ref().map(|credential| {
            CredentialEvidence::new(credential.scheme.clone(), credential.value.clone())
        });
        let response = match auth
            .authenticate_with_context(context.clone(), authenticate_request(evidence))
            .await
        {
            Ok(response) => response,
            Err(AuthInvocationError::Domain(error)) => {
                return Err(authentication_rejection(&error)?.into());
            }
            Err(AuthInvocationError::Runtime(error)) => {
                return Err(EndpointHandleInvocationError::Runtime(error).into());
            }
        };
        let assertion = match decode_auth_response(response).map_err(internal)? {
            AuthOutcome::Absent => {
                return Err(unauthorized(
                    "authentication_required",
                    "Authentication credentials are required.",
                )?
                .into());
            }
            AuthOutcome::Authenticated(assertion) => assertion,
        };
        if assertion.actor_kind() != A::KIND {
            return Err(response::problem(
                StatusCode::FORBIDDEN,
                "unexpected_actor_kind",
                "The authenticated actor cannot access this endpoint.",
            )
            .into());
        }
        *context = assertion.attach(context.clone()).map_err(internal)?;
        Ok(A::from_assertion(&assertion))
    })
}

fn authentication_rejection(
    error: &AuthenticateError,
) -> Result<HandleResponse, EndpointHandleInvocationError> {
    let (code, detail) = match error {
        AuthenticateError::Expired => (
            "expired_credential",
            "The supplied authentication credential has expired.",
        ),
        AuthenticateError::Invalid => (
            "invalid_credential",
            "The supplied authentication credential is invalid.",
        ),
        AuthenticateError::Revoked => (
            "revoked_credential",
            "The supplied authentication credential has been revoked.",
        ),
        AuthenticateError::Unsupported => (
            "unsupported_credential",
            "The supplied authentication credential is not supported.",
        ),
        AuthenticateError::Unknown(_) => (
            "authentication_failed",
            "The supplied authentication credential was not accepted.",
        ),
    };
    unauthorized(code, detail)
}

fn unauthorized(
    code: &'static str,
    detail: &'static str,
) -> Result<HandleResponse, EndpointHandleInvocationError> {
    Ok(
        response::problem(StatusCode::UNAUTHORIZED, code, detail).with_header(
            &header::WWW_AUTHENTICATE,
            &HeaderValue::from_static("Bearer"),
        )?,
    )
}

fn internal(error: impl std::fmt::Debug) -> EndpointHandleInvocationError {
    EndpointHandleInvocationError::Runtime(RuntimeFailure::Internal {
        detail: format!("{error:?}"),
    })
}
