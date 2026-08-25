use std::{
    cell::RefCell, collections::BTreeMap, fmt::Write as _, net::SocketAddr, rc::Rc, time::Duration,
};

use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    ModuleInstancePlan,
};
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionIssuer, ActorProjectionError, FixedClock, TypedActor, Validity,
    audience, authenticated_response,
};
use lenso_capability_auth::{
    AUTHENTICATE_OPERATION, Auth, AuthClient, AuthEndpoint, AuthProvider,
    CAPABILITY_ID as AUTH_CAPABILITY_ID, DESCRIPTOR_VERSION as AUTH_DESCRIPTOR_VERSION,
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID as HTTP_CAPABILITY_ID, DESCRIBE_OPERATION,
    DESCRIPTOR_VERSION as HTTP_DESCRIPTOR_VERSION, EndpointEndpoint, EndpointHandleInvocationError,
    ExtractorFuture, FromRequest, HANDLE_OPERATION, HandleRequest, HandleResponse, Path, endpoint,
    response::{self, StatusCode},
};
use lenso_http_auth::{AuthClientSource, AuthenticatedHttpActor, extract_authenticated_actor};
use lenso_kernel::{
    ActivateContext, DeactivateContext, InvocationContext, Kernel, ModuleDependencies,
    ModuleFuture, ModuleLifecycle, NativeRequestEndpoint, NativeRequestFuture, NativeRequestHandle,
    RequestCapability, RuntimeFailure, ShutdownOutcome,
};
use lenso_native_adapter::{
    NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance, NativeModuleRegistry,
};
use lenso_runner::TokioDriver;
use lenso_web_ingress::{PACKAGE_ID as INGRESS_PACKAGE_ID, WebIngressFactory};
use serde::{Deserialize, Serialize};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::LocalSet,
};

const AUTH_PACKAGE_ID: &str = "fixture.auth";
const ENDPOINT_PACKAGE_ID: &str = "fixture.authenticated-http";
const ORDERS_PACKAGE_ID: &str = "fixture.orders";
const PACKAGE_VERSION: &str = "0.0.0";
const ORDERS_CAPABILITY_ID: &str = "example.orders@1";
const ORDERS_DESCRIPTOR_VERSION: &str = "1.0.0";
const READ_ORDER_OPERATION: &str = "read";

#[tokio::test(flavor = "current_thread")]
async fn web_authenticates_evidence_and_target_authorizes_the_assertion() {
    LocalSet::new()
        .run_until(async {
            let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
            let issuer = ActorAssertionIssuer::new("auth.api-token", b"test-signing-key");
            let observed_actor = Rc::new(RefCell::new(None));
            let ingress = WebIngressFactory::default();
            let endpoint = AuthenticatedHttpFactory::default();
            let app = Kernel::start_native(
                plan(),
                TokioDriver::new(),
                NativeModuleRegistry::new()
                    .with_factory(ingress.clone())
                    .with_factory(TokenAuthFactory {
                        issuer: issuer.clone(),
                        now,
                    })
                    .with_factory(OrdersFactory {
                        verifier: issuer.verifier(),
                        now,
                        observed_actor: observed_actor.clone(),
                    })
                    .with_factory(endpoint.clone()),
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();

            let absent = request(address, &[]).await;
            assert_eq!(absent.status, 401);
            assert_eq!(
                absent.headers.get("content-type").map(String::as_str),
                Some("application/problem+json; charset=utf-8")
            );
            assert_eq!(
                absent.headers.get("www-authenticate").map(String::as_str),
                Some("Bearer")
            );

            let invalid = request(address, &[("Authorization", "Bearer bad-token")]).await;
            assert_eq!(invalid.status, 401);

            let unavailable = request(
                address,
                &[("Authorization", "Bearer auth-unavailable-token")],
            )
            .await;
            assert_eq!(unavailable.status, 503);

            let forbidden = request(
                address,
                &[("Authorization", "Bearer insufficient-scope-token")],
            )
            .await;
            assert_eq!(forbidden.status, 403);

            let wrong_actor_kind = request_path(
                address,
                "/admin/orders",
                &[("Authorization", "Bearer good-token")],
            )
            .await;
            assert_eq!(wrong_actor_kind.status, 403);

            let accepted = request(address, &[("Authorization", "Bearer good-token")]).await;
            assert_eq!(accepted.status, 200);
            assert_eq!(accepted.body, r#"{"id":"order-42","owner":"user-123"}"#);
            assert_eq!(observed_actor.borrow().as_deref(), Some("user-123"));

            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
            assert!(endpoint.dependencies.borrow().is_none());
        })
        .await;
}

fn plan() -> lenso_app_plan::ResolvedAppPlan {
    let ingress = ModuleInstancePlan::new("web-ingress", INGRESS_PACKAGE_ID).with_requirement(
        CapabilityRequirementPlan::many(HTTP_CAPABILITY_ID, HTTP_DESCRIPTOR_VERSION),
    );
    let endpoint = ModuleInstancePlan::new("orders-http", ENDPOINT_PACKAGE_ID)
        .with_capability(CapabilityEndpointPlan::new(
            HTTP_CAPABILITY_ID,
            HTTP_DESCRIPTOR_VERSION,
            [DESCRIBE_OPERATION, HANDLE_OPERATION],
        ))
        .with_requirement(CapabilityRequirementPlan::one(
            AUTH_CAPABILITY_ID,
            AUTH_DESCRIPTOR_VERSION,
        ))
        .with_requirement(CapabilityRequirementPlan::one(
            ORDERS_CAPABILITY_ID,
            ORDERS_DESCRIPTOR_VERSION,
        ));
    let auth = ModuleInstancePlan::new("auth", AUTH_PACKAGE_ID).with_capability(
        CapabilityEndpointPlan::new(
            AUTH_CAPABILITY_ID,
            AUTH_DESCRIPTOR_VERSION,
            [AUTHENTICATE_OPERATION],
        ),
    );
    let orders = ModuleInstancePlan::new("orders", ORDERS_PACKAGE_ID).with_capability(
        CapabilityEndpointPlan::new(
            ORDERS_CAPABILITY_ID,
            ORDERS_DESCRIPTOR_VERSION,
            [READ_ORDER_OPERATION],
        ),
    );
    AppComposition::new(
        vec![ingress, endpoint, auth, orders],
        vec![
            CapabilityBinding::new(
                "web-ingress",
                HTTP_CAPABILITY_ID,
                HTTP_DESCRIPTOR_VERSION,
                "orders-http",
            ),
            CapabilityBinding::new(
                "orders-http",
                AUTH_CAPABILITY_ID,
                AUTH_DESCRIPTOR_VERSION,
                "auth",
            ),
            CapabilityBinding::new(
                "orders-http",
                ORDERS_CAPABILITY_ID,
                ORDERS_DESCRIPTOR_VERSION,
                "orders",
            ),
        ],
    )
    .resolve()
    .unwrap()
}

#[derive(Clone, Debug)]
struct TokenAuthFactory {
    issuer: ActorAssertionIssuer,
    now: OffsetDateTime,
}

impl NativeModuleFactory for TokenAuthFactory {
    fn package_id(&self) -> &'static str {
        AUTH_PACKAGE_ID
    }
    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }
    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        Ok(NativeModuleInstance::new(vec![Rc::new(AuthEndpoint::new(
            TokenAuth {
                issuer: self.issuer.clone(),
                now: self.now,
            },
        ))]))
    }
}

#[derive(Debug)]
struct TokenAuth {
    issuer: ActorAssertionIssuer,
    now: OffsetDateTime,
}

impl AuthProvider for TokenAuth {
    fn authenticate(
        &self,
        _context: InvocationContext,
        request: lenso_capability_auth::AuthenticateRequest,
    ) -> NativeRequestFuture<Auth> {
        let outcome = match request.credential {
            None => Ok(Ok(lenso_auth_sdk::absent_response())),
            Some(credential)
                if credential.scheme == "bearer"
                    && matches!(
                        credential.value.as_str(),
                        "good-token" | "insufficient-scope-token"
                    ) =>
            {
                let operation = if credential.value == "good-token" {
                    READ_ORDER_OPERATION
                } else {
                    "list"
                };
                let assertion = self.issuer.issue(
                    "user-123",
                    "user",
                    "api-token",
                    [audience(ORDERS_CAPABILITY_ID, operation)],
                    Validity::new(
                        self.now - TimeDuration::seconds(1),
                        self.now + TimeDuration::minutes(1),
                    )
                    .unwrap(),
                    BTreeMap::new(),
                );
                Ok(Ok(authenticated_response(&assertion)))
            }
            Some(credential) if credential.value == "auth-unavailable-token" => {
                Err(RuntimeFailure::Unavailable {
                    capability: AUTH_CAPABILITY_ID,
                })
            }
            Some(_) => Ok(Err(lenso_capability_auth::AuthenticateError::Invalid)),
        };
        Box::pin(futures::future::ready(outcome))
    }
}

#[derive(Clone, Debug, Default)]
struct AuthenticatedHttpFactory {
    dependencies: Rc<RefCell<Option<EndpointDependencies>>>,
}

impl NativeModuleFactory for AuthenticatedHttpFactory {
    fn package_id(&self) -> &'static str {
        ENDPOINT_PACKAGE_ID
    }
    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }
    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        Ok(NativeModuleInstance::with_lifecycle(
            vec![Rc::new(EndpointEndpoint::new(AuthenticatedOrdersHttp {
                dependencies: self.dependencies.clone(),
            }))],
            EndpointLifecycle {
                dependencies: self.dependencies.clone(),
            },
        ))
    }
}

#[derive(Clone, Debug)]
struct EndpointDependencies {
    auth: Rc<AuthClient>,
    orders: Rc<OrdersClient>,
}

#[derive(Debug)]
struct EndpointLifecycle {
    dependencies: Rc<RefCell<Option<EndpointDependencies>>>,
}

impl ModuleLifecycle for EndpointLifecycle {
    fn activate(&self, context: ActivateContext) -> ModuleFuture {
        let result = (|| {
            self.dependencies
                .borrow_mut()
                .replace(EndpointDependencies {
                    auth: Rc::new(AuthClient::from_dependencies(context.dependencies())?),
                    orders: Rc::new(OrdersClient::from_dependencies(context.dependencies())?),
                });
            Ok(())
        })();
        Box::pin(futures::future::ready(result))
    }

    fn deactivate(&self, _context: DeactivateContext) -> ModuleFuture {
        self.dependencies.borrow_mut().take();
        Box::pin(futures::future::ready(Ok(())))
    }
}

#[derive(Clone, Debug)]
struct AuthenticatedOrdersHttp {
    dependencies: Rc<RefCell<Option<EndpointDependencies>>>,
}

#[endpoint]
impl AuthenticatedOrdersHttp {
    #[get("orders.read", "/orders/{order_id}")]
    async fn read(
        &self,
        _actor: UserActor,
        context: InvocationContext,
        Path(path): Path<OrderPath>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        let dependencies = self.dependencies()?;
        let Ok(order) = dependencies
            .orders
            .read(
                context,
                ReadOrderRequest {
                    order_id: path.order_id,
                },
            )
            .await
            .map_err(EndpointHandleInvocationError::Runtime)?
        else {
            return Ok(response::problem(
                StatusCode::FORBIDDEN,
                "insufficient_permission",
                "The authenticated actor cannot read this order.",
            ));
        };
        Ok(response::json(StatusCode::OK, &order)?)
    }

    #[get("orders.admin", "/admin/orders")]
    async fn admin(
        &self,
        _actor: AdminActor,
        context: InvocationContext,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        let dependencies = self.dependencies()?;
        let _result = dependencies
            .orders
            .read(
                context,
                ReadOrderRequest {
                    order_id: "order-42".to_owned(),
                },
            )
            .await
            .map_err(EndpointHandleInvocationError::Runtime)?;
        Ok(response::empty(StatusCode::NO_CONTENT))
    }
}

#[derive(Debug)]
pub struct UserActor {
    pub subject: String,
}

impl AuthenticatedHttpActor for UserActor {
    const KIND: &'static str = "user";

    fn from_assertion(assertion: &ActorAssertion) -> Self {
        Self {
            subject: assertion.subject().to_owned(),
        }
    }
}

#[derive(Debug)]
struct AdminActor;

impl AuthenticatedHttpActor for AdminActor {
    const KIND: &'static str = "admin";

    fn from_assertion(_assertion: &ActorAssertion) -> Self {
        Self
    }
}

impl FromRequest<AuthenticatedOrdersHttp> for AdminActor {
    fn from_request<'a>(
        provider: &'a AuthenticatedOrdersHttp,
        context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self> {
        extract_authenticated_actor(provider, context, request)
    }
}

impl FromRequest<AuthenticatedOrdersHttp> for UserActor {
    fn from_request<'a>(
        provider: &'a AuthenticatedOrdersHttp,
        context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self> {
        extract_authenticated_actor(provider, context, request)
    }
}

impl AuthClientSource for AuthenticatedOrdersHttp {
    fn auth_client(&self) -> Result<Rc<AuthClient>, EndpointHandleInvocationError> {
        Ok(self.dependencies()?.auth)
    }
}

impl AuthenticatedOrdersHttp {
    fn dependencies(&self) -> Result<EndpointDependencies, EndpointHandleInvocationError> {
        self.dependencies.borrow().clone().ok_or({
            EndpointHandleInvocationError::Runtime(RuntimeFailure::Unavailable {
                capability: AUTH_CAPABILITY_ID,
            })
        })
    }
}

#[derive(Debug, Deserialize)]
struct OrderPath {
    order_id: String,
}

#[derive(Debug)]
struct ReadOrder;

impl RequestCapability for ReadOrder {
    type Request = ReadOrderRequest;
    type Response = ReadOrderResponse;
    type DomainError = ReadOrderError;
    const ID: &'static str = ORDERS_CAPABILITY_ID;
    const DESCRIPTOR_VERSION: &'static str = ORDERS_DESCRIPTOR_VERSION;
}

#[derive(Debug)]
struct OrdersClient {
    handle: NativeRequestHandle<ReadOrder>,
}

impl OrdersClient {
    fn from_dependencies(dependencies: &ModuleDependencies) -> Result<Self, RuntimeFailure> {
        Ok(Self {
            handle: dependencies.one::<ReadOrder>()?,
        })
    }

    async fn read(
        &self,
        context: InvocationContext,
        request: ReadOrderRequest,
    ) -> Result<Result<ReadOrderResponse, ReadOrderError>, RuntimeFailure> {
        self.handle
            .invoke_with_context(READ_ORDER_OPERATION, context, request)
            .await
    }
}

#[derive(Debug)]
struct ReadOrderRequest {
    order_id: String,
}

#[derive(Debug, Serialize)]
struct ReadOrderResponse {
    id: String,
    owner: String,
}

#[derive(Debug)]
struct ReadOrderError;

#[derive(Clone, Debug)]
struct OrdersFactory {
    verifier: lenso_auth_sdk::ActorAssertionVerifier,
    now: OffsetDateTime,
    observed_actor: Rc<RefCell<Option<String>>>,
}

impl NativeModuleFactory for OrdersFactory {
    fn package_id(&self) -> &'static str {
        ORDERS_PACKAGE_ID
    }
    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }
    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        Ok(NativeModuleInstance::new(vec![Rc::new(OrdersEndpoint {
            verifier: self.verifier.clone(),
            now: self.now,
            observed_actor: self.observed_actor.clone(),
        })]))
    }
}

#[derive(Debug)]
struct OrdersEndpoint {
    verifier: lenso_auth_sdk::ActorAssertionVerifier,
    now: OffsetDateTime,
    observed_actor: Rc<RefCell<Option<String>>>,
}

impl NativeRequestEndpoint for OrdersEndpoint {
    fn capability_id(&self) -> &'static str {
        ORDERS_CAPABILITY_ID
    }
    fn descriptor_version(&self) -> &'static str {
        ORDERS_DESCRIPTOR_VERSION
    }
    fn operations(&self) -> &'static [&'static str] {
        &[READ_ORDER_OPERATION]
    }
    fn invoke(
        &self,
        operation: &str,
        request: Box<dyn std::any::Any>,
        context: InvocationContext,
    ) -> futures::future::LocalBoxFuture<
        'static,
        Result<Result<Box<dyn std::any::Any>, Box<dyn std::any::Any>>, RuntimeFailure>,
    > {
        if operation != READ_ORDER_OPERATION {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::UnknownOperation {
                    capability: ORDERS_CAPABILITY_ID,
                    operation: operation.to_owned(),
                },
            )));
        }
        let Ok(request) = request.downcast::<ReadOrderRequest>() else {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::ProtocolViolation {
                    capability: ORDERS_CAPABILITY_ID,
                },
            )));
        };
        let result = self
            .verifier
            .project_context::<OrdersActor>(
                &context,
                ORDERS_CAPABILITY_ID,
                READ_ORDER_OPERATION,
                &FixedClock::new(self.now),
            )
            .map(|actor| {
                self.observed_actor
                    .borrow_mut()
                    .replace(actor.user_id.clone());
                Box::new(ReadOrderResponse {
                    id: request.order_id,
                    owner: actor.user_id,
                }) as Box<dyn std::any::Any>
            })
            .map_err(|_| Box::new(ReadOrderError) as Box<dyn std::any::Any>);
        Box::pin(futures::future::ready(Ok(result)))
    }
}

#[derive(Debug)]
struct OrdersActor {
    user_id: String,
}

impl TypedActor for OrdersActor {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        if assertion.actor_kind() != "user" {
            return Err(ActorProjectionError::UnexpectedActorKind {
                expected: "user".to_owned(),
                actual: assertion.actor_kind().to_owned(),
            });
        }
        Ok(Self {
            user_id: assertion.subject().to_owned(),
        })
    }
}

struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

async fn request(address: SocketAddr, headers: &[(&str, &str)]) -> HttpResponse {
    request_path(address, "/orders/order-42", headers).await
}

async fn request_path(address: SocketAddr, path: &str, headers: &[(&str, &str)]) -> HttpResponse {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let headers = headers
        .iter()
        .fold(String::new(), |mut wire, (name, value)| {
            write!(wire, "{name}: {value}\r\n").unwrap();
            wire
        });
    let wire = format!(
        "GET {path} HTTP/1.1\r\nHost: {address}\r\n{headers}Content-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(wire.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    HttpResponse {
        status: head.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers: head
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
            .collect(),
        body: body.to_owned(),
    }
}
