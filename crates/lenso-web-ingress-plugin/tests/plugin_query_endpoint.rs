use std::time::Duration;

use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan,
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, HANDLE_OPERATION,
};
use lenso_kernel::{Kernel, ShutdownOutcome};
use lenso_native_adapter::NativePluginRegistry;
use lenso_runner::TokioDriver;
use lenso_web_ingress_plugin::{
    PACKAGE_ID as INGRESS_PACKAGE_ID, WebIngressConfig, WebIngressFactory,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::LocalSet,
};

#[tokio::test(flavor = "current_thread")]
async fn plugin_authored_query_endpoint_routes_through_the_real_ingress() {
    LocalSet::new()
        .run_until(async {
            lenso_web_query_endpoint_fixture::link();

            let endpoint = PluginInstancePlan::new(
                "order-search",
                lenso_web_query_endpoint_fixture::PACKAGE_ID,
            )
            .with_capability(
                CapabilityEndpointPlan::new(
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                    [DESCRIBE_OPERATION, HANDLE_OPERATION],
                )
                .with_cross_lane_transfer(),
            );
            let ingress = WebIngressFactory::new();
            let ingress_plan = PluginInstancePlan::new("web-ingress", INGRESS_PACKAGE_ID)
                .with_configuration(serde_json::to_string(&WebIngressConfig::default()).unwrap())
                .with_requirement(CapabilityRequirementPlan::many(
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                ));
            let plan = AppComposition::new(
                vec![endpoint, ingress_plan],
                vec![CapabilityBinding::new(
                    "web-ingress",
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                    "order-search",
                )],
            )
            .resolve()
            .unwrap();
            let registry = NativePluginRegistry::new()
                .with_linked_factories()
                .with_factory(ingress.clone());
            let app = Kernel::start_native(plan, TokioDriver::new(), registry)
                .await
                .expect("Plugin-authored QUERY Endpoint should compose with Web Ingress");

            let address = ingress.local_address().expect("Ingress should be bound");
            let body = r#"{"term":"open orders"}"#;
            let request = format!(
                "QUERY /orders/search HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
            connection.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            connection.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();

            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            assert!(response.ends_with(r#"{"term":"open orders"}"#), "{response}");
            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
        })
        .await;
}
