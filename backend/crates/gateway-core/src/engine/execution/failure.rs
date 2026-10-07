//! 执行边界的错误分类与资源释放诊断

use super::contract::ClientAuthenticationError;
use crate::{
    engine::{EngineError, ModelRequestId},
    error::{GatewayError, GatewayErrorKind},
};

pub(super) fn authentication_gateway_error(error: ClientAuthenticationError) -> GatewayError {
    match error {
        ClientAuthenticationError::InvalidKey => {
            GatewayError::new(GatewayErrorKind::Unauthorized, "client API key is invalid")
        }
        ClientAuthenticationError::SnapshotUnavailable => GatewayError::new(
            GatewayErrorKind::Internal,
            "runtime snapshot is unavailable",
        ),
        ClientAuthenticationError::ProviderUnavailable => GatewayError::new(
            GatewayErrorKind::Internal,
            "frontend authentication provider is unavailable",
        ),
    }
}

pub(super) fn map_routing_error(error: crate::validation::RoutingError) -> GatewayError {
    match error {
        crate::validation::RoutingError::ModelNotFound {
            model,
            mapped_model,
        } => GatewayError::new(
            GatewayErrorKind::ModelNotFound,
            if model == mapped_model {
                "the requested model was not found in the provider catalogs available to this API key; check the model name"
            } else {
                "the requested model maps to an upstream model that was not found in the provider catalogs available to this API key; check the configured model mapping"
            },
        ),
        crate::validation::RoutingError::NoCapableProvider { .. }
        | crate::validation::RoutingError::NoCapableProviderEndpoint { .. }
        | crate::validation::RoutingError::EmptyAccountScope => GatewayError::new(
            GatewayErrorKind::NoAvailableProvider,
            "no provider can execute this request",
        ),
        crate::validation::RoutingError::UnsupportedProviderEndpoint { .. } => GatewayError::new(
            GatewayErrorKind::Unsupported,
            "the selected provider does not support this operation",
        ),
        _ => GatewayError::new(
            GatewayErrorKind::Internal,
            "runtime routing configuration is invalid",
        ),
    }
}

pub fn gateway_error_from_engine(error: &EngineError) -> GatewayError {
    match error {
        EngineError::Cancelled => {
            GatewayError::new(GatewayErrorKind::Cancelled, "request was cancelled")
        }
        EngineError::Deadline => {
            GatewayError::new(GatewayErrorKind::Timeout, "request deadline elapsed")
        }
        EngineError::Provider(provider) => GatewayError::from_provider(provider),
        EngineError::EmptyRoutingPlan | EngineError::ProviderNotRegistered { .. } => {
            GatewayError::new(
                GatewayErrorKind::NoAvailableProvider,
                "no provider is available",
            )
        }
        EngineError::Store(source) => {
            GatewayError::new(GatewayErrorKind::Internal, "request execution failed")
                .with_source(source.clone())
        }
        _ => GatewayError::new(GatewayErrorKind::Internal, "request execution failed"),
    }
}

pub(super) async fn record_resource_failure(
    diagnostics: &dyn crate::diagnostics::OperationalDiagnostics,
    request_id: &ModelRequestId,
    operation: &'static str,
    message: &'static str,
    source: impl Into<crate::error::ErrorSource>,
) {
    let mut failure = crate::diagnostics::OperationalFailure::new(
        "core",
        operation,
        "resource_unavailable",
        message,
    );
    failure.correlation_id = Some(request_id.as_str().to_owned());
    failure.details = crate::error::ErrorDetails::capture(Some(&source.into()), None, false);
    if diagnostics.record_failure(failure).await.is_err() {
        tracing::warn!(
            request_id = request_id.as_str(),
            operation,
            "resource diagnostic could not be recorded"
        );
    }
}
