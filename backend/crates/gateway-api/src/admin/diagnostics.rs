//! 管理请求在 HTTP 出口记录一次受控详情，响应只保留安全投影

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use gateway_core::diagnostics::{OperationalDiagnostics, OperationalFailure};
use tower_http::request_id::RequestId;

pub(crate) async fn record_failure(
    State(diagnostics): State<Arc<dyn OperationalDiagnostics>>,
    request: Request,
    next: Next,
) -> Response {
    let correlation_id = request
        .extensions()
        .get::<RequestId>()
        .and_then(|id| id.header_value().to_str().ok())
        .map(str::to_owned);
    let mut response = next.run(request).await;
    if let Some(error) = response
        .extensions_mut()
        .remove::<gateway_admin::model::AdminError>()
        && let Some(details) = error.error_details()
    {
        let mut failure = OperationalFailure::new(
            "admin",
            "http_request",
            error.kind().as_str(),
            error.message(),
        );
        failure.correlation_id = correlation_id;
        failure.details = Some(details);
        if diagnostics.record_failure(failure).await.is_err() {
            tracing::warn!("admin diagnostic could not be recorded");
        }
    }
    response
}
