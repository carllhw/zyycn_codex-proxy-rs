//! 核心领域、执行引擎与运行时模块的测试入口

mod account;
mod concurrency;
mod diagnostics;
mod engine;
mod error;
mod event;
mod health;
mod lifecycle;
mod live;
mod metering;
mod middleware;
mod operation;
mod policy;
mod provider_ports;
mod routing;
mod runtime;
mod task;
mod upstream;

mod time;

#[derive(Default)]
struct RecordingDiagnostics(std::sync::Mutex<Vec<gateway_core::diagnostics::OperationalFailure>>);
#[async_trait::async_trait]
impl gateway_core::diagnostics::OperationalDiagnostics for RecordingDiagnostics {
    async fn record_failure(
        &self,
        failure: gateway_core::diagnostics::OperationalFailure,
    ) -> Result<(), gateway_core::error::StoreError> {
        self.0.lock().unwrap().push(failure);
        Ok(())
    }
}
