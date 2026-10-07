//! 执行会话的准入释放、费用结算与终态收敛

use super::COORDINATION_TIMEOUT;
use super::admission::settle_budget;
use super::contract::ExecutionSession;
use super::scope::{ActiveRequestLease, ExecutionAdmission};
use crate::{
    engine::{
        CoordinatedEvent, EngineError, ExecutionStore,
        budget::ClientBudgetPort,
        continuation::{NativeContinuationPin, NativeContinuationPort},
        coordinator::ResponseExecutionSession,
    },
    event::{ProviderEvent, ProviderResponseHeader},
    operation::ProviderSessionState,
};
use futures::{FutureExt as _, future::BoxFuture, pin_mut, select_biased};
use futures_timer::Delay;
use std::sync::Arc;

pub(super) struct DefaultExecutionSession {
    pub(super) core: ResponseExecutionSession<dyn ExecutionStore>,
    pub(super) admission: Option<ExecutionAdmission>,
    pub(super) active_request: Option<ActiveRequestLease>,
    pub(super) cleanup: Option<BoxFuture<'static, ()>>,
    pub(super) continuation: Arc<dyn NativeContinuationPort>,
    pub(super) continuation_recorded: bool,
    pub(super) budget: Option<Arc<dyn ClientBudgetPort>>,
    pub(super) diagnostics: Arc<dyn crate::diagnostics::OperationalDiagnostics>,
}

impl DefaultExecutionSession {
    pub(super) fn new(
        core: ResponseExecutionSession<dyn ExecutionStore>,
        admission: ExecutionAdmission,
        active_request: Option<ActiveRequestLease>,
        continuation: Arc<dyn NativeContinuationPort>,
        budget: Option<Arc<dyn ClientBudgetPort>>,
        diagnostics: Arc<dyn crate::diagnostics::OperationalDiagnostics>,
    ) -> Self {
        Self {
            core,
            admission: Some(admission),
            active_request,
            cleanup: None,
            continuation,
            continuation_recorded: false,
            budget,
            diagnostics,
        }
    }

    pub(super) async fn settle_if_finalized(&mut self) {
        if self.core.is_finalized()
            && let Some(admission) = self.admission.take()
        {
            if let Some(active_request) = self.active_request.take() {
                active_request.release();
            }
            let budget = self.budget.take();
            let charge = self.core.budget_charge();
            let diagnostics = self.diagnostics.clone();
            // 在首次 await 前把完整清理责任留在会话内
            // 事件等待被取消后，后续 poll
            // 或 detach 继续同一个 future，既不丢失费用，也不重启已完成的结算
            self.cleanup = Some(Box::pin(async move {
                if let Some(budget) = budget {
                    settle_budget(budget.as_ref(), charge, diagnostics.as_ref()).await;
                }
                admission.release().await;
            }));
        }
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.await;
            self.cleanup = None;
        }
    }

    pub(super) async fn record_continuation(&mut self, state: Option<&ProviderSessionState>) {
        if self.continuation_recorded {
            return;
        }
        let Some(state) = state else {
            return;
        };
        let Some(pin) = self.core.native_continuation_pin(state) else {
            return;
        };
        self.continuation_recorded = true;
        record_native_continuation(self.continuation.as_ref(), pin).await;
    }

    pub(super) async fn finalize_detached(&mut self) {
        if let Err(error) = self.core.cancel_and_finalize().await {
            tracing::warn!(request_id = self.core.request_id().as_str(), operation = "finalize_detached_execution", %error, "Detached execution 终态收敛失败");
        }
        self.settle_if_finalized().await;
    }
}

impl Drop for DefaultExecutionSession {
    fn drop(&mut self) {
        self.core.cancel();
        drop(self.active_request.take());
    }
}

impl ExecutionSession for DefaultExecutionSession {
    fn trace(&self) -> crate::diagnostics::TraceContext {
        self.core.trace()
    }
    fn next_event(&mut self) -> BoxFuture<'_, Result<Option<CoordinatedEvent>, EngineError>> {
        Box::pin(async move {
            let result = self.core.next_event().await;
            if let Ok(Some(event)) = result.as_ref() {
                self.record_continuation(event.session_update()).await;
            }
            self.settle_if_finalized().await;
            result
        })
    }

    fn collect_uncommitted(&mut self) -> BoxFuture<'_, Result<Vec<ProviderEvent>, EngineError>> {
        Box::pin(async move {
            let result = self.core.collect_uncommitted().await;
            if let Ok(events) = result.as_ref() {
                let state = events.iter().find_map(ProviderEvent::session_update);
                self.record_continuation(state).await;
            }
            self.settle_if_finalized().await;
            result
        })
    }

    fn response_headers(&self) -> &[ProviderResponseHeader] {
        self.core.response_headers()
    }

    fn response_status_code(&self) -> Option<u16> {
        self.core.response_status_code()
    }

    fn discard_pending_delivery(&mut self) -> Result<(), EngineError> {
        self.core.discard_pending_delivery()
    }

    fn commit_downstream(
        &mut self,
        client_status_code: Option<u16>,
    ) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            let result = self.core.commit_downstream(client_status_code).await;
            self.settle_if_finalized().await;
            result
        })
    }

    fn record_client_status(
        &mut self,
        client_status_code: u16,
    ) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            let result = self.core.record_client_status(client_status_code).await;
            self.settle_if_finalized().await;
            result
        })
    }

    fn is_finalized(&self) -> bool {
        self.core.is_finalized()
            && self.admission.is_none()
            && self.active_request.is_none()
            && self.cleanup.is_none()
    }

    fn cancel(&self) {
        self.core.cancel();
    }

    fn detach_finalize(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move { self.finalize_detached().await })
    }
}

pub(super) async fn record_native_continuation(
    continuation: &dyn NativeContinuationPort,
    pin: NativeContinuationPin,
) {
    let provider = pin.provider().as_str().to_owned();
    let account = pin.account().as_str().to_owned();
    let record = continuation.record(pin).fuse();
    let timeout = Delay::new(COORDINATION_TIMEOUT).fuse();
    pin_mut!(record, timeout);
    select_biased! {
        result = record => {
            if let Err(error) = result {
                tracing::warn!(
                    provider = %provider,
                    account = %account,
                    %error,
                    "Continuation affinity 写入失败，后续请求将退化为外部续接"
                );
            }
        },
        _ = timeout => {
            tracing::warn!(
                provider = %provider,
                account = %account,
                "Continuation affinity 后台写入超时，已丢弃本次亲和记录"
            );
        },
    }
}
