//! 诊断探测使用的非持久执行记录与观测事实

use crate::{
    engine::{
        AttemptRecord, ExecutionStore, IntermediateFailure, ModelRequestFinalization,
        ModelRequestId, NewModelRequest, ProviderAccountId, RecoveryReport, UpstreamSendState,
    },
    error::StoreError,
    identity::ProviderKind,
    routing::UpstreamModelId,
};
use std::time::SystemTime;

pub(super) struct ProbeObservation {
    pub(super) provider_kind: ProviderKind,
    pub(super) account_id: ProviderAccountId,
    pub(super) upstream_model: UpstreamModelId,
}

pub(super) struct TransientExecutionStore;

#[async_trait::async_trait]
impl ExecutionStore for TransientExecutionStore {
    async fn create_model_request(&self, _: NewModelRequest) -> Result<(), StoreError> {
        Ok(())
    }

    async fn record_attempt(&self, _: AttemptRecord) -> Result<(), StoreError> {
        Ok(())
    }

    async fn mark_send_state(
        &self,
        _: &ModelRequestId,
        _: UpstreamSendState,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    async fn mark_downstream_committed(
        &self,
        _: &ModelRequestId,
        _: SystemTime,
        _: Option<u16>,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        Ok(())
    }

    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        Ok(())
    }

    async fn finalize_model_request(&self, _: ModelRequestFinalization) -> Result<(), StoreError> {
        Ok(())
    }

    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        Ok(RecoveryReport::default())
    }
}
