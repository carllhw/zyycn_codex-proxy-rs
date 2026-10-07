//! Provider 凭据运行状态读写端口

use super::ProviderStoreError;
use crate::account::{CredentialRevision, CredentialState, ProviderAccountId};
use futures::future::BoxFuture;
use std::time::{Duration, SystemTime};

/// Redis 中可重建的账号状态投影
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCredentialState {
    account_id: ProviderAccountId,
    credential_revision: CredentialRevision,
    enabled: bool,
    credential_state: CredentialState,
    observed_at: SystemTime,
}

impl ProviderCredentialState {
    #[must_use]
    pub const fn new(
        account_id: ProviderAccountId,
        credential_revision: CredentialRevision,
        enabled: bool,
        credential_state: CredentialState,
        observed_at: SystemTime,
    ) -> Self {
        Self {
            account_id,
            credential_revision,
            enabled,
            credential_state,
            observed_at,
        }
    }

    #[must_use]
    pub const fn account_id(&self) -> &ProviderAccountId {
        &self.account_id
    }

    #[must_use]
    pub const fn credential_revision(&self) -> CredentialRevision {
        self.credential_revision
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn credential_state(&self) -> CredentialState {
        self.credential_state
    }

    #[must_use]
    pub const fn observed_at(&self) -> SystemTime {
        self.observed_at
    }
}

pub trait ProviderCredentialStatePort: Send + Sync {
    fn replace(
        &self,
        state: ProviderCredentialState,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>>;

    fn read<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<ProviderCredentialState>, ProviderStoreError>>;

    fn clear<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>>;

    /// 记录一次瞬态刷新失败并返回窗口内累计失败次数；每次失败刷新 TTL
    fn record_refresh_backoff<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        window: Duration,
    ) -> BoxFuture<'a, Result<u32, ProviderStoreError>>;

    /// 凭据完整成功轮换后清零失败计数，退避窗口重新从 base 起步
    fn clear_refresh_backoff<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>>;
}
