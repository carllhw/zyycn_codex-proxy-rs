//! Provider 调度与刷新租约的中立端口

use super::ProviderStoreError;
use crate::{
    account::{AccountConcurrency, AccountRuntimeSignals, CredentialRevision, ProviderAccountId},
    identity::ProviderKind,
    policy::ClientApiKeyId,
};
use futures::future::BoxFuture;
use std::{collections::BTreeMap, fmt, num::NonZeroU32, time::Duration};

/// 一个 Provider 的完整可重建调度状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSchedulingState {
    signals: BTreeMap<ProviderAccountId, AccountRuntimeSignals>,
    round_robin_cursor: u64,
}

impl ProviderSchedulingState {
    #[must_use]
    pub const fn new(
        signals: BTreeMap<ProviderAccountId, AccountRuntimeSignals>,
        round_robin_cursor: u64,
    ) -> Self {
        Self {
            signals,
            round_robin_cursor,
        }
    }

    #[must_use]
    pub const fn signals(&self) -> &BTreeMap<ProviderAccountId, AccountRuntimeSignals> {
        &self.signals
    }

    #[must_use]
    pub const fn round_robin_cursor(&self) -> u64 {
        self.round_robin_cursor
    }
}

/// 请求级账号 lease 的全部中立事实
#[derive(Debug, Clone)]
pub struct ProviderSchedulingLeaseRequest {
    provider_kind: ProviderKind,
    account_id: ProviderAccountId,
    credential_revision: CredentialRevision,
    max_concurrent: AccountConcurrency,
    request_interval: Duration,
    deadline: crate::lifecycle::Deadline,
    cancellation: crate::lifecycle::CancellationToken,
}

impl ProviderSchedulingLeaseRequest {
    #[must_use]
    pub fn new(
        provider_kind: ProviderKind,
        account_id: ProviderAccountId,
        credential_revision: CredentialRevision,
        max_concurrent: impl Into<AccountConcurrency>,
        request_interval: Duration,
        deadline: impl Into<crate::lifecycle::Deadline>,
    ) -> Self {
        Self {
            provider_kind,
            account_id,
            credential_revision,
            max_concurrent: max_concurrent.into(),
            request_interval,
            deadline: deadline.into(),
            cancellation: crate::lifecycle::CancellationToken::new(),
        }
    }

    #[must_use]
    pub fn with_cancellation(mut self, cancellation: crate::lifecycle::CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[must_use]
    pub fn cancellation(&self) -> crate::lifecycle::CancellationToken {
        self.cancellation.clone()
    }

    #[must_use]
    pub const fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
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
    pub const fn max_concurrent(&self) -> AccountConcurrency {
        self.max_concurrent
    }

    #[must_use]
    pub const fn request_interval(&self) -> Duration {
        self.request_interval
    }

    #[must_use]
    pub const fn deadline(&self) -> crate::lifecycle::Deadline {
        self.deadline
    }
}

/// Lease 生命周期由具体 Store guard 管理，Provider 只能持有
pub trait ProviderLeaseGuard: Send + Sync + 'static {}

impl<T> ProviderLeaseGuard for T where T: Send + Sync + 'static {}

pub enum ProviderLeaseAcquisition {
    Acquired(Box<dyn ProviderLeaseGuard>),
    Busy { retry_after: Option<Duration> },
}

impl fmt::Debug for ProviderLeaseAcquisition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acquired(_) => formatter.write_str("Acquired([LEASE])"),
            Self::Busy { retry_after } => formatter
                .debug_struct("Busy")
                .field("retry_after", retry_after)
                .finish(),
        }
    }
}

/// Provider 运行时会持有的三类 lease；刷新必须同时持有全局容量与账号互斥 lease
#[derive(Debug, Clone)]
pub enum ProviderLeaseRequest {
    Scheduling(ProviderSchedulingLeaseRequest),
    RefreshCapacity(ProviderRefreshCapacityRequest),
    Refresh(ProviderRefreshLeaseRequest),
}

pub trait ProviderLeasePort: Send + Sync {
    fn load_state<'a>(
        &'a self,
        client_api_key_id: &'a ClientApiKeyId,
        provider_kind: &'a ProviderKind,
        accounts: &'a [ProviderAccountId],
    ) -> BoxFuture<'a, Result<ProviderSchedulingState, ProviderStoreError>>;

    fn try_acquire(
        &self,
        request: ProviderLeaseRequest,
    ) -> BoxFuture<'_, Result<ProviderLeaseAcquisition, ProviderStoreError>>;

    /// 读取指定账号当前的在途请求数；只用于容量熔断的峰值证据，
    /// 支持租约信号的存储实现覆盖，否则视为不可观测（空映射）
    fn account_in_flight<'a>(
        &'a self,
        account_ids: &'a [ProviderAccountId],
    ) -> BoxFuture<'a, Result<BTreeMap<ProviderAccountId, u32>, ProviderStoreError>> {
        let _ = account_ids;
        Box::pin(async move { Ok(BTreeMap::new()) })
    }
}

/// 所有 Provider 共享的 OAuth refresh 并发容量
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRefreshCapacityRequest {
    max_concurrent: NonZeroU32,
}

impl ProviderRefreshCapacityRequest {
    #[must_use]
    pub const fn new(max_concurrent: NonZeroU32) -> Self {
        Self { max_concurrent }
    }

    #[must_use]
    pub const fn max_concurrent(self) -> NonZeroU32 {
        self.max_concurrent
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRefreshLeaseRequest {
    account_id: ProviderAccountId,
    credential_revision: CredentialRevision,
}

impl ProviderRefreshLeaseRequest {
    #[must_use]
    pub const fn new(
        account_id: ProviderAccountId,
        credential_revision: CredentialRevision,
    ) -> Self {
        Self {
            account_id,
            credential_revision,
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
}
