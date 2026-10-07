//! Provider 冷却、容量窗口和运行状态端口

use super::ProviderStoreError;
/// 账号级 cooldown 的来源类别；调度状态统一按 `rate_limited` 处理
pub use crate::account::AccountCooldownKind as ProviderCooldownKind;
use crate::{
    account::{CredentialRevision, ProviderAccountId},
    routing::UpstreamModelId,
};
use futures::future::BoxFuture;
use std::time::{Duration, SystemTime};

/// 可丢失的账号冷却事实，不进入持久状态；探测冻结到期后仍需确认恢复
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCooldown {
    account_id: ProviderAccountId,
    credential_revision: CredentialRevision,
    until: SystemTime,
    kind: ProviderCooldownKind,
}

impl ProviderCooldown {
    #[must_use]
    pub const fn new(
        account_id: ProviderAccountId,
        credential_revision: CredentialRevision,
        until: SystemTime,
    ) -> Self {
        Self::new_with_kind(
            account_id,
            credential_revision,
            until,
            ProviderCooldownKind::RateLimit,
        )
    }

    #[must_use]
    pub const fn new_with_kind(
        account_id: ProviderAccountId,
        credential_revision: CredentialRevision,
        until: SystemTime,
        kind: ProviderCooldownKind,
    ) -> Self {
        Self {
            account_id,
            credential_revision,
            until,
            kind,
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
    pub const fn until(&self) -> SystemTime {
        self.until
    }

    #[must_use]
    pub const fn scheduling_state(&self) -> crate::account::AccountCooldown {
        crate::account::AccountCooldown {
            until: self.until,
            kind: self.kind,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ProviderCooldownKind {
        self.kind
    }
}

/// 可丢失 cooldown 的细粒度作用域
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProviderCooldownScope {
    /// 只阻止同一账号调用指定上游模型
    UpstreamModel(UpstreamModelId),
}

impl ProviderCooldownScope {
    #[must_use]
    pub const fn upstream_model(model: UpstreamModelId) -> Self {
        Self::UpstreamModel(model)
    }

    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::UpstreamModel(_) => "model",
        }
    }

    #[must_use]
    pub fn value(&self) -> &str {
        match self {
            Self::UpstreamModel(model) => model.as_str(),
        }
    }
}

/// 不进入账号持久状态的账号+作用域 cooldown
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderScopedCooldown {
    account_id: ProviderAccountId,
    credential_revision: CredentialRevision,
    scope: ProviderCooldownScope,
    until: SystemTime,
}

impl ProviderScopedCooldown {
    #[must_use]
    pub const fn new(
        account_id: ProviderAccountId,
        credential_revision: CredentialRevision,
        scope: ProviderCooldownScope,
        until: SystemTime,
    ) -> Self {
        Self {
            account_id,
            credential_revision,
            scope,
            until,
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
    pub const fn scope(&self) -> &ProviderCooldownScope {
        &self.scope
    }

    #[must_use]
    pub const fn until(&self) -> SystemTime {
        self.until
    }
}

pub trait ProviderCooldownPort: Send + Sync {
    fn put_if_later(
        &self,
        cooldown: ProviderCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>>;

    fn read<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<ProviderCooldown>, ProviderStoreError>>;

    fn clear<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>>;

    fn put_scoped_if_later(
        &self,
        cooldown: ProviderScopedCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>>;

    fn read_scoped<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        scope: &'a ProviderCooldownScope,
    ) -> BoxFuture<'a, Result<Option<ProviderScopedCooldown>, ProviderStoreError>>;

    fn clear_scoped<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        scope: &'a ProviderCooldownScope,
        through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>>;

    /// 删除账号时清除该账号全部 account/model scope cooldown key
    /// 生命周期与 credential revision 无关；不要求调用方持有 revision
    fn clear_all<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>>;

    /// 记录一次容量类失败并返回滑动窗口内的累计次数，同时把观测到的账号
    /// 在途并发并入窗口峰值（`in_flight` 为 0 表示本次未观测，跳过峰值更新）
    /// 只服务容量熔断触发器；调用频率受失败频率约束，不需要批量接口
    fn record_capacity_failure<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        window: Duration,
        in_flight: u32,
    ) -> BoxFuture<'a, Result<u32, ProviderStoreError>>;

    /// 普通请求成功后原子清除临时限流及失败证据；必须保留任何容量冻结
    fn clear_after_success<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>>;

    /// 读取窗口内观测到的在途并发峰值；无证据时返回 `None`
    fn capacity_peak_in_flight<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<u32>, ProviderStoreError>>;
}
