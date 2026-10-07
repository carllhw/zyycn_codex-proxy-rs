//! OAuth 授权 pending flow 的领取与消费合同

use super::{ProviderStoreError, ProviderStoreErrorKind};
use crate::{account::OpaqueProviderData, identity::ProviderKind};
use futures::future::BoxFuture;
use std::{fmt, time::Duration};

const MAX_PENDING_FLOW_TTL: Duration = Duration::from_secs(30 * 60);

/// OAuth pending flow 的原始绑定只在 Provider 与 Store 边界内短暂存在
#[derive(Clone, PartialEq, Eq)]
pub struct OAuthPendingBinding(String);

impl OAuthPendingBinding {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ProviderStoreError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate OAuth pending binding",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn expose_to_store(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OAuthPendingBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OAuthPendingBinding([REDACTED])")
    }
}

#[derive(Clone, PartialEq)]
pub struct NewOAuthPendingFlow {
    provider_kind: ProviderKind,
    flow: OAuthPendingBinding,
    owner: OAuthPendingBinding,
    ttl: Duration,
    payload: OpaqueProviderData,
}

impl NewOAuthPendingFlow {
    pub fn try_new(
        provider_kind: ProviderKind,
        flow: OAuthPendingBinding,
        owner: OAuthPendingBinding,
        ttl: Duration,
        payload: OpaqueProviderData,
    ) -> Result<Self, ProviderStoreError> {
        if ttl.is_zero() || ttl > MAX_PENDING_FLOW_TTL {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate OAuth pending TTL",
            ));
        }
        Ok(Self {
            provider_kind,
            flow,
            owner,
            ttl,
            payload,
        })
    }

    #[must_use]
    pub const fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    #[must_use]
    pub const fn flow(&self) -> &OAuthPendingBinding {
        &self.flow
    }

    #[must_use]
    pub const fn owner(&self) -> &OAuthPendingBinding {
        &self.owner
    }

    #[must_use]
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }

    #[must_use]
    pub const fn payload(&self) -> &OpaqueProviderData {
        &self.payload
    }
}

impl fmt::Debug for NewOAuthPendingFlow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewOAuthPendingFlow")
            .field("provider_kind", &self.provider_kind)
            .field("flow", &self.flow)
            .field("owner", &self.owner)
            .field("ttl", &self.ttl)
            .field("payload", &"[PROVIDER-OWNED]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthPendingPutOutcome {
    Stored,
    AlreadyExists,
}

/// OAuth 回调处理取得临时 flow 的独占权结果
///
/// 与一次性消费不同，Provider 在上游换取令牌失败时可以释放 claim，让同一 flow
/// 使用新的回调地址重试；只有完整校验成功后才会消费 flow
#[derive(Clone, PartialEq)]
pub enum OAuthPendingClaimOutcome {
    Claimed(OpaqueProviderData),
    NotFound,
    OwnerMismatch,
    InProgress,
}

impl fmt::Debug for OAuthPendingClaimOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Claimed(_) => formatter.write_str("Claimed([PROVIDER-OWNED])"),
            Self::NotFound => formatter.write_str("NotFound"),
            Self::OwnerMismatch => formatter.write_str("OwnerMismatch"),
            Self::InProgress => formatter.write_str("InProgress"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthPendingReleaseOutcome {
    Released,
    NotFound,
    OwnerMismatch,
    ClaimMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthPendingConsumeOutcome {
    Consumed,
    NotFound,
    OwnerMismatch,
    ClaimMismatch,
}

pub trait OAuthPendingFlowPort: Send + Sync {
    fn put_if_absent(
        &self,
        flow: NewOAuthPendingFlow,
    ) -> BoxFuture<'_, Result<OAuthPendingPutOutcome, ProviderStoreError>>;

    fn claim_if_owner<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
        claim_ttl: Duration,
    ) -> BoxFuture<'a, Result<OAuthPendingClaimOutcome, ProviderStoreError>>;

    fn release_claim<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingReleaseOutcome, ProviderStoreError>>;

    fn consume_claim<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingConsumeOutcome, ProviderStoreError>>;
}
