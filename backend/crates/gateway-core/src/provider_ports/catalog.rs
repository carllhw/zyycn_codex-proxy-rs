//! Provider 目录与身份画像缓存端口

use super::ProviderStoreError;
use crate::{
    account::OpaqueProviderData,
    identity::ProviderKind,
    validation::{IdentifierError, validate_text},
};
use futures::future::BoxFuture;
use std::{
    fmt,
    time::{Duration, SystemTime},
};

/// Provider 定义的 catalog cache 作用域
///
/// Core 不解释其值；例如 Provider 可以使用套餐、区域或产品线作为共享目录边界
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderCatalogScope(String);

impl ProviderCatalogScope {
    /// 创建一个稳定且可用于 Redis 隔离键的 Provider-owned 作用域
    ///
    /// # Errors
    ///
    /// 空值、过长文本或控制字符会被拒绝
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        validate_text(&value, 128, false, None)?;
        Ok(Self(value))
    }

    /// 返回 Provider-owned 作用域文本
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque catalog cache 的 Provider 与 Provider-owned 作用域
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCatalogCacheKey {
    provider_kind: ProviderKind,
    scope: ProviderCatalogScope,
}

impl ProviderCatalogCacheKey {
    #[must_use]
    pub const fn new(provider_kind: ProviderKind, scope: ProviderCatalogScope) -> Self {
        Self {
            provider_kind,
            scope,
        }
    }

    #[must_use]
    pub const fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    #[must_use]
    pub const fn scope(&self) -> &ProviderCatalogScope {
        &self.scope
    }
}

pub trait ProviderCatalogCachePort: Send + Sync {
    fn replace<'a>(
        &'a self,
        key: &'a ProviderCatalogCacheKey,
        catalog: &'a OpaqueProviderData,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>>;

    fn read<'a>(
        &'a self,
        key: &'a ProviderCatalogCacheKey,
    ) -> BoxFuture<'a, Result<Option<OpaqueProviderData>, ProviderStoreError>>;
}

/// Provider 从官方制品核验出的可重建请求画像
///
/// Core 只用单调制品序号约束覆盖顺序；具体版本字段由对应 Provider 放在
/// `profile` 中解释
/// 每个 Provider 的各制品分别保留一份最新画像
#[derive(Clone, PartialEq)]
pub struct ProviderArtifactProfile {
    provider_kind: ProviderKind,
    artifact_key: String,
    artifact_sequence: u64,
    verified_at: SystemTime,
    profile: OpaqueProviderData,
}

impl ProviderArtifactProfile {
    #[must_use]
    pub const fn new(
        provider_kind: ProviderKind,
        artifact_key: String,
        artifact_sequence: u64,
        verified_at: SystemTime,
        profile: OpaqueProviderData,
    ) -> Self {
        Self {
            provider_kind,
            artifact_key,
            artifact_sequence,
            verified_at,
            profile,
        }
    }

    #[must_use]
    pub const fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    #[must_use]
    pub fn artifact_key(&self) -> &str {
        &self.artifact_key
    }

    #[must_use]
    pub const fn artifact_sequence(&self) -> u64 {
        self.artifact_sequence
    }

    #[must_use]
    pub const fn verified_at(&self) -> SystemTime {
        self.verified_at
    }

    #[must_use]
    pub const fn profile(&self) -> &OpaqueProviderData {
        &self.profile
    }
}

impl fmt::Debug for ProviderArtifactProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderArtifactProfile")
            .field("provider_kind", &self.provider_kind)
            .field("artifact_sequence", &self.artifact_sequence)
            .field("verified_at", &self.verified_at)
            .field("profile", &"[PROVIDER-OWNED]")
            .finish()
    }
}

pub trait ProviderArtifactProfileCachePort: Send + Sync {
    /// 覆盖同一 Provider、同一制品的固定 cache key
    ///
    /// 返回 `false` 表示 Store 已持有更高的制品序号；相同序号但内容不同必须返回
    /// [`super::ProviderStoreErrorKind::Conflict`]，不能静默改写已核验画像
    fn replace_if_newer(
        &self,
        profile: ProviderArtifactProfile,
        ttl: Duration,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>>;

    fn read<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        artifact_key: &'a str,
    ) -> BoxFuture<'a, Result<Option<ProviderArtifactProfile>, ProviderStoreError>>;
}
