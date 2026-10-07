//! Provider 存储端口的错误分类与原始来源

/// Provider 可据此决定是否重试，但看不到 SQL、Redis 或秘密原文
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderStoreErrorKind {
    Unavailable,
    InvalidData,
    Conflict,
}

/// Provider 存储端口的脱敏错误
#[derive(Debug, Clone, thiserror::Error)]
#[error("provider store {operation} failed: {kind:?}")]
pub struct ProviderStoreError {
    kind: ProviderStoreErrorKind,
    operation: &'static str,
    source: Option<crate::error::ErrorSource>,
}

impl ProviderStoreError {
    #[must_use]
    pub const fn new(kind: ProviderStoreErrorKind, operation: &'static str) -> Self {
        Self {
            kind,
            operation,
            source: None,
        }
    }

    /// 包装基础设施失败，分类与原始来源分别保留
    #[must_use]
    pub fn caused_by(
        kind: ProviderStoreErrorKind,
        operation: &'static str,
        source: impl Into<crate::error::ErrorSource>,
    ) -> Self {
        Self {
            kind,
            operation,
            source: Some(source.into()),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ProviderStoreErrorKind {
        self.kind
    }
}
