//! Store 值类型、错误与跨层映射

use super::*;

/// 发生错误的基础设施边界
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreBackend {
    PostgreSql,
    Redis,
}

/// 上层状态机需要区分的稳定冲突类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictKind {
    StaleRevision,
    DuplicateName,
    AlreadyFinalized,
    DownstreamAlreadyCommitted,
    RequestNotRunning,
    InvalidTransition,
    LeaseLost,
    FencingTokenStale,
}

/// Store adapter 的稳定错误边界
#[derive(Debug, Clone, thiserror::Error)]
pub enum StoreError {
    #[error("{backend:?} store is unavailable: {message}")]
    Unavailable {
        backend: StoreBackend,
        message: String,
        source: Option<gateway_core::error::ErrorSource>,
    },
    #[error("{entity} {id} was not found")]
    NotFound {
        entity: &'static str,
        id: String,
        source: Option<gateway_core::error::ErrorSource>,
    },
    #[error("store conflict for {entity} {id}: {kind:?}")]
    Conflict {
        entity: &'static str,
        id: String,
        kind: ConflictKind,
        source: Option<gateway_core::error::ErrorSource>,
    },
    #[error("invalid persisted {entity}: {message}")]
    InvalidData {
        entity: &'static str,
        message: String,
        source: Option<gateway_core::error::ErrorSource>,
    },
}

impl StoreError {
    /// 本地校验可没有来源；转换已有失败时附加真实原因
    #[must_use]
    pub(crate) fn with_source(
        mut self,
        error: impl Into<gateway_core::error::ErrorSource>,
    ) -> Self {
        let error = error.into();
        match &mut self {
            Self::Unavailable { source, .. }
            | Self::InvalidData { source, .. }
            | Self::Conflict { source, .. }
            | Self::NotFound { source, .. } => *source = Some(error),
        }
        self
    }

    pub(crate) fn with_cleanup(
        mut self,
        cleanup: impl Into<gateway_core::error::ErrorSource>,
    ) -> Self {
        let source = match &mut self {
            Self::Unavailable { source, .. }
            | Self::InvalidData { source, .. }
            | Self::Conflict { source, .. }
            | Self::NotFound { source, .. } => source,
        };
        *source = Some(gateway_core::error::ErrorSource::cleanup(
            source.take(),
            cleanup,
        ));
        self
    }

    fn admin_kind(&self) -> AdminStoreErrorKind {
        match self {
            Self::NotFound { .. } => AdminStoreErrorKind::NotFound,
            Self::Conflict {
                kind: ConflictKind::StaleRevision,
                ..
            } => AdminStoreErrorKind::StaleRevision,
            Self::Conflict {
                kind: ConflictKind::DuplicateName,
                ..
            } => AdminStoreErrorKind::DuplicateName,
            Self::Conflict { .. } => AdminStoreErrorKind::Conflict,
            Self::InvalidData { .. } => AdminStoreErrorKind::Invalid,
            Self::Unavailable { .. } => AdminStoreErrorKind::Unavailable,
        }
    }

    fn core_kind(&self) -> gateway_core::error::StoreErrorKind {
        use gateway_core::error::StoreErrorKind;
        match self {
            Self::Unavailable { .. } => StoreErrorKind::Unavailable,
            Self::Conflict { .. } => StoreErrorKind::Conflict,
            Self::NotFound { .. } | Self::InvalidData { .. } => StoreErrorKind::InvalidData,
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;
pub(crate) fn store_revision(revision: AdminRevision) -> AdminStoreResult<Revision> {
    Revision::new(revision.get()).map_err(|error| admin_store_error("config revision", error))
}

pub(crate) fn admin_revision(revision: Revision) -> AdminStoreResult<AdminRevision> {
    AdminRevision::new(revision.get()).map_err(|_| {
        AdminStoreError::new(
            AdminStoreErrorKind::Invalid,
            "config revision",
            "config revision is invalid",
        )
    })
}

pub(crate) fn mutation_audit(
    context: &MutationContext,
    operation: gateway_admin::model::audit::MutationAuditOperation,
    entity_ref: &str,
    changed_fields: Vec<String>,
) -> postgres::AdminAuditEvent {
    let event = gateway_admin::model::audit::MutationAuditIntent {
        operation,
        entity_ref,
    }
    .event(context, changed_fields);
    postgres::AdminAuditEvent {
        id: event.id,
        actor_kind: event.actor_kind.into(),
        actor_admin_user_id: event.actor_admin_user_id,
        actor_ref: event.actor_ref,
        admin_request_id: event.request_id,
        action: event.action,
        entity_kind: event.entity_kind,
        entity_ref: event.entity_ref,
        config_revision: None,
        changed_fields: event.changed_fields,
        created_at: event.occurred_at,
    }
}

pub(crate) fn admin_store_error(resource: &'static str, error: StoreError) -> AdminStoreError {
    AdminStoreError::new(error.admin_kind(), resource, "store operation failed").with_source(error)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(NonZeroU64);

impl Revision {
    pub fn new(value: u64) -> StoreResult<Self> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| StoreError::InvalidData {
                source: None,
                entity: "revision",
                message: "must be greater than zero".to_owned(),
            })
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

pub use gateway_admin::model::observability::DecimalAmount;

/// Provider-owned JSON object
/// Store 只验证 object 与大小，不解释内部 key
#[derive(Clone, PartialEq)]
pub struct JsonObject(Map<String, Value>);

impl JsonObject {
    pub fn try_from_value(
        entity: &'static str,
        value: Value,
        max_serialized_bytes: usize,
    ) -> StoreResult<Self> {
        let serialized_bytes = serde_json::to_vec(&value)
            .map_err(|error| StoreError::InvalidData {
                source: Some(error.into()),
                entity,
                message: "JSON encoding failed".to_owned(),
            })?
            .len();
        let Value::Object(fields) = value else {
            return Err(StoreError::InvalidData {
                source: None,
                entity,
                message: "top-level JSON value must be an object".to_owned(),
            });
        };
        if serialized_bytes > max_serialized_bytes {
            return Err(StoreError::InvalidData {
                source: None,
                entity,
                message: format!("serialized JSON exceeds {max_serialized_bytes} bytes"),
            });
        }
        Ok(Self(fields))
    }

    #[must_use]
    pub fn as_value(&self) -> Value {
        Value::Object(self.0.clone())
    }

    #[must_use]
    pub fn fields(&self) -> &Map<String, Value> {
        &self.0
    }
}

impl fmt::Debug for JsonObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JsonObject([REDACTED])")
    }
}

pub(crate) fn require_nonempty(
    entity: &'static str,
    field: &'static str,
    value: &str,
) -> StoreResult<()> {
    if value.trim().is_empty() {
        Err(StoreError::InvalidData {
            source: None,
            entity,
            message: format!("{field} must not be empty"),
        })
    } else {
        Ok(())
    }
}

pub(crate) fn postgres_unavailable(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> StoreError {
    StoreError::Unavailable {
        backend: StoreBackend::PostgreSql,
        message: operation.to_owned(),
        source: Some(gateway_core::error::ErrorSource::new(source)),
    }
}

pub(crate) fn redis_unavailable(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> StoreError {
    StoreError::Unavailable {
        backend: StoreBackend::Redis,
        message: operation.to_owned(),
        source: Some(gateway_core::error::ErrorSource::new(source)),
    }
}

/// Provider 存储端口共用不可用分类，实际后端及原因由来源链保留
pub(crate) fn provider_unavailable(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> gateway_core::provider_ports::ProviderStoreError {
    gateway_core::provider_ports::ProviderStoreError::caused_by(
        gateway_core::provider_ports::ProviderStoreErrorKind::Unavailable,
        operation,
        source,
    )
}

pub(crate) fn core_store_error(error: StoreError) -> gateway_core::error::StoreError {
    gateway_core::error::StoreError::caused_by(error.core_kind(), error)
}
