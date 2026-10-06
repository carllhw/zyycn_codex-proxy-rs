//! 将运行设置校验并编译为账号选择、并发排队与请求限制策略

use std::time::Duration;

use super::{InvalidSettings, SettingsValues};
use crate::{
    account::{AccountConcurrency, AccountSelectionPolicy, RotationStrategy},
    concurrency::ConcurrencyQueuePolicy,
    policy::CodexClientMinVersions,
};

// 持久快照和请求覆盖共用同一编译结果，避免参数校验与派生规则分叉
#[derive(Debug, Clone)]
pub(crate) struct CompiledSettings {
    pub(crate) values: SettingsValues,
    pub(crate) account_selection_policy: AccountSelectionPolicy,
    pub(crate) client_queue_policy: ConcurrencyQueuePolicy,
    pub(crate) responses_max_decompressed_body_bytes: std::num::NonZeroUsize,
    pub(crate) min_codex_client_versions: CodexClientMinVersions,
    pub(crate) request_location: Option<crate::account::RequestLocation>,
}

impl CompiledSettings {
    pub(crate) fn new(settings: SettingsValues) -> Result<Self, InvalidSettings> {
        super::validate_request_limits(
            settings.max_waiting_per_key,
            settings.max_waiting_per_account,
            settings.concurrency_wait_timeout_seconds,
            settings.max_account_rotations,
            settings.openai_session_affinity_ttl_hours,
        )
        .map_err(|_| InvalidSettings)?;
        let queue_timeout =
            Duration::from_secs(u64::from(settings.concurrency_wait_timeout_seconds));
        let responses_max_decompressed_body_bytes =
            super::response_body_limit(settings.responses_max_decompressed_body_bytes)
                .map_err(|_| InvalidSettings)?;
        let min_codex_client_versions = super::client_min_versions(
            settings.min_codex_desktop_version.as_deref(),
            settings.min_codex_cli_version.as_deref(),
        )
        .map_err(|_| InvalidSettings)?;
        let strategy =
            RotationStrategy::parse(&settings.rotation_strategy).ok_or(InvalidSettings)?;
        let account_selection_policy = AccountSelectionPolicy::new(
            strategy,
            AccountConcurrency::new(settings.max_concurrent_per_account),
            Duration::from_millis(settings.request_interval_ms),
        )
        .with_openai_guardian_reserved_concurrency(settings.openai_guardian_reserved_concurrency)
        .with_openai_account_affinity(settings.openai_account_affinity)
        .with_max_account_rotations(settings.max_account_rotations)
        .with_openai_session_affinity_ttl(Duration::from_secs(
            u64::from(settings.openai_session_affinity_ttl_hours) * 3600,
        ))
        .with_smart_scheduling(settings.smart_scheduling)
        .with_queue(ConcurrencyQueuePolicy {
            max_waiting: settings.max_waiting_per_account,
            timeout: queue_timeout,
        });
        let client_queue_policy = ConcurrencyQueuePolicy {
            max_waiting: settings.max_waiting_per_key,
            timeout: queue_timeout,
        };
        let request_location = settings
            .request_location_enabled
            .then(|| settings.request_location.clone().normalized())
            .transpose()
            .map_err(|_| InvalidSettings)?;
        for models in settings.pricing.values() {
            for price in models.values() {
                price.validate().map_err(|_| InvalidSettings)?;
            }
        }
        Ok(Self {
            account_selection_policy,
            client_queue_policy,
            responses_max_decompressed_body_bytes,
            min_codex_client_versions,
            request_location,
            values: settings,
        })
    }
}
