//! Provider 运行策略端口及冻结、预热配置

use super::{ProviderRefreshPolicy, ProviderStoreError, ProviderStoreErrorKind};
use crate::{account::OpaqueProviderData, identity::ProviderKind, routing::ConfigRevision};
use futures::future::BoxFuture;
use std::time::Duration;

pub trait ProviderRuntimePolicyPort: Send + Sync {
    /// 原子推进预热执行游标，重启或时钟回拨后不重复领取已消费的时刻
    fn claim_warmup_slot<'a>(
        &'a self,
        _timezone: crate::time::DeploymentTimeZone,
        _slot: chrono::NaiveDateTime,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async {
            Err(ProviderStoreError::new(
                ProviderStoreErrorKind::Unavailable,
                "claim warmup slot",
            ))
        })
    }
    /// 仅首次启动写入该 Provider 的默认选择；已保存的管理配置始终优先
    fn initialize_request_profile<'a>(
        &'a self,
        _provider: &'a ProviderKind,
        initial: OpaqueProviderData,
    ) -> BoxFuture<'a, Result<OpaqueProviderData, ProviderStoreError>> {
        Box::pin(async move { Ok(initial) })
    }

    /// 读取候选配置版本实际引用的全局与 Client Key 画像配置
    ///
    /// 实现必须在同一数据库快照内核对 revision，且只返回画像投影，不能读取 Key
    /// 明文
    /// Provider 代次据此在发布前拒绝已失效的选择
    fn load_request_profile_configurations<'a>(
        &'a self,
        _revision: ConfigRevision,
        _provider: &'a ProviderKind,
    ) -> BoxFuture<'a, Result<Vec<OpaqueProviderData>, ProviderStoreError>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn load_refresh_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderRefreshPolicy, ProviderStoreError>>;

    /// 读取账号容量熔断策略；默认关闭，只有实现运行时设置的存储需要覆盖
    fn load_freeze_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderFreezePolicy, ProviderStoreError>> {
        Box::pin(async move { Ok(ProviderFreezePolicy::disabled()) })
    }

    /// 读取账号模型预激活策略；默认关闭，只有实现运行时设置的存储需要覆盖
    fn load_warmup_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderWarmupPolicy, ProviderStoreError>> {
        Box::pin(async move { Ok(ProviderWarmupPolicy::disabled()) })
    }
}

/// 账号容量熔断（自动冻结）策略；来源于 `runtime_settings`，
/// 由 Provider 触发路径与恢复 worker 共享同一份配置事实
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderFreezePolicy {
    enabled: bool,
    threshold: u32,
    window: Duration,
    freeze_duration: Duration,
    probe_enabled: bool,
    probe_model: Option<String>,
    adaptive_concurrency: bool,
}

impl ProviderFreezePolicy {
    /// 管理写入与运行策略构造共用的冻结约束
    pub fn validate_values(
        threshold: u32,
        window_seconds: u64,
        duration_seconds: u64,
        probe_model: Option<&str>,
    ) -> Result<(), &'static str> {
        for (valid, field) in [
            (
                (2..=1_000).contains(&threshold),
                "account_auto_freeze_threshold",
            ),
            (
                (60..=3_600).contains(&window_seconds),
                "account_auto_freeze_window_seconds",
            ),
            (
                (300..=604_800).contains(&duration_seconds),
                "account_auto_freeze_duration_seconds",
            ),
            (
                valid_optional_probe_model(probe_model),
                "account_auto_freeze_probe_model",
            ),
        ] {
            if !valid {
                return Err(field);
            }
        }
        Ok(())
    }

    /// 边界与迁移 `0010_account_auto_freeze.sql` 的 check 约束一致；
    /// store 层写入前已校验，这里兜底防御越界配置
    pub fn try_new(
        enabled: bool,
        threshold: u32,
        window_seconds: u64,
        freeze_duration_seconds: u64,
        probe_enabled: bool,
        probe_model: Option<String>,
        adaptive_concurrency: bool,
    ) -> Result<Self, ProviderStoreError> {
        if Self::validate_values(
            threshold,
            window_seconds,
            freeze_duration_seconds,
            probe_model.as_deref(),
        )
        .is_err()
        {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate freeze policy",
            ));
        }
        Ok(Self {
            enabled,
            threshold,
            window: Duration::from_secs(window_seconds),
            freeze_duration: Duration::from_secs(freeze_duration_seconds),
            probe_enabled,
            probe_model,
            adaptive_concurrency,
        })
    }

    /// 功能关闭时的全零策略；触发路径与 worker 都以此短路
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            threshold: 2,
            window: Duration::from_secs(60),
            freeze_duration: Duration::from_secs(300),
            probe_enabled: false,
            probe_model: None,
            adaptive_concurrency: false,
        }
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// 窗口内触发冻结的请求级失败次数阈值
    #[must_use]
    pub const fn threshold(&self) -> u32 {
        self.threshold
    }

    /// 失败计数滑动窗口；每次失败都会顺延窗口
    #[must_use]
    pub const fn window(&self) -> Duration {
        self.window
    }

    /// 冻结时长；探测失败后的顺延也使用该值
    #[must_use]
    pub const fn freeze_duration(&self) -> Duration {
        self.freeze_duration
    }

    #[must_use]
    pub const fn probe_enabled(&self) -> bool {
        self.probe_enabled
    }

    /// 探测模型；`None` 表示由 worker 选择账号可用的第一个模型
    #[must_use]
    pub fn probe_model(&self) -> Option<&str> {
        self.probe_model.as_deref()
    }

    #[must_use]
    pub const fn adaptive_concurrency(&self) -> bool {
        self.adaptive_concurrency
    }
}

/// 校验每日预激活时间格式，如 "08:00" 或 "08:00,13:00"
#[must_use]
pub fn valid_warmup_schedule_time(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 255
        || value != value.trim()
        || value.chars().any(char::is_control)
    {
        return false;
    }
    for part in value.split(',') {
        let bytes = part.as_bytes();
        if bytes.len() != 5 || bytes[2] != b':' {
            return false;
        }
        let Ok(hour) = part[0..2].parse::<u32>() else {
            return false;
        };
        let Ok(minute) = part[3..5].parse::<u32>() else {
            return false;
        };
        if hour > 23 || minute > 59 {
            return false;
        }
    }
    true
}

/// 账号模型预激活（预热）策略；来源于 `runtime_settings`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderWarmupPolicy {
    enabled: bool,
    schedule_time: String,
    model: Option<String>,
}

impl ProviderWarmupPolicy {
    /// 保存设置与创建预热策略使用相同的时间表和模型约束
    pub fn validate_values(
        enabled: bool,
        schedule_time: &str,
        model: Option<&str>,
    ) -> Result<(), &'static str> {
        if !valid_warmup_schedule_time(schedule_time) {
            return Err("account_warmup_schedule_time");
        }
        if (enabled && model.is_none()) || !valid_optional_probe_model(model) {
            return Err("account_warmup_model");
        }
        Ok(())
    }

    pub fn try_new(
        enabled: bool,
        schedule_time: String,
        model: Option<String>,
    ) -> Result<Self, ProviderStoreError> {
        if Self::validate_values(enabled, &schedule_time, model.as_deref()).is_err() {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate warmup policy",
            ));
        }
        Ok(Self {
            enabled,
            schedule_time,
            model,
        })
    }

    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            schedule_time: String::new(),
            model: None,
        }
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub fn schedule_time(&self) -> &str {
        &self.schedule_time
    }

    #[must_use]
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// 解析每日时间列表，返回如 `vec![(8, 0)]`
    #[must_use]
    pub fn scheduled_times(&self) -> Vec<(u32, u32)> {
        self.schedule_time
            .split(',')
            .filter_map(|part| {
                let part = part.trim();
                let mut iter = part.split(':');
                let hour = iter.next()?.parse::<u32>().ok()?;
                let minute = iter.next()?.parse::<u32>().ok()?;
                Some((hour, minute))
            })
            .collect()
    }
}

fn valid_optional_probe_model(model: Option<&str>) -> bool {
    model.is_none_or(|model| {
        !model.is_empty()
            && model.len() <= 128
            && model == model.trim()
            && !model.bytes().any(|byte| byte.is_ascii_control())
    })
}
