//! Provider 刷新时机、退避与重试策略

use super::{ProviderStoreError, ProviderStoreErrorKind};
use crate::account::ProviderAccountId;
use std::{
    num::NonZeroU32,
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRefreshPolicy {
    margin: Duration,
    concurrency: NonZeroU32,
}

impl ProviderRefreshPolicy {
    pub fn try_new(margin: Duration, concurrency: NonZeroU32) -> Result<Self, ProviderStoreError> {
        if margin.is_zero() {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate refresh policy",
            ));
        }
        Ok(Self {
            margin,
            concurrency,
        })
    }

    #[must_use]
    pub const fn margin(self) -> Duration {
        self.margin
    }

    #[must_use]
    pub const fn concurrency(self) -> NonZeroU32 {
        self.concurrency
    }

    /// 判断 AT 是否已经进入当前配置的提前刷新窗口
    ///
    /// `margin` 是运行时策略，不投影为账号的持久化时间字段；已过期 AT 也需要
    /// 尝试用 RT 恢复，因此视为到期
    #[must_use]
    pub fn is_refresh_due(
        self,
        access_token_expires_at: SystemTime,
        observed_at: SystemTime,
    ) -> bool {
        match access_token_expires_at.duration_since(observed_at) {
            Ok(remaining) => remaining <= self.margin,
            Err(_) => true,
        }
    }

    /// 按账号派生 `[0, margin]` 内的整秒错峰偏移。
    ///
    /// 网关以固定周期扫描到期账号，到期时刻相同的账号会在同一轮齐刷，
    /// 在 auth.openai.com 侧形成单 IP 批量刷新特征。偏移从账号 ID 的
    /// 稳定哈希派生（与退避扰动同源、独立 salt），跨扫描与重启保持不变；
    /// 只会增大有效提前量，不会比配置的 margin 更贴近过期时刻。
    #[must_use]
    pub fn refresh_stagger(self, account_id: &ProviderAccountId) -> Duration {
        // margin 以整秒配置；秒级粒度在默认 300s 下对应约 10 个扫描槽位。
        let bound = u32::try_from(self.margin.as_secs()).unwrap_or(u32::MAX);
        Duration::from_secs(u64::from(stable_factor(
            account_id.as_str(),
            "refresh-stagger",
            0,
            bound,
        )))
    }

    /// 含账号错峰偏移的到期判定；扫描路径先按 [`Self::staggered_refresh_bound`]
    /// 取回候选超集，再用它在内存中收窄到本轮真正到期的账号。
    #[must_use]
    pub fn is_refresh_due_staggered(
        self,
        account_id: &ProviderAccountId,
        access_token_expires_at: SystemTime,
        observed_at: SystemTime,
    ) -> bool {
        let staggered_margin = self.margin.saturating_add(self.refresh_stagger(account_id));
        match access_token_expires_at.duration_since(observed_at) {
            Ok(remaining) => remaining <= staggered_margin,
            Err(_) => true,
        }
    }

    /// 错峰候选窗口上界；覆盖 margin 与最大偏移之和。
    ///
    /// 与 [`Self::refresh_stagger`] 的值域同址维护：偏移上限为 margin，
    /// 因此 `2 × margin` 必然覆盖最大有效提前量。
    #[must_use]
    pub fn staggered_refresh_bound(self) -> Duration {
        self.margin.saturating_mul(2)
    }
}

/// 指数退避基准延迟；attempt=1 即为该值
const REFRESH_BACKOFF_BASE_DELAY: Duration = Duration::from_secs(5);
/// 每多一次连续失败，基准延迟乘以该因子
const REFRESH_BACKOFF_FACTOR: u32 = 3;
/// 退避延迟上限，避免连续失败时无限增长
const REFRESH_BACKOFF_CAP: Duration = Duration::from_secs(300);

/// 基于连续失败计数的指数退避重试时刻；复用 `provider_refresh_retry_at` 的稳定扰动
///
/// `attempt` 为窗口内累计失败次数（0 与 1 等价，均取基准延迟）
/// 延迟按
/// `base * factor^(attempt-1)` 增长并封顶到 `REFRESH_BACKOFF_CAP`
pub fn provider_refresh_backoff_at(
    account_id: &ProviderAccountId,
    observed_at: SystemTime,
    attempt: u32,
    reason: &'static str,
) -> Result<SystemTime, ProviderStoreError> {
    let exponent = attempt.saturating_sub(1);
    let multiplier = REFRESH_BACKOFF_FACTOR.saturating_pow(exponent);
    let scaled_seconds = REFRESH_BACKOFF_BASE_DELAY
        .as_secs()
        .saturating_mul(u64::from(multiplier))
        .min(REFRESH_BACKOFF_CAP.as_secs());
    let base_delay = Duration::from_secs(scaled_seconds);
    provider_refresh_retry_at(account_id, observed_at, base_delay, reason)
}

/// 临时失败后的持久重试时刻；稳定扰动避免多实例同频重试
pub fn provider_refresh_retry_at(
    account_id: &ProviderAccountId,
    observed_at: SystemTime,
    base_delay: Duration,
    reason: &'static str,
) -> Result<SystemTime, ProviderStoreError> {
    if base_delay.is_zero() {
        return Err(invalid_refresh_policy("schedule refresh retry"));
    }
    let factor = stable_factor(account_id.as_str(), reason, 800, 1_200);
    let millis = u64::try_from(base_delay.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::from(factor))
        .saturating_add(500)
        / 1_000;
    observed_at
        .checked_add(Duration::from_millis(millis.max(1)))
        .ok_or_else(|| invalid_refresh_policy("schedule refresh retry"))
}

fn stable_factor(value: &str, salt: &str, minimum: u32, maximum: u32) -> u32 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.bytes().chain([0]).chain(salt.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let width = u64::from(maximum - minimum) + 1;
    minimum + u32::try_from(hash % width).unwrap_or_default()
}

fn invalid_refresh_policy(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}
