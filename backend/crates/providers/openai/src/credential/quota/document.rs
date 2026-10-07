//! OpenAI quota 文档的规范化 map 与上游 `/usage` 输入适配

use std::collections::BTreeMap;

use gateway_protocol::openai::events::{RateLimitDetails, RateLimitKeySource};
use serde_json::{Map, Value};

pub(super) const DEFAULT_CODEX_LIMIT_ID: &str = "codex";
pub(super) const RATE_LIMITS_BY_LIMIT_ID: &str = "rate_limits_by_limit_id";

/// 官方新协议的多桶视图：每个 `limit_id` 对应一个独立快照
#[derive(Debug, Default)]
pub(super) struct RateLimitSnapshotsByLimitId {
    snapshots: BTreeMap<String, Map<String, Value>>,
}

impl RateLimitSnapshotsByLimitId {
    /// 接收上游 `/usage` 形状或已经规范化的 map；输出始终只保留 map
    pub(super) fn take_from_document(quota: &mut Map<String, Value>) -> Self {
        let mut result = Self::default();

        if let Some(by_limit_id) = quota
            .remove(RATE_LIMITS_BY_LIMIT_ID)
            .and_then(|value| value.as_object().cloned())
        {
            for (raw_limit_id, value) in by_limit_id {
                let Some(limit_id) = normalize_limit_id(&raw_limit_id) else {
                    continue;
                };
                let mut snapshot = snapshot_object(value);
                snapshot.insert("limit_id".to_owned(), Value::String(limit_id.clone()));
                result.snapshots.insert(limit_id, snapshot);
            }
        }

        if let Some(rate_limit) = quota.remove("rate_limit") {
            result.insert_usage_snapshot(DEFAULT_CODEX_LIMIT_ID, None, rate_limit);
        }
        if let Some(rate_limit) = quota.remove("code_review_rate_limit") {
            result.insert_usage_snapshot("code_review", Some("code_review"), rate_limit);
        }

        let additional = quota
            .remove("additional_rate_limits")
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        for (index, value) in additional.into_iter().enumerate() {
            let Value::Object(mut item) = value else {
                result.insert_usage_snapshot(&format!("additional_{index}"), None, value);
                continue;
            };
            let limit_id = item_limit_id(&item).unwrap_or_else(|| format!("additional_{index}"));
            let limit_name = item
                .get("limit_name")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let rate_limit = item.remove("rate_limit").unwrap_or(Value::Null);
            result.insert_usage_snapshot(&limit_id, limit_name.as_deref(), rate_limit);
        }
        result
    }

    fn insert_usage_snapshot(
        &mut self,
        limit_id: &str,
        limit_name: Option<&str>,
        rate_limit: Value,
    ) {
        let Some(limit_id) = normalize_limit_id(limit_id) else {
            return;
        };
        let mut snapshot = snapshot_object(rate_limit);
        snapshot.insert("limit_id".to_owned(), Value::String(limit_id.clone()));
        if let Some(limit_name) = limit_name {
            snapshot.insert(
                "limit_name".to_owned(),
                Value::String(limit_name.to_owned()),
            );
        }
        // map 协议中一个 ID 只能有一个快照；规范化 map 与顶层账号桶优先
        self.snapshots.entry(limit_id).or_insert(snapshot);
    }

    pub(super) fn upsert(
        &mut self,
        limit_id: &str,
        details: &RateLimitDetails,
        rate_limit: Map<String, Value>,
    ) {
        let mut snapshot = self.snapshots.remove(limit_id).unwrap_or_default();
        for field in [
            "allowed",
            "limit_reached",
            "primary_window",
            "secondary_window",
        ] {
            snapshot.remove(field);
        }
        snapshot.extend(rate_limit);
        snapshot.insert("limit_id".to_owned(), Value::String(limit_id.to_owned()));
        if let Some(name) = details.limit_name.as_ref() {
            snapshot.insert("limit_name".to_owned(), Value::String(name.clone()));
        }
        self.snapshots.insert(limit_id.to_owned(), snapshot);
    }

    /// 把 wire 观察解析到稳定 `limit_id`
    ///
    /// WebSocket 的 `additional_rate_limits` 可能只返回可读名称；这时只接受
    /// `/usage` 或 HTTP headers 已建立的唯一名称映射
    /// 无法唯一解析就不落库，
    /// 避免凭名称猜 ID 或污染默认 `codex` 桶
    pub(super) fn resolve_limit_id(&self, details: &RateLimitDetails) -> Option<String> {
        if details.key_source == RateLimitKeySource::LimitId {
            return normalize_limit_id(&details.limit_id);
        }

        let limit_name = details.limit_name.as_deref()?.trim();
        let mut matches = self
            .snapshots
            .iter()
            .filter(|(_, snapshot)| {
                snapshot
                    .get("limit_name")
                    .and_then(Value::as_str)
                    .is_some_and(|stored| stored.trim().eq_ignore_ascii_case(limit_name))
            })
            .map(|(limit_id, _)| limit_id.clone());
        let resolved = matches.next()?;
        matches.next().is_none().then_some(resolved)
    }

    pub(super) fn write_to_document(self, quota: &mut Map<String, Value>) {
        let by_limit_id = self
            .snapshots
            .into_iter()
            .map(|(limit_id, snapshot)| (limit_id, Value::Object(snapshot)))
            .collect::<Map<_, _>>();
        quota.insert(
            RATE_LIMITS_BY_LIMIT_ID.to_owned(),
            Value::Object(by_limit_id),
        );
    }
}

pub(super) fn canonicalize_rate_limit_document(
    mut quota: Map<String, Value>,
) -> Map<String, Value> {
    let snapshots = RateLimitSnapshotsByLimitId::take_from_document(&mut quota);
    snapshots.write_to_document(&mut quota);
    quota
}

fn snapshot_object(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_else(|| {
        let mut snapshot = Map::new();
        snapshot.insert("rate_limit".to_owned(), value);
        snapshot
    })
}

fn item_limit_id(item: &Map<String, Value>) -> Option<String> {
    item.get("limit_id")
        .or_else(|| item.get("metered_feature"))
        .or_else(|| item.get("limit_name"))
        .and_then(Value::as_str)
        .and_then(normalize_limit_id)
}

fn normalize_limit_id(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control))
        .then(|| value.to_ascii_lowercase().replace('-', "_"))
}

/// 丢弃 core 限额中上游给出的无事实 `secondary_window` 占位
///
/// 生产响应头可能携带 `used_percent=0`、零时长和空 reset 的占位，`/usage` 也会
/// 返回 `secondary_window: null`
/// 正常被动同步保留该响应事实，Admin 展示会将其
/// 隐藏；但主动 `/usage` 刷新或 402 确认投影会把一个存在但无事实的字段写成
/// 100%，从而显示并不存在的“次级额度”
/// 因此这些非被动写入口在落库前移除
/// 无事实值
/// 带 reset、时长、正用量、触顶、未知或非法字段的次级窗口均完全按
/// 原有额度逻辑保留
pub(super) fn normalize_quota_window_placeholders(
    mut quota: Map<String, Value>,
) -> Map<String, Value> {
    quota = canonicalize_rate_limit_document(quota);
    if let Some(rate_limit) = quota
        .get_mut(RATE_LIMITS_BY_LIMIT_ID)
        .and_then(Value::as_object_mut)
        .and_then(|limits| limits.get_mut(DEFAULT_CODEX_LIMIT_ID))
        .and_then(Value::as_object_mut)
    {
        drop_secondary_window_placeholder(rate_limit);
    }
    quota
}

fn drop_secondary_window_placeholder(rate_limit: &mut Map<String, Value>) {
    let placeholder = rate_limit.get("secondary_window").is_some_and(|window| {
        window.is_null()
            || window
                .as_object()
                .is_some_and(secondary_window_is_placeholder)
    });
    if placeholder {
        rate_limit.remove("secondary_window");
    }
}

fn secondary_window_is_placeholder(window: &Map<String, Value>) -> bool {
    window.iter().all(|(field, value)| match field.as_str() {
        "used_percent" => value
            .as_f64()
            .is_some_and(|used_percent| used_percent.is_finite() && used_percent == 0.0),
        "limit_reached" => value.as_bool() == Some(false),
        _ => false,
    })
}

/// 上游额度可确认套餐变更；同族泛化值不能丢弃 JWT 已给出的具体 SKU
pub(super) fn observed_account_plan(
    current: Option<&str>,
    observed: Option<&str>,
) -> Option<String> {
    let plan = observed?.trim().to_ascii_lowercase();
    if plan.is_empty() || plan == "unknown" {
        return None;
    }
    // 套餐族沿用官方 codex_protocol::account::PlanType 的分类
    let current = current.unwrap_or_default().trim().to_ascii_lowercase();
    let generalized = matches!(
        (plan.as_str(), current.as_str()),
        (
            "team",
            "self_serve_business_prolite" | "self_serve_business_usage_based"
        ) | (
            "business",
            "ent26" | "enterprise_cbp_automation" | "enterprise_cbp_usage_based"
        ) | ("edu" | "education", "edu_plus" | "edu_pro")
    );
    (!generalized).then_some(plan)
}
