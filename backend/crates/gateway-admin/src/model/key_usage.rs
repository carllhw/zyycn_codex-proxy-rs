//! 单页 Key 用量的查询合同；身份范围不由调用方提供

use super::{
    client_keys::ClientKeyRecord,
    observability::{
        HealthTimeline, ObservabilityPageSize, OpsErrorPage, RequestMetricPoint, TimeRange,
        UsageOverview, UsagePage,
    },
};

#[derive(Debug, Clone)]
pub struct KeyUsageQuery {
    pub range: TimeRange,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum KeyUsageRecordKind {
    Success,
    Error,
}

#[derive(Debug, Clone)]
pub struct KeyUsageRecordsQuery {
    pub usage: KeyUsageQuery,
    pub kind: KeyUsageRecordKind,
    pub current_page: u32,
    pub page_size: ObservabilityPageSize,
}

pub struct KeyUsageOverview {
    pub key: ClientKeyRecord,
    pub overview: UsageOverview,
    pub trend: Vec<RequestMetricPoint>,
    pub health_timeline: HealthTimeline,
}

pub enum KeyUsageRecords {
    Success(UsagePage),
    Error(OpsErrorPage),
}
