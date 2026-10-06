//! 原因链与上游正文的受控诊断投影，不参与普通错误格式化

use super::{ErrorSource, RawUpstreamError};

#[derive(Clone)]
pub struct ErrorDetails(String);

impl ErrorDetails {
    #[must_use]
    pub fn capture(
        source: Option<&ErrorSource>,
        upstream: Option<&RawUpstreamError>,
        redacted: bool,
    ) -> Option<Self> {
        if source.is_none() && upstream.is_none() {
            return None;
        }
        Some(Self(
            serde_json::json!({
                "causes": source.map(ErrorSource::snapshot),
                "upstream": upstream.map(RawUpstreamError::as_str),
                "redacted": redacted,
            })
            .to_string(),
        ))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Debug for ErrorDetails {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ErrorDetails(<restricted>)")
    }
}
