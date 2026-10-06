//! 跨领域边界共享原始错误来源，不在普通格式化中展开详情

use std::{error::Error, fmt, ops::Deref, sync::Arc};

/// 保留底层错误的类型和来源链，避免领域端口依赖具体基础设施
#[derive(Clone)]
pub struct ErrorSource(Arc<dyn Error + Send + Sync>);

impl ErrorSource {
    #[must_use]
    pub fn new(error: impl Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(error))
    }

    /// 清理失败是附属事实，标准来源链仍指向最初失败
    #[must_use]
    pub fn with_cleanup(self, cleanup: impl Into<Self>) -> Self {
        Self::new(CleanupFailure {
            primary: Some(self),
            cleanup: cleanup.into(),
        })
    }

    /// 附属清理错误与可选的原始底层原因分别保留
    #[must_use]
    pub fn cleanup(primary: Option<Self>, cleanup: impl Into<Self>) -> Self {
        Self::new(CleanupFailure {
            primary,
            cleanup: cleanup.into(),
        })
    }

    /// 仅供受控错误详情落盘，限制链深度和总文本大小并显式标记缺口
    pub(super) fn snapshot(&self) -> serde_json::Value {
        snapshot(self.0.as_ref(), &mut 32, &mut (64 * 1024))
    }

    /// 观测队列估算来源占用，包含附属清理失败；过深或循环的来源按超限处理
    #[must_use]
    pub fn estimated_chain_bytes(source: Option<&(dyn Error + 'static)>) -> usize {
        struct ByteCount(usize);
        impl fmt::Write for ByteCount {
            fn write_str(&mut self, value: &str) -> fmt::Result {
                self.0 = self.0.saturating_add(value.len());
                Ok(())
            }
        }
        let mut pending = Vec::from_iter(source);
        let mut bytes = ByteCount(0);
        for _ in 0..32 {
            let Some(error) = pending.pop() else {
                return bytes.0;
            };
            bytes.0 = bytes.0.saturating_add(std::mem::size_of_val(error));
            if fmt::write(&mut bytes, format_args!("{error}")).is_err() {
                return usize::MAX;
            }
            if let Some(cleanup) = error.downcast_ref::<CleanupFailure>() {
                pending.push(cleanup.cleanup.0.as_ref());
            }
            if let Some(source) = error.source() {
                pending.push(source);
            }
        }
        if pending.is_empty() {
            bytes.0
        } else {
            usize::MAX
        }
    }
}

impl<E: Error + Send + Sync + 'static> From<E> for ErrorSource {
    fn from(error: E) -> Self {
        Self::new(error)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("operation failed and cleanup also failed")]
struct CleanupFailure {
    #[source]
    primary: Option<ErrorSource>,
    cleanup: ErrorSource,
}

fn snapshot(
    error: &(dyn Error + 'static),
    nodes: &mut usize,
    bytes: &mut usize,
) -> serde_json::Value {
    let mut messages = Vec::new();
    let mut cleanup_sources = Vec::new();
    let mut source = Some(error);
    let mut truncated = false;
    while *nodes > 0 {
        let Some(error) = source else { break };
        *nodes -= 1;
        if let Some(failure) = error.downcast_ref::<CleanupFailure>() {
            cleanup_sources.push(failure.cleanup.0.as_ref());
            source = failure
                .primary
                .as_ref()
                .map(|primary| primary.0.as_ref() as &(dyn Error + 'static));
            continue;
        }
        let mut message = BoundedMessage {
            text: String::new(),
            remaining: *bytes,
        };
        if fmt::write(&mut message, format_args!("{error}")).is_err() {
            truncated = true;
        }
        *bytes = message.remaining;
        messages.push(message.text);
        source = error.source();
        if truncated || *bytes == 0 {
            break;
        }
    }
    let mut result =
        serde_json::json!({"messages": messages, "truncated": truncated || source.is_some()});
    if !cleanup_sources.is_empty() {
        result["cleanup"] = serde_json::Value::Array(
            cleanup_sources
                .into_iter()
                .map(|error| snapshot(error, nodes, bytes))
                .collect(),
        );
    }
    result
}

struct BoundedMessage {
    text: String,
    remaining: usize,
}

impl fmt::Write for BoundedMessage {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let end = value.floor_char_boundary(self.remaining.min(value.len()));
        self.text.push_str(&value[..end]);
        self.remaining -= end;
        if end == value.len() {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

impl Deref for ErrorSource {
    type Target = dyn Error + Send + Sync;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl fmt::Debug for ErrorSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ErrorSource(<restricted>)")
    }
}
