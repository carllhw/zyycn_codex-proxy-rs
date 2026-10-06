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

    /// 仅供受控错误详情落盘，限制链深度和总文本大小并显式标记缺口
    pub(super) fn snapshot(&self) -> serde_json::Value {
        let mut messages = Vec::new();
        let mut source: Option<&(dyn Error + 'static)> = Some(self.0.as_ref());
        let mut remaining = 64 * 1024;
        let mut truncated = false;
        for _ in 0..32 {
            let Some(error) = source else { break };
            let mut message = BoundedMessage {
                text: String::new(),
                remaining,
            };
            if fmt::write(&mut message, format_args!("{error}")).is_err() {
                truncated = true;
            }
            remaining = message.remaining;
            messages.push(message.text);
            source = error.source();
            if truncated || remaining == 0 {
                break;
            }
        }
        serde_json::json!({"messages": messages, "truncated": truncated || source.is_some()})
    }
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
