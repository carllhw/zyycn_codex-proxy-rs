//! 更新日志事件的有界广播、递增标识与终态标记

use chrono::Utc;
use gateway_admin::model::system::{SystemUpdateEvent, SystemUpdateEventLevel};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

pub(super) struct UpdateEvents {
    sender: broadcast::Sender<(SystemUpdateEvent, bool)>,
    sequence: AtomicU64,
}

impl Default for UpdateEvents {
    fn default() -> Self {
        let (sender, _) = broadcast::channel(256);
        Self {
            sender,
            sequence: AtomicU64::new(0),
        }
    }
}

impl UpdateEvents {
    pub(super) fn subscribe(&self) -> broadcast::Receiver<(SystemUpdateEvent, bool)> {
        self.sender.subscribe()
    }

    pub(super) fn info(
        &self,
        operation_id: Option<&str>,
        step: Option<&str>,
        message: impl Into<String>,
    ) {
        self.emit(
            SystemUpdateEventLevel::Info,
            operation_id,
            step,
            message,
            None,
            false,
        );
    }

    pub(super) fn success(
        &self,
        operation_id: Option<&str>,
        step: Option<&str>,
        message: impl Into<String>,
    ) {
        self.emit(
            SystemUpdateEventLevel::Success,
            operation_id,
            step,
            message,
            None,
            false,
        );
    }

    pub(super) fn success_terminal(
        &self,
        operation_id: Option<&str>,
        step: Option<&str>,
        message: impl Into<String>,
    ) {
        self.emit(
            SystemUpdateEventLevel::Success,
            operation_id,
            step,
            message,
            Some(100),
            true,
        );
    }

    pub(super) fn error_terminal(
        &self,
        operation_id: Option<&str>,
        step: Option<&str>,
        message: impl Into<String>,
    ) {
        self.emit(
            SystemUpdateEventLevel::Error,
            operation_id,
            step,
            message,
            None,
            true,
        );
    }

    pub(super) fn emit_progress(&self, operation_id: &str, message: &str, progress: u8) {
        self.emit(
            SystemUpdateEventLevel::Info,
            Some(operation_id),
            Some("download"),
            message,
            Some(progress),
            false,
        );
    }

    fn emit(
        &self,
        level: SystemUpdateEventLevel,
        operation_id: Option<&str>,
        step: Option<&str>,
        message: impl Into<String>,
        progress_percent: Option<u8>,
        terminal: bool,
    ) {
        let now = Utc::now();
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let event = SystemUpdateEvent {
            id: format!(
                "update-log-{}-{sequence}",
                now.timestamp_nanos_opt()
                    .unwrap_or_else(|| now.timestamp_millis())
            ),
            operation_id: operation_id.map(str::to_owned),
            level,
            step: step.map(str::to_owned),
            message: message.into(),
            terminal,
            progress_percent,
            occurred_at: now,
        };
        let _ = self.sender.send((event, terminal));
    }
}
