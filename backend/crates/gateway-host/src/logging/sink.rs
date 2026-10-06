//! 非阻塞文件日志接收与后台批量写入，错误记录保留独立容量，关闭时排空

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use futures::future::BoxFuture;
use gateway_core::health::{HealthProbe, HealthState};
use tracing_subscriber::fmt::MakeWriter;

use super::writer::RotatingLogWriter;

const QUEUE_CAPACITY: usize = 4_096;
const QUEUE_BYTES: usize = 64 * 1024 * 1024;
const ERROR_CAPACITY: usize = 512;
const ERROR_BYTES: usize = 8 * 1024 * 1024;
const WRITE_BATCH: usize = 64;

// 通道不另设容量；预留在复制正文之前完成，并随记录释放，包含正在写入的记录
struct Budget {
    items: AtomicUsize,
    bytes: AtomicUsize,
    maximum_items: usize,
    maximum_bytes: usize,
}

impl Budget {
    fn new(maximum_items: usize, maximum_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            items: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            maximum_items,
            maximum_bytes,
        })
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        self.items
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |items| {
                items
                    .checked_add(1)
                    .filter(|items| *items <= self.maximum_items)
            })
            .ok()?;
        if self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|used| *used <= self.maximum_bytes)
            })
            .is_err()
        {
            self.items.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Reservation {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.items.fetch_sub(1, Ordering::AcqRel);
    }
}

enum Command {
    Write(Vec<u8>, Reservation),
    Shutdown,
}

#[derive(Default)]
pub(super) struct LogHealth {
    write_failures: AtomicU64,
    maintenance_failures: AtomicU64,
    rejected_records: AtomicU64,
    rejected_errors: AtomicU64,
}

impl LogHealth {
    pub(super) fn write_failed(&self, kind: io::ErrorKind) {
        if self.write_failures.fetch_add(1, Ordering::Relaxed) == 0 {
            eprintln!("file_logging: log completeness compromised; write failure ({kind:?})");
        }
    }

    pub(super) fn maintenance_failed(&self, kind: io::ErrorKind) {
        if self.maintenance_failures.fetch_add(1, Ordering::Relaxed) == 0 {
            eprintln!("file_logging: archive/retention maintenance failed ({kind:?})");
        }
    }

    fn rejected(&self, important: bool) {
        self.rejected_records.fetch_add(1, Ordering::Relaxed);
        if important {
            self.rejected_errors.fetch_add(1, Ordering::Relaxed);
        }
        // 接收路径只更新状态，不能回退到 stderr 或递归打印新的拥堵日志
    }
}

impl HealthProbe for LogHealth {
    fn name(&self) -> &'static str {
        "file_logging"
    }

    fn check(&self) -> BoxFuture<'_, HealthState> {
        Box::pin(async move {
            let failures = self.write_failures.load(Ordering::Relaxed);
            let rejected = self.rejected_records.load(Ordering::Relaxed);
            let errors = self.rejected_errors.load(Ordering::Relaxed);
            if failures > 0 || rejected > 0 {
                return HealthState::Unhealthy(format!(
                    "log completeness compromised: {failures} write/flush failures, {rejected} rejected records ({errors} warnings/errors) since startup"
                ));
            }
            let failures = self.maintenance_failures.load(Ordering::Relaxed);
            if failures > 0 {
                return HealthState::Degraded(format!(
                    "log archive/retention maintenance failed {failures} times since startup"
                ));
            }
            HealthState::Healthy
        })
    }
}

#[derive(Clone)]
pub(super) struct FileLogSink {
    sender: mpsc::Sender<Command>,
    regular: Arc<Budget>,
    errors: Arc<Budget>,
    important: bool,
    health: Arc<LogHealth>,
}

pub(super) struct FileLogGuard {
    sender: mpsc::Sender<Command>,
    worker: Option<JoinHandle<()>>,
    health: Arc<LogHealth>,
}

impl FileLogSink {
    pub(super) fn spawn(
        mut writer: RotatingLogWriter,
        name: &'static str,
        health: Arc<LogHealth>,
    ) -> io::Result<(Self, FileLogGuard)> {
        let (sender, receiver) = mpsc::channel();
        let worker_health = Arc::clone(&health);
        let worker = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let mut reservations = Vec::with_capacity(WRITE_BATCH);
                'consume: while let Ok(mut command) = receiver.recv() {
                    for index in 0..WRITE_BATCH {
                        match command {
                            Command::Write(bytes, reservation) => {
                                if let Err(error) = writer.write_all(&bytes) {
                                    worker_health.write_failed(error.kind());
                                }
                                reservations.push(reservation);
                            }
                            Command::Shutdown => break 'consume,
                        }
                        if index + 1 == WRITE_BATCH {
                            break;
                        }
                        match receiver.try_recv() {
                            Ok(next) => command = next,
                            Err(_) => break,
                        }
                    }
                    if let Err(error) = writer.flush() {
                        worker_health.write_failed(error.kind());
                    }
                    // 缓冲刷入文件后才归还额度，慢输出不能让在途记录逃逸预算
                    reservations.clear();
                }
                if let Err(error) = writer.sync() {
                    worker_health.write_failed(error.kind());
                }
            })?;
        Ok((
            Self {
                sender: sender.clone(),
                regular: Budget::new(QUEUE_CAPACITY, QUEUE_BYTES),
                errors: Budget::new(ERROR_CAPACITY, ERROR_BYTES),
                important: false,
                health: Arc::clone(&health),
            },
            FileLogGuard {
                sender,
                worker: Some(worker),
                health,
            },
        ))
    }
}

impl<'a> MakeWriter<'a> for FileLogSink {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }

    fn make_writer_for(&'a self, metadata: &tracing::Metadata<'_>) -> Self::Writer {
        let mut writer = self.clone();
        writer.important = *metadata.level() <= tracing::Level::WARN;
        writer
    }
}

impl Write for FileLogSink {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let bytes = buffer.len().saturating_add(std::mem::size_of::<Command>());
        let reservation = self
            .regular
            .reserve(bytes)
            .or_else(|| self.important.then(|| self.errors.reserve(bytes)).flatten());
        if let Some(reservation) = reservation {
            if self
                .sender
                .send(Command::Write(buffer.to_vec(), reservation))
                .is_err()
            {
                self.health.rejected(self.important);
            }
        } else {
            self.health.rejected(self.important);
        }
        // tracing 的 writer 失败回退可能同步写 stderr；缺口由独立健康状态报告
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for FileLogGuard {
    fn drop(&mut self) {
        // 仅进程关闭时等待已接收日志落盘，请求侧不承担排空责任
        let _ = self.sender.send(Command::Shutdown);
        if self
            .worker
            .take()
            .is_some_and(|worker| worker.join().is_err())
        {
            self.health.write_failed(io::ErrorKind::BrokenPipe);
        }
    }
}
