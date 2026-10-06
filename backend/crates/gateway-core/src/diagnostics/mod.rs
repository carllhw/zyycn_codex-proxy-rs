//! 请求诊断与操作故障的观测合同，不参与路由、重试或客户端协议决策

mod capture;
mod failure;
mod selection;
mod stream;
mod trace;

pub use capture::{body_fingerprint, diagnostic_headers, diagnostic_json};
pub use failure::{OperationalDiagnostics, OperationalFailure};
pub use stream::{StreamCapture, StreamFormat};
pub use trace::TraceContext;
