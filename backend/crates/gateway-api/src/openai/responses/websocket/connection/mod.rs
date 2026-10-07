//! 下游客户端 WebSocket 的连接合同、句柄与单 owner pump

mod frame;
mod handle;
mod middleware;
mod pump;
mod state;

pub use frame::{
    ConnectionConfig, ConnectionEvent, ConnectionWriteError, FramePhase, PumpExitReason,
    WriteContext, WriteOutcome,
};
pub use handle::{ResponsesWebSocketConnection, spawn_connection};
