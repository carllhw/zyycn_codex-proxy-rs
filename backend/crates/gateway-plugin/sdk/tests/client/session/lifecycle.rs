//! 会话期限、取消钩子、排空与关闭测试

use super::*;

#[tokio::test]
async fn call_deadline_cancels_handler_and_keeps_the_session_available() {
    let handler = TestHandler::default();
    let state = Arc::clone(&handler.lifecycle);
    let (mut host, task) = start_session(handler).await;
    send_call_with_timeout(
        &mut host,
        1,
        "pending",
        json!({}),
        Vec::new(),
        Duration::from_millis(20),
    )
    .await;
    assert!(matches!(
        receive(&mut host).await.message,
        Message::Error {
            id: 1,
            error: PluginFault {
                code: ErrorCode::Timeout,
                ..
            },
        }
    ));
    tokio::time::timeout(Duration::from_millis(100), async {
        while !state.cancel_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deadline did not notify the lifecycle hook");

    send_call(&mut host, 3, "echo", json!("still-ready"), Vec::new()).await;
    assert!(matches!(
        receive(&mut host).await.message,
        Message::Result { id: 3, result } if result == json!("still-ready")
    ));
    shutdown(&mut host, task).await;
}

#[tokio::test]
async fn blocking_cancel_hook_cannot_delay_cancelled_or_eof_settlement() {
    let handler = TestHandler {
        cancel_delay: Some(Duration::from_millis(200)),
        ..TestHandler::default()
    };
    let state = Arc::clone(&handler.lifecycle);
    let (mut host, task) = start_session(handler).await;
    send_call(&mut host, 1, "pending", json!({}), Vec::new()).await;
    send_control(&mut host, Message::Cancel { id: 1 }).await;

    let cancelled = tokio::time::timeout(Duration::from_millis(50), receive(&mut host))
        .await
        .expect("blocking lifecycle hook delayed Cancelled");
    assert!(matches!(cancelled.message, Message::Cancelled { id: 1 }));
    tokio::time::timeout(Duration::from_millis(50), async {
        while !state.cancel_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancel hook never started");

    host.writer.shutdown().await.unwrap();
    let result = tokio::time::timeout(Duration::from_millis(50), task)
        .await
        .expect("blocking lifecycle hook delayed EOF settlement")
        .expect("plugin session task panicked");
    assert!(matches!(result, Err(SessionError::Closed)));
}

#[tokio::test]
async fn quiesce_rejects_new_calls_and_shutdown_closes_the_session() {
    let handler = TestHandler::default();
    let state = Arc::clone(&handler.lifecycle);
    let (mut host, task) = start_session(handler).await;
    send_control(&mut host, Message::Quiesce).await;
    send_call(&mut host, 1, "echo", json!({}), Vec::new()).await;

    assert!(matches!(
        receive(&mut host).await.message,
        Message::Error {
            id: 1,
            error: PluginFault {
                code: ErrorCode::Capacity,
                ..
            },
        }
    ));
    send_control(
        &mut host,
        Message::Credit {
            id: 1,
            bytes: 1,
            frames: 1,
        },
    )
    .await;
    tokio::time::timeout(Duration::from_millis(50), async {
        while !state.quiesced.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiesce hook was not signalled");

    shutdown(&mut host, task).await;
}
