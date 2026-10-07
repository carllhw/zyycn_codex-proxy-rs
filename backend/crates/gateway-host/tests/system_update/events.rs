//! 系统更新事件的订阅、阶段进度与终态可见性测试

use super::*;

#[tokio::test]
async fn update_events_should_close_after_terminal_update_log() {
    let fixture = Fixture::new();
    let mut config = fixture.config("https://api.github.com/repos");
    config.build_type = "source".to_owned();
    let service = ProcessSystemOperations::new(CancellationToken::new(), config);
    let mut events = service.update_events();
    let _ = service
        .perform_test_update(Some(TARGET_VERSION.to_owned()))
        .await;
    let first = events.next().await.expect("terminal event");

    assert_eq!(first.level, SystemUpdateEventLevel::Error);
    assert!(events.next().await.is_none());
}

#[tokio::test]
async fn update_events_should_open_authenticated_sse_stream() {
    let fixture = Fixture::new();
    let mut config = fixture.config("https://api.github.com/repos");
    config.build_type = "source".to_owned();
    let service = ProcessSystemOperations::new(CancellationToken::new(), config);
    let mut stream = service.update_events();
    let _ = service
        .perform_test_update(Some(TARGET_VERSION.to_owned()))
        .await;

    assert!(stream.next().await.is_some());
}

#[tokio::test]
async fn update_events_should_preserve_complete_release_stage_sequence() {
    let server = MockServer::start().await;
    let fixture = Fixture::new();
    fixture
        .mount_release(
            &server,
            TARGET_VERSION,
            ArchiveKind::Safe,
            ChecksumKind::Valid,
        )
        .await;
    let service = fixture.service(&server);
    let events = service.update_events();
    assert_eq!(
        complete_update(&service, TARGET_VERSION)
            .await
            .operation
            .status,
        SystemOperationStatus::Succeeded
    );
    let steps = events
        .map(|event| event.step.expect("release update step"))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        steps,
        [
            "release",
            "prepare",
            "asset",
            "asset",
            "verify",
            "prepare",
            "download",
            "download",
            "download",
            "checksum",
            "checksum",
            "extract",
            "extract",
            "preflight",
            "preflight",
            "replace",
            "replace",
            "done",
        ]
    );
}

#[tokio::test]
async fn update_events_should_report_download_bytes_and_percent() {
    let server = MockServer::start().await;
    let fixture = Fixture::new();
    fixture
        .mount_release(
            &server,
            TARGET_VERSION,
            ArchiveKind::Safe,
            ChecksumKind::Valid,
        )
        .await;
    let service = fixture.service(&server);
    let events = service.update_events();
    assert_eq!(
        complete_update(&service, TARGET_VERSION)
            .await
            .operation
            .status,
        SystemOperationStatus::Succeeded
    );
    let progress = events
        .filter_map(
            |event| async move { event.progress_percent.map(|value| (value, event.message)) },
        )
        .collect::<Vec<_>>()
        .await;

    assert!(progress.iter().any(|(percent, message)| {
        *percent == 100 && message.starts_with("已下载 ") && message.ends_with(" (100%)")
    }));
}

#[tokio::test]
async fn terminal_event_should_only_be_visible_after_status_is_persisted() {
    let upstream = MockServer::start().await;
    let fixture = Fixture::new();
    fixture
        .mount_release(
            &upstream,
            TARGET_VERSION,
            ArchiveKind::Safe,
            ChecksumKind::Mismatch,
        )
        .await;
    let service = fixture.service(&upstream);
    let mut events = service.update_events();
    let SystemOperationAccepted::Update { operation_id, .. } = service
        .perform_test_update(Some(TARGET_VERSION.to_owned()))
        .await
        .expect("accepted")
    else {
        panic!("update")
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = events.next().await {
            assert_eq!(event.operation_id.as_deref(), Some(operation_id.as_str()));
            if event.terminal {
                let status = service.update_status().await.expect("terminal status");
                assert_eq!(status.operation.status, SystemOperationStatus::Failed);
                assert!(status.operation.finished_at.is_some());
                return;
            }
        }
        panic!("missing terminal event");
    })
    .await
    .expect("terminal event");
}
