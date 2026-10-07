//! 验证运行时整体关闭回收当前代次并拒绝新候选

use gateway_core::routing::ConfigRevision;
use gateway_core::routing::extensions::ExtensionPreparationPort;

#[tokio::test]
async fn runtime_shutdown_reaps_live_generation_and_rejects_late_preparation() {
    let (cache, store, runtime) = super::super::setup().await;
    let published = ExtensionPreparationPort::prepare(&runtime, ConfigRevision::new(1).unwrap())
        .await
        .unwrap();
    assert!(published.is_ready());
    assert_eq!(std::fs::read_dir(cache.path()).unwrap().count(), 1);

    runtime.shutdown().await;
    assert!(!published.is_ready());
    super::super::wait_until_empty(cache.path()).await;
    assert!(
        ExtensionPreparationPort::prepare(&runtime, ConfigRevision::new(1).unwrap())
            .await
            .is_err()
    );
    runtime.shutdown().await;

    drop(published);
    drop(store);
}
