//! 验证候选复用与最后一个代次持有者释放后的回收

use gateway_admin::model::Revision;
use gateway_admin::ports::plugins::PluginPreparation;
use gateway_core::routing::ConfigRevision;
use gateway_core::routing::extensions::ExtensionPreparationPort;

#[tokio::test]
async fn candidate_is_reused_after_commit_and_released_when_no_generation_owner_remains() {
    let (cache, store, runtime) = super::super::setup().await;
    let mut snapshot = store.snapshot.lock().unwrap().clone();
    snapshot.instances[0].configuration =
        serde_json::from_str(r#"{"z":1,"a":{"z":2,"a":3}}"#).unwrap();
    let candidate = PluginPreparation::prepare(&runtime, snapshot.clone())
        .await
        .unwrap();
    assert!(
        runtime.observer_registry().resolve(&candidate).is_none(),
        "没有观察绑定时不应编译派发计划"
    );
    snapshot.instances[0].configuration =
        serde_json::from_str(r#"{"a":{"a":3,"z":2},"z":1}"#).unwrap();
    snapshot.config_revision = Revision::new(2).unwrap();
    *store.snapshot.lock().unwrap() = snapshot;
    let published = ExtensionPreparationPort::prepare(&runtime, ConfigRevision::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(candidate.id(), published.id());
    assert!(published.is_ready());
    drop(candidate);
    assert_eq!(std::fs::read_dir(cache.path()).unwrap().count(), 1);
    drop(published);
    super::super::wait_until_empty(cache.path()).await;
}
