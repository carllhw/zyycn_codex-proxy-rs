//! 验证管理调用绑定、执行身份与候选配置

use gateway_admin::model::plugins::instances::PluginCapabilityBinding;
use gateway_admin::model::plugins::instances::PluginFailurePolicy;
use gateway_admin::ports::plugins::PluginPreparation;
use gateway_plugin_sdk::Capability;
use gateway_plugin_sdk::Contributions;
use gateway_plugin_sdk::Stage;

#[tokio::test]
async fn management_needs_no_binding_and_rejects_stale_execution_identity_configuration() {
    let (_cache, store, runtime) = super::super::setup_with_contributions(Contributions::from([
        crate::support::contribution(
            Capability::Management,
            vec![Stage::Management],
            Vec::new(),
            Vec::new(),
        ),
    ]))
    .await;
    let mut instance = store.snapshot.lock().unwrap().instances[0].clone();
    assert!(
        PluginPreparation::validate(&runtime, instance.clone())
            .await
            .is_ok()
    );
    instance.bindings = vec![PluginCapabilityBinding {
        contribution: "test.example.management".into(),
        stage: "management".into(),
        order: 0,
        failure_policy: PluginFailurePolicy::Reject,
        client_key_ids: vec!["key-one".into(), "key-two".into()],
        account_group_ids: Vec::new(),
        provider_ids: Vec::new(),
        models: Vec::new(),
        event: None,
        identity_bindings: Vec::new(),
    }];
    assert!(
        PluginPreparation::validate(&runtime, instance.clone())
            .await
            .is_err()
    );
    instance.enabled = false;
    assert!(
        PluginPreparation::validate(&runtime, instance)
            .await
            .is_err()
    );
}
