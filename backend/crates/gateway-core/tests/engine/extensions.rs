//! 执行扩展索引的代次隔离、计划保活与租约替身

use std::sync::Arc;

use gateway_core::engine::extensions::{ExecutionExtensionIndex, ExecutionExtensionPlans};
use gateway_core::engine::upstream_adapter::{UpstreamAdapter, UpstreamAdapterPlan};
use gateway_core::routing::extensions::{ExtensionSetId, ExtensionSetLease, ExtensionSetReference};
use gateway_core::{
    engine::AttemptContext, error::ProviderError, identity::ProviderKind, routing::UpstreamModelId,
};

struct ReadyLease;

impl ExtensionSetLease for ReadyLease {
    fn is_ready(&self) -> bool {
        true
    }
}

pub(super) fn reference(id: &str) -> ExtensionSetReference {
    let id = ExtensionSetId::new(id.into()).unwrap();
    ExtensionSetReference::new(id, Arc::new(ReadyLease))
}

#[derive(Debug)]
struct EmptyAdapterPlan;

impl UpstreamAdapterPlan for EmptyAdapterPlan {
    fn select(
        &self,
        _: &AttemptContext,
        _: &ProviderKind,
        _: &UpstreamModelId,
    ) -> Result<Option<Arc<dyn UpstreamAdapter>>, ProviderError> {
        Ok(None)
    }
}

struct PublishedPlans(Arc<ExecutionExtensionPlans>);

impl ExtensionSetLease for PublishedPlans {
    fn is_ready(&self) -> bool {
        self.0.upstream_adapters.is_some()
    }
}

#[test]
fn adapter_only_generation_stays_alive_until_the_last_frozen_plan_is_released() {
    let index = ExecutionExtensionIndex::default();
    let id = ExtensionSetId::new("adapter-only".into()).unwrap();
    let plans = Arc::new(ExecutionExtensionPlans {
        middleware: None,
        upstream_adapters: Some(Arc::new(EmptyAdapterPlan)),
    });
    let weak_plans = Arc::downgrade(&plans);
    let published = Arc::new(PublishedPlans(index.register(id.clone(), plans).unwrap()));
    let weak_publication = Arc::downgrade(&published);
    let reference = ExtensionSetReference::new(id.clone(), published);
    assert!(index.middleware(&reference).is_none());
    let frozen = index.upstream_adapters(&reference).unwrap();
    drop(reference);

    // 当前发布可以退场；在途计划仍保活整个代次，不能覆盖相同身份
    assert!(weak_publication.upgrade().is_some());
    assert!(weak_plans.upgrade().is_some());
    assert!(index.register(id.clone(), Arc::default()).is_err());
    let in_flight = frozen.clone();
    drop(frozen);
    assert!(weak_publication.upgrade().is_some());
    drop(in_flight);

    assert!(weak_publication.upgrade().is_none());
    assert!(weak_plans.upgrade().is_none());
    let released = ExtensionSetReference::new(id.clone(), Arc::new(ReadyLease));
    assert!(index.upstream_adapters(&released).is_none());
    let _replacement = index.register(id, Arc::default()).unwrap();
}

#[test]
fn execution_index_does_not_own_registered_generations() {
    let index = ExecutionExtensionIndex::default();
    let reference = reference("unpublished");
    let plans = Arc::new(ExecutionExtensionPlans::default());
    let weak = Arc::downgrade(&plans);
    let owner = index.register(reference.id().clone(), plans).unwrap();
    assert!(weak.upgrade().is_some());
    drop(owner);
    assert!(weak.upgrade().is_none());
    assert!(index.middleware(&reference).is_none());
    assert!(index.upstream_adapters(&reference).is_none());
}
