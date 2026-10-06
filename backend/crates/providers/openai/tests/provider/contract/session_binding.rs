//! 验证最终请求身份、官方图片轮次关联与共享账号迁移

use super::*;

#[derive(Debug)]
struct ChangeTurnMetadata(&'static [u8]);

impl MiddlewarePlan for ChangeTurnMetadata {
    fn handle(
        &self,
        _context: MiddlewareContext,
        request: MiddlewareRequest,
        next: MiddlewareNext,
    ) -> BoxFuture<'static, Result<MiddlewareResponse, MiddlewareError>> {
        let metadata = self.0;
        Box::pin(async move {
            let (protocol, mut headers, body) = request.into_parts();
            headers.push(MiddlewareHeader::new(
                "x-codex-turn-metadata",
                Bytes::from_static(metadata),
            ));
            next.run(MiddlewareRequest::new(protocol, headers, body))
                .await
        })
    }
}

#[tokio::test]
async fn final_middleware_session_header_cannot_use_another_sessions_account() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url(&store, affinity, server.uri());
    let request = |session| {
        planned_request(
            "openai",
            Operation::Generate(generate_with_session_context(session, None, None)),
        )
    };
    drop(
        provider
            .clone()
            .execute(
                request("session-a"),
                context("req_seed_a", CancellationToken::new()),
            )
            .await
            .unwrap(),
    );
    create_account(&store, "acct_subagent_b").await;
    store.set_scheduling("acct_subagent_b", None, AccountWeight::new(100).unwrap());
    drop(
        provider
            .clone()
            .execute(
                request("session-b"),
                context("req_seed_b", CancellationToken::new()),
            )
            .await
            .unwrap(),
    );
    let error = provider
        .execute(
            request("session-a"),
            context_with_middleware(
                "req_final_identity",
                Arc::new(ChangeTurnMetadata(br#"{"session_id":"session-b"}"#)),
                FastMode::Default,
            ),
        )
        .await
        .err()
        .expect("mismatched owner must fail before send");
    assert!(error.retry_is_prohibited());
    assert!(server.received_requests().await.unwrap().is_empty());
}

fn turn_request(session: &str, turn: &str) -> Operation {
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", json!({
            "model":"gpt-5.4", "input":"hello",
            "client_metadata":{"x-codex-turn-metadata":json!({"session_id":session,"thread_id":session,"turn_id":turn}).to_string()}
        }).as_object().unwrap().clone()).unwrap()
        .with_context(Map::from_iter([("use_websocket".into(), json!(false))])),
    ))
}

fn image_for_turn(kind: ImageRequestKind, turn: &str) -> Operation {
    Operation::GenerateImage(ImageRequest::from_raw_json(
        kind,
        RawJsonPayload::new(
            "openai",
            Bytes::from_static(br#"{"prompt":"a square","model":"gpt-image-1"}"#),
        )
        .unwrap()
        .with_context(Map::from_iter([("image_turn_id".into(), json!(turn))])),
    ))
}

#[tokio::test]
async fn official_image_turn_uses_current_session_owner_even_after_migration() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url(&store, affinity.clone(), server.uri());
    drop(
        provider
            .clone()
            .execute(
                planned_request("openai", turn_request("root", "known-turn")),
                context("req_seed_turn", CancellationToken::new()),
            )
            .await
            .unwrap(),
    );
    create_account(&store, "acct_subagent_b").await;
    store.set_scheduling("acct_subagent_b", None, AccountWeight::new(100).unwrap());
    for expected in ["acct_subagent_a", "acct_subagent_b"] {
        if expected == "acct_subagent_b" {
            let current = store.account("acct_subagent_a").unwrap();
            store.set_enabled(current.id(), false).await.unwrap();
            drop(
                provider
                    .clone()
                    .execute(
                        planned_request("openai", turn_request("root", "next-turn")),
                        context("req_migrate_turn", CancellationToken::new()),
                    )
                    .await
                    .unwrap(),
            );
        }
        for kind in [ImageRequestKind::Generation, ImageRequestKind::Edit] {
            let stream = provider
                .clone()
                .execute(
                    planned_provider_endpoint_request("openai", image_for_turn(kind, "known-turn")),
                    context("req_known_image_turn", CancellationToken::new()),
                )
                .await
                .unwrap();
            assert_eq!(stream.metadata().provider_account_id().as_str(), expected);
        }
        assert_eq!(affinity.binding_count(), 1);
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn unknown_image_turn_does_not_infer_a_conversation() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let provider = provider_with_affinity(&store, affinity.clone());
    drop(
        provider
            .execute(
                planned_provider_endpoint_request(
                    "openai",
                    image_for_turn(ImageRequestKind::Generation, "unknown-turn"),
                ),
                context("req_unknown_image_turn", CancellationToken::new()),
            )
            .await
            .unwrap(),
    );
    assert_eq!(affinity.binding_count(), 0);
}

#[tokio::test]
async fn child_waits_for_first_root_claim_and_cancellation_releases_waiter() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let provider = provider_with_affinity(&store, affinity.clone());
    let child = || {
        planned_request(
            "openai",
            Operation::Generate(generate_with_session_context("root", Some("child"), None)),
        )
    };
    let cancel = CancellationToken::new();
    let mut pending = Box::pin(
        provider
            .clone()
            .execute(child(), context("req_wait_before_root", cancel.clone())),
    );
    assert!(
        timeout(Duration::from_millis(150), pending.as_mut())
            .await
            .is_err()
    );
    assert_eq!(
        affinity.binding_count(),
        0,
        "a child must not claim the first account"
    );
    cancel.cancel();
    let error = pending.await.err().expect("cancelled child");
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
    assert!(error.retry_is_prohibited());
    let mut pending = Box::pin(
        provider
            .clone()
            .execute(child(), context("req_wait_again", CancellationToken::new())),
    );
    assert!(
        timeout(Duration::from_millis(150), pending.as_mut())
            .await
            .is_err()
    );
    let root = provider
        .clone()
        .execute(
            planned_request("openai", turn_request("root", "root-turn")),
            context("req_first_root", CancellationToken::new()),
        )
        .await
        .unwrap();
    let selected = root.metadata().provider_account_id().clone();
    drop(root);
    let resumed = timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.metadata().provider_account_id(), &selected);
    assert_eq!(affinity.binding_count(), 1);
}

#[tokio::test]
async fn descendant_images_wait_for_the_owner_even_when_ordinary_queues_are_disabled() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let leases = Arc::new(TestLeaseCoordinator::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url_and_leases(
        &store,
        affinity.clone(),
        server.uri(),
        leases.clone(),
    );
    drop(
        provider
            .clone()
            .execute(
                planned_request("openai", turn_request("root", "root-turn")),
                context("req_seed_root", CancellationToken::new()),
            )
            .await
            .unwrap(),
    );
    let child_turn = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", json!({"model":"gpt-5.4","input":"child","client_metadata":{"session_id":"root","thread_id":"child","turn_id":"child-turn"}}).as_object().unwrap().clone()).unwrap()
    ));
    drop(
        provider
            .clone()
            .execute(
                planned_request("openai", child_turn),
                context("req_seed_child", CancellationToken::new()),
            )
            .await
            .unwrap(),
    );
    create_account(&store, "acct_subagent_b").await;
    leases
        .busy_accounts
        .lock()
        .unwrap()
        .insert(ProviderAccountId::new("acct_subagent_a").unwrap());
    for (kind, turn, explicit_child) in [
        (ImageRequestKind::Generation, "child-turn", false),
        (ImageRequestKind::Edit, "child-turn", false),
        (ImageRequestKind::Generation, "root-turn", true),
        (ImageRequestKind::Edit, "root-turn", true),
    ] {
        let image = if explicit_child {
            Operation::GenerateImage(ImageRequest::from_raw_json(
                kind,
                RawJsonPayload::new(
                    "openai",
                    Bytes::from_static(
                        br#"{"prompt":"a square","model":"gpt-image-1","session_id":"root"}"#,
                    ),
                )
                .unwrap()
                .with_context(Map::from_iter([
                    ("image_turn_id".into(), json!(turn)),
                    (
                        "turn_metadata".into(),
                        json!(json!({"session_id":"root","thread_id":"child"}).to_string()),
                    ),
                ])),
            ))
        } else {
            image_for_turn(kind, turn)
        };
        let mut pending = Box::pin(provider.clone().execute(
            planned_provider_endpoint_request("openai", image),
            context("req_child_image_wait", CancellationToken::new()),
        ));
        assert!(
            timeout(Duration::from_millis(150), pending.as_mut())
                .await
                .is_err()
        );
        assert!(
            leases
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request.account_id().as_str() == "acct_subagent_a")
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        leases.busy_accounts.lock().unwrap().clear();
        let stream = timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stream.metadata().provider_account_id().as_str(),
            "acct_subagent_a"
        );
        drop(stream);
        leases
            .busy_accounts
            .lock()
            .unwrap()
            .insert(ProviderAccountId::new("acct_subagent_a").unwrap());
    }
}

#[tokio::test]
async fn child_queue_timeout_does_not_rebind_or_allow_provider_fallback() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let provider = provider_with_affinity(&store, affinity.clone());
    let attempt = AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_child_timeout").unwrap(),
            ClientApiKeyId::new("key_openai_contract").unwrap(),
        ),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(5),
        account_policy().with_queue(gateway_core::concurrency::ConcurrencyQueuePolicy {
            max_waiting: 1,
            timeout: Duration::from_millis(120),
        }),
        AccountAttemptContext::new(BTreeSet::new(), None, None)
            .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    );
    let error = provider
        .execute(
            planned_request(
                "openai",
                Operation::Generate(generate_with_session_context("root", Some("child"), None)),
            ),
            attempt,
        )
        .await
        .err()
        .expect("queue timeout");
    assert_eq!(error.kind(), ProviderErrorKind::ConcurrencyQueueTimeout);
    assert!(error.retry_is_prohibited());
    assert_eq!(affinity.binding_count(), 0);
}

#[derive(Debug)]
struct SessionScheduler {
    explicit: bool,
}
impl gateway_core::engine::policy::RequestPolicyPlan for SessionScheduler {
    fn route_model(
        &self,
        _: gateway_core::engine::policy::ModelRouteInput,
    ) -> BoxFuture<
        'static,
        Result<
            gateway_core::engine::policy::ModelRouteDecision,
            gateway_core::engine::policy::RequestPolicyFault,
        >,
    > {
        Box::pin(async { Ok(gateway_core::engine::policy::ModelRouteDecision::Unhandled) })
    }
    fn schedule_account(
        &self,
        input: gateway_core::engine::policy::AccountScheduleInput,
    ) -> BoxFuture<
        'static,
        Result<
            gateway_core::engine::policy::AccountScheduleDecision,
            gateway_core::engine::policy::RequestPolicyFault,
        >,
    > {
        use gateway_core::engine::policy::AccountScheduleDecision;
        assert!(
            input
                .candidates()
                .iter()
                .any(|candidate| candidate.account_id().as_str() == "acct_subagent_b")
        );
        let decision = if self.explicit {
            AccountScheduleDecision::Pick(ProviderAccountId::new("acct_subagent_b").unwrap())
        } else {
            AccountScheduleDecision::Delegate
        };
        Box::pin(async move { Ok(decision) })
    }
}
struct SessionExtensionLease;
impl gateway_core::runtime::extensions::ExtensionSetLease for SessionExtensionLease {
    fn is_ready(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn child_binding_only_constrains_builtin_scheduling_and_preserves_plugin_choices() {
    use gateway_core::engine::policy::RequestPolicyContext;
    use gateway_core::runtime::extensions::{ExtensionSetId, ExtensionSetReference};
    for explicit in [false, true] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_subagent_a").await;
        let affinity = Arc::new(MemorySessionAffinity::default());
        let leases = Arc::new(TestLeaseCoordinator::default());
        let server = MockServer::start().await;
        let provider = provider_with_affinity_and_base_url_and_leases(
            &store,
            affinity,
            server.uri(),
            leases.clone(),
        );
        drop(
            provider
                .clone()
                .execute(
                    planned_request("openai", turn_request("root", "root-turn")),
                    context("req_plugin_root", CancellationToken::new()),
                )
                .await
                .unwrap(),
        );
        create_account(&store, "acct_subagent_b").await;
        leases
            .busy_accounts
            .lock()
            .unwrap()
            .insert(ProviderAccountId::new("acct_subagent_a").unwrap());
        let id = ModelRequestId::new("req_plugin_child").unwrap();
        let key = ClientApiKeyId::new("key_openai_contract").unwrap();
        let policy = RequestPolicyContext::new(
            Arc::new(SessionScheduler { explicit }),
            ExtensionSetReference::new(
                ExtensionSetId::new("session-choice".into()).unwrap(),
                Arc::new(SessionExtensionLease),
            ),
            id.clone(),
            key.clone(),
            vec![],
        );
        let attempt = AttemptContext::new(
            RequestAttemptContext::new(id, key).with_request_policy(Some(policy)),
            NonZeroU32::new(1).unwrap(),
            SystemTime::now() + Duration::from_secs(5),
            account_policy(),
            AccountAttemptContext::new(BTreeSet::new(), None, None)
                .with_account_scope(contract_account_scope()),
            None,
            CancellationToken::new(),
        );
        let mut pending = Box::pin(provider.clone().execute(
            planned_request(
                "openai",
                Operation::Generate(generate_with_session_context("root", Some("child"), None)),
            ),
            attempt,
        ));
        if explicit {
            let stream = timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                stream.metadata().provider_account_id().as_str(),
                "acct_subagent_b"
            );
        } else {
            assert!(
                timeout(Duration::from_millis(150), pending.as_mut())
                    .await
                    .is_err()
            );
            leases.busy_accounts.lock().unwrap().clear();
            let stream = timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                stream.metadata().provider_account_id().as_str(),
                "acct_subagent_a"
            );
        }
    }
}

#[tokio::test]
async fn old_native_continuation_cannot_restore_the_pre_migration_account() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let leases = Arc::new(TestLeaseCoordinator::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url_and_leases(
        &store,
        affinity,
        server.uri(),
        leases.clone(),
    );
    let root = || planned_request("openai", turn_request("root", "native-root-turn"));
    drop(
        provider
            .clone()
            .execute(root(), context("req_native_root", CancellationToken::new()))
            .await
            .unwrap(),
    );
    create_account(&store, "acct_subagent_b").await;
    leases
        .busy_accounts
        .lock()
        .unwrap()
        .insert(ProviderAccountId::new("acct_subagent_a").unwrap());
    drop(
        provider
            .clone()
            .execute(
                root(),
                context("req_native_migration", CancellationToken::new()),
            )
            .await
            .unwrap(),
    );
    leases.busy_accounts.lock().unwrap().clear();
    let error = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                Operation::Generate(generate_with_session_context("root", Some("child"), None)),
            ),
            pinned_continuation_context(
                "req_old_native_state",
                "acct_subagent_a",
                "resp_old",
                "resp_old",
                1,
                ContinuationAttempt::Native,
            ),
        )
        .await
        .err()
        .expect("old native state requires full replay");
    assert_eq!(
        error.kind(),
        ProviderErrorKind::ContinuationRecoveryRequired
    );
    let stream = provider
        .execute(
            root(),
            context("req_native_still_current", CancellationToken::new()),
        )
        .await
        .unwrap();
    assert_eq!(
        stream.metadata().provider_account_id().as_str(),
        "acct_subagent_b"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

fn relaxed_context(request_id: &str, scope: Arc<FrozenAccountScope>) -> AttemptContext {
    relaxed_context_with_ttl(request_id, scope, Duration::from_secs(24 * 3600))
}

fn relaxed_context_with_ttl(
    request_id: &str,
    scope: Arc<FrozenAccountScope>,
    ttl: Duration,
) -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new(request_id).unwrap(),
            ClientApiKeyId::new("key_openai_contract").unwrap(),
        ),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(5),
        gateway_core::account::AccountSelectionPolicy::new(
            gateway_core::account::RotationStrategy::Smart,
            NonZeroU32::new(2).unwrap(),
            Duration::ZERO,
        )
        .with_openai_session_affinity_ttl(ttl),
        AccountAttemptContext::new(BTreeSet::new(), None, None).with_account_scope(scope),
        None,
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn relaxed_children_keep_independent_bindings_across_busy_accounts_and_root_migration() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let leases = Arc::new(TestLeaseCoordinator::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url_and_leases(
        &store,
        affinity.clone(),
        server.uri(),
        leases.clone(),
    );
    let select = |thread: &'static str| {
        let provider = provider.clone();
        async move {
            provider
                .execute(
                    planned_request(
                        "openai",
                        Operation::Generate(generate_with_session_context(
                            "root",
                            Some(thread),
                            None,
                        )),
                    ),
                    relaxed_context("req_relaxed", contract_account_scope()),
                )
                .await
                .unwrap()
        }
    };
    let root = select("root").await;
    create_account(&store, "acct_subagent_b").await;
    store.set_scheduling("acct_subagent_b", None, AccountWeight::new(100).unwrap());
    let sibling = select("sibling").await;
    assert_eq!(
        sibling.metadata().provider_account_id().as_str(),
        "acct_subagent_a",
        "new child inherits root preference over weight"
    );
    drop(sibling);
    leases
        .busy_accounts
        .lock()
        .unwrap()
        .insert(ProviderAccountId::new("acct_subagent_a").unwrap());
    let child = select("child").await;
    assert_eq!(
        child.metadata().provider_account_id().as_str(),
        "acct_subagent_b"
    );
    drop(child);
    leases.busy_accounts.lock().unwrap().clear();
    for (thread, expected) in [
        ("root", "acct_subagent_a"),
        ("sibling", "acct_subagent_a"),
        ("child", "acct_subagent_b"),
    ] {
        assert_eq!(
            select(thread)
                .await
                .metadata()
                .provider_account_id()
                .as_str(),
            expected
        );
    }
    leases
        .busy_accounts
        .lock()
        .unwrap()
        .insert(ProviderAccountId::new("acct_subagent_a").unwrap());
    assert_eq!(
        select("root")
            .await
            .metadata()
            .provider_account_id()
            .as_str(),
        "acct_subagent_b"
    );
    leases.busy_accounts.lock().unwrap().clear();
    assert_eq!(
        select("sibling")
            .await
            .metadata()
            .provider_account_id()
            .as_str(),
        "acct_subagent_a",
        "root migration cannot move an existing child"
    );
    assert_eq!(
        select("new-sibling")
            .await
            .metadata()
            .provider_account_id()
            .as_str(),
        "acct_subagent_b"
    );
    assert_eq!(
        root.metadata().provider_account_id().as_str(),
        "acct_subagent_a",
        "the admitted root lease stays on its original account"
    );
    assert_eq!(affinity.binding_count(), 4);
}

#[tokio::test]
async fn relaxed_child_can_claim_without_a_root_and_model_access_can_split_accounts() {
    use gateway_core::account::{AccountModelAccess, AccountModelAccessMode};
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let provider = provider_with_affinity(&store, affinity.clone());
    let select = |thread, model: &'static str, scope| {
        provider.clone().execute(
            planned_request_for_model(
                "openai",
                Operation::Generate(generate_with_session_context("root", Some(thread), None)),
                model,
            ),
            relaxed_context("req_relaxed_model", scope),
        )
    };
    drop(
        select("early-child", "gpt-5.4", contract_account_scope())
            .await
            .unwrap(),
    );
    assert_eq!(
        affinity.binding_count(),
        1,
        "early child must not claim the root binding"
    );
    drop(
        select("root", "gpt-5.4", contract_account_scope())
            .await
            .unwrap(),
    );
    create_account(&store, "acct_subagent_b").await;
    let directory = RuntimeAccountDirectory::new(
        [
            ("acct_subagent_a", "gpt-5.4"),
            ("acct_subagent_b", "gpt-5.5"),
        ]
        .into_iter()
        .map(|(id, model)| {
            (
                ProviderAccountId::new(id).unwrap(),
                RuntimeAccount::new(ProviderKind::new("openai").unwrap(), BTreeSet::new())
                    .with_model_access(
                        AccountModelAccess::new(
                            AccountModelAccessMode::Allowlist,
                            vec![model.to_owned()],
                        )
                        .unwrap(),
                    ),
            )
        })
        .collect(),
    );
    let scope = Arc::new(FrozenAccountScope::new(
        Arc::new(directory),
        ClientRoutingScope::all_accounts(),
    ));
    let child = select("model-child", "gpt-5.5", scope.clone())
        .await
        .unwrap();
    assert_eq!(
        child.metadata().provider_account_id().as_str(),
        "acct_subagent_b"
    );
    drop(child);
    assert_eq!(
        select("root", "gpt-5.4", scope)
            .await
            .unwrap()
            .metadata()
            .provider_account_id()
            .as_str(),
        "acct_subagent_a"
    );
    assert_eq!(affinity.binding_count(), 3);
}

#[tokio::test]
async fn relaxed_image_turn_follows_child_binding_and_accepts_matching_root_identity() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let leases = Arc::new(TestLeaseCoordinator::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url_and_leases(
        &store,
        affinity,
        server.uri(),
        leases.clone(),
    );
    drop(
        provider
            .clone()
            .execute(
                planned_request("openai", turn_request("root", "root-turn")),
                relaxed_context("req_relaxed_root", contract_account_scope()),
            )
            .await
            .unwrap(),
    );
    create_account(&store, "acct_subagent_b").await;
    leases
        .busy_accounts
        .lock()
        .unwrap()
        .insert(ProviderAccountId::new("acct_subagent_a").unwrap());
    let child = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", json!({"model":"gpt-5.4","input":"child","client_metadata":{"session_id":"root","thread_id":"child","turn_id":"child-turn"}}).as_object().unwrap().clone()).unwrap()
    ));
    drop(
        provider
            .clone()
            .execute(
                planned_request("openai", child),
                relaxed_context("req_relaxed_child", contract_account_scope()),
            )
            .await
            .unwrap(),
    );
    leases.busy_accounts.lock().unwrap().clear();
    for session in [None, Some("root")] {
        let mut body = json!({"prompt":"a square", "model":"gpt-image-1"});
        if let Some(session) = session {
            body["session_id"] = json!(session);
        }
        let image = Operation::GenerateImage(ImageRequest::from_raw_json(
            ImageRequestKind::Generation,
            RawJsonPayload::new("openai", Bytes::from(serde_json::to_vec(&body).unwrap()))
                .unwrap()
                .with_context(Map::from_iter([(
                    "image_turn_id".into(),
                    json!("child-turn"),
                )])),
        ));
        let stream = provider
            .clone()
            .execute(
                planned_provider_endpoint_request("openai", image),
                relaxed_context("req_relaxed_image", contract_account_scope()),
            )
            .await
            .unwrap();
        assert_eq!(
            stream.metadata().provider_account_id().as_str(),
            "acct_subagent_b"
        );
    }
    // 新请求切到严格模式后，旧子线程轮次必须跟随根绑定；再切回宽松仍保留子绑定
    for strict in [true, false] {
        let attempt = if strict {
            context("req_strict_old_turn", CancellationToken::new())
        } else {
            relaxed_context("req_relaxed_old_turn", contract_account_scope())
        };
        let stream = provider
            .clone()
            .execute(
                planned_provider_endpoint_request(
                    "openai",
                    image_for_turn(ImageRequestKind::Generation, "child-turn"),
                ),
                attempt,
            )
            .await
            .unwrap();
        assert_eq!(
            stream.metadata().provider_account_id().as_str(),
            if strict {
                "acct_subagent_a"
            } else {
                "acct_subagent_b"
            }
        );
    }
}

#[tokio::test]
async fn configured_ttl_renews_binding_and_image_alias_only_at_successful_admission() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url(&store, affinity.clone(), server.uri());
    let week = Duration::from_secs(7 * 24 * 3600);
    let hour = Duration::from_secs(3600);
    for ttl in [week, hour] {
        let stream = provider
            .clone()
            .execute(
                planned_request("openai", turn_request("root", "ttl-turn")),
                relaxed_context_with_ttl("req_ttl_root", contract_account_scope(), ttl),
            )
            .await
            .unwrap();
        let before = affinity.renewal_ttls();
        assert_eq!(before.last(), Some(&ttl));
        assert_eq!(affinity.alias_ttls().last(), Some(&ttl));
        drop(stream);
        assert_eq!(
            affinity.renewal_ttls(),
            before,
            "dropping the response cannot renew the binding"
        );
    }
    drop(
        provider
            .execute(
                planned_provider_endpoint_request(
                    "openai",
                    image_for_turn(ImageRequestKind::Generation, "ttl-turn"),
                ),
                relaxed_context_with_ttl("req_ttl_image", contract_account_scope(), week),
            )
            .await
            .unwrap(),
    );
    assert_eq!(affinity.renewal_ttls(), vec![week, hour, week]);
    assert_eq!(affinity.alias_ttls(), vec![week, hour, week]);
}

#[tokio::test]
async fn relaxed_final_thread_rewrite_cannot_use_another_threads_account() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_subagent_a").await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let server = MockServer::start().await;
    let provider = provider_with_affinity_and_base_url(&store, affinity, server.uri());
    let request = |thread| {
        planned_request(
            "openai",
            Operation::Generate(generate_with_session_context("root", Some(thread), None)),
        )
    };
    drop(
        provider
            .clone()
            .execute(
                request("root"),
                relaxed_context("req_seed_root", contract_account_scope()),
            )
            .await
            .unwrap(),
    );
    create_account(&store, "acct_subagent_b").await;
    let a = store.account("acct_subagent_a").unwrap();
    store.set_enabled(a.id(), false).await.unwrap();
    drop(
        provider
            .clone()
            .execute(
                request("child"),
                relaxed_context("req_seed_child", contract_account_scope()),
            )
            .await
            .unwrap(),
    );
    store.set_enabled(a.id(), true).await.unwrap();
    let plan = FrozenMiddlewarePlan::new(
        Arc::new(ChangeTurnMetadata(
            br#"{"session_id":"root","thread_id":"child"}"#,
        )),
        ExtensionSetReference::new(
            ExtensionSetId::new("rewrite-thread".into()).unwrap(),
            Arc::new(TestExtensionLease),
        ),
    );
    let attempt = AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_final_thread").unwrap(),
            ClientApiKeyId::new("key_openai_contract").unwrap(),
        )
        .with_middleware(
            Some(plan),
            Arc::from([]),
            "/v1/responses".into(),
            ClientTransport::HttpSse,
        ),
        NonZeroU32::MIN,
        SystemTime::now() + Duration::from_secs(5),
        account_policy()
            .with_openai_account_affinity(gateway_core::account::AccountAffinity::Relaxed),
        AccountAttemptContext::new(BTreeSet::new(), None, None)
            .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    );
    let error = provider
        .execute(request("root"), attempt)
        .await
        .err()
        .expect("thread owner mismatch");
    assert!(error.retry_is_prohibited());
    assert!(server.received_requests().await.unwrap().is_empty());
}
