//! 验证最终请求身份、官方图片轮次关联与共享账号迁移

use super::*;

#[derive(Debug)]
struct ChangeSessionHeader;

impl MiddlewarePlan for ChangeSessionHeader {
    fn handle(
        &self,
        _context: MiddlewareContext,
        request: MiddlewareRequest,
        next: MiddlewareNext,
    ) -> BoxFuture<'static, Result<MiddlewareResponse, MiddlewareError>> {
        Box::pin(async move {
            let (protocol, mut headers, body) = request.into_parts();
            headers.push(MiddlewareHeader::new(
                "x-codex-turn-metadata",
                Bytes::from_static(br#"{"session_id":"session-b"}"#),
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
                Arc::new(ChangeSessionHeader),
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

/// 显式运行才读取本地导出并访问官方服务；只输出状态与测试账号别名
#[tokio::test]
#[ignore = "requires CPR_LIVE_ACCOUNTS_FILE and sends real requests"]
async fn real_conversation_requests_keep_the_imported_account() {
    let (store, account) = super::live::imported_account().await;
    let field = |key| account.get(key).and_then(Value::as_str).map(str::to_owned);
    let model = super::live::model(&store).await;
    let affinity = Arc::new(MemorySessionAffinity::default());
    let provider = provider_with_affinity(&store, affinity.clone());
    let session = uuid::Uuid::new_v4().to_string();
    let turn = uuid::Uuid::new_v4().to_string();
    let response = |thread: &str| {
        let request_turn = if thread == session {
            format!("root-{turn}")
        } else {
            turn.clone()
        };
        Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", json!({
            "model":model, "instructions":"Reply with OK only.", "stream":true, "store":false,
            "input":[{"role":"user","content":[{"type":"input_text","text":"Reply with OK."}]}],
            "client_metadata":{"x-codex-turn-metadata":json!({"session_id":session,"thread_id":thread,"turn_id":request_turn}).to_string()}
        }).as_object().unwrap().clone()).unwrap().with_context(Map::from_iter([
            ("use_websocket".into(), json!(false)), ("session_id".into(), json!("different-cache-routing-key"))
        ])),
    ))
    };
    let search = Operation::Search(StandaloneSearchRequest::from_raw_json(RawJsonPayload::new("openai", Bytes::from(json!({
        "id":session, "model":model, "commands":{"search_query":[{"q":"site:openai.com ChatGPT"}]}, "max_output_tokens":128
    }).to_string())).unwrap()));
    let image = Operation::GenerateImage(ImageRequest::from_raw_json(ImageRequestKind::Generation, RawJsonPayload::new("openai", Bytes::from(json!({
        "prompt":"A plain blue square on a white background.", "model":"gpt-image-2", "n":1, "quality":"low", "size":"1024x1024"
    }).to_string())).unwrap().with_context(Map::from_iter([("image_turn_id".into(), json!(turn))]))));
    let summarize_error = |stage: &str, error: gateway_core::error::ProviderError| {
        let message = error.client_visible_upstream_error().map(|upstream| {
            let mut message = upstream.message().to_owned();
            for key in [
                "accessToken",
                "refreshToken",
                "idToken",
                "email",
                "accountId",
                "userId",
            ] {
                if let Some(value) = field(key).filter(|value| !value.is_empty()) {
                    message = message.replace(&value, "[redacted]");
                }
            }
            message.chars().take(256).collect::<String>()
        });
        json!({"stage":stage,"kind":format!("{:?}",error.kind()),"status":error.upstream_status(),"code":error.client_visible_upstream_error().and_then(|error| error.code()),"message":message})
    };
    let mut results = Vec::new();
    for (name, operation) in [
        ("root", response(&session)),
        ("child", response("child-thread")),
        ("search", search),
        ("image-turn-alias", image),
    ] {
        let request = if matches!(operation, Operation::Generate(_)) {
            planned_request_for_model("openai", operation, &model)
        } else {
            planned_provider_endpoint_request("openai", operation)
        };
        let run = async {
            let mut stream = provider
                .clone()
                .execute(
                    request,
                    context("req_live_binding_probe", CancellationToken::new()),
                )
                .await
                .map_err(|error| summarize_error("selection", error))?;
            assert_eq!(
                stream.metadata().provider_account_id().as_str(),
                "acct_provider_contract",
                "never send decoy credentials upstream"
            );
            let mut events = 0;
            while let Some(event) = stream.next().await {
                event.map_err(|error| summarize_error("upstream", error))?;
                events += 1;
            }
            Ok::<_, Value>(events)
        };
        let result = match timeout(Duration::from_secs(90), run).await {
            Ok(Ok(events)) => {
                json!({"case":name,"account":"imported-account","ok":true,"events":events})
            }
            Ok(Err(error)) => {
                json!({"case":name,"account":"imported-account","ok":false,"error":error})
            }
            Err(_) => {
                json!({"case":name,"account":"imported-account","ok":false,"error":"timeout"})
            }
        };
        eprintln!("LIVE_BINDING {result}");
        results.push(result);
        if name == "root" {
            // 高权重候选验证后续请求没有重新按权重选号，发送前断言阻止误用占位凭据
            create_account(&store, "acct_subagent_b").await;
            store.set_scheduling("acct_subagent_b", None, AccountWeight::new(100).unwrap());
        }
    }
    assert_eq!(affinity.binding_count(), 1);
    if let Ok(path) = std::env::var("CPR_LIVE_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&results).unwrap())
            .expect("write sanitized report");
    }
    assert!(
        results.iter().all(|result| result["ok"] == true),
        "some real upstream requests failed; see sanitized status report"
    );
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
    for kind in [ImageRequestKind::Generation, ImageRequestKind::Edit] {
        let mut pending = Box::pin(provider.clone().execute(
            planned_provider_endpoint_request("openai", image_for_turn(kind, "child-turn")),
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
