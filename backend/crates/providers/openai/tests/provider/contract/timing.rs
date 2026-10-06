//! 验证 SSE 与 WebSocket 的首字边界、首块计时和请求级计时原点

use std::time::Instant;

use gateway_core::engine::provider::ProviderStream;
use gateway_core::error::ProviderError;
use gateway_core::event::ProviderResponseTimings;

use super::*;

fn timed_context(started_at: Instant, attempt: u32) -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_output_timing").unwrap(),
            ClientApiKeyId::new("key_openai_contract").unwrap(),
        )
        .with_timing_started_at(started_at),
        NonZeroU32::new(attempt).unwrap(),
        SystemTime::now() + Duration::from_secs(30),
        account_policy(),
        AccountAttemptContext::new(BTreeSet::new(), None, None)
            .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    )
}

async fn first_upstream_timings(stream: &mut ProviderStream) -> ProviderResponseTimings {
    timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            if let Some(observation) = event.unwrap().response_observation()
                && observation.timings().first_event_ms.is_some()
            {
                return observation.timings();
            }
        }
        panic!("missing upstream timing observation");
    })
    .await
    .expect("upstream timing before stream completion")
}

async fn finish_timings(
    stream: &mut ProviderStream,
    mut timings: ProviderResponseTimings,
) -> ProviderResponseTimings {
    timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            if let Some(observation) = event.unwrap().response_observation() {
                timings = observation.timings();
            }
        }
        timings
    })
    .await
    .expect("complete upstream stream")
}

#[tokio::test]
async fn first_sse_chunk_records_content_on_the_request_clock_for_every_attempt() {
    for attempt in [1, 2] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let first_chunk = concat!(
            r#"data: {"type":"response.created","response":{"id":"resp_scope_capture","model":"gpt-5.4"}}"#,
            "\n\n",
            r#"data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}"#,
            "\n\n",
        )
        .to_owned();
        let (base_url, release, _, server) =
            paused_chunked_sse_server(first_chunk, CAPTURE_COMPLETED_SSE.to_owned()).await;
        // 用已有请求原点模拟选号或之前尝试的等待，不依赖 sleep 的调度精度
        let started_at = Instant::now() - Duration::from_secs(3);
        let mut stream = provider_with_base_url(&store, base_url)
            .execute(
                planned_request("openai", http_generate_operation()),
                timed_context(started_at, attempt),
            )
            .await
            .unwrap();
        let first = first_upstream_timings(&mut stream).await;
        assert!(first.first_event_ms.is_some_and(|value| value >= 3_000));
        assert!(first.first_token_ms.is_some_and(|value| value >= 3_000));
        assert!(
            first
                .first_text_ms
                .is_some_and(|text| text >= first.first_token_ms.unwrap())
        );
        release.send(()).unwrap();
        let final_timings = finish_timings(&mut stream, first).await;
        assert_eq!(final_timings.first_token_ms, first.first_token_ms);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn structural_frames_wait_for_semantic_output_on_http_and_websocket() {
    for websocket in [false, true] {
        for (output, has_output) in [
            (
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}),
                true,
            ),
            (
                json!({"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":"thinking"}),
                true,
            ),
            (
                json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"}),
                true,
            ),
            (
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":""}),
                false,
            ),
            (
                json!({"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":""}),
                false,
            ),
        ] {
            let store = Arc::new(MemoryAccountStore::default());
            create_account(&store, "acct_provider_contract").await;
            let created = json!({"type":"response.created","response":{"id":"resp_timing","model":"gpt-5.4"}});
            let added = json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","content":[]}});
            let completed = json!({"type":"response.completed","response":{"id":"resp_timing","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}});
            let (base_url, release, server) = if websocket {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let base_url = format!("http://{}", listener.local_addr().unwrap());
                let (release, released) = oneshot::channel();
                let server = tokio::spawn(async move {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut ws = accept_codex_test_websocket(socket).await;
                    ws.next().await.unwrap().unwrap();
                    for event in [created, added] {
                        ws.send(Message::Text(event.to_string().into()))
                            .await
                            .unwrap();
                    }
                    released.await.unwrap();
                    for event in [output, completed] {
                        ws.send(Message::Text(event.to_string().into()))
                            .await
                            .unwrap();
                    }
                });
                (base_url, release, server)
            } else {
                let (base_url, release, _, server) = paused_chunked_sse_server(
                    format!("data: {created}\n\ndata: {added}\n\n"),
                    format!("data: {output}\n\ndata: {completed}\n\n"),
                )
                .await;
                (base_url, release, server)
            };
            let operation = if websocket {
                generate_operation()
            } else {
                http_generate_operation()
            };
            let mut stream = provider_with_base_url(&store, base_url)
                .execute(
                    planned_request("openai", operation),
                    timed_context(Instant::now(), 1),
                )
                .await
                .unwrap();
            let first = first_upstream_timings(&mut stream).await;
            assert_eq!(first.first_token_ms, None);
            release.send(()).unwrap();
            let final_timings = finish_timings(&mut stream, first).await;
            assert_eq!(final_timings.first_token_ms.is_some(), has_output);
            server.await.unwrap();
        }
    }
}

/// 只发送短文本请求，输出请求级耗时与用量，不记录凭据、账号身份或响应正文
#[tokio::test]
#[ignore = "requires CPR_LIVE_ACCOUNTS_FILE and sends real requests"]
async fn real_http_and_websocket_requests_report_output_timings() {
    let (store, _) = super::live::imported_account().await;
    let provider = provider_with_base_url(&store, OFFICIAL_CODEX_BASE_URL.to_owned());
    let model = super::live::model(&store).await;
    for websocket in [false, true] {
        let expected_transport = if websocket { "websocket" } else { "http_sse" };
        let mut protocol_context = Map::from_iter([("use_websocket".into(), json!(websocket))]);
        if websocket {
            // 模拟下游 WebSocket 新链，避免快路径预算到期后自动回退 HTTP
            protocol_context.insert(
                "downstream_websocket_connection_id".into(),
                json!("ws_live_timing"),
            );
        }
        let mut generate = GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object(
                "openai",
                json!({
                    "model": model,
                    "instructions": "Reply with the numbers 1 through 30, separated by spaces, and nothing else.",
                    "stream": true,
                    "store": false,
                    "input": [{"role": "user", "content": [{"type": "input_text", "text": "Count now."}]}]
                })
                .as_object()
                .unwrap()
                .clone(),
            )
            .unwrap()
            .with_context(protocol_context),
        );
        if websocket {
            // 测试 Provider 未装配持久会话标识，显式提供本地池键，不声明上游续接 ID
            generate = generate.with_provider_session_state(
                ProviderSessionState::new(
                    "openai",
                    Map::from_iter([
                        ("account_id".into(), json!("acct_provider_contract")),
                        ("conversation_id".into(), json!("live_timing_conversation")),
                        ("continuation_scope".into(), json!("persisted")),
                    ]),
                )
                .unwrap(),
            );
        }
        let operation = Operation::Generate(generate);
        let started_at = Instant::now();
        let run = async {
            let mut stream = provider
                .clone()
                .execute(
                    planned_request_for_model("openai", operation, &model),
                    timed_context(started_at, 1),
                )
                .await?;
            let mut timings = ProviderResponseTimings::default();
            let mut output_tokens = None;
            let mut completed = false;
            while let Some(event) = stream.next().await {
                let event = event?;
                if let Some(observation) = event.response_observation() {
                    assert_eq!(observation.transport().as_str(), expected_transport);
                    timings = observation.timings();
                }
                for fact in event.canonical_facts() {
                    match fact {
                        GatewayEvent::Usage(usage) => output_tokens = usage.output_tokens,
                        GatewayEvent::Completed(_) => completed = true,
                        _ => {}
                    }
                }
            }
            Ok::<_, ProviderError>((timings, output_tokens, completed))
        };
        let (timings, output_tokens, completed) = timeout(Duration::from_secs(35), run)
            .await
            .expect("live request deadline")
            .unwrap_or_else(|error| {
                panic!(
                    "live request failed: kind={:?}, status={:?}, code={:?}",
                    error.kind(),
                    error.upstream_status(),
                    error
                        .client_visible_upstream_error()
                        .and_then(|error| error.code())
                );
            });
        let total_ms = started_at.elapsed().as_millis();
        let first_event_ms = timings.first_event_ms.expect("first upstream packet");
        let first_token_ms = timings.first_token_ms.expect("first semantic output");
        let first_text_ms = timings.first_text_ms.expect("first text output");
        let output_tokens = output_tokens.expect("upstream output usage");
        assert!(completed && output_tokens > 0);
        assert!(first_event_ms <= first_token_ms && first_token_ms <= first_text_ms);
        assert!(u128::from(first_text_ms) <= total_ms);
        eprintln!(
            "LIVE_TIMING model={model} transport={expected_transport} first_event_ms={first_event_ms} first_token_ms={first_token_ms} first_text_ms={first_text_ms} total_ms={total_ms} output_tokens={output_tokens}"
        );
    }
}
