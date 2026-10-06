//! 验证 SSE 与 WebSocket 的首字边界、首块计时和请求级计时原点

use std::time::Instant;

use gateway_core::engine::provider::ProviderStream;
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
