//! 跨轮重放区分已编码输入与客户端可见输出，避免工具别名缺失或二次映射

use provider_xai::GrokResponsesRequest;

use super::*;

fn generate(body: Value) -> GenerateRequest {
    GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().expect("body").clone())
            .expect("payload"),
    )
}

fn function(name: &str, label: &str) -> Value {
    json!({"type": "function", "name": name, "description": label,
        "parameters": {"type": "object"}})
}

fn history(names: [&str; 2], prefix: &str) -> Vec<Value> {
    names.into_iter().enumerate().flat_map(|(index, name)| {
        let call_id = format!("{prefix}_{index}");
        [
            json!({"type": "function_call", "name": name, "call_id": call_id, "arguments": "{}"}),
            json!({"type": "function_call_output", "call_id": call_id, "output": "done"}),
        ]
    }).collect()
}

fn declared_alias(body: &Value, label: &str) -> String {
    body["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .find(|tool| tool["description"] == label)
        .expect("function")["name"]
        .as_str()
        .expect("alias")
        .to_owned()
}

async fn assert_replay_tool_names(attempt: Option<ContinuationAttempt>) {
    for hosted in ["web_search", "x_search"] {
        for literal_first in [false, true] {
            let key = ClientApiKeyId::new("key_xai_contract").expect("key");
            let isolated = GrokResponsesRequest::encode(
                &generate(json!({"tools": [function(hosted, "reserved")]})),
                MODEL,
                &key,
            )
            .expect("isolated function");
            let literal = isolated.body()["tools"][0]["name"]
                .as_str()
                .expect("generated alias");
            let mut tools = vec![function(hosted, "reserved"), function(literal, "literal")];
            if literal_first {
                tools.reverse();
            }
            if hosted == "web_search" {
                tools.push(json!({"type": "web_search"}));
            }
            let first_body = json!({"model": "client-model", "store": false,
                "prompt_cache_key": "replay_tool_names", "tools": tools,
                "input": history([hosted, literal], "prior")});
            let encoded = GrokResponsesRequest::encode(&generate(first_body.clone()), MODEL, &key)
                .expect("first request");
            let encoded_body = Value::Object(encoded.body().clone());
            let aliases = [
                declared_alias(&encoded_body, "reserved"),
                declared_alias(&encoded_body, "literal"),
            ];
            assert_ne!(aliases[0], aliases[1]);
            let output: Vec<_> = aliases
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    json!({"type": "function_call", "id": format!("fc_returned_{index}"),
                    "name": name, "call_id": format!("returned_{index}"), "arguments": "{}"})
                })
                .collect();
            let response = [
                json!({"type": "response.created", "response": {"id": "resp_tool_names"}}),
                json!({"type": "response.output_item.done", "output_index": 0, "item": output[0]}),
                json!({"type": "response.output_item.done", "output_index": 1, "item": output[1]}),
                json!({"type": "response.completed", "response": {"id": "resp_tool_names",
                    "status": "completed", "output": output}}),
            ]
            .into_iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>();
            let transport = StubInferenceTransport::sequence([
                InferenceMode::SuccessBody(response.into_bytes()),
                InferenceMode::Success,
            ]);
            let provider = provider(StubSelector::success(), transport.clone()).await;
            let mut first = Arc::clone(&provider)
                .execute(
                    provider_request_with_operation(
                        "xai",
                        Operation::Generate(generate(first_body)),
                    ),
                    context(CancellationToken::new(), None),
                )
                .await
                .expect("first stream");
            let state = collect_provider_state(&mut first)
                .await
                .expect("session state");

            let mut second_input = history([hosted, literal], "current");
            for index in 0..2 {
                second_input.push(json!({"type": "function_call_output",
                    "call_id": format!("returned_{index}"), "output": "returned result"}));
            }
            let mut second_body = json!({"model": "client-model", "store": false,
                "prompt_cache_key": "replay_tool_names", "tools": tools, "input": second_input});
            let (operation, context) = if let Some(attempt) = attempt {
                second_body["previous_response_id"] = json!("resp_gateway_tool_names");
                let pin = NativeContinuationPin::new(
                    PreviousResponseId::new("resp_gateway_tool_names"),
                    PreviousResponseId::new("resp_tool_names"),
                    key,
                    ProviderKind::new("xai").expect("provider"),
                    account_id("provider"),
                );
                (
                    operation_with_state(second_body, state),
                    context_with_continuation_attempt(ContinuationBinding::Pinned(pin), attempt),
                )
            } else {
                (
                    Operation::Generate(generate(second_body)),
                    context(CancellationToken::new(), None),
                )
            };
            let mut second = provider
                .execute(provider_request_with_operation("xai", operation), context)
                .await
                .expect("second stream");
            while let Some(event) = second.next().await {
                event.expect("second event");
            }
            let requests = transport.requests.lock().expect("requests");
            let replay: Value = serde_json::from_slice(requests[1].body()).expect("replay body");
            assert!(replay.get("previous_response_id").is_none());
            for (index, expected) in aliases.iter().enumerate() {
                assert_eq!(
                    declared_alias(&replay, if index == 0 { "reserved" } else { "literal" }),
                    *expected
                );
                for prefix in ["prior", "returned", "current"] {
                    if attempt.is_none() && prefix == "prior" {
                        continue;
                    }
                    let call_id = format!("{prefix}_{index}");
                    let call = replay["input"]
                        .as_array()
                        .expect("input")
                        .iter()
                        .find(|item| item["type"] == "function_call" && item["call_id"] == call_id)
                        .expect("replayed call");
                    assert_eq!(
                        call["name"], *expected,
                        "{hosted}, literal_first={literal_first}, {call_id}"
                    );
                    assert_eq!(call["arguments"], "{}");
                }
            }
        }
    }
}

#[tokio::test]
async fn replay_tool_names_reencode_outputs_on_store_false_native_continuation() {
    assert_replay_tool_names(Some(ContinuationAttempt::Native)).await;
}

#[tokio::test]
async fn replay_tool_names_reencode_outputs_on_replay_owner() {
    assert_replay_tool_names(Some(ContinuationAttempt::ReplayOwner)).await;
}

#[tokio::test]
async fn replay_tool_names_reencode_cached_reasoning_calls_without_reencoding_input() {
    assert_replay_tool_names(None).await;
}
