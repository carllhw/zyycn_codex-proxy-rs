//! Hosted tool 与客户端函数同名时，验证声明、历史和响应使用同一别名合同

use gateway_core::event::ProviderEvent;
use provider_xai::GrokCanonicalDecoder;

use super::*;

fn function_tool(name: &str) -> Value {
    json!({"type": "function", "name": name, "parameters": {
        "type": "object", "properties": {"query": {"type": "string"}}
    }})
}

fn function_history(name: &str) -> Value {
    json!([
        {"type": "function_call", "id": "fc_previous", "call_id": "call_previous",
            "name": name, "arguments": "{\"query\":\"previous\"}"},
        {"type": "function_call_output", "call_id": "call_previous", "output": "result"}
    ])
}

fn assert_function_response_restored(request: &GrokResponsesRequest, alias: &str, name: &str) {
    let item = json!({"type": "function_call", "id": "fc_next", "call_id": "call_next",
        "name": alias, "arguments": "{\"query\":\"next\"}"});
    let frames = [
        json!({"type": "response.created", "response": {"id": "resp_names"}}),
        json!({"type": "response.output_item.added", "output_index": 0, "item": item}),
        json!({"type": "response.function_call_arguments.delta", "output_index": 0,
            "item_id": "fc_next", "call_id": "call_next", "delta": "{\"query\":\"next\"}"}),
        json!({"type": "response.output_item.done", "output_index": 0, "item": item}),
        json!({"type": "response.completed", "response": {"id": "resp_names", "status": "completed",
            "tools": request.body()["tools"], "output": [item]}}),
    ]
    .into_iter()
    .map(|frame| format!("data: {frame}\n\n"))
    .collect::<String>();
    let events = GrokCanonicalDecoder::for_request("grok-4.7", request)
        .push(frames.as_bytes())
        .expect("function response");
    let wire: Vec<_> = events
        .iter()
        .filter_map(ProviderEvent::wire_event)
        .collect();
    let calls: Vec<_> = wire
        .iter()
        .filter_map(|event| event.data().get("item"))
        .collect();
    assert_eq!(
        calls.len(),
        2,
        "client functions must not be filtered as cache calls"
    );
    for call in calls {
        assert_eq!(call["name"], name);
        assert_eq!(call["type"], "function_call");
        assert_eq!(call["call_id"], "call_next");
        assert_eq!(call["arguments"], "{\"query\":\"next\"}");
    }
    let completed = wire.last().expect("completed response").data();
    assert_eq!(completed["response"]["output"][0]["name"], name);
    assert!(
        completed["response"]["tools"]
            .as_array()
            .expect("visible tools")
            .iter()
            .any(|tool| tool["type"] == "function" && tool["name"] == name)
    );
}

#[test]
fn hosted_names_preserve_explicit_function_declarations_choices_history_and_output() {
    for hosted in ["web_search", "x_search"] {
        for hosted_first in [false, true] {
            for nested_choice in [false, true] {
                let mut tools = vec![function_tool(hosted), json!({"type": hosted})];
                if hosted_first {
                    tools.reverse();
                }
                let choice = if nested_choice {
                    json!({"type": "function", "function": {"name": hosted}})
                } else {
                    json!({"type": "function", "name": hosted})
                };
                let request = raw_request(json!({
                    "input": function_history(hosted), "tools": tools, "tool_choice": choice
                }));
                let encoded = GrokResponsesRequest::encode(&request, "grok-4.7", &client_key())
                    .expect("hosted and client function");
                let body = encoded.body();
                let tools = body["tools"].as_array().expect("tools");
                let function = tools
                    .iter()
                    .find(|tool| tool["type"] == "function")
                    .expect("function retained");
                let alias = function["name"].as_str().expect("function alias");
                assert_ne!(alias, hosted, "hosted wire name must be unique");
                assert_eq!(tools.len(), 2);
                assert!(tools.iter().any(|tool| tool["type"] == hosted));
                let choice_name = if nested_choice {
                    &body["tool_choice"]["function"]["name"]
                } else {
                    &body["tool_choice"]["name"]
                };
                assert_eq!(choice_name, alias);
                assert_eq!(body["input"][0]["name"], alias);
                assert_eq!(body["input"][0]["call_id"], "call_previous");
                assert_eq!(body["input"][1]["output"], "result");
                assert_function_response_restored(&encoded, alias, hosted);
            }
        }
    }
}

#[test]
fn hosted_names_avoid_session_injected_x_search_and_keep_aliases_stable() {
    for name in ["web_search", "x_search"] {
        let base = json!({"input": function_history(name), "tools": [function_tool(name)],
            "tool_choice": {"type": "function", "name": name}});
        let encoded =
            GrokResponsesRequest::encode(&raw_request(base.clone()), "grok-4.7", &client_key())
                .expect("without session");
        let alias = encoded.body()["tools"][0]["name"].as_str().expect("alias");
        let mut with_session = base;
        with_session["prompt_cache_key"] = json!("hosted_name_session");
        let session =
            GrokResponsesRequest::encode(&raw_request(with_session), "grok-4.7", &client_key())
                .expect("with session");
        assert_eq!(session.body()["tools"][0]["name"], alias);
        assert_eq!(session.body()["tools"][1], json!({"type": "x_search"}));
        assert_ne!(alias, "x_search");
        assert_eq!(session.body()["tool_choice"]["name"], alias);
        assert_eq!(session.body()["input"][0]["name"], alias);
        assert_function_response_restored(&session, alias, name);
    }
}

#[test]
fn hosted_names_rewrite_history_when_empty_tools_trigger_both_cache_tools() {
    for name in ["web_search", "x_search"] {
        let request = raw_request(json!({"prompt_cache_key": "history_only_session",
            "input": function_history(name)}));
        let encoded = GrokResponsesRequest::encode(&request, "grok-4.7", &client_key())
            .expect("history only");
        assert_eq!(
            encoded.body()["tools"],
            json!([{"type": "web_search"}, {"type": "x_search"}])
        );
        assert_eq!(encoded.body()["tool_choice"], "none");
        assert_ne!(encoded.body()["input"][0]["name"], name);
        assert_eq!(encoded.body()["input"][0]["call_id"], "call_previous");
    }
}

#[test]
fn hosted_names_do_not_rename_similar_or_namespaced_client_functions() {
    let request = raw_request(json!({"tools": [function_tool("web_search_local"),
        {"type": "namespace", "name": "local", "tools": [function_tool("x_search")]},
        {"type": "web_search"}, {"type": "x_search"}], "input": "search"}));
    let encoded =
        GrokResponsesRequest::encode(&request, "grok-4.7", &client_key()).expect("ordinary names");
    assert_eq!(encoded.body()["tools"][0]["name"], "web_search_local");
    assert_eq!(encoded.body()["tools"][1]["name"], "local__x_search");
}

#[test]
fn hosted_names_preserve_custom_tools_lowered_to_functions() {
    let request = raw_request(json!({
        "tools": [{"type": "custom", "name": "web_search"}, {"type": "web_search"}],
        "tool_choice": {"type": "custom", "name": "web_search"},
        "input": [{"type": "custom_tool_call", "call_id": "call_previous",
            "name": "web_search", "input": "previous"}]
    }));
    let encoded = GrokResponsesRequest::encode(&request, "grok-4.7", &client_key())
        .expect("custom and hosted search");
    let alias = encoded.body()["tools"][0]["name"]
        .as_str()
        .expect("custom alias");
    assert_ne!(alias, "web_search");
    assert_eq!(encoded.body()["tool_choice"]["name"], alias);
    assert_eq!(encoded.body()["input"][0]["name"], alias);
    let item = json!({"type": "function_call", "id": "fc_custom", "call_id": "call_custom",
        "name": alias, "arguments": "{\"input\":\"next\"}"});
    let frames = [
        json!({"type": "response.created", "response": {"id": "resp_custom"}}),
        json!({"type": "response.output_item.added", "output_index": 0, "item": item}),
        json!({"type": "response.output_item.done", "output_index": 0, "item": item}),
        json!({"type": "response.completed", "response": {"id": "resp_custom",
            "status": "completed", "output": [item]}}),
    ]
    .into_iter()
    .map(|frame| format!("data: {frame}\n\n"))
    .collect::<String>();
    let events = GrokCanonicalDecoder::for_request("grok-4.7", &encoded)
        .push(frames.as_bytes())
        .expect("custom response");
    let completed = events
        .iter()
        .filter_map(ProviderEvent::wire_event)
        .next_back()
        .expect("completed response")
        .data();
    let restored = &completed["response"]["output"][0];
    assert_eq!(restored["type"], "custom_tool_call");
    assert_eq!(restored["name"], "web_search");
    assert_eq!(restored["call_id"], "call_custom");
    assert_eq!(restored["input"], "next");
}

#[test]
fn hosted_names_keep_existing_function_names_distinct_from_generated_aliases() {
    let first = raw_request(json!({"tools": [function_tool("x_search")]}));
    let first =
        GrokResponsesRequest::encode(&first, "grok-4.7", &client_key()).expect("initial alias");
    let generated = first.body()["tools"][0]["name"].as_str().expect("alias");
    assert_ne!(generated, "x_search");
    for generated_first in [false, true] {
        let mut tools = vec![function_tool("x_search"), function_tool(generated)];
        if generated_first {
            tools.reverse();
        }
        let request = raw_request(
            json!({"tools": tools, "prompt_cache_key": "collision_session", "input": "search"}),
        );
        let encoded = GrokResponsesRequest::encode(&request, "grok-4.7", &client_key())
            .expect("alias collision");
        let first = encoded.body()["tools"][0]["name"]
            .as_str()
            .expect("first alias");
        let second = encoded.body()["tools"][1]["name"]
            .as_str()
            .expect("second alias");
        assert_ne!(first, second);
        assert_ne!(first, "x_search");
        assert_ne!(second, "x_search");
        let original_names = if generated_first {
            [generated, "x_search"]
        } else {
            ["x_search", generated]
        };
        assert_function_response_restored(&encoded, first, original_names[0]);
        assert_function_response_restored(&encoded, second, original_names[1]);
    }
}
