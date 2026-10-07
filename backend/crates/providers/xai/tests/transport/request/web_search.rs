//! Web Search 域名过滤的协议转换与字段归属测试

use super::*;

#[test]
fn web_search_filters_preserve_excluded_domains_for_supported_aliases() {
    for kind in [
        "web_search",
        "web_search_preview",
        "web_search_preview_2025_03_11",
        "web_search_2025_08_26",
    ] {
        let request = raw_request(json!({
            "input": "search",
            "tools": [{"type": kind, "filters": {"excluded_domains": ["example.com"]}}]
        }));
        let encoded = GrokResponsesRequest::encode(&request, "grok-4.7", &client_key())
            .expect("excluded domains");

        assert_eq!(
            encoded.body()["tools"],
            json!([{"type": "web_search", "filters": {"excluded_domains": ["example.com"]}}]),
            "{kind}"
        );
    }
}

#[test]
fn web_search_filters_reject_combined_nonempty_allow_and_block_lists() {
    for tool in [
        json!({"type": "web_search", "filters": {
            "allowed_domains": ["allowed.example"], "excluded_domains": ["excluded.example"]
        }}),
        json!({"type": "web_search_preview", "allowed_domains": ["allowed.example"],
            "filters": {"excluded_domains": ["excluded.example"]}
        }),
    ] {
        let request = raw_request(json!({"input": "search", "tools": [tool]}));
        assert!(matches!(
            GrokResponsesRequest::encode(&request, "grok-4.7", &client_key()),
            Err(GrokRequestEncodeError::InvalidRequestField { field: "tools" })
        ));
    }
}

#[test]
fn web_search_filters_keep_empty_lists_unbounded() {
    for (filters, expected) in [
        (
            json!({"allowed_domains": [], "excluded_domains": ["example.com"]}),
            json!({"excluded_domains": ["example.com"]}),
        ),
        (
            json!({"allowed_domains": ["example.com"], "excluded_domains": []}),
            json!({"allowed_domains": ["example.com"]}),
        ),
        (json!({"excluded_domains": []}), Value::Null),
        (
            json!({"allowed_domains": [], "excluded_domains": []}),
            Value::Null,
        ),
        (json!({"allowed_domains": []}), Value::Null),
    ] {
        let request = raw_request(json!({
            "input": "search", "tools": [{"type": "web_search", "filters": filters}]
        }));
        let encoded = GrokResponsesRequest::encode(&request, "grok-4.7", &client_key())
            .expect("empty lists are unbounded");
        assert_eq!(encoded.body()["tools"][0]["filters"], expected);
    }
}

#[test]
fn web_search_filters_reject_retired_top_level_allowed_domains() {
    for tool in [
        json!({"type": "web_search_preview", "allowed_domains": ["example.com"]}),
        json!({"type": "web_search", "allowed_domains": ["example.com"],
            "filters": {"allowed_domains": ["example.com"]}}),
        json!({"type": "web_search", "allowed_domains": ["one.example"],
            "filters": {"allowed_domains": ["other.example"]}}),
        json!({"type": "web_search", "allowed_domains": []}),
    ] {
        let request = raw_request(json!({"input": "search", "tools": [tool]}));
        assert!(matches!(
            GrokResponsesRequest::encode(&request, "grok-4.7", &client_key()),
            Err(GrokRequestEncodeError::InvalidRequestField { field: "tools" })
        ));
    }
}

#[test]
fn web_search_filters_reject_invalid_excluded_domains() {
    for excluded in [
        Value::Null,
        json!("example.com"),
        json!([" "]),
        json!([3]),
        json!(["example.com", false]),
    ] {
        let request = raw_request(json!({"input": "search", "tools": [{
            "type": "web_search", "filters": {"excluded_domains": excluded}
        }]}));
        assert!(matches!(
            GrokResponsesRequest::encode(&request, "grok-4.7", &client_key()),
            Err(GrokRequestEncodeError::InvalidRequestField { field: "tools" })
        ));
    }
}

#[test]
fn web_search_filters_only_interpret_owned_tool_fields() {
    let schema = json!({"type": "object", "properties": {
        "filters": {"type": "string"}, "excluded_domains": {"type": "integer"}
    }});
    let opaque = json!({"excluded_domains": "application data"});
    let request = raw_request(json!({
        "input": "search",
        "excluded_domains": opaque,
        "tools": [
            {"type": "function", "name": "lookup", "parameters": schema},
            {"type": "x_search", "excluded_domains": opaque},
            {"type": "web_search", "excluded_domains": opaque,
                "filters": {"future_filter": opaque, "excluded_domains": ["example.com"]}}
        ]
    }));
    let encoded =
        GrokResponsesRequest::encode(&request, "grok-4.7", &client_key()).expect("field ownership");

    assert_eq!(encoded.body()["excluded_domains"], opaque);
    assert_eq!(encoded.body()["tools"][0]["parameters"], schema);
    assert_eq!(encoded.body()["tools"][1]["excluded_domains"], opaque);
    assert_eq!(
        encoded.body()["tools"][2],
        json!({"type": "web_search", "filters": {"excluded_domains": ["example.com"]}})
    );
}
