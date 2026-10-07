//! 路由已选定真实模型，xAI 编码器不再解释旧别名或 Provider 前缀

use super::*;

#[test]
fn routed_model_identity_is_preserved_without_provider_local_aliases() {
    let request = raw_request(json!({"model": "client-model", "input": "hello"}));
    for model in [
        "grok",
        "grok-latest",
        "grok-build-latest",
        "grok-4.5-latest",
        "grok-4.6-latest",
        "grok-4.3-latest",
        "grok-build",
        "grok-composer",
        "composer-2.5",
        "grok-4.20-reasoning",
        "grok-4.20-non-reasoning",
        "grok-4.20-multi-agent",
        "grok-4.20-multi-agent-latest",
        "grok-4.7",
        "Grok-4.6",
        "xai/grok-4.7",
        "x-ai/grok-4.7",
        "grok/grok-4.7",
        "tenant/custom-model",
    ] {
        let encoded = GrokResponsesRequest::encode(&request, model, &client_key())
            .expect("routed upstream model");
        assert_eq!(encoded.body()["model"], model);
    }
}

#[test]
fn routed_model_identity_does_not_infer_unsupported_fields_from_old_slugs() {
    let fields = json!({"presence_penalty": 0.1, "frequency_penalty": 0.2,
        "stop": ["done"], "logprobs": true, "top_logprobs": 5});
    let request = raw_request(fields.clone());
    for model in [
        "grok-4.5",
        "grok-4.6",
        "grok-4.3",
        "grok-4.20-reasoning",
        "tenant/grok-4.6",
        "grok-4.7",
    ] {
        let encoded =
            GrokResponsesRequest::encode(&request, model, &client_key()).expect("request");
        for (key, value) in fields.as_object().expect("fields") {
            assert_eq!(encoded.body().get(key), Some(value), "{model} {key}");
        }
    }
}
