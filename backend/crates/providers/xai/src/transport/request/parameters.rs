//! 当前 Responses 参数默认值与 Grok wire 字段校验

use super::*;

pub(super) fn normalize_build_request(
    body: &mut Map<String, Value>,
) -> Result<(), GrokRequestEncodeError> {
    apply_build_response_defaults(body)?;
    normalize_build_reasoning_effort(body)?;
    sanitize_build_fields(body);
    Ok(())
}

fn apply_build_response_defaults(
    body: &mut Map<String, Value>,
) -> Result<(), GrokRequestEncodeError> {
    if body.get("store").is_none_or(Value::is_null) {
        body.insert("store".to_owned(), Value::Bool(false));
    }

    let include = body
        .entry("include".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    if include.is_null() {
        *include = Value::Array(Vec::new());
    }
    let include = include
        .as_array_mut()
        .ok_or(GrokRequestEncodeError::InvalidRequestField { field: "include" })?;
    if include.iter().any(|value| !value.is_string()) {
        return Err(GrokRequestEncodeError::InvalidRequestField { field: "include" });
    }
    if !include
        .iter()
        .any(|value| value.as_str() == Some("reasoning.encrypted_content"))
    {
        include.push(Value::String("reasoning.encrypted_content".to_owned()));
    }
    Ok(())
}

fn normalize_build_reasoning_effort(
    body: &mut Map<String, Value>,
) -> Result<(), GrokRequestEncodeError> {
    let Some(reasoning) = body.get_mut("reasoning").filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let reasoning = reasoning
        .as_object_mut()
        .ok_or(GrokRequestEncodeError::InvalidRequestField { field: "reasoning" })?;
    let Some(effort) = reasoning.get_mut("effort").filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let normalized = effort
        .as_str()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| {
            matches!(
                value.as_str(),
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            )
        })
        .ok_or(GrokRequestEncodeError::InvalidRequestField {
            field: "reasoning.effort",
        })?;
    // 菜单属于目录事实；请求编码只校验 wire，不根据 slug 改写用户选择的档位
    *effort = Value::String(normalized);
    Ok(())
}

fn sanitize_build_fields(body: &mut Map<String, Value>) {
    for field in ["prompt_cache_retention", "safety_identifier"] {
        body.remove(field);
    }
    body.remove("external_web_access");
}
