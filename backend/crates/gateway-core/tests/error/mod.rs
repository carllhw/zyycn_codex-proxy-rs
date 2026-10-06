//! 验证执行错误的敏感信息脱敏、稳定快照与请求局部数据边界

use bytes::Bytes;
use gateway_core::error::{
    ClientVisibleUpstreamError, ClientVisibleUpstreamResponse, GatewayError, OpaqueUpstreamValue,
    ProviderDiagnostic, ProviderError, ProviderErrorKind, RawUpstreamError,
};
use gateway_core::event::{ProtocolWireEvent, ProviderEvent, ProviderResponseHeader};
use gateway_core::upstream::UpstreamSendState;
use serde_json::json;

#[test]
fn source_chain_survives_domain_conversion_and_snapshot_without_entering_formatting() {
    use gateway_core::error::{StoreError, StoreErrorKind};
    use gateway_core::provider_ports::{ProviderStoreError, ProviderStoreErrorKind};
    use std::error::Error as _;

    let store = StoreError::caused_by(
        StoreErrorKind::Unavailable,
        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "PRIVATE_DB_CAUSE"),
    );
    let port =
        ProviderStoreError::caused_by(ProviderStoreErrorKind::Unavailable, "select account", store);
    let error = ProviderError::new(
        ProviderErrorKind::ProviderInfrastructureUnavailable,
        UpstreamSendState::NotSent,
    )
    .with_source(port);
    let snapshot = error.stable_snapshot();
    drop(error);

    let port = snapshot
        .source()
        .unwrap()
        .downcast_ref::<ProviderStoreError>()
        .unwrap();
    let store = port.source().unwrap().downcast_ref::<StoreError>().unwrap();
    let original = store
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(original.kind(), std::io::ErrorKind::ConnectionRefused);
    assert_eq!(original.to_string(), "PRIVATE_DB_CAUSE");
    assert!(
        !format!("{snapshot:?} {snapshot} {port:?} {port} {store:?} {store}")
            .contains("PRIVATE_DB_CAUSE")
    );
    assert_eq!(snapshot.send_state(), UpstreamSendState::NotSent);
}

#[test]
fn restricted_details_preserve_causes_and_verbatim_upstream_body_separately() {
    let body = "{ \"error\": {\"code\":\"Vendor.Unknown\",\"message\":\"PRIVATE_BODY\"} }";
    let error = ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
        .with_source(std::io::Error::other("PRIVATE_CAUSE"))
        .with_raw_upstream_error(RawUpstreamError::new(body));
    let details: serde_json::Value = serde_json::from_str(&error.error_details().unwrap()).unwrap();
    assert_eq!(details["upstream"], body);
    assert_eq!(details["causes"]["messages"], json!(["PRIVATE_CAUSE"]));
    assert_eq!(details["causes"]["truncated"], false);
    assert!(!format!("{error:?} {error}").contains("PRIVATE_"));
    assert!(
        ProviderError::new(ProviderErrorKind::Cancelled, UpstreamSendState::NotSent)
            .error_details()
            .is_none()
    );
}

#[test]
fn restricted_cause_snapshot_bounds_unicode_and_cycles_with_an_explicit_marker() {
    let error = ProviderError::new(ProviderErrorKind::Protocol, UpstreamSendState::NotSent)
        .with_source(std::io::Error::other("原始原因".repeat(20_000)));
    let details: serde_json::Value = serde_json::from_str(&error.error_details().unwrap()).unwrap();
    assert_eq!(details["causes"]["truncated"], true);
    assert!(details["causes"]["messages"][0].as_str().unwrap().len() <= 64 * 1024);

    #[derive(Debug)]
    struct CyclicError;
    impl std::fmt::Display for CyclicError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("cyclic source")
        }
    }
    impl std::error::Error for CyclicError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self)
        }
    }
    let error = ProviderError::new(ProviderErrorKind::Protocol, UpstreamSendState::NotSent)
        .with_source(CyclicError);
    let details: serde_json::Value = serde_json::from_str(&error.error_details().unwrap()).unwrap();
    assert_eq!(details["causes"]["truncated"], true);
    assert_eq!(details["causes"]["messages"].as_array().unwrap().len(), 32);
}

#[test]
fn provider_error_debug_should_not_expose_sensitive_context() {
    let secret = "sk-do-not-log-this";
    let error = ProviderError::new(ProviderErrorKind::Unauthorized, UpstreamSendState::Sent)
        .redact_sensitive_context(secret);

    assert!(!format!("{error:?}").contains(secret));
}

#[test]
fn provider_error_debug_should_not_print_classified_upstream_values() {
    let diagnostic = "request-visible-only-through-explicit-accessor";
    let value = OpaqueUpstreamValue::new(diagnostic);
    let error = ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
        .with_upstream_request_id(value);

    assert!(!format!("{error:?}").contains(diagnostic));
}

#[test]
fn classified_provider_diagnostic_survives_stable_snapshot_without_entering_debug() {
    let message = "OpenAI WebSocket closed before terminal response (close code 1000)";
    let error = ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Ambiguous)
        .with_diagnostic(
            ProviderDiagnostic::new(message)
                .with_classification("receive", "closed_before_terminal"),
        );
    let snapshot = error.stable_snapshot();
    let gateway = GatewayError::from_provider(&snapshot);

    assert_eq!(
        gateway.diagnostic().map(ProviderDiagnostic::as_str),
        Some(message)
    );
    assert_eq!(
        gateway.diagnostic().and_then(ProviderDiagnostic::stage),
        Some("receive")
    );
    assert_eq!(
        gateway.diagnostic().and_then(ProviderDiagnostic::code),
        Some("closed_before_terminal")
    );
    assert!(!format!("{error:?}").contains(message));
    assert!(!format!("{gateway:?}").contains(message));
}

#[test]
fn io_diagnostic_keeps_os_cause_without_copying_custom_error_text() {
    let os_error = std::fs::File::open(
        std::env::temp_dir().join(format!("cpr-missing-ca-{}/missing.pem", std::process::id())),
    )
    .unwrap_err();
    let diagnostic = ProviderDiagnostic::new("CA read failed")
        .with_classification("prepare", "custom_ca_read_failed")
        .with_io_cause(&os_error);
    assert!(diagnostic.as_str().contains(&os_error.to_string()));
    assert_eq!(diagnostic.stage(), Some("prepare"));
    assert_eq!(diagnostic.code(), Some("io_not_found"));

    let private = std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "PRIVATE_URL_AND_CREDENTIAL",
    );
    let diagnostic = ProviderDiagnostic::new("Connect failed").with_io_cause(&private);
    assert_eq!(diagnostic.code(), Some("connection_refused"));
    assert!(!diagnostic.as_str().contains("PRIVATE_"));
}

#[test]
fn raw_upstream_error_survives_stable_snapshot_without_entering_debug() {
    let raw = r#"{"error":{"message":"verbatim upstream marker"}}"#;
    let error = ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Sent)
        .with_raw_upstream_error(RawUpstreamError::new(raw));
    let snapshot = error.stable_snapshot();

    assert_eq!(
        snapshot.raw_upstream_error().map(RawUpstreamError::as_str),
        Some(raw)
    );
    assert!(!format!("{error:?}").contains("verbatim upstream marker"));
}

#[test]
fn opaque_upstream_value_should_preserve_arbitrary_text_without_logging_it() {
    let original = format!("\0{}\n", "x".repeat(9_000));
    let value = OpaqueUpstreamValue::new(original.clone());

    assert_eq!(value.as_str(), original);
    assert!(!format!("{value:?}").contains(&original));
}

#[test]
fn provider_error_replay_proof_should_default_to_false() {
    let error = ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent);

    assert!(!error.replay_is_safe());
}

#[test]
fn provider_error_replay_proof_should_be_explicit() {
    let error = ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
        .with_replay_safe();

    assert!(error.replay_is_safe());
}

#[test]
fn provider_error_atomic_client_events_should_be_take_only_and_debug_redacted() {
    let marker = "atomic-wire-must-not-enter-debug";
    let event = ProviderEvent::wire(
        ProtocolWireEvent::json(
            "openai",
            Some("response.failed".to_owned()),
            json!({"type": "response.failed", "message": marker}),
        )
        .expect("atomic wire"),
    );
    let mut error = ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
        .with_atomic_client_events(vec![event]);

    assert!(error.has_atomic_client_events());
    assert!(!format!("{error:?}").contains(marker));
    assert_eq!(error.take_atomic_client_events().len(), 1);
    assert!(!error.has_atomic_client_events());
}

#[test]
fn provider_error_stable_snapshot_should_drop_request_local_upstream_response() {
    let marker = Bytes::from_static(b"raw-client-response-only");
    let error = ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
        .with_client_visible_upstream_response(
            ClientVisibleUpstreamResponse::new(
                429,
                Some(b"application/problem+json".to_vec()),
                marker.clone(),
            )
            .with_headers(vec![ProviderResponseHeader::new(
                "x-future-error",
                Bytes::from_static(b"opaque-header-value"),
            )]),
        );

    let snapshot = error.stable_snapshot();

    assert_eq!(
        error
            .client_visible_upstream_response()
            .map(ClientVisibleUpstreamResponse::body),
        Some(&marker)
    );
    assert!(snapshot.client_visible_upstream_response().is_none());
    assert!(!format!("{error:?}").contains("raw-client-response-only"));
    assert!(!format!("{error:?}").contains("opaque-header-value"));
}

#[test]
fn provider_error_stable_snapshot_should_drop_atomic_client_events() {
    let event = ProviderEvent::wire(
        ProtocolWireEvent::json(
            "openai",
            Some("response.failed".to_owned()),
            json!({"type": "response.failed"}),
        )
        .expect("atomic wire"),
    );
    let error = ProviderError::new(ProviderErrorKind::RateLimited, UpstreamSendState::Sent)
        .with_atomic_client_events(vec![event]);
    let snapshot = error.stable_snapshot();

    assert!(error.has_atomic_client_events());
    assert!(!snapshot.has_atomic_client_events());
}

#[test]
fn gateway_error_should_keep_client_visible_upstream_fields_out_of_safe_diagnostics() {
    let message = "Your Codex quota is exhausted";
    let error = ProviderError::new(ProviderErrorKind::QuotaExhausted, UpstreamSendState::Sent)
        .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
            message,
            Some("quota_exhausted".to_owned()),
            Some("rate_limit_error".to_owned()),
        ));
    let gateway = GatewayError::from_provider(&error);

    assert_eq!(
        gateway.safe_message(),
        "upstream capacity is temporarily unavailable"
    );
    assert_eq!(gateway.client_message(), message);
    assert_eq!(gateway.client_error_code(), Some("quota_exhausted"));
    assert_eq!(gateway.client_error_type(), Some("rate_limit_error"));
    assert!(!format!("{gateway:?}").contains(message));
}

#[test]
fn continuation_recovery_should_keep_its_internal_classification_and_client_contract() {
    let error = ProviderError::new(
        ProviderErrorKind::ContinuationRecoveryRequired,
        UpstreamSendState::NotSent,
    )
    .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
        "Previous response was not found. Retrying the full request.",
        Some("previous_response_not_found".to_owned()),
        Some("invalid_request_error".to_owned()),
    ));
    let gateway = GatewayError::from_provider(&error);

    assert_eq!(error.kind().as_str(), "continuation_recovery_required");
    assert_eq!(
        gateway.safe_message(),
        "conversation continuation must be rebuilt"
    );
    assert_eq!(
        gateway.client_error_code(),
        Some("previous_response_not_found")
    );
}

#[test]
fn message_too_big_should_map_to_a_request_scoped_client_contract() {
    let error = ProviderError::new(
        ProviderErrorKind::MessageTooBig,
        UpstreamSendState::Ambiguous,
    )
    .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
        "upstream websocket message too big",
        Some("message_too_big".to_owned()),
        Some("invalid_request_error".to_owned()),
    ));
    let gateway = GatewayError::from_provider(&error);

    assert_eq!(error.kind().as_str(), "message_too_big");
    assert_eq!(gateway.kind().as_str(), "message_too_big");
    assert_eq!(
        gateway.safe_message(),
        "upstream rejected the request because the message is too large"
    );
    assert_eq!(gateway.client_error_code(), Some("message_too_big"));
    assert_eq!(gateway.client_error_type(), Some("invalid_request_error"));
}

#[test]
fn client_visible_upstream_error_should_preserve_opaque_structured_fields() {
    let message = format!("\0{}\n", "m".repeat(9_000));
    let code = format!("\0{}", "c".repeat(300));
    let error_type = String::new();
    let detail = ClientVisibleUpstreamError::new(
        message.clone(),
        Some(code.clone()),
        Some(error_type.clone()),
    );

    assert_eq!(detail.message(), message);
    assert_eq!(detail.code(), Some(code.as_str()));
    assert_eq!(detail.error_type(), Some(error_type.as_str()));
    assert!(!format!("{detail:?}").contains(&message));
}

#[test]
fn cleanup_failure_keeps_the_primary_cause_and_separate_bounded_details() {
    use gateway_core::error::{ErrorDetails, ErrorSource};
    let primary = ErrorSource::new(std::io::Error::other("PRIMARY_NATIVE_FAILURE"));
    let combined = primary.with_cleanup(std::io::Error::other("ROLLBACK_NATIVE_FAILURE"));
    assert_eq!(
        combined.source().unwrap().to_string(),
        "PRIMARY_NATIVE_FAILURE"
    );
    let details = ErrorDetails::capture(Some(&combined), None, false).unwrap();
    let value: serde_json::Value = serde_json::from_str(details.as_str()).unwrap();
    assert_eq!(
        value["causes"]["messages"],
        json!(["PRIMARY_NATIVE_FAILURE"])
    );
    assert_eq!(
        value["causes"]["cleanup"][0]["messages"],
        json!(["ROLLBACK_NATIVE_FAILURE"])
    );
    assert_eq!(value["causes"]["truncated"], false);
    assert!(!format!("{combined:?} {details:?}").contains("NATIVE_FAILURE"));

    let oversized = ErrorSource::new(std::io::Error::other("界".repeat(24_000)))
        .with_cleanup(std::io::Error::other("ROLLBACK_NATIVE_FAILURE"));
    let details = ErrorDetails::capture(Some(&oversized), None, false).unwrap();
    let value: serde_json::Value = serde_json::from_str(details.as_str()).unwrap();
    assert_eq!(value["causes"]["truncated"], true);
    assert_eq!(value["causes"]["cleanup"][0]["truncated"], true);
    assert!(details.as_str().len() < 66_000);
}
