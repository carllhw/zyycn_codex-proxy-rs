//! 验证实际基础设施错误在 Store 边界保留来源而不进入普通格式化

use std::error::Error as _;

#[tokio::test]
async fn invalid_postgres_options_preserve_the_sqlx_source() {
    let error = gateway_store::postgres::connect_and_migrate(
        "postgres://localhost:invalid-port/test",
        gateway_store::StorePoolConfig::default(),
    )
    .await
    .unwrap_err();

    let source = error.source().expect("PostgreSQL source");
    assert!(source.downcast_ref::<sqlx::Error>().is_some());
    assert!(source.source().is_some());
    assert!(
        error
            .to_string()
            .contains("parse PostgreSQL connection options")
    );
    assert!(!format!("{error:?} {error}").contains("invalid-port"));
}
