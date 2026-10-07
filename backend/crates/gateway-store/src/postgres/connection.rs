//! PostgreSQL 迁移屏障、连接池与会话级资源预算

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::{
    POSTGRES_IDLE_TRANSACTION_TIMEOUT, POSTGRES_LOCK_TIMEOUT, POSTGRES_STATEMENT_TIMEOUT,
    StoreError, StorePoolConfig, StoreResult, postgres_unavailable,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// 建立 PostgreSQL pool 并只执行冻结的 migration 集
pub async fn connect_and_migrate(
    database_url: &str,
    pool_config: StorePoolConfig,
) -> StoreResult<PgPool> {
    if database_url.trim().is_empty() {
        return Err(StoreError::InvalidData {
            source: None,
            entity: "PostgreSQL configuration",
            message: "database URL is empty".to_owned(),
        });
    }
    pool_config.validate()?;
    let connect_options = database_url
        .parse::<PgConnectOptions>()
        .map_err(|source| postgres_unavailable("parse PostgreSQL connection options", source))?;
    let migration_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            connect_options
                .clone()
                .application_name("codex-proxy-rs:migration"),
        )
        .await
        .map_err(|source| postgres_unavailable("connect PostgreSQL for migrations", source))?;
    if let Err(error) = MIGRATOR.run(&migration_pool).await {
        migration_pool.close().await;
        return Err(postgres_unavailable("apply PostgreSQL migrations", error));
    }
    migration_pool.close().await;

    connect_pool(connect_options, pool_config, false).await
}

/// 帮助查询不执行迁移，并用连接默认只读事务阻止意外业务写入
pub(crate) async fn connect_read_only(
    database_url: &str,
    pool_config: StorePoolConfig,
) -> StoreResult<PgPool> {
    pool_config.validate()?;
    let options = database_url
        .parse::<PgConnectOptions>()
        .map_err(|source| postgres_unavailable("parse PostgreSQL connection options", source))?;
    connect_pool(options, pool_config, true).await
}

async fn connect_pool(
    connect_options: PgConnectOptions,
    pool_config: StorePoolConfig,
    read_only: bool,
) -> StoreResult<PgPool> {
    let statement_timeout = postgres_duration_setting(POSTGRES_STATEMENT_TIMEOUT);
    let lock_timeout = postgres_duration_setting(POSTGRES_LOCK_TIMEOUT);
    let idle_in_transaction_session_timeout =
        postgres_duration_setting(POSTGRES_IDLE_TRANSACTION_TIMEOUT);
    let pool = PgPoolOptions::new()
        .max_connections(pool_config.max_connections)
        .acquire_timeout(std::time::Duration::from_secs(
            pool_config.acquire_timeout_seconds,
        ))
        .after_connect(move |connection, _metadata| {
            let statement_timeout = statement_timeout.clone();
            let lock_timeout = lock_timeout.clone();
            let idle_in_transaction_session_timeout = idle_in_transaction_session_timeout.clone();
            Box::pin(async move {
                sqlx::query(
                    "select set_config('statement_timeout', $1, false),
                            set_config('lock_timeout', $2, false),
                            set_config('idle_in_transaction_session_timeout', $3, false),
                            set_config('default_transaction_read_only', $4, false),
                            set_config('TimeZone', 'UTC', false)",
                )
                .bind(statement_timeout)
                .bind(lock_timeout)
                .bind(idle_in_transaction_session_timeout)
                .bind(if read_only { "on" } else { "off" })
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect_with(connect_options.application_name("codex-proxy-rs"))
        .await
        .map_err(|source| postgres_unavailable("connect PostgreSQL", source))?;
    Ok(pool)
}

fn postgres_duration_setting(duration: std::time::Duration) -> String {
    format!("{}ms", duration.as_millis())
}
