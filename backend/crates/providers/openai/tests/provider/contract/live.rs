//! 显式真实请求测试的凭据导入，只创建内存账号，不修改本地导出

use super::*;

pub(super) async fn imported_account() -> (Arc<MemoryAccountStore>, Value) {
    use provider_openai::credential::{CodexAccountProfile, CodexOAuthSecret};
    use secrecy::SecretString;

    let file = std::env::var("CPR_LIVE_ACCOUNTS_FILE").expect("credential file path required");
    let document: Value =
        serde_json::from_slice(&std::fs::read(file).expect("read credential file"))
            .expect("parse credential export");
    let account = document["documents"][0]["document"]["accounts"][0].clone();
    let field = |key| account.get(key).and_then(Value::as_str).map(str::to_owned);
    let store = Arc::new(MemoryAccountStore::default());
    store
        .seed_oauth_credential(ImportCodexOAuthCredential {
            account_id: "acct_provider_contract".into(),
            name: "local-live-validation".into(),
            enabled: true,
            secret: CodexOAuthSecret {
                access_token: SecretString::from(
                    field("accessToken").expect("access token required"),
                ),
                refresh_token: field("refreshToken").map(SecretString::from),
                id_token: field("idToken").map(SecretString::from),
            },
            verified_account: CodexAccountProfile {
                email: None,
                oauth_subject: field("userId").expect("user identity required"),
                poid: None,
                chatgpt_account_id: field("accountId").expect("account identity required"),
                chatgpt_user_id: field("userId").expect("user identity required"),
                plan_type: field("planType"),
                access_token_expires_at: field("accessTokenExpiresAt")
                    .and_then(|date| chrono::DateTime::parse_from_rfc3339(&date).ok())
                    .map(|date| date.with_timezone(&Utc)),
            },
            next_refresh_at: Some(Utc::now() + chrono::Duration::hours(1)),
        })
        .await;
    (store, account)
}

pub(super) async fn model(store: &Arc<MemoryAccountStore>) -> String {
    let catalog = CodexCredentialCatalogService::new(
        store.repository(),
        wire_profile(),
        reqwest::Client::builder().no_proxy().build().unwrap(),
        OFFICIAL_CODEX_BASE_URL.to_owned(),
        catalog_cache(),
    );
    let models = catalog
        .refresh_account_catalog(&ProviderAccountId::new("acct_provider_contract").unwrap())
        .await
        .expect("load supported models");
    std::env::var("CPR_LIVE_MODEL").unwrap_or_else(|_| {
        models
            .models()
            .first()
            .expect("available text model")
            .clone()
    })
}
