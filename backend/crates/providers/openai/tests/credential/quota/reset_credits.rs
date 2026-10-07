//! 验证重置卡消费的历史账号资源回收，以及取消后的账号串行边界

use std::{sync::Arc, time::Duration};

use gateway_core::account::ProviderAccountId;
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use super::{MemoryAccountStore, create_account, quota_service_with_base_url};

#[tokio::test]
async fn cancelled_consumer_keeps_the_same_lock_for_queued_and_new_requests() {
    let server = MockServer::start().await;
    let (observed, mut requests) = mpsc::unbounded_channel();
    Mock::given(method("POST"))
        .and(path("/api/codex/rate-limit-reset-credits/consume"))
        .respond_with(move |_: &wiremock::Request| {
            observed.send(()).expect("request observer");
            ResponseTemplate::new(200)
                .set_body_json(json!({"code": "reset", "credit": {"id": "credit_1", "reset_type": "codex_rate_limits"}}))
                .set_delay(Duration::from_millis(200))
        })
        .expect(3)
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_reset_concurrency").await;
    let account = ProviderAccountId::new("acct_reset_concurrency").expect("account ID");
    let quota = Arc::new(quota_service_with_base_url(
        &store,
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client"),
        server.uri(),
    ));

    let first = {
        let quota = quota.clone();
        let account = account.clone();
        tokio::spawn(async move {
            quota
                .consume_reset_credit(&account, None, Uuid::new_v4())
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .expect("first request sent")
        .expect("observer open");

    let queued = {
        let quota = quota.clone();
        let account = account.clone();
        Box::pin(async move {
            quota
                .consume_reset_credit(&account, None, Uuid::new_v4())
                .await
        })
    };
    let mut queued = queued;
    assert!(futures::poll!(&mut queued).is_pending());
    // 已排队的请求持有锁身份；取消当前消费者不能让后来者创建另一把账号锁
    first.abort();
    assert!(
        first
            .await
            .expect_err("first consumer cancelled")
            .is_cancelled()
    );
    let newcomer = {
        let quota = quota.clone();
        let account = account.clone();
        tokio::spawn(async move {
            quota
                .consume_reset_credit(&account, None, Uuid::new_v4())
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(50), requests.recv())
            .await
            .is_err()
    );

    let queued = tokio::spawn(queued);
    tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .expect("queued request sent")
        .expect("observer open");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), requests.recv())
            .await
            .is_err()
    );
    queued.await.expect("queued task").expect("queued consume");
    newcomer
        .await
        .expect("newcomer task")
        .expect("newcomer consume");
}

#[tokio::test]
async fn cancelling_a_waiter_does_not_replace_the_active_accounts_lock() {
    let server = MockServer::start().await;
    let (observed, mut requests) = mpsc::unbounded_channel();
    Mock::given(method("POST"))
        .and(path("/api/codex/rate-limit-reset-credits/consume"))
        .respond_with(move |_: &wiremock::Request| {
            observed.send(()).expect("request observer");
            ResponseTemplate::new(200)
                .set_body_json(json!({"code": "reset", "credit": {"id": "credit_1", "reset_type": "codex_rate_limits"}}))
                .set_delay(Duration::from_millis(200))
        })
        .expect(2)
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_reset_waiter").await;
    let account = ProviderAccountId::new("acct_reset_waiter").expect("account ID");
    let quota = Arc::new(quota_service_with_base_url(
        &store,
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client"),
        server.uri(),
    ));
    let first = {
        let quota = quota.clone();
        let account = account.clone();
        tokio::spawn(async move {
            quota
                .consume_reset_credit(&account, None, Uuid::new_v4())
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .expect("first request sent")
        .expect("observer open");
    let mut cancelled = Box::pin(quota.consume_reset_credit(&account, None, Uuid::new_v4()));
    assert!(futures::poll!(&mut cancelled).is_pending());
    drop(cancelled);
    let next = {
        let quota = quota.clone();
        let account = account.clone();
        tokio::spawn(async move {
            quota
                .consume_reset_credit(&account, None, Uuid::new_v4())
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(50), requests.recv())
            .await
            .is_err()
    );
    first.await.expect("first task").expect("first consume");
    next.await.expect("next task").expect("next consume");
}

#[test]
fn finished_consumers_do_not_retain_historical_account_locks() {
    let store = Arc::new(MemoryAccountStore::default());
    let quota = quota_service_with_base_url(
        &store,
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client"),
        "http://127.0.0.1".to_owned(),
    );
    let consume_missing_account = |index| {
        let account = ProviderAccountId::new(format!("acct_deleted_{index}")).expect("account ID");
        let result =
            futures::executor::block_on(quota.consume_reset_credit(&account, None, Uuid::nil()));
        assert!(matches!(
            result,
            Err(provider_openai::credential::CodexResetCreditsError::NotFound)
        ));
    };
    // 预热执行器和锁表容量；账号不存在会提前返回，不涉及后台 HTTP 分配
    consume_missing_account(0);
    let allocations = allocation_counter::measure(|| {
        for index in 1..=1_000 {
            consume_missing_account(index);
        }
    });
    // 服务仍然存活，已经返回的请求不能留下历史账号键或锁对象
    assert_eq!(allocations.count_current, 0, "{allocations:?}");
    assert_eq!(allocations.bytes_current, 0, "{allocations:?}");
}

#[test]
fn concurrently_cancelled_consumers_release_historical_account_locks() {
    let store = Arc::new(MemoryAccountStore::with_pending_account_reads());
    let quota = quota_service_with_base_url(
        &store,
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client"),
        "http://127.0.0.1".to_owned(),
    );
    let barrier = std::sync::Barrier::new(2);
    let allocations = std::thread::scope(|scope| {
        let exercise = || {
            let consume_and_cancel = |index| {
                let account =
                    ProviderAccountId::new(format!("acct_cancelled_{index}")).expect("account ID");
                let mut consume = Box::pin(quota.consume_reset_credit(&account, None, Uuid::nil()));
                let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
                assert!(std::future::Future::poll(consume.as_mut(), &mut context).is_pending());
                // 一个请求停在账号读取，另一个停在账号锁；两者均已持有同一 entry 身份
                barrier.wait();
                drop(consume);
                barrier.wait();
            };
            // 在线程开始计数前预热 TLS 和锁表容量；之后只有当前两个线程产生被测分配
            consume_and_cancel(0);
            barrier.wait();
            allocation_counter::measure(|| {
                barrier.wait();
                for index in 1..=100_000 {
                    consume_and_cancel(index);
                }
                barrier.wait();
            })
        };
        let first = scope.spawn(exercise);
        let second = scope.spawn(exercise);
        let mut allocations = first.join().expect("first consumer thread");
        allocations += second.join().expect("second consumer thread");
        allocations
    });
    // Arc 可能跨线程分配和释放，因此汇总两个同步计数区间，不依赖全局分配或后台清理
    assert_eq!(allocations.count_current, 0, "{allocations:?}");
    assert_eq!(allocations.bytes_current, 0, "{allocations:?}");
}
