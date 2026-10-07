//! 验证宿主服务关闭时的连接排空、超时与并发唤醒

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::{Router, routing::get};
use futures::future::BoxFuture;
use gateway_core::diagnostics::{OperationalDiagnostics, OperationalFailure};
use gateway_core::lifecycle::CancellationToken;
use gateway_core::lifecycle::ConnectionLifecycle as _;
use gateway_core::task::{
    DaemonRestartPolicy, DaemonTask, WorkerContribution, WorkerId, WorkerKind,
    WorkerLeaderLeasePort, WorkerLeaseAcquisition, WorkerLeaseError, WorkerLeaseRequest,
    WorkerRegistration, WorkerRunnable, WorkerTaskError,
};
use gateway_host::HostBundle;
use gateway_host::serve::{ConnectionTracker, bind_listener};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Semaphore, mpsc};

use crate::support::host::{configuration, run_in_child};

#[test]
fn host_bundle_serve_is_a_consuming_process_entrypoint() {
    let _serve = HostBundle::serve;

    assert_eq!(std::mem::size_of_val(&_serve), 0);
}

#[tokio::test(start_paused = true)]
async fn wait_until_idle_should_return_immediately_without_active_connections() {
    let tracker = ConnectionTracker::new(CancellationToken::new());
    let started = tokio::time::Instant::now();

    tracker.wait_until_idle(Duration::from_secs(30)).await;

    assert_eq!(started.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn wait_until_idle_should_give_up_at_timeout_while_connections_remain() {
    let tracker = ConnectionTracker::new(CancellationToken::new());
    let _guard = tracker.try_register().expect("register connection");
    let started = tokio::time::Instant::now();

    tracker.wait_until_idle(Duration::from_secs(30)).await;

    assert_eq!(started.elapsed(), Duration::from_secs(30));
}

#[tokio::test]
async fn wait_until_idle_should_wake_when_last_guard_drops_after_first_poll() {
    let tracker = ConnectionTracker::new(CancellationToken::new());
    let guard = tracker.try_register().expect("register connection");
    let mut wait = Box::pin(tracker.wait_until_idle(Duration::from_secs(30)));
    assert!(futures::poll!(wait.as_mut()).is_pending());

    drop(guard);

    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .expect("woken by last guard drop instead of waiting out the timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_until_idle_should_observe_guard_drop_racing_the_idle_check() {
    // 回归约束：notified 必须在读取活跃计数前完成注册（enable），否则最后
    // 一个 guard 在计数检查与首次 poll 之间 drop 时唤醒丢失，只能等满整个
    // 超时
    // 多线程反复交错，任何一次丢唤醒都会撞上 2s 超时并触发断言
    for _ in 0..256 {
        let tracker = Arc::new(ConnectionTracker::new(CancellationToken::new()));
        let guard = tracker.try_register().expect("register connection");
        let waiter = tokio::spawn({
            let tracker = Arc::clone(&tracker);
            async move { tracker.wait_until_idle(Duration::from_secs(2)).await }
        });
        let dropper = std::thread::spawn(move || drop(guard));
        let started = std::time::Instant::now();
        waiter.await.expect("waiter completes");
        dropper.join().expect("dropper completes");
        assert!(
            started.elapsed() < Duration::from_millis(1_900),
            "idle wakeup was lost and the waiter slept until the drain timeout"
        );
    }
}

#[tokio::test]
async fn bind_listener_should_retry_until_previous_listener_releases_port() {
    let holder = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("holder listener");
    let address = holder.local_addr().expect("holder address").to_string();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(holder);
    });

    let listener = tokio::time::timeout(Duration::from_secs(8), bind_listener(&address))
        .await
        .expect("bound within the retry window")
        .expect("bound after the previous listener released the port");

    assert_eq!(
        listener.local_addr().expect("bound address").to_string(),
        address
    );
}

#[test]
fn http_drain_keeps_writer_running_until_the_request_finishes() {
    run_in_child(
        "serve::http_drain_keeps_writer_running_until_the_request_finishes",
        async {
            let directory = tempfile::tempdir().unwrap();
            let (host, address) = listening_host(directory.path()).await;
            let cancellation = host.cancellation();
            let connections = host.connection_lifecycle();
            let probe = start_workers(&host, false);
            probe.started.cancelled().await;
            let (router, entered, finish_request) = request_router(&host, &probe);
            let server = tokio::spawn(host.serve(router));
            let mut client = connect_request(address).await;
            entered.cancelled().await;

            cancellation.cancel();
            wait_for_draining(connections.as_ref()).await;
            assert!(connections.try_register().is_err());
            assert!(
                tokio::time::timeout(Duration::from_millis(50), probe.cancelled.cancelled())
                    .await
                    .is_err(),
                "writer cancelled before the in-flight request finished"
            );
            finish_request.add_permits(1);
            let mut response = String::new();
            client.read_to_string(&mut response).await.unwrap();
            server.await.unwrap().unwrap();

            assert!(response.starts_with("HTTP/1.1 200 OK"));
            assert_eq!(*probe.writes.lock().unwrap(), [1]);
            assert!(probe.finished.is_cancelled());
        },
    );
}

#[test]
fn drain_timeout_starts_a_separate_worker_shutdown_phase() {
    run_in_child(
        "serve::drain_timeout_starts_a_separate_worker_shutdown_phase",
        async {
            let directory = tempfile::tempdir().unwrap();
            let (host, address) = listening_host(directory.path()).await;
            let cancellation = host.cancellation();
            let connections = host.connection_lifecycle();
            let probe = start_workers(&host, true);
            probe.started.cancelled().await;
            let (router, entered, finish_request) = request_router(&host, &probe);
            let server = tokio::spawn(host.serve(router));
            let client = connect_request(address).await;
            entered.cancelled().await;

            let started = tokio::time::Instant::now();
            cancellation.cancel();
            wait_for_draining(connections.as_ref()).await;
            probe.cancelled.cancelled().await;
            assert!(started.elapsed() >= Duration::from_secs(1));
            assert!(!server.is_finished(), "worker cleanup was not joined");
            assert!(!probe.finished.is_cancelled());
            probe.finish_shutdown.add_permits(1);
            server.await.unwrap().unwrap();
            assert!(probe.finished.is_cancelled());

            // 超时后真实连接可以仍在 axum 任务中；释放测试端，避免遗留挂起 handler
            drop(client);
            finish_request.add_permits(1);
        },
    );
}

#[test]
fn bind_failure_cancels_and_joins_workers_before_returning() {
    run_in_child(
        "serve::bind_failure_cancels_and_joins_workers_before_returning",
        async {
            let directory = tempfile::tempdir().unwrap();
            let mut config = configuration(directory.path());
            config.listen.host = "invalid address".to_owned();
            let host = gateway_host::initialize(config).await.unwrap();
            let cancellation = host.cancellation();
            let probe = start_workers(&host, false);
            probe.started.cancelled().await;

            let result = host.serve(Router::new()).await;

            assert!(matches!(
                result,
                Err(gateway_host::HostError::Serve(
                    gateway_host::serve::ServeError::Bind(_)
                ))
            ));
            assert!(cancellation.is_cancelled());
            assert!(probe.finished.is_cancelled());
        },
    );
}

#[test]
fn dropping_serve_cancels_public_operations_and_aborts_workers() {
    run_in_child(
        "serve::dropping_serve_cancels_public_operations_and_aborts_workers",
        async {
            let directory = tempfile::tempdir().unwrap();
            let (host, address) = listening_host(directory.path()).await;
            let cancellation = host.cancellation();
            let probe = start_workers(&host, true);
            probe.started.cancelled().await;
            let server = tokio::spawn(host.serve(Router::new()));
            let client = connect_request(address).await;

            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
            probe.dropped.cancelled().await;

            assert!(cancellation.is_cancelled());
            assert!(!probe.finished.is_cancelled());
            drop(client);
        },
    );
}

#[test]
fn dropping_serve_during_worker_shutdown_aborts_the_joined_worker() {
    run_in_child(
        "serve::dropping_serve_during_worker_shutdown_aborts_the_joined_worker",
        async {
            let directory = tempfile::tempdir().unwrap();
            let (host, address) = listening_host(directory.path()).await;
            let cancellation = host.cancellation();
            let probe = start_workers(&host, true);
            probe.started.cancelled().await;
            let server = tokio::spawn(host.serve(Router::new()));
            let client = connect_request(address).await;

            cancellation.cancel();
            probe.cancelled.cancelled().await;
            assert!(!probe.finished.is_cancelled());
            assert!(
                !server.is_finished(),
                "worker shutdown must still be waiting"
            );
            // Worker 已观察独立的取消信号，serve 正持有 shutdown 的 join future
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
            tokio::time::timeout(Duration::from_secs(1), probe.dropped.cancelled())
                .await
                .expect("aborting shutdown must not detach the worker");

            assert!(!probe.finished.is_cancelled());
            drop(client);
        },
    );
}

async fn listening_host(directory: &std::path::Path) -> (HostBundle, std::net::SocketAddr) {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    let mut config = configuration(directory);
    config.listen.port = address.port();
    let host = gateway_host::initialize(config).await.unwrap();
    drop(reservation);
    (host, address)
}

async fn connect_request(address: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut client = loop {
        match tokio::net::TcpStream::connect(address).await {
            Ok(client) => break client,
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("connect host: {error}"),
        }
    };
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    client
}

async fn wait_for_draining(connections: &dyn gateway_core::lifecycle::ConnectionLifecycle) {
    while !connections.is_draining() {
        tokio::task::yield_now().await;
    }
}

fn request_router(
    host: &HostBundle,
    probe: &WriterProbe,
) -> (Router, CancellationToken, Arc<Semaphore>) {
    let entered = CancellationToken::new();
    let finish = Arc::new(Semaphore::new(0));
    let connections = host.connection_lifecycle();
    let sender = probe.sender.clone();
    let router = Router::new().route(
        "/",
        get({
            let entered = entered.clone();
            let finish = Arc::clone(&finish);
            move || {
                let entered = entered.clone();
                let finish = Arc::clone(&finish);
                let connections = Arc::clone(&connections);
                let sender = sender.clone();
                async move {
                    let _connection = connections.try_register().unwrap();
                    entered.cancel();
                    finish.acquire().await.unwrap().forget();
                    // 对应请求结束后写入终态的时机；drain 超时后的写入允许被拒绝
                    if sender.send(1).is_ok() {
                        "recorded"
                    } else {
                        "writer stopped"
                    }
                }
            }
        }),
    );
    (router, entered, finish)
}

struct WriterProbe {
    sender: mpsc::UnboundedSender<u8>,
    writes: Arc<Mutex<Vec<u8>>>,
    started: CancellationToken,
    cancelled: CancellationToken,
    finish_shutdown: Arc<Semaphore>,
    finished: CancellationToken,
    dropped: CancellationToken,
}

struct WriterTask {
    received: tokio::sync::Mutex<mpsc::UnboundedReceiver<u8>>,
    probe: Arc<WriterProbe>,
}

struct TaskLifetime(CancellationToken);

impl Drop for TaskLifetime {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl DaemonTask for WriterTask {
    fn run(&self, cancellation: CancellationToken) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let _lifetime = TaskLifetime(self.probe.dropped.clone());
            let mut received = self.received.lock().await;
            self.probe.started.cancel();
            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    value = received.recv() => {
                        self.probe.writes.lock().unwrap().push(value.unwrap());
                    }
                }
            }
            self.probe.cancelled.cancel();
            received.close();
            while let Some(value) = received.recv().await {
                self.probe.writes.lock().unwrap().push(value);
            }
            self.probe.finish_shutdown.acquire().await.unwrap().forget();
            self.probe.finished.cancel();
            Ok(())
        })
    }
}

struct IdleTask;

impl DaemonTask for IdleTask {
    fn run(&self, cancellation: CancellationToken) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            cancellation.cancelled().await;
            Ok(())
        })
    }
}

struct UnusedLease;

impl WorkerLeaderLeasePort for UnusedLease {
    fn try_acquire(
        &self,
        _: WorkerLeaseRequest,
    ) -> BoxFuture<'_, Result<WorkerLeaseAcquisition, WorkerLeaseError>> {
        Box::pin(async { Err(WorkerLeaseError::safe("daemon must not acquire a lease")) })
    }
}

struct Diagnostics;

#[async_trait::async_trait]
impl OperationalDiagnostics for Diagnostics {
    async fn record_failure(
        &self,
        _: OperationalFailure,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
}

fn start_workers(host: &HostBundle, hold_shutdown: bool) -> Arc<WriterProbe> {
    let (sender, received) = mpsc::unbounded_channel();
    let probe = Arc::new(WriterProbe {
        sender,
        writes: Arc::new(Mutex::new(Vec::new())),
        started: CancellationToken::new(),
        cancelled: CancellationToken::new(),
        finish_shutdown: Arc::new(Semaphore::new(usize::from(!hold_shutdown))),
        finished: CancellationToken::new(),
        dropped: CancellationToken::new(),
    });
    let mut writer = Some(WriterTask {
        received: tokio::sync::Mutex::new(received),
        probe: Arc::clone(&probe),
    });
    let plan = WorkerKind::ALL
        .into_iter()
        .map(|kind| {
            let task: Box<dyn DaemonTask> = if kind == WorkerKind::OpsFlush {
                Box::new(writer.take().unwrap())
            } else {
                Box::new(IdleTask)
            };
            WorkerContribution::Registration(
                WorkerRegistration::try_new(
                    WorkerId::try_new(kind, "host_test").unwrap(),
                    WorkerRunnable::Daemon {
                        restart: DaemonRestartPolicy::try_new(
                            Duration::from_secs(1),
                            Duration::from_secs(2),
                        )
                        .unwrap(),
                        task,
                    },
                )
                .unwrap(),
            )
        })
        .collect();
    host.start_workers(plan, Arc::new(UnusedLease), Arc::new(Diagnostics))
        .unwrap();
    probe
}
