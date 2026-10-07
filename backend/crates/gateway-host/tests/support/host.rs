//! 真实 Host 生命周期测试的配置与独立进程，隔离全局日志订阅器和信号监听

use std::{future::Future, path::Path, process::Command, time::Duration};

use gateway_host::config::{FileLoggingConfig, HostConfig, ListenConfig, LoggingConfig};

pub const CHILD_PROCESS_ENV: &str = "CPR_HOST_LIFECYCLE_TEST_CHILD";

pub fn run_in_child(name: &str, scenario: impl Future<Output = ()>) {
    if std::env::var(CHILD_PROCESS_ENV).as_deref() == Ok(name) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(10), scenario)
                    .await
                    .expect("host lifecycle scenario timed out");
            });
        return;
    }
    let output = child_command(name).output().unwrap();
    assert!(
        output.status.success(),
        "child failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn child_command(name: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", name, "--nocapture"])
        .env(CHILD_PROCESS_ENV, name)
        .env("RUST_LOG", "off");
    command
}

pub fn configuration(directory: &Path) -> HostConfig {
    HostConfig {
        timezone: Default::default(),
        listen: ListenConfig {
            host: "127.0.0.1".to_owned(),
            port: 0,
        },
        runtime_data_dir: directory.to_path_buf(),
        logging: LoggingConfig {
            level: "off".to_owned(),
            stdout: false,
            file: FileLoggingConfig {
                enabled: true,
                directory: directory.join("logs"),
                retention_days: 1,
                max_file_size_mb: 1,
            },
            oauth_recovery: false,
            request_dump: false,
            request_dump_retention_days: 1,
        },
        system_update: Default::default(),
        drain_timeout_seconds: 1,
        worker_shutdown_timeout_seconds: 1,
    }
}
