//! 验证 CLI 不进入 HTTP serve 时的信号取消与 Host 析构

use crate::support::host::{configuration, run_in_child};

#[test]
fn dropping_command_line_host_cancels_issued_capabilities() {
    run_in_child(
        "command_line::dropping_command_line_host_cancels_issued_capabilities",
        async {
            let directory = tempfile::tempdir().unwrap();
            let host = gateway_host::initialize_command_line(configuration(directory.path()))
                .await
                .unwrap();
            let command = host.cancellation().child_token();
            let connection = host.connection_lifecycle().cancellation();

            drop(host);

            assert!(command.is_cancelled());
            assert!(connection.is_cancelled());
        },
    );
}

#[cfg(unix)]
#[test]
fn sigterm_cancels_command_line_without_http_serve() {
    use std::io::{BufRead as _, Write as _};
    use std::process::{Command, Stdio};

    use crate::support::host::{CHILD_PROCESS_ENV, child_command};

    const NAME: &str = "command_line::sigterm_cancels_command_line_without_http_serve";
    const READY: &str = "host command signal listener ready";

    if std::env::var(CHILD_PROCESS_ENV).as_deref() == Ok(NAME) {
        run_in_child(NAME, async {
            let directory = tempfile::tempdir().unwrap();
            let host = gateway_host::initialize_command_line(configuration(directory.path()))
                .await
                .unwrap();
            let command = host.cancellation().child_token();
            // SignalGuard 必须先 poll，父进程才可发送信号，避免测试命中默认 SIGTERM 行为
            tokio::task::yield_now().await;
            println!("{READY}");
            std::io::stdout().flush().unwrap();
            command.cancelled().await;
            assert!(host.connection_lifecycle().cancellation().is_cancelled());
            assert!(host.worker_health().snapshot().is_empty());
        });
        return;
    }

    let mut child = child_command(NAME)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "child was not ready"
        );
        if line.contains(READY) {
            break;
        }
        line.clear();
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
