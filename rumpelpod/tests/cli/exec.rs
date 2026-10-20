// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use indoc::{formatdoc, indoc};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use rumpelpod::daemon::protocol::{ClientContext, ConnectPodRequest, Daemon, DaemonClient};
use rumpelpod::CommandExt;

use crate::common::{
    create_commit, pod_command, write_test_devcontainer, TestDaemon, TestHome, TestRepo,
    TEST_REPO_PATH, TEST_USER,
};
use crate::executor::{executor_supports_stop, ExecutorResources};

fn wait_connected(repo: &TestRepo, daemon: &TestDaemon) {
    let client = DaemonClient::new_unix(&daemon.socket_path);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if client
            .prepared_pod_connection(ConnectPodRequest {
                pod_name: "test".into(),
                repo_path: repo.path().to_path_buf(),
                client_context: ClientContext::default(),
            })
            .unwrap()
            .is_some()
        {
            return;
        }
        assert!(Instant::now() < deadline, "pod did not connect");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn exec_preserves_argv_environment_user_directory_and_status() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::create_dir_all(repo.path().join("nested dir")).unwrap();
    fs::write(repo.path().join("nested dir/keep"), "").unwrap();
    Command::new("git")
        .current_dir(repo.path())
        .args(["add", "nested dir/keep"])
        .success()
        .unwrap();
    create_commit(
        repo.path(),
        "Keep the test working directory in the pod checkout",
    );
    write_test_devcontainer(
        &repo,
        &formatdoc! {r#"
            RUN echo 'export EXEC_PROBED=from-profile' >> /home/{TEST_USER}/.bashrc
        "#},
        r#", "userEnvProbe": "interactiveShell", "remoteEnv": {"EXEC_REMOTE": "${localEnv:EXEC_LOCAL}"}"#,
    );
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .env("EXEC_LOCAL", "from-launch")
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);

    let output = pod_command(&repo, &daemon)
        .current_dir(repo.path().join("nested dir"))
        .env("EXEC_LOCAL", "must-not-leak")
        .args([
            "exec", "test", "--", "sh", "-c",
            "id -un; pwd; printf '%s\\n' \"$EXEC_PROBED\" \"$EXEC_REMOTE\" \"$1\"; printf 'failure\\n' >&2; exit 42",
            "sh", "$HOME; $(touch /tmp/exec-argv-expanded)",
        ])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(42),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("{TEST_USER}\n{TEST_REPO_PATH}/nested dir\nfrom-profile\nfrom-launch\n$HOME; $(touch /tmp/exec-argv-expanded)\n")
    );
    assert_eq!(output.stderr, b"failure\n");
    pod_command(&repo, &daemon)
        .args([
            "exec",
            "test",
            "--",
            "test",
            "!",
            "-e",
            "/tmp/exec-argv-expanded",
        ])
        .success()
        .unwrap();
    let output = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "echo shell syntax"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("starting exec program"));
}

#[test]
fn exec_streams_binary_input_and_output_before_stdin_eof() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);

    let mut child = pod_command(&repo, &daemon)
        .args([
            "exec",
            "test",
            "--",
            "sh",
            "-c",
            "printf 'ready\\n'; tee /dev/stderr",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut greeting = [0; 6];
    stdout.read_exact(&mut greeting).unwrap();
    assert_eq!(
        &greeting, b"ready\n",
        "output must arrive while stdin is still open"
    );
    child.stdout = Some(stdout);
    let input: Vec<u8> = (0..3 * 1024 * 1024).map(|index| index as u8).collect();
    let expected = input.clone();
    let mut stdin = child.stdin.take().unwrap();
    let writer = thread::spawn(move || stdin.write_all(&input).unwrap());
    let output = child.wait_with_output().unwrap();
    writer.join().unwrap();
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, expected);
    assert_eq!(output.stderr, expected);
}

#[test]
fn exec_reuses_connected_transport_for_first_and_concurrent_commands() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let calls = home.path().join("engine-calls");
    fs::write(&calls, "").unwrap();
    for engine in ["docker", "podman", "kubectl"] {
        let wrapper = home.bin_dir().join(engine);
        if !wrapper.exists() {
            continue;
        }
        let real = fs::canonicalize(&wrapper).unwrap();
        fs::remove_file(&wrapper).unwrap();
        let real = real.to_str().unwrap().replace('\'', "'\\''");
        let calls = calls.to_str().unwrap().replace('\'', "'\\''");
        fs::write(
            &wrapper,
            formatdoc! {"
            #!/bin/sh
            printf '<%s>' \"$@\" >> '{calls}'
            printf '\\n' >> '{calls}'
            exec '{real}' \"$@\"
        "},
        )
        .unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);
    let before = fs::read_to_string(&calls).unwrap();
    let execs_before = before
        .lines()
        .filter(|line| line.contains("<exec>"))
        .count();
    // A cached execution must also work while local launch configuration is
    // being edited, without falling through to pod preparation.
    fs::write(
        repo.path().join(".devcontainer/devcontainer.json"),
        "invalid json",
    )
    .unwrap();
    let started = Instant::now();
    pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "true"])
        .success()
        .unwrap();
    eprintln!("First connected exec true: {:?}", started.elapsed());

    let mut waiting = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "cat"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = waiting.stdin.take().unwrap();
    stdin.write_all(b"ready").unwrap();
    let mut stdout = waiting.stdout.take().unwrap();
    let mut ready = [0; 5];
    stdout.read_exact(&mut ready).unwrap();
    assert_eq!(&ready, b"ready");
    waiting.stdout = Some(stdout);
    let commands: Vec<_> = (0..4)
        .map(|_| {
            pod_command(&repo, &daemon)
                .args(["exec", "test", "--", "true"])
                .spawn()
                .unwrap()
        })
        .collect();
    for mut command in commands {
        assert!(command.wait().unwrap().success());
    }
    drop(stdin);
    assert!(waiting.wait_with_output().unwrap().status.success());
    let after = fs::read_to_string(&calls).unwrap();
    assert_eq!(
        after.lines().filter(|line| line.contains("<exec>")).count(),
        execs_before,
        "exec opened a new container transport:\n{after}"
    );
}

#[test]
fn exec_blocked_input_does_not_stall_another_command() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);

    let mut blocked_commands = Vec::new();
    for _ in 0..2 {
        let mut blocked = pod_command(&repo, &daemon)
            .args([
                "exec",
                "test",
                "--",
                "sh",
                "-c",
                "printf ready; exec sleep 60",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = [0; 5];
        blocked
            .stdout
            .as_mut()
            .unwrap()
            .read_exact(&mut ready)
            .unwrap();
        assert_eq!(&ready, b"ready");
        let mut stdin = blocked.stdin.take().unwrap();
        let writer = thread::spawn(
            move || match stdin.write_all(&vec![b'x'; 16 * 1024 * 1024]) {
                Ok(()) => panic!("sleep unexpectedly consumed stdin"),
                Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe),
            },
        );
        blocked_commands.push((blocked, writer));
        thread::sleep(Duration::from_millis(200));
    }
    let mut probe = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "true"])
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = probe.try_wait().unwrap() {
            break Some(status);
        }
        if Instant::now() >= deadline {
            probe.kill().unwrap();
            probe.wait().unwrap();
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    for (mut blocked, writer) in blocked_commands {
        kill(Pid::from_raw(blocked.id() as i32), Signal::SIGINT).unwrap();
        assert_eq!(blocked.wait().unwrap().code(), Some(130));
        writer.join().unwrap();
    }
    assert!(
        status.is_some_and(|status| status.success()),
        "commands that stopped consuming stdin blocked another exec"
    );
}

#[test]
fn exec_blocked_output_does_not_stall_another_command() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);

    let mut blocked_commands = Vec::new();
    for _ in 0..3 {
        let mut child = pod_command(&repo, &daemon)
            .args(["exec", "test", "--", "sh", "-c", "printf ready; exec yes"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = [0; 5];
        child
            .stdout
            .as_mut()
            .unwrap()
            .read_exact(&mut ready)
            .unwrap();
        assert_eq!(&ready, b"ready");
        blocked_commands.push(child);
        thread::sleep(Duration::from_millis(200));
    }
    let mut probe = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "true"])
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = probe.try_wait().unwrap() {
            break Some(status);
        }
        if Instant::now() >= deadline {
            probe.kill().unwrap();
            probe.wait().unwrap();
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    for mut child in blocked_commands {
        child.kill().unwrap();
        child.wait().unwrap();
    }
    assert!(
        status.is_some_and(|status| status.success()),
        "commands with paused output consumers blocked another exec"
    );
}

#[test]
fn exec_forwards_signals_to_blocked_input_and_remaining_children() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);

    let mut child = pod_command(&repo, &daemon)
        .args([
            "exec",
            "test",
            "--",
            "sh",
            "-c",
            "printf ready; exec sleep 60",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = [0; 5];
    child
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut ready)
        .unwrap();
    assert_eq!(&ready, b"ready");
    let mut stdin = child.stdin.take().unwrap();
    let writer = thread::spawn(
        move || match stdin.write_all(&vec![b'x'; 16 * 1024 * 1024]) {
            Ok(()) => panic!("sleep unexpectedly consumed stdin"),
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe),
        },
    );
    // Fill the child pipe and HTTP flow-control window before interrupting.
    thread::sleep(Duration::from_millis(100));
    let interrupted = Instant::now();
    kill(Pid::from_raw(child.id() as i32), Signal::SIGINT).unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(130));
    assert!(
        interrupted.elapsed() < Duration::from_secs(5),
        "interrupt was blocked by stdin"
    );
    writer.join().unwrap();

    // The shell exits immediately, but its background child retains stdout.
    // Cancellation must still reach that child while output is being drained.
    let mut child = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "sh", "-c", "sleep 60 & printf ready"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut ready)
        .unwrap();
    assert_eq!(&ready, b"ready");
    thread::sleep(Duration::from_millis(100));
    let interrupted = Instant::now();
    kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM).unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(0));
    assert!(
        interrupted.elapsed() < Duration::from_secs(5),
        "background child ignored termination"
    );
}

#[test]
fn exec_client_disconnect_terminates_children_without_losing_connection() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);

    let mut child = pod_command(&repo, &daemon)
        .args([
            "exec",
            "test",
            "--",
            "sh",
            "-c",
            "sleep 60 & printf '%s\\n' \"$!\"; wait",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut pid = Vec::new();
    loop {
        let mut byte = [0];
        stdout.read_exact(&mut byte).unwrap();
        if byte[0] == b'\n' {
            break;
        }
        pid.push(byte[0]);
    }
    let pid = String::from_utf8(pid).unwrap();
    kill(Pid::from_raw(child.id() as i32), Signal::SIGKILL).unwrap();
    child.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = pod_command(&repo, &daemon)
            .args([
                "exec",
                "test",
                "--",
                "sh",
                "-c",
                indoc! {r#"
                if ! kill -0 "$1" 2>/dev/null; then exit 0; fi
                grep -q '^State:.*Z' "/proc/$1/status"
            "#},
                "sh",
                &pid,
            ])
            .output()
            .unwrap();
        if output.status.success() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "remote child survived client disconnect: {:?}",
            output.stderr
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn exec_does_not_replay_a_command_when_the_server_dies() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);

    let output = pod_command(&repo, &daemon)
        .args([
            "exec",
            "test",
            "--",
            "sh",
            "-c",
            indoc! {r#"
            echo attempt >> /tmp/exec-attempts
            kill -KILL "$PPID"
        "#},
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("outcome is unknown"),
        "{:?}",
        output.stderr
    );

    pod_command(&repo, &daemon)
        .args(["connect", "test"])
        .success()
        .unwrap();
    wait_connected(&repo, &daemon);
    let output = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "cat", "/tmp/exec-attempts"])
        .success()
        .unwrap();
    assert_eq!(output, b"attempt\n");
}

#[test]
fn exec_requires_an_existing_connected_pod() {
    if !executor_supports_stop() {
        return;
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    let missing = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "true"])
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("not found"));
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .unwrap();
    let stopped = pod_command(&repo, &daemon)
        .args(["exec", "test", "--", "true"])
        .output()
        .unwrap();
    assert!(!stopped.status.success());
    assert!(String::from_utf8_lossy(&stopped.stderr).contains("not connected"));
}
