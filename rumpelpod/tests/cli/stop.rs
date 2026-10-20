// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the `rumpel stop` subcommand (multi-pod support).

use std::fs;
use std::time::{Duration, Instant};

use rumpelpod::daemon::protocol::DaemonClient;
use rumpelpod::daemon::reconnect::ReconnectEvent;
use rumpelpod::CommandExt;
use rusqlite::Connection;

use crate::common::{pod_command, write_test_devcontainer, TestDaemon, TestHome, TestRepo};
use crate::executor::{executor_supports_stop, ExecutorResources};

#[test]
fn stop_invalid_stored_compose_config_does_not_strand_pod() {
    if !executor_supports_stop() {
        return;
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    pod_command(&repo, &daemon)
        .args(["enter", "--create", "stop-compose-error", "--", "true"])
        .success()
        .expect("create pod for stop error test");
    let database = home.path().join("state/rumpelpod/db.sqlite");
    let conn = Connection::open(&database).expect("open daemon database");
    conn.execute(
        "UPDATE pods SET agent_service = 'agent', compose_config = '{' WHERE name = 'stop-compose-error'",
        [],
    )
    .expect("corrupt stored Compose model");

    let output = pod_command(&repo, &daemon)
        .args(["stop", "--wait", "stop-compose-error"])
        .output()
        .expect("stop pod with corrupt Compose model");
    assert!(!output.status.success());
    let status: String = conn
        .query_row(
            "SELECT status FROM pods WHERE name = 'stop-compose-error'",
            [],
            |row| row.get(0),
        )
        .expect("read pod status after failed stop");
    assert_eq!(status, "ready");

    conn.execute(
        "UPDATE pods SET agent_service = '', compose_config = '' WHERE name = 'stop-compose-error'",
        [],
    )
    .expect("restore single-container metadata");
    pod_command(&repo, &daemon)
        .args(["connect", "stop-compose-error"])
        .success()
        .expect("failed stop should preserve the live connection");
    pod_command(&repo, &daemon)
        .args(["delete", "--force", "--wait", "stop-compose-error"])
        .success()
        .expect("delete stop error test pod");
}

#[test]
fn stop_multiple_pods() {
    if !executor_supports_stop() {
        return;
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create three pods
    for name in ["sm-a", "sm-b", "sm-c"] {
        let output = pod_command(&repo, &daemon)
            .args(["enter", "--create", name, "--", "true"])
            .output()
            .expect("Failed to run rumpel enter");
        assert!(
            output.status.success(),
            "creating pod '{name}' failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Stop all three in one command
    let output = pod_command(&repo, &daemon)
        .args(["stop", "--wait", "sm-a", "sm-b", "sm-c"])
        .output()
        .expect("Failed to run rumpel stop");
    assert!(
        output.status.success(),
        "multi-stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify all are stopped
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("Failed to list");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for name in ["sm-a", "sm-b", "sm-c"] {
        let line = stdout
            .lines()
            .find(|l| l.contains(name))
            .unwrap_or_else(|| panic!("pod '{name}' should still be listed: {stdout}"));
        assert!(
            line.contains("stopped"),
            "pod '{name}' should be stopped: {line}"
        );
    }
}

#[test]
fn stop_multiple_continues_past_unknown() {
    // When one pod in a multi-stop does not exist, the others should
    // still be stopped and the command should report the failure.
    if !executor_supports_stop() {
        return;
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create two pods
    for name in ["su-a", "su-c"] {
        let output = pod_command(&repo, &daemon)
            .args(["enter", "--create", name, "--", "true"])
            .output()
            .expect("Failed to run rumpel enter");
        assert!(output.status.success());
    }

    // Stop with a nonexistent pod in the middle
    let output = pod_command(&repo, &daemon)
        .args(["stop", "--wait", "su-a", "no-such-pod", "su-c"])
        .output()
        .expect("Failed to run rumpel stop");

    // Command should fail overall because one pod was unknown
    assert!(
        !output.status.success(),
        "should fail when a pod is unknown"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("1 pod(s) could not be stopped"),
        "should report failure count: {stderr}"
    );

    // The other two should still have been stopped
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("Failed to list");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for name in ["su-a", "su-c"] {
        let line = stdout
            .lines()
            .find(|l| l.contains(name))
            .unwrap_or_else(|| panic!("pod '{name}' should still be listed: {stdout}"));
        assert!(
            line.contains("stopped"),
            "pod '{name}' should be stopped despite the unknown one: {line}"
        );
    }
}

/// A client subscribed to pod reconnect events must receive a Stopped
/// event when the pod is intentionally stopped, rather than retrying
/// forever.
#[test]
fn stop_sends_reconnect_stopped_event() {
    if !executor_supports_stop() {
        return;
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let pod_name = "stop-ev";
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", pod_name, "--", "true"])
        .output()
        .expect("Failed to run rumpel enter");
    assert!(
        output.status.success(),
        "creating pod failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Subscribe to reconnect events before stopping.  The daemon's pod
    // event listener is running, so subscribe() returns a receiver.
    let socket_path = daemon.socket_path.clone();
    let repo_path = repo.path().to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let client = DaemonClient::new_unix(&socket_path);
        loop {
            let stream = client
                .pod_reconnect_events(&repo_path, pod_name)
                .expect("pod_reconnect_events failed");
            let mut connected = false;
            for event in stream {
                let event = event.expect("error reading reconnect event");
                connected |= matches!(event, ReconnectEvent::Connected);
                let done = matches!(event, ReconnectEvent::Stopped);
                tx.send(event).expect("stop test receiver disappeared");
                if done {
                    return;
                }
            }
            // Connected normally ends a reconnect attempt. An initial greeting
            // can race this subscription, so subscribe again to observe stop.
            assert!(connected, "reconnect stream ended without a terminal event");
        }
    });

    // Stop the pod -- the subscriber should get a Stopped event.
    let output = pod_command(&repo, &daemon)
        .args(["stop", "--wait", pod_name])
        .output()
        .expect("Failed to run rumpel stop");
    assert!(
        output.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // A subscription after stop must also observe Stopped, so ending and
    // reopening an initial Connected attempt cannot lose the notification.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = rx
            .recv_timeout(remaining)
            .expect("no Stopped event from reconnect stream after stop");
        if matches!(event, ReconnectEvent::Stopped) {
            break;
        }
    }
}

/// When a pod has already been stopped, subscribing to its reconnect
/// events must immediately return a Stopped event (no active listener).
#[test]
fn stop_reconnect_after_stop() {
    if !executor_supports_stop() {
        return;
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let pod_name = "stop-after";
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", pod_name, "--", "true"])
        .output()
        .expect("Failed to run rumpel enter");
    assert!(output.status.success());

    // Stop the pod first.
    let output = pod_command(&repo, &daemon)
        .args(["stop", "--wait", pod_name])
        .output()
        .expect("Failed to run rumpel stop");
    assert!(output.status.success());

    // Now subscribe -- the listener is already gone, so the endpoint
    // should return a Stopped event immediately.
    let client = DaemonClient::new_unix(&daemon.socket_path);
    let stream = client
        .pod_reconnect_events(repo.path(), pod_name)
        .expect("pod_reconnect_events failed");

    let event = stream
        .into_iter()
        .next()
        .expect("stream should have at least one event")
        .expect("error reading reconnect event");
    assert!(
        matches!(event, ReconnectEvent::Stopped),
        "expected Stopped event, got {event:?}"
    );
}
