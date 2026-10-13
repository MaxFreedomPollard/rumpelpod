// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};

use indoc::indoc;

use rumpelpod::daemon::protocol::{ClientContext, ConnectPodRequest, Daemon, DaemonClient};
use rumpelpod::CommandExt;

use super::common::{setup_controlled_home, ClaudeSession};
use crate::common::{pod_command, write_test_devcontainer, TestDaemon, TestHome, TestRepo};
use crate::executor::{executor_supports_stop, ExecutorResources};

// A terminal process is enough to exercise reconnect and session lifetime.
// Keeping it local avoids coupling these tests to Claude downloads or APIs.
fn setup_claude_lifecycle_repo() -> (TestHome, TestRepo, ExecutorResources, TestDaemon) {
    let repo = TestRepo::new();
    let dockerfile = indoc! {r#"
        RUN mkdir -p /opt/rumpelpod/bin
        RUN printf '%s\n' '#!/bin/sh' \
            'set -eu' \
            'test -s "$HOME/.claude/.credentials.json"' \
            'test -s "$HOME/.claude.json"' \
            'printf "Claude ready in %s\n" "$PWD"' \
            'while IFS= read -r line; do printf "received: %s\n" "$line"; done' \
            > /opt/rumpelpod/bin/claude
        RUN chmod 755 /opt/rumpelpod/bin/claude
    "#};
    write_test_devcontainer(&repo, dockerfile, "");
    let home = TestHome::new();
    setup_controlled_home(&home);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    std::fs::write(repo.path().join(".rumpelpod.json"), &executor.json)
        .expect("write .rumpelpod.json");
    (home, repo, executor, daemon)
}

#[test]
fn claude_prepares_config_in_existing_pod() {
    let (home, repo, _executor, daemon) = setup_claude_lifecycle_repo();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .expect("create pod without Claude configuration");

    let client = DaemonClient::new_unix(&daemon.socket_path);
    let connection = client
        .claude_connection(ConnectPodRequest {
            pod_name: "test".into(),
            repo_path: repo.path().to_path_buf(),
            client_context: ClientContext::default(),
        })
        .expect("query unprepared Claude connection");
    assert!(
        connection.is_none(),
        "Claude still needs its credentials copied"
    );

    let mut session = ClaudeSession::spawn_for_pod(
        &repo,
        &daemon,
        home.path(),
        "test",
        false,
        "claude-haiku-4-5",
        &[],
    );
    session.wait_for("Claude ready in /home/testuser/workspace");
    session.send("first session");
    session.wait_for("received: first session");
}

#[test]
fn claude_restarts_stopped_pod() {
    if !executor_supports_stop() {
        return;
    }
    let (home, repo, _executor, daemon) = setup_claude_lifecycle_repo();
    let mut session = ClaudeSession::spawn(&repo, &daemon, home.path(), "claude-haiku-4-5", &[]);
    session.wait_for("Claude ready in /home/testuser/workspace");
    session.write_raw(&[0x01, b'd']);
    session.wait_for_exit();

    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .expect("stop Claude pod");

    let client = DaemonClient::new_unix(&daemon.socket_path);
    let connection = client
        .claude_connection(ConnectPodRequest {
            pod_name: "test".into(),
            repo_path: repo.path().to_path_buf(),
            client_context: ClientContext::default(),
        })
        .expect("query stopped Claude connection");
    assert!(connection.is_none(), "a stopped pod needs preparation");

    let mut session2 = ClaudeSession::spawn_for_pod(
        &repo,
        &daemon,
        home.path(),
        "test",
        false,
        "claude-haiku-4-5",
        &[],
    );
    session2.wait_for("Claude ready in /home/testuser/workspace");
}

#[test]
fn claude_reconnects_after_delete_recreate_same_name() {
    let (home, repo, _executor, daemon) = setup_claude_lifecycle_repo();
    let mut session = ClaudeSession::spawn(&repo, &daemon, home.path(), "claude-haiku-4-5", &[]);
    session.wait_for("Claude ready in /home/testuser/workspace");
    session.write_raw(&[0x01, b'd']);
    session.wait_for_exit();

    pod_command(&repo, &daemon)
        .args(["delete", "--wait", "--force", "test"])
        .success()
        .expect("delete Claude pod");

    let mut session2 = ClaudeSession::spawn(&repo, &daemon, home.path(), "claude-haiku-4-5", &[]);
    session2.wait_for("Claude ready in /home/testuser/workspace");
    session2.send("replacement session");
    session2.wait_for("received: replacement session");
}

#[test]
fn claude_warm_attach_skips_pod_preparation() {
    let (home, repo, _executor, daemon) = setup_claude_lifecycle_repo();
    let mut session = ClaudeSession::spawn(&repo, &daemon, home.path(), "unused", &[]);
    session.wait_for("Claude ready in /home/testuser/workspace");
    session.send("retained session");
    session.wait_for("received: retained session");
    session.write_raw(&[0x01, b'd']);
    session.wait_for_exit();

    // The fast path requires the daemon's asynchronous readiness greeting.
    // A tiny shell can render before that event reaches the daemon.
    let client = DaemonClient::new_unix(&daemon.socket_path);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .claude_connection(ConnectPodRequest {
                pod_name: "test".into(),
                repo_path: repo.path().to_path_buf(),
                client_context: ClientContext::default(),
            })
            .expect("query prepared Claude connection")
            .is_some()
        {
            break;
        }
        assert!(Instant::now() < deadline, "pod readiness was not observed");
        std::thread::sleep(Duration::from_millis(20));
    }

    std::fs::write(
        repo.path().join(".devcontainer/devcontainer.json"),
        "invalid json",
    )
    .expect("invalidate host devcontainer config");
    std::fs::remove_file(home.path().join(".claude/.credentials.json"))
        .expect("remove already copied credentials");

    let mut session2 =
        ClaudeSession::spawn_for_pod(&repo, &daemon, home.path(), "test", false, "unused", &[]);
    session2.wait_for("received: retained session");
    session2.send("still running");
    session2.wait_for("received: still running");
    session2.write_raw(&[0x04]);
    session2.wait_for_exit();

    // An exited terminal needs a new process, but the pod is still prepared.
    let mut session3 =
        ClaudeSession::spawn_for_pod(&repo, &daemon, home.path(), "test", false, "unused", &[]);
    session3.wait_for("Claude ready in /home/testuser/workspace");
    session3.send("new session");
    session3.wait_for("received: new session");
}
