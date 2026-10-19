// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Native sockets must isolate Codex even when pods share the host network.

use std::fs;
use std::time::Duration;

use rumpelpod::CommandExt;

use super::common::{setup_controlled_home, CodexSession};
use crate::common::{pod_command, write_test_devcontainer, TestDaemon, TestHome, TestRepo};
use crate::executor::ExecutorResources;

const CTRL_A: u8 = 0x01;

/// `--network=host` has no equivalent on a remote cluster node and the
/// daemon silently drops it there, so this test only makes sense
/// against the local Docker executor.
fn skip_unless_docker() -> bool {
    if !matches!(
        crate::executor::executor_mode(),
        crate::executor::ExecutorMode::Docker
    ) {
        crate::executor::skip_test();
        return true;
    }
    false
}

#[test]
fn codex_daemons_are_isolated_on_host_network() {
    if skip_unless_docker() {
        return;
    }

    let repo = TestRepo::new();
    write_test_devcontainer(
        &repo,
        "",
        r#",
            "runArgs": ["--network=host"]"#,
    );

    let home = TestHome::new();
    setup_controlled_home(&home);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start_with_local_llm_clis(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).expect("write .rumpelpod.json");

    let alpha_socket = start_codex_and_read_socket(&repo, &daemon, home.path(), "alpha");
    let beta_socket = start_codex_and_read_socket(&repo, &daemon, home.path(), "beta");

    assert_ne!(
        alpha_socket, beta_socket,
        "two host-networked pods must not share the Codex daemon socket"
    );
}

fn start_codex_and_read_socket(
    repo: &TestRepo,
    daemon: &TestDaemon,
    home: &std::path::Path,
    pod_name: &str,
) -> String {
    let mut session = CodexSession::spawn_named(repo, daemon, home, pod_name, &[]);
    session.wait_for_ready_with_timeout(Duration::from_secs(120));

    let output = pod_command(repo, daemon)
        .arg("enter")
        .arg(pod_name)
        .arg("--")
        .args([
            "sh",
            "-c",
            "stat -Lc '%d:%i' \"$HOME/.codex/app-server-control/app-server-control.sock\"",
        ])
        .success()
        .unwrap_or_else(|e| panic!("reading Codex daemon socket identity in {pod_name}: {e}"));

    session.write_raw(&[CTRL_A, b'd']);
    session.wait_for_exit();

    let identity = String::from_utf8(output).expect("socket identity is UTF-8");
    assert!(
        !identity.trim().is_empty(),
        "missing native socket in {pod_name}"
    );
    identity
}
