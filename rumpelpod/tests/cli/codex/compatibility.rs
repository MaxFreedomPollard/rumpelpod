// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Exercise the current host against a real pod binary from before native mode.

use std::path::PathBuf;
use std::time::Duration;

use indoc::indoc;
use rumpelpod::CommandExt;

use super::common::{setup_codex_test_repo, CodexSession};
use crate::common::pod_command;
use crate::executor::{executor_supports_stop, skip_test};

#[test]
fn codex_host_supports_previous_pod_daemon() {
    // Supply a musl binary built from 239fe48 for the executor's architecture.
    // Keeping the fixture external avoids building historical sources or
    // downloading a release during ordinary offline test runs.
    let Some(binary) = std::env::var_os("RUMPELPOD_TEST_OLD_RUMPEL") else {
        skip_test();
        return;
    };
    if !executor_supports_stop() {
        return;
    }
    let binary = PathBuf::from(binary)
        .canonicalize()
        .expect("locate previous pod binary");
    let (home, repo, _executor, daemon) = setup_codex_test_repo();
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .expect("create a pod before installing its previous daemon");
    pod_command(&repo, &daemon)
        .arg("cp")
        .arg(&binary)
        .arg("test:/opt/rumpelpod/bin/rumpel-previous")
        .success()
        .expect("upload the previous pod binary");
    pod_command(&repo, &daemon)
        .args([
            "enter", "test", "--", "sh", "-c",
            "chmod +x /opt/rumpelpod/bin/rumpel-previous && mv /opt/rumpelpod/bin/rumpel-previous /opt/rumpelpod/bin/rumpel",
        ])
        .success()
        .expect("replace the executable without changing the running process");
    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .expect("stop before starting the previous pod daemon");

    let mut first = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    first.wait_for_ready();
    first.send("What is the capital of France? Reply with just the city name, nothing else.");
    first.wait_for_with_timeout("Paris", Duration::from_secs(30));
    first.send("/status");
    first.wait_for_with_timeout("Full Access", Duration::from_secs(30));
    first.send("/exit");
    first.wait_for_exit();
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "sh",
            "-c",
            indoc! {r#"
            set -eu
            test ! -e /opt/rumpelpod/codex-daemon-enabled
            test ! -e "$HOME/.codex/app-server-daemon/daemon.pid"
            pgrep -af '^/opt/rumpelpod/bin/codex app-server --listen ws://'
        "#},
        ])
        .success()
        .expect("the old pod still uses manual app-server lifecycle");

    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .expect("stop the old pod after a conversation");
    let mut resumed = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    resumed.wait_for_with_timeout("Paris", Duration::from_secs(30));
    resumed.send("/status");
    resumed.wait_for_with_timeout("Full Access", Duration::from_secs(30));
}
