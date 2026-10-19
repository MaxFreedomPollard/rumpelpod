// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Native lifecycle commands must address the server used by rumpel codex.

use std::fs;
use std::time::Duration;

use indoc::indoc;
use rumpelpod::CommandExt;

use super::common::{setup_codex_test_repo, setup_controlled_home, CodexSession};
use crate::common::{pod_command, write_test_devcontainer};
use crate::executor::executor_supports_stop;

#[test]
fn codex_daemon_is_not_activated_by_installation() {
    let (_home, repo, _executor, daemon) = setup_codex_test_repo();
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "test",
            "--",
            "sh",
            "-c",
            indoc! {r#"
                test -x /opt/rumpelpod/bin/codex &&
                test ! -e /opt/rumpelpod/codex-daemon-enabled &&
                test ! -e "$HOME/.codex/app-server-daemon/daemon.pid"
            "#},
        ])
        .success()
        .expect("having Codex available on both machines must not start a daemon");

    if !executor_supports_stop() {
        return;
    }

    // Even a native installation in an image or home volume is not activation
    // by rumpel codex. Native commands remain free to start and stop it.
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "sh",
            "-c",
            "/opt/rumpelpod/bin/codex app-server daemon start && /opt/rumpelpod/bin/codex app-server daemon stop",
        ])
        .success()
        .expect("prepare a stopped native installation");
    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .expect("stop unactivated pod");
    let output = pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "/opt/rumpelpod/bin/codex",
            "app-server",
            "daemon",
            "stop",
        ])
        .success()
        .expect("probe daemon after pod restart");
    let stopped: serde_json::Value = serde_json::from_slice(&output).expect("native stop JSON");
    assert_eq!(
        stopped["status"], "notRunning",
        "unactivated daemon restarted"
    );
}

#[test]
fn codex_first_use_requires_credentials() {
    let (home, repo, _executor, daemon) = setup_codex_test_repo();
    fs::remove_file(home.path().join(".codex/auth.json"))
        .expect("remove credentials before first use");
    let mut missing_login = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    missing_login.wait_for_with_timeout(
        "no codex credentials found on the host or in the pod",
        Duration::from_secs(30),
    );
    missing_login.wait_for_exit();

    // A failed initialization must still accept credentials on the next try.
    setup_controlled_home(&home);
    let mut logged_in = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    logged_in.wait_for_ready();
}

#[test]
fn codex_failed_restart_keeps_pod_accessible() {
    if !executor_supports_stop() {
        return;
    }
    let (home, repo, _executor, daemon) = setup_codex_test_repo();
    let mut first = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    first.wait_for_ready();
    first.send("/exit");
    first.wait_for_exit();

    pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "sh",
            "-c",
            r#"printf '{' > "$HOME/.codex/app-server-daemon/settings.json""#,
        ])
        .success()
        .expect("make native daemon settings invalid before restarting the pod");
    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .expect("stop the activated pod");

    let settings = pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "sh",
            "-c",
            "cat \"$HOME/.codex/app-server-daemon/settings.json\"",
        ])
        .success()
        .expect("enter must not depend on successful Codex startup");
    assert_eq!(settings, b"{");
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "sh",
            "-c",
            "rm \"$HOME/.codex/app-server-daemon/settings.json\"",
        ])
        .success()
        .expect("restore native defaults through the ordinary shell path");

    let mut recovered = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    recovered.wait_for_ready();
}

#[test]
fn codex_native_commands_manage_the_frontends_daemon() {
    let (home, repo, _executor, daemon) = setup_codex_test_repo();
    let mut first = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    first.wait_for_ready();
    first.send("What is the capital of France? Reply with just the city name, nothing else.");
    first.wait_for_with_timeout("Paris", Duration::from_secs(30));
    first.send("/exit");
    first.wait_for_exit();

    let output = pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "/opt/rumpelpod/bin/codex",
            "app-server",
            "daemon",
            "start",
        ])
        .success()
        .expect("native start recognizes rumpel's daemon");
    let started: serde_json::Value = serde_json::from_slice(&output).expect("native startup JSON");
    assert_eq!(started["status"], "alreadyRunning");
    assert_eq!(started["backend"], "pid");
    assert!(started["socketPath"]
        .as_str()
        .unwrap()
        .ends_with("app-server-control.sock"));

    // Pairing must reach this daemon's RPC endpoint. A real pairing needs the
    // user's ChatGPT account; our API-key fixture should stop at enablement.
    let pairing = pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "/opt/rumpelpod/bin/codex",
            "remote-control",
            "pair",
        ])
        .output()
        .expect("native pairing command reaches the control socket");
    assert!(!pairing.status.success());
    let stderr = String::from_utf8_lossy(&pairing.stderr);
    assert!(
        stderr.contains("remote control pairing requires remote control to be enabled"),
        "{stderr}"
    );

    // An old host can resend its original auth/config after losing live state.
    // The pod must retain native refreshes and local config edits on reconnect.
    pod_command(&repo, &daemon)
        .args(["enter", "test", "--", "sh", "-c", indoc! {r#"
            set -eu
            printf '{"auth_mode":"apikey","OPENAI_API_KEY":"sk-pod-owned-credentials"}' > "$HOME/.codex/auth.json"
            printf '\n# pod-owned-config\n' >> "$HOME/.codex/config.toml"
        "#}])
        .success()
        .expect("simulate native credential refresh and config edits");

    let output = pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "/opt/rumpelpod/bin/codex",
            "app-server",
            "daemon",
            "stop",
        ])
        .success()
        .expect("native stop works without a service manager");
    let stopped: serde_json::Value = serde_json::from_slice(&output).expect("native stop JSON");
    assert_eq!(stopped["status"], "stopped");

    // Give the status monitor time to reconnect: observing must not undo stop.
    std::thread::sleep(Duration::from_secs(3));
    let output = pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "/opt/rumpelpod/bin/codex",
            "app-server",
            "daemon",
            "stop",
        ])
        .success()
        .expect("probe stopped native daemon");
    let stopped: serde_json::Value = serde_json::from_slice(&output).expect("native stop JSON");
    assert_eq!(stopped["status"], "notRunning", "monitor restarted daemon");

    // Native login belongs to the pod, including when the host logs out while
    // the daemon is stopped and there is no live state to suppress an upload.
    fs::remove_file(home.path().join(".codex/auth.json"))
        .expect("remove host credentials before reconnecting to the pod-owned login");

    let mut resumed = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    resumed.wait_for_with_timeout("Paris", Duration::from_secs(30));
    resumed.send("/status");
    resumed.wait_for_with_timeout("Full Access", Duration::from_secs(30));
    resumed.send("/exit");
    resumed.wait_for_exit();
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "sh",
            "-c",
            indoc! {r#"
            set -eu
            grep sk-pod-owned-credentials "$HOME/.codex/auth.json"
            grep pod-owned-config "$HOME/.codex/config.toml"
        "#},
        ])
        .success()
        .expect("host credential uploads preserve native-owned state");

    pod_command(&repo, &daemon)
        .args([
            "enter",
            "test",
            "--",
            "/opt/rumpelpod/bin/codex",
            "app-server",
            "daemon",
            "restart",
        ])
        .success()
        .expect("native restart addresses the same instance");
    let mut restarted = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    restarted.wait_for_with_timeout("Paris", Duration::from_secs(30));
}

#[test]
fn codex_fork_preserves_threads_without_cloning_mobile_identity() {
    let (home, repo, _executor, daemon) = setup_codex_test_repo();
    // Keep the fixture CLI compatible with the test image's libc; Wolfi's
    // newest SQLite binary currently requires a newer libc than it provides.
    write_test_devcontainer(&repo, "RUN apk add --no-cache sqlite=3.51.1-r0", "");
    let mut source = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    source.wait_for_ready();
    source.send("What is the capital of France? Reply with just the city name, nothing else.");
    source.wait_for_with_timeout("Paris", Duration::from_secs(30));
    source.send("/exit");
    source.wait_for_exit();

    // Seed enrollment metadata without a real mobile account. The source's
    // live daemon keeps remote control disabled until a native restart.
    let identity = pod_command(&repo, &daemon)
        .args(["enter", "test", "--", "sh", "-c", indoc! {r#"
            set -eu
            cat "$HOME/.codex/installation_id"
            printf '{"remoteControlEnabled":true}' > "$HOME/.codex/app-server-daemon/settings.json"
            sqlite3 "$HOME/.codex/state_5.sqlite" "INSERT INTO remote_control_enrollments
                (websocket_url, account_id, app_server_client_name, server_id, environment_id, server_name, updated_at, remote_control_enabled)
                VALUES ('wss://example.invalid', 'test-account', '', 'source-server', 'source-env', 'source', 0, 1);
                CREATE TABLE rumpel_snapshot_test(value TEXT);
                INSERT INTO rumpel_snapshot_test VALUES ('preserved')"
            test -s "$HOME/.codex/state_5.sqlite-wal"
        "#}])
        .success()
        .expect("prepare source mobile identity");

    pod_command(&repo, &daemon)
        .args(["fork", "test", "forked"])
        .success()
        .expect("snapshot a running native daemon, including its updater socket");
    pod_command(&repo, &daemon)
        .args(["enter", "forked", "--", "sh", "-c", indoc! {r#"
            set -eu
            test ! -e /opt/rumpelpod/codex-daemon-enabled
            test ! -e "$HOME/.codex/installation_id"
            test ! -e "$HOME/.codex/app-server-daemon/daemon.pid"
            test ! -e "$HOME/.codex/app-server-daemon/daemon-updater.sock"
            test "$(sqlite3 "$HOME/.codex/state_5.sqlite" 'SELECT count(*) FROM remote_control_enrollments')" = 0
            test "$(sqlite3 "$HOME/.codex/state_5.sqlite" 'SELECT value FROM rumpel_snapshot_test')" = preserved
            grep '"remoteControlEnabled": false' "$HOME/.codex/app-server-daemon/settings.json"
        "#}])
        .success()
        .expect("fork starts unactivated and unpaired");

    let mut forked = CodexSession::spawn_named(&repo, &daemon, home.path(), "forked", &[]);
    forked.wait_for_with_timeout("Paris", Duration::from_secs(30));
    let fork_identity = pod_command(&repo, &daemon)
        .args([
            "enter",
            "forked",
            "--",
            "sh",
            "-c",
            "cat \"$HOME/.codex/installation_id\"",
        ])
        .success()
        .expect("read new fork identity");
    assert_ne!(identity, fork_identity);

    let source_identity = pod_command(&repo, &daemon)
        .args(["enter", "test", "--", "sh", "-c", indoc! {r#"
            set -eu
            test "$(sqlite3 "$HOME/.codex/state_5.sqlite" 'SELECT count(*) FROM remote_control_enrollments')" = 1
            cat "$HOME/.codex/installation_id"
        "#}])
        .success()
        .expect("source enrollment is unchanged");
    assert_eq!(identity, source_identity);
}

#[test]
fn codex_fork_reports_database_snapshot_failure() {
    let (_home, repo, _executor, daemon) = setup_codex_test_repo();
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "test",
            "--",
            "sh",
            "-c",
            indoc! {r#"
            mkdir -p "$HOME/.codex" &&
            printf 'not a SQLite database' > "$HOME/.codex/state_5.sqlite"
        "#},
        ])
        .success()
        .expect("prepare unreadable Codex database");
    let output = pod_command(&repo, &daemon)
        .args(["fork", "test", "forked"])
        .output()
        .expect("attempt snapshot");
    assert!(!output.status.success(), "partial snapshot was accepted");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("codex"), "{stderr}");
}
