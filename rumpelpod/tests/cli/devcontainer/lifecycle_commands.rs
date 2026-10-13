// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for devcontainer.json lifecycle commands.
//!
//! Tests verify the execution semantics of lifecycle commands as specified in
//! the Dev Container spec: ordering, frequency, command formats, failure
//! propagation, and the `waitFor` setting.

use indoc::{formatdoc, indoc};
use rumpelpod::CommandExt;
use std::fs;
use std::process::Command;

use crate::common::{pod_command, TestDaemon, TestHome, TestRepo, TEST_REPO_PATH, TEST_USER};
use crate::executor::ExecutorResources;

/// Write a devcontainer.json with the given lifecycle command properties spliced
/// in alongside a standard build section.
fn write_devcontainer_with_lifecycle(repo: &TestRepo, lifecycle_json: &str) {
    let devcontainer_dir = repo.path().join(".devcontainer");
    fs::create_dir_all(&devcontainer_dir).expect("Failed to create .devcontainer directory");

    let dockerfile = formatdoc! {r#"
        FROM cgr.dev/chainguard/wolfi-base
        RUN apk add --no-cache git shadow
        RUN useradd -m -u 1000 {TEST_USER}
        COPY --chown={TEST_USER}:{TEST_USER} . {TEST_REPO_PATH}
        USER {TEST_USER}
    "#};
    fs::write(devcontainer_dir.join("Dockerfile"), dockerfile).expect("Failed to write Dockerfile");

    let devcontainer_json = formatdoc! {r#"
        {{
            "build": {{
                "dockerfile": "Dockerfile",
                "context": ".."
            }},
            "workspaceFolder": "{TEST_REPO_PATH}",
            "containerUser": "{TEST_USER}",
                        {lifecycle_json}
        }}
    "#};

    fs::write(
        devcontainer_dir.join("devcontainer.json"),
        devcontainer_json,
    )
    .expect("Failed to write devcontainer.json");
}

#[test]
fn lifecycle_commands_expand_container_workspace_folder() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        indoc! {r#"
            "onCreateCommand": {
                "check": ["sh", "${containerWorkspaceFolder}/.devcontainer/check.sh", "on-create", "${containerWorkspaceFolderBasename}"]
            },
            "updateContentCommand": ["sh", "${containerWorkspaceFolder}/.devcontainer/check.sh", "update-content", "${containerWorkspaceFolderBasename}"],
            "postCreateCommand": "sh '${containerWorkspaceFolder}/.devcontainer/check.sh' post-create '${containerWorkspaceFolderBasename}'",
            "postStartCommand": {
                "check": "sh '${containerWorkspaceFolder}/.devcontainer/check.sh' post-start '${containerWorkspaceFolderBasename}'"
            },
            "waitFor": "postStartCommand"
        "#},
    );
    fs::write(
        repo.path().join(".devcontainer/check.sh"),
        "printf '%s:%s\\n' \"$1\" \"$2\" >> /tmp/container_workspace_folder_expanded\n",
    )
    .expect("Failed to write lifecycle check script");
    // Checkout preparation removes untracked files from the baked workspace.
    Command::new("git")
        .args(["add", ".devcontainer/check.sh"])
        .current_dir(repo.path())
        .success()
        .expect("staging the lifecycle script failed");
    Command::new("git")
        .args(["commit", "-m", "Keep the lifecycle script in the checkout"])
        .current_dir(repo.path())
        .success()
        .expect("committing the lifecycle script failed");

    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-container-workspace",
            "--",
            "cat",
            "/tmp/container_workspace_folder_expanded",
        ])
        .success()
        .expect("rumpel enter failed");

    assert_eq!(
        String::from_utf8_lossy(&stdout).trim(),
        indoc! {"
            on-create:workspace
            update-content:workspace
            post-create:workspace
            post-start:workspace
        "}
        .trim(),
        "all lifecycle command forms must resolve the container workspace"
    );
}

#[test]
fn lifecycle_commands_expand_actual_container_workspace_folder() {
    let repo = TestRepo::new();
    write_devcontainer_with_lifecycle(
        &repo,
        indoc! {r#"
            "onCreateCommand": [
                "sh", "-c", "printf '%s\\n' \"$@\" > /tmp/lifecycle-workspace", "check",
                "${containerWorkspaceFolder}", "${containerWorkspaceFolderBasename}"
            ]
        "#},
    );
    // The suffix distinguishes the host directory name from the container's.
    let config_path = repo.path().join(".devcontainer/devcontainer.json");
    let mut config: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["workspaceFolder"] =
        serde_json::json!("/workspaces/${localWorkspaceFolderBasename}-pod");
    fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();

    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-actual-workspace",
            "--",
            "cat",
            "/tmp/lifecycle-workspace",
        ])
        .success()
        .expect("enter with a workspace derived from the host path failed");
    let host_basename = repo.path().file_name().unwrap().to_str().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&stdout),
        format!("/workspaces/{host_basename}-pod\n{host_basename}-pod\n"),
        "lifecycle commands must use the workspace that was actually created"
    );
}

#[test]
fn lifecycle_commands_preserve_literal_local_env_values() {
    let repo = TestRepo::new();
    write_devcontainer_with_lifecycle(
        &repo,
        indoc! {r#"
            "remoteEnv": { "LITERAL_VALUE": "${localEnv:RUMPELPOD_LIFECYCLE_LITERAL}" },
            "onCreateCommand": "printf '%s' \"$LITERAL_VALUE\" > /tmp/lifecycle-literal"
        "#},
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    let literal = "${localEnv:RUMPELPOD_LITERAL_UNSET:unexpected-expansion}";
    let stdout = pod_command(&repo, &daemon)
        .env("RUMPELPOD_LIFECYCLE_LITERAL", literal)
        .args([
            "enter",
            "--create",
            "lc-literal-env",
            "--",
            "cat",
            "/tmp/lifecycle-literal",
        ])
        .success()
        .expect("enter with a literal environment value failed");
    assert_eq!(
        String::from_utf8_lossy(&stdout),
        literal,
        "forwarded environment values must not be expanded a second time"
    );
}

/// onCreateCommand should run only on first container creation — not on
/// subsequent enters that reuse the same container.
#[test]
fn on_create_command_runs_once() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(&repo, r#""onCreateCommand": "touch /tmp/on_create_marker""#);
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // First enter -- onCreateCommand should have created the marker file.
    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-once",
            "--",
            "cat",
            "/tmp/on_create_marker",
        ])
        .success()
        .expect("first rumpel enter failed");
    // File exists, so cat succeeds (empty output is fine for an empty file).
    let _ = String::from_utf8_lossy(&stdout);

    // Remove the marker so we can detect if the command runs again.
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-once",
            "--",
            "rm",
            "/tmp/on_create_marker",
        ])
        .success()
        .expect("marker removal failed");

    // Second enter -- onCreateCommand must NOT run again.
    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-once",
            "--",
            "sh",
            "-c",
            "test -f /tmp/on_create_marker && echo exists || echo missing",
        ])
        .success()
        .expect("second rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(
        stdout.trim(),
        "missing",
        "onCreateCommand should not re-run on subsequent enters"
    );
}

/// postCreateCommand must run after onCreateCommand — verify via sequence
/// numbers written to files.
#[test]
fn post_create_command_runs_after_on_create() {
    let repo = TestRepo::new();

    // Each command appends a line; ordering is then verifiable.
    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": "echo 1 >> /tmp/lifecycle_order",
            "postCreateCommand": "echo 2 >> /tmp/lifecycle_order""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-order",
            "--",
            "cat",
            "/tmp/lifecycle_order",
        ])
        .success()
        .expect("rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(
        lines,
        vec!["1", "2"],
        "onCreateCommand must run before postCreateCommand"
    );
}

/// postStartCommand should execute every time the container starts — including
/// after a stop/start cycle.
#[test]
fn post_start_command_runs_each_start() {
    // Needs `rumpel stop`, which k8s does not support.
    if !crate::executor::executor_supports_stop() {
        return;
    }
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        r#""postStartCommand": "echo start >> /tmp/start_count""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // First enter -- triggers a start.
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-start", "--", "echo", "ok"])
        .success()
        .expect("first enter failed");

    // Stop the pod, then start it again.
    pod_command(&repo, &daemon)
        .args(["stop", "lc-start"])
        .success()
        .expect("rumpel stop failed");

    pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-start", "--", "echo", "ok"])
        .success()
        .expect("second enter failed");

    // Verify the command ran twice.
    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-start",
            "--",
            "sh",
            "-c",
            "grep -c start /tmp/start_count",
        ])
        .success()
        .expect("count check failed");

    let count: usize = String::from_utf8_lossy(&stdout)
        .trim()
        .parse()
        .expect("expected a number");
    assert!(
        count >= 2,
        "postStartCommand should have run at least twice, ran {count} times"
    );
}

/// postAttachCommand is not supported (it runs in the attaching shell in
/// VS Code, which has no equivalent in an agent runner). Setting it should
/// emit an unsupported-field warning and not execute the command.
#[test]
fn post_attach_command_is_unsupported() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(&repo, r#""postAttachCommand": "touch /tmp/attach_ran""#);
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-attach", "--", "echo", "ok"])
        .output()
        .expect("failed to run pod command");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("postAttachCommand") && stderr.contains("not supported"),
        "stderr should warn about unsupported postAttachCommand, got: {stderr}",
    );

    // The command must not have been executed.
    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-attach",
            "--",
            "sh",
            "-c",
            "test -f /tmp/attach_ran && echo exists || echo missing",
        ])
        .success()
        .expect("check enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(
        stdout.trim(),
        "missing",
        "postAttachCommand must not be executed"
    );
}

/// A lifecycle command given as a plain string should be executed via a shell.
#[test]
fn lifecycle_command_string_format() {
    let repo = TestRepo::new();

    // Use shell-specific syntax (variable expansion) to prove a shell is used.
    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": "echo hello > /tmp/string_fmt""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-str",
            "--",
            "cat",
            "/tmp/string_fmt",
        ])
        .success()
        .expect("rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(stdout.trim(), "hello");
}

/// A lifecycle command given as an array should be executed directly (no shell).
#[test]
fn lifecycle_command_array_format() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": ["sh", "-c", "echo hello > /tmp/array_fmt"]"#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-arr", "--", "cat", "/tmp/array_fmt"])
        .success()
        .expect("rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(stdout.trim(), "hello");
}

/// A lifecycle command given as an object should run each value in parallel.
/// Both commands must have completed by the time we inspect results.
#[test]
fn lifecycle_command_object_parallel() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": {
                "a": "echo alpha > /tmp/parallel_a",
                "b": "echo bravo > /tmp/parallel_b"
            }"#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-obj",
            "--",
            "sh",
            "-c",
            "cat /tmp/parallel_a /tmp/parallel_b | sort",
        ])
        .success()
        .expect("rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(
        lines,
        vec!["alpha", "bravo"],
        "both parallel commands should have run"
    );
}

/// If onCreateCommand fails (non-zero exit), postCreateCommand should NOT run.
#[test]
fn lifecycle_command_failure_stops_chain() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": "exit 1",
            "postCreateCommand": "touch /tmp/should_not_exist""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // The first enter should fail because onCreateCommand exits 1.
    let _ = pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-fail", "--", "echo", "ok"])
        .output();

    // Second enter should succeed (failed lifecycle commands are marked as
    // "ran" and not retried), and postCreateCommand should never have executed.
    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-fail",
            "--",
            "sh",
            "-c",
            "test -f /tmp/should_not_exist && echo exists || echo missing",
        ])
        .success()
        .expect("check enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(
        stdout.trim(),
        "missing",
        "postCreateCommand must not run when onCreateCommand fails"
    );
}

/// The `waitFor` property should block `rumpel enter` until the specified
/// lifecycle command has completed.
#[test]
fn wait_for_setting() {
    let repo = TestRepo::new();

    // postCreateCommand sleeps briefly then writes a marker. With
    // "waitFor": "postCreateCommand", the enter should block until it finishes.
    write_devcontainer_with_lifecycle(
        &repo,
        r#""postCreateCommand": "sleep 1 && echo done > /tmp/wait_marker",
            "waitFor": "postCreateCommand""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-wait",
            "--",
            "cat",
            "/tmp/wait_marker",
        ])
        .success()
        .expect("rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(
        stdout.trim(),
        "done",
        "rumpel enter should have waited for postCreateCommand to finish"
    );
}

/// On first creation, updateContentCommand must run after onCreateCommand
/// and before postCreateCommand.
#[test]
fn update_content_command_runs_after_on_create() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": "echo 1 >> /tmp/uc_order",
            "updateContentCommand": "echo 2 >> /tmp/uc_order",
            "postCreateCommand": "echo 3 >> /tmp/uc_order""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-uc-order",
            "--",
            "cat",
            "/tmp/uc_order",
        ])
        .success()
        .expect("rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(
        lines,
        vec!["1", "2", "3"],
        "order must be onCreateCommand -> updateContentCommand -> postCreateCommand"
    );
}

/// updateContentCommand runs during server startup (alongside git sync)
/// but does not re-run on re-entry since content does not change.
#[test]
fn update_content_command_runs_on_reentry() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        r#""updateContentCommand": "echo update >> /tmp/uc_count""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // First enter (creation -- lifecycle runs during server startup)
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-uc-reentry", "--", "echo", "ok"])
        .success()
        .expect("first enter failed");

    // Second enter (re-entry -- server already running, no lifecycle re-run)
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-uc-reentry", "--", "echo", "ok"])
        .success()
        .expect("second enter failed");

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-uc-reentry",
            "--",
            "sh",
            "-c",
            "grep -c update /tmp/uc_count",
        ])
        .success()
        .expect("count check failed");

    let count: usize = String::from_utf8_lossy(&stdout)
        .trim()
        .parse()
        .expect("expected a number");
    assert_eq!(
        count, 1,
        "updateContentCommand should run once at startup, ran {count} times"
    );
}

/// If updateContentCommand fails, postCreateCommand must not run.
#[test]
fn update_content_command_failure_stops_chain() {
    let repo = TestRepo::new();

    // Fail only on first run so re-entry succeeds and we can inspect state.
    write_devcontainer_with_lifecycle(
        &repo,
        r#""updateContentCommand": "if [ ! -f /tmp/uc_fail_done ]; then touch /tmp/uc_fail_done && exit 1; fi",
            "postCreateCommand": "touch /tmp/uc_should_not_exist""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // First enter fails because updateContentCommand exits 1 on first run.
    let _ = pod_command(&repo, &daemon)
        .args(["enter", "--create", "lc-uc-fail", "--", "echo", "ok"])
        .output();

    // Second enter succeeds (updateContentCommand passes on re-run,
    // postCreateCommand was marked as ran after the failure).
    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-uc-fail",
            "--",
            "sh",
            "-c",
            "test -f /tmp/uc_should_not_exist && echo exists || echo missing",
        ])
        .success()
        .expect("check enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(
        stdout.trim(),
        "missing",
        "postCreateCommand must not run when updateContentCommand fails"
    );
}

/// With waitFor set to onCreateCommand, commands after it (like
/// postCreateCommand) should run in the background so enter returns
/// without waiting for them.
///
/// The background postCreateCommand polls for a gate file, then
/// writes a done marker and removes the gate.  The test creates
/// the gate after entering, then waits for it to disappear --
/// proving the background command actually ran.
#[test]
fn wait_for_on_create_attaches_early() {
    let repo = TestRepo::new();

    // The background postCreateCommand waits for /tmp/bg_gate to
    // appear, writes a done marker, and removes the gate file.
    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": "true",
            "postCreateCommand": "echo postCreate: waiting for gate; while [ ! -f /tmp/bg_gate ]; do sleep 0.2; done; echo postCreate: gate found, writing marker; echo done > /tmp/bg_done; rm /tmp/bg_gate; echo postCreate: finished",
            "waitFor": "onCreateCommand""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Enter must return while postCreateCommand is still waiting
    // for the gate file.
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-wait-early",
            "--",
            "echo",
            "attached",
        ])
        .success()
        .expect("enter must return while postCreateCommand is still running");

    // Create the gate file so the background command can proceed.
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-wait-early",
            "--",
            "touch",
            "/tmp/bg_gate",
        ])
        .success()
        .expect("failed to create gate file");

    // Wait for the background command to consume the gate.
    // Loops indefinitely -- a hang here means the background command
    // is broken; the eprintln breadcrumbs help diagnose.
    let mut i = 0;
    loop {
        let result = pod_command(&repo, &daemon)
            .args([
                "enter",
                "--create",
                "lc-wait-early",
                "--",
                "cat",
                "/tmp/bg_done",
            ])
            .output()
            .expect("failed to check done marker");
        if result.status.success() {
            let stdout = String::from_utf8_lossy(&result.stdout);
            if stdout.trim() == "done" {
                return;
            }
        }
        eprintln!("wait_for_on_create_attaches_early: poll {i}, not done yet");
        std::thread::sleep(std::time::Duration::from_secs(1));
        i += 1;
    }
}

/// Without an explicit waitFor the default is updateContentCommand.
/// With only onCreateCommand set, all configured commands complete
/// before enter returns since onCreateCommand precedes the default.
#[test]
fn wait_for_default_waits_for_all() {
    let repo = TestRepo::new();

    write_devcontainer_with_lifecycle(
        &repo,
        r#""onCreateCommand": "echo done > /tmp/default_marker""#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let stdout = pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "lc-wait-default",
            "--",
            "cat",
            "/tmp/default_marker",
        ])
        .success()
        .expect("rumpel enter failed");

    let stdout = String::from_utf8_lossy(&stdout);
    assert_eq!(
        stdout.trim(),
        "done",
        "onCreateCommand should complete before enter returns with default waitFor"
    );
}
