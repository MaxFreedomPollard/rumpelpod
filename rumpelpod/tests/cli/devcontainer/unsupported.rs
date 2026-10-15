// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests that unsupported devcontainer.json properties emit warnings.

use std::fs;

use indoc::formatdoc;

use crate::common::{pod_command, TestDaemon, TestHome, TestRepo, TEST_REPO_PATH, TEST_USER};
use crate::executor::ExecutorResources;

/// Write a Dockerfile and devcontainer.json where `extra_fields` is injected
/// as additional top-level JSON properties (include leading comma).
fn write_devcontainer_with_unsupported(repo: &TestRepo, extra_fields: &str) {
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
            "containerUser": "{TEST_USER}"{extra_fields}
        }}
    "#};

    fs::write(
        devcontainer_dir.join("devcontainer.json"),
        devcontainer_json,
    )
    .expect("Failed to write devcontainer.json");
}

#[test]
fn workspace_mount_does_not_warn() {
    let repo = TestRepo::new();

    write_devcontainer_with_unsupported(
        &repo,
        &formatdoc! {r#",
        "workspaceMount": "source=${{localWorkspaceFolder}},target={TEST_REPO_PATH},type=bind""#},
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "workspace-mount", "--", "pwd"])
        .output()
        .expect("Failed to run pod command");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "rumpel enter should succeed with workspaceMount, got: {stderr}",
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        TEST_REPO_PATH,
        "workspaceMount should not interfere with the custom workspaceFolder",
    );
    assert!(
        !stderr.contains("workspaceMount"),
        "stderr should not warn about workspaceMount, got: {stderr}",
    );
}

#[test]
fn warns_on_app_port() {
    let repo = TestRepo::new();

    write_devcontainer_with_unsupported(
        &repo,
        r#",
            "appPort": [3000, 8080]"#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "unsupported-appport", "--", "true"])
        .output()
        .expect("Failed to run pod command");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("appPort") && stderr.contains("not supported"),
        "stderr should warn about unsupported appPort, got: {stderr}",
    );
}

#[test]
fn warns_on_multiple_unsupported() {
    let repo = TestRepo::new();

    write_devcontainer_with_unsupported(
        &repo,
        r#",
            "postAttachCommand": "true",
            "appPort": 3000"#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "unsupported-multi", "--", "true"])
        .output()
        .expect("Failed to run pod command");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("postAttachCommand"),
        "stderr should warn about postAttachCommand, got: {stderr}",
    );
    assert!(
        stderr.contains("appPort"),
        "stderr should warn about appPort, got: {stderr}",
    );
}

#[test]
fn warns_on_features() {
    let repo = TestRepo::new();

    write_devcontainer_with_unsupported(
        &repo,
        r#",
            "features": {
                "ghcr.io/devcontainers/features/node:1": { "version": "20" }
            }"#,
    );
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "unsupported-features", "--", "true"])
        .output()
        .expect("Failed to run pod command");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("features") && stderr.contains("not supported"),
        "stderr should warn about unsupported features, got: {stderr}",
    );
}
