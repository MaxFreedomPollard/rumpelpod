// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};

use indoc::indoc;
use rumpelpod::CommandExt;

use crate::common::TestRepo;

fn hook_repo(remote: &str) -> TestRepo {
    let repo = TestRepo::new();
    for args in [
        vec!["branch", "-M", "test"],
        vec!["config", "rumpelpod.pod-name", "test"],
        vec!["remote", "add", "rumpelpod", remote],
    ] {
        Command::new("git")
            .args(args)
            .current_dir(repo.path())
            .success()
            .expect("configure hook repository");
    }
    let hook = repo.path().join(".git/hooks/reference-transaction");
    fs::write(
        &hook,
        indoc! {r#"
            #!/bin/sh
            exec rumpel git-hook reference-transaction "$@"
        "#},
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    repo
}

fn commit(repo: &TestRepo) -> Output {
    let output = Command::new("git")
        .args(["commit", "--allow-empty", "-m", "offline work"])
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_HTTP_LOW_SPEED_LIMIT", "1")
        .env("GIT_HTTP_LOW_SPEED_TIME", "1")
        .current_dir(repo.path())
        .output()
        .expect("commit with automatic push");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "commit should survive: {stderr}");
    output
}

#[test]
fn gateway_hook_connection_refused_is_quiet() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let repo = hook_repo(&format!("http://{address}/repo.git"));
    let output = commit(&repo);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.is_empty(), "{stderr}");
}

#[test]
fn gateway_hook_repository_errors_are_quiet() {
    let missing = tempfile::tempdir().unwrap();
    let remote = missing.path().join("missing.git");
    let repo = hook_repo(remote.to_str().unwrap());
    let output = commit(&repo);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.is_empty(), "{stderr}");
}

#[test]
fn gateway_hook_lfs_new_branch_offline_is_quiet() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let repo = hook_repo(&format!("http://{address}/repo.git"));
    for args in [
        vec!["lfs", "install", "--local"],
        vec!["lfs", "track", "*.bin"],
        vec!["update-ref", "refs/remotes/host/main", "HEAD"],
    ] {
        Command::new("git")
            .args(args)
            .current_dir(repo.path())
            .success()
            .unwrap();
    }
    fs::write(repo.path().join("payload.bin"), "pending LFS content\n").unwrap();
    Command::new("git")
        .args(["add", ".gitattributes", "payload.bin"])
        .current_dir(repo.path())
        .success()
        .unwrap();
    let output = commit(&repo);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.is_empty(), "{stderr}");

    let output = Command::new("git")
        .args(["-c", "lfs.transfer.maxretries=0", "branch", "offline-lfs"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        stderr.is_empty(),
        "offline LFS push should be quiet: {stderr}"
    );
}

#[test]
fn gateway_hook_offline_deletion_is_quiet() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let repo = hook_repo(&format!("http://{address}/repo.git"));
    Command::new("git")
        .args(["branch", "doomed"])
        .current_dir(repo.path())
        .success()
        .unwrap();
    let output = Command::new("git")
        .args(["branch", "-D", "doomed"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(stderr.is_empty(), "{stderr}");
}

#[test]
fn gateway_hook_rejected_push_is_quiet_until_explicit_push() {
    let remote = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "--bare"])
        .arg(remote.path())
        .success()
        .unwrap();
    let hook = remote.path().join("hooks/pre-receive");
    fs::write(
        &hook,
        indoc! {r#"
            #!/bin/sh
            echo "push rejected by repository policy" >&2
            exit 1
        "#},
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    let repo = hook_repo(remote.path().to_str().unwrap());
    let output = commit(&repo);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.is_empty(), "{stderr}");

    let output = Command::new("git")
        .args(["push", "rumpelpod", "test"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "explicit push should fail");
    assert!(
        stderr.contains("push rejected by repository policy"),
        "{stderr}"
    );
    assert!(stderr.contains("pre-receive hook declined"), "{stderr}");
}
