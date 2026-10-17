// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the `rumpel delete` subcommand.

use std::fs;
use std::io::ErrorKind;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use git2::Repository;
use indoc::{formatdoc, indoc};
use rumpelpod::config::{ContainerEngine, Host};
use rumpelpod::daemon::db::{self, PodId, PodStatus};

use crate::common::{pod_command, write_test_devcontainer, TestDaemon, TestHome, TestRepo};
use crate::executor::{executor_mode, skip_test, ExecutorMode, ExecutorResources};

// Populate local state without provisioning infrastructure that the test
// is specifically meant to work without.
fn record_disconnected_pod(repo: &TestRepo, home: &TestHome, name: &str, host: &Host) -> PodId {
    let conn = db::open_db(&home.path().join("state/rumpelpod/db.sqlite")).unwrap();
    let id = db::create_pod(
        &conn,
        repo.path(),
        name,
        host,
        "test-token",
        "test-image",
        "{}",
        "[]",
    )
    .unwrap();
    db::update_pod_status(&conn, id, PodStatus::Ready).unwrap();
    id
}

fn retired_cluster() -> Host {
    Host::Kubernetes {
        context: "retired-cluster".to_string(),
        namespace: "dev".to_string(),
        registry: "registry.invalid".to_string(),
        node_selector: None,
        tolerations: None,
        builder: None,
        image_builder: ContainerEngine::Docker,
    }
}

#[test]
fn delete_forget_k8s_without_kubeconfig() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let kubeconfig = home.path().join("removed-kubeconfig");
    let mut daemon =
        TestDaemon::start_with_env(&home, &[("KUBECONFIG", kubeconfig.to_str().unwrap())]);
    let id = record_disconnected_pod(&repo, &home, "old-k8s", &retired_cluster());
    let conn = db::open_db(&home.path().join("state/rumpelpod/db.sqlite")).unwrap();
    db::insert_forwarded_port(&conn, id, "", 8080, 54321, "old port").unwrap();

    let git = Repository::open(repo.path()).unwrap();
    let head = git.head().unwrap().target().unwrap();
    for name in ["refs/rumpelpod/old-k8s", "refs/rumpelpod/main@old-k8s"] {
        git.reference(name, head, false, "record pod tracking")
            .unwrap();
    }

    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", "--force", "old-k8s"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "normal deletion must require the backend"
    );
    assert!(
        stderr.contains("--forget"),
        "missing recovery hint: {stderr}"
    );
    assert!(db::get_pod(&conn, repo.path(), "old-k8s")
        .unwrap()
        .is_some());
    assert!(git.find_reference("refs/rumpelpod/old-k8s").is_ok());

    let output = pod_command(&repo, &daemon)
        .args(["delete", "--forget", "old-k8s"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "forget failed: {stderr}");
    assert!(db::get_pod(&conn, repo.path(), "old-k8s")
        .unwrap()
        .is_none());
    assert!(db::list_forwarded_ports(&conn, id).unwrap().is_empty());
    for name in ["refs/rumpelpod/old-k8s", "refs/rumpelpod/main@old-k8s"] {
        assert_eq!(
            git.find_reference(name)
                .err()
                .expect("tracking ref remains")
                .code(),
            git2::ErrorCode::NotFound
        );
    }

    // A daemon restart must not resurrect the forgotten entry.
    daemon.kill();
    daemon = TestDaemon::start_with_env(&home, &[("KUBECONFIG", kubeconfig.to_str().unwrap())]);
    let output = pod_command(&repo, &daemon).arg("list").output().unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("old-k8s"));
}

#[test]
fn delete_forget_never_contacts_hosts_and_continues_past_missing() {
    let repo = TestRepo::new();
    let other_repo = TestRepo::new();
    let home = TestHome::new();
    // Expose accidental probes even if the backend error is ignored.
    let api = TcpListener::bind("127.0.0.1:0").unwrap();
    api.set_nonblocking(true).unwrap();
    let api_address = api.local_addr().unwrap();
    let kubeconfig = home.path().join("kubeconfig");
    fs::write(
        &kubeconfig,
        formatdoc! {"
        apiVersion: v1
        kind: Config
        clusters:
        - name: retired
          cluster:
            server: http://{api_address}
        contexts:
        - name: retired-cluster
          context:
            cluster: retired
            user: retired
        users:
        - name: retired
          user: {{}}
    "},
    )
    .unwrap();
    let ssh = home.bin_dir().join("ssh");
    fs::write(
        &ssh,
        indoc! {r#"
            #!/bin/sh
            touch "$HOME/ssh-contacted"
            exit 1
        "#},
    )
    .unwrap();
    fs::set_permissions(&ssh, fs::Permissions::from_mode(0o755)).unwrap();
    let daemon = TestDaemon::start_with_env(&home, &[("KUBECONFIG", kubeconfig.to_str().unwrap())]);
    // Socket creation precedes startup reconnection. Wait for a response so
    // that only the forget request can initiate probes of these fixtures.
    let output = pod_command(&repo, &daemon).arg("list").output().unwrap();
    assert!(output.status.success());
    record_disconnected_pod(&repo, &home, "old-k8s", &retired_cluster());
    record_disconnected_pod(
        &repo,
        &home,
        "old-ssh",
        &Host::Ssh {
            ssh_destination: "retired-ssh".to_string(),
            engine: ContainerEngine::Docker,
        },
    );
    record_disconnected_pod(&other_repo, &home, "old-k8s", &retired_cluster());

    let output = pod_command(&repo, &daemon)
        .args(["delete", "--forget", "old-k8s", "missing", "old-ssh"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("pod 'missing' not found"), "{stderr}");
    assert!(stderr.contains("1 pod(s) could not be deleted"), "{stderr}");
    let conn = db::open_db(&home.path().join("state/rumpelpod/db.sqlite")).unwrap();
    assert!(db::list_pods(&conn, repo.path()).unwrap().is_empty());
    assert!(db::get_pod(&conn, other_repo.path(), "old-k8s")
        .unwrap()
        .is_some());
    assert_eq!(api.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
    assert!(!home.path().join("ssh-contacted").exists());

    // Avoid asking test cleanup to contact the deliberately unresponsive API.
    let output = pod_command(&other_repo, &daemon)
        .args(["delete", "--forget", "old-k8s"])
        .output()
        .unwrap();
    assert!(output.status.success());
}

#[test]
fn delete_forget_unmerged_requires_force() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let daemon = TestDaemon::start(&home);
    record_disconnected_pod(&repo, &home, "unmerged", &retired_cluster());
    let git = Repository::open(repo.path()).unwrap();
    let head = git.head().unwrap().peel_to_commit().unwrap();
    let signature = git.signature().unwrap();
    git.commit(
        Some("refs/rumpelpod/unmerged"),
        &signature,
        &signature,
        "Work retained from the lost pod",
        &head.tree().unwrap(),
        &[&head],
    )
    .unwrap();

    let output = pod_command(&repo, &daemon)
        .args(["delete", "--forget", "unmerged"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("unmerged commits") && stderr.contains("--force"),
        "{stderr}"
    );
    let conn = db::open_db(&home.path().join("state/rumpelpod/db.sqlite")).unwrap();
    assert!(db::get_pod(&conn, repo.path(), "unmerged")
        .unwrap()
        .is_some());
    assert!(git.find_reference("refs/rumpelpod/unmerged").is_ok());

    let output = pod_command(&repo, &daemon)
        .args(["delete", "--forget", "--force", "unmerged"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "forced forget failed: {stderr}");
    assert!(db::get_pod(&conn, repo.path(), "unmerged")
        .unwrap()
        .is_none());
    assert_eq!(
        git.find_reference("refs/rumpelpod/unmerged")
            .err()
            .expect("tracking ref remains")
            .code(),
        git2::ErrorCode::NotFound
    );
}

#[test]
fn delete_forget_leaves_running_container() {
    match executor_mode() {
        ExecutorMode::Docker => {}
        ExecutorMode::Podman | ExecutorMode::Ssh | ExecutorMode::K8s => {
            skip_test();
            return;
        }
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "keep-running", "--", "true"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let repo_path = repo.path().display();
    let output = Command::new("docker")
        .args([
            "ps",
            "-q",
            "--filter",
            &format!("label=dev.rumpelpod.repo_path={repo_path}"),
            "--filter",
            "label=dev.rumpelpod.name=keep-running",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let id = String::from_utf8(output.stdout).unwrap().trim().to_string();
    assert!(!id.is_empty());

    let output = pod_command(&repo, &daemon)
        .args(["delete", "--forget", "keep-running"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let inspected = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Running}}", &id])
        .output()
        .unwrap();
    // The daemon no longer owns this container, so clean it up explicitly.
    let removed = Command::new("docker")
        .args(["rm", "-f", &id])
        .output()
        .unwrap();
    assert!(
        removed.status.success(),
        "{}",
        String::from_utf8_lossy(&removed.stderr)
    );
    assert!(inspected.status.success());
    assert_eq!(String::from_utf8_lossy(&inspected.stdout).trim(), "true");
    let output = pod_command(&repo, &daemon).arg("list").output().unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("keep-running"));
}

#[test]
fn delete_forget_conflicts_with_wait() {
    let output = Command::new("rumpel")
        .args(["delete", "--forget", "--wait", "old-pod"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(
        stderr.contains("cannot be used with") && stderr.contains("--wait"),
        "{stderr}"
    );
}

#[test]
fn delete_smoke_test() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // First create a pod by entering it
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "test-delete", "--", "echo", "created"])
        .output()
        .expect("Failed to run rumpel enter command");

    assert!(
        output.status.success(),
        "rumpel enter failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Now delete the pod
    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", "test-delete"])
        .output()
        .expect("Failed to run rumpel delete command");

    assert!(
        output.status.success(),
        "rumpel delete failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn delete_unknown_pod_fails() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    let output = pod_command(&repo, &daemon)
        .args(["delete", "nonexistent"])
        .output()
        .expect("Failed to run rumpel delete command");

    assert!(
        !output.status.success(),
        "rumpel delete of unknown pod should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "Expected 'not found' in stderr: {}",
        stderr
    );
}

/// Helper: create a pod and make a commit inside it so it becomes "ahead 1".
fn create_ahead_pod(repo: &TestRepo, daemon: &TestDaemon, name: &str) {
    let output = pod_command(repo, daemon)
        .args(["enter", "--create", name, "--", "touch", "newfile"])
        .output()
        .expect("Failed to touch file");
    assert!(output.status.success(), "touch failed");

    let output = pod_command(repo, daemon)
        .args(["enter", "--create", name, "--", "git", "add", "newfile"])
        .output()
        .expect("Failed to git add");
    assert!(output.status.success(), "git add failed");

    let output = pod_command(repo, daemon)
        .args([
            "enter",
            "--create",
            name,
            "--",
            "git",
            "commit",
            "--no-verify",
            "-m",
            "new file",
        ])
        .output()
        .expect("Failed to git commit");
    assert!(output.status.success(), "git commit failed");
}

#[test]
fn delete_unmerged_pod_fails_without_tty() {
    // In non-tty mode (piped output), deleting an unmerged pod should fail
    // with a helpful error message.
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    create_ahead_pod(&repo, &daemon, "unmerged");

    // pod_command pipes stdout/stderr, so this is non-tty
    let output = pod_command(&repo, &daemon)
        .args(["delete", "unmerged"])
        .output()
        .expect("Failed to run rumpel delete command");

    assert!(
        !output.status.success(),
        "delete of unmerged pod should fail without --force"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unmerged commits") && stderr.contains("--force"),
        "Expected 'unmerged commits' and '--force' hint in stderr: {}",
        stderr
    );
}

#[test]
fn delete_unmerged_pod_succeeds_with_force() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    create_ahead_pod(&repo, &daemon, "force-del");

    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", "--force", "force-del"])
        .output()
        .expect("Failed to run rumpel delete command");

    assert!(
        output.status.success(),
        "delete with --force should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify pod is gone
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("Failed to list");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("force-del"),
        "pod should be gone after forced delete: {}",
        stdout
    );
}

#[test]
fn delete_then_recreate_same_name() {
    // After deleting a pod, we should be able to create a new one with the same name.
    // This verifies that all resources (container, network) are properly cleaned up.
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create pod
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "recyclable", "--", "echo", "first"])
        .output()
        .expect("Failed to run rumpel enter command");
    assert!(
        output.status.success(),
        "first rumpel enter failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Delete pod (--wait so the container is fully gone before re-entering)
    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", "recyclable"])
        .output()
        .expect("Failed to run rumpel delete command");
    assert!(
        output.status.success(),
        "rumpel delete failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Create pod with the same name again
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "recyclable", "--", "echo", "second"])
        .output()
        .expect("Failed to run rumpel enter command");
    assert!(
        output.status.success(),
        "second rumpel enter failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn delete_multiple_pods() {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create three pods
    for name in ["multi-a", "multi-b", "multi-c"] {
        let output = pod_command(&repo, &daemon)
            .args(["enter", "--create", name, "--", "true"])
            .output()
            .expect("Failed to run rumpel enter");
        assert!(
            output.status.success(),
            "creating pod '{}' failed: {}",
            name,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Delete all three in one command
    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", "multi-a", "multi-b", "multi-c"])
        .output()
        .expect("Failed to run rumpel delete");
    assert!(
        output.status.success(),
        "multi-delete failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify all are gone
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("Failed to list");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for name in ["multi-a", "multi-b", "multi-c"] {
        assert!(
            !stdout.contains(name),
            "pod '{}' should be gone after multi-delete: {}",
            name,
            stdout
        );
    }
}

#[test]
fn delete_multiple_continues_past_missing() {
    // When one pod in a multi-delete does not exist, the others should
    // still be deleted and the command should report the failure.
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create two pods, leave a gap for a nonexistent one
    for name in ["surv-a", "surv-c"] {
        let output = pod_command(&repo, &daemon)
            .args(["enter", "--create", name, "--", "true"])
            .output()
            .expect("Failed to run rumpel enter");
        assert!(output.status.success());
    }

    // Delete with a nonexistent pod in the middle
    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", "surv-a", "no-such-pod", "surv-c"])
        .output()
        .expect("Failed to run rumpel delete");

    // Command should fail overall because one pod was missing
    assert!(
        !output.status.success(),
        "should fail when a pod is missing"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "should mention the missing pod: {}",
        stderr
    );
    assert!(
        stderr.contains("1 pod(s) could not be deleted"),
        "should report failure count: {}",
        stderr
    );

    // The other two should still have been deleted
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("Failed to list");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("surv-a") && !stdout.contains("surv-c"),
        "existing pods should be deleted despite the missing one: {}",
        stdout
    );
}

#[test]
fn delete_multiple_skips_unmerged_without_tty() {
    // In non-tty mode, unmerged pods are skipped with an error but
    // the remaining pods are still deleted.
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create a clean pod and an ahead pod
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "clean-pod", "--", "true"])
        .output()
        .expect("Failed to run rumpel enter");
    assert!(output.status.success());

    create_ahead_pod(&repo, &daemon, "ahead-pod");

    // Try to delete both (non-tty, no --force)
    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", "clean-pod", "ahead-pod"])
        .output()
        .expect("Failed to run rumpel delete");

    // Should fail overall because the ahead pod could not be deleted
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unmerged commits"),
        "should warn about unmerged pod: {}",
        stderr
    );

    // The clean pod should be deleted, the ahead pod should remain
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("Failed to list");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("clean-pod"),
        "clean pod should be deleted: {}",
        stdout
    );
    assert!(
        stdout.contains("ahead-pod"),
        "ahead pod should remain: {}",
        stdout
    );
}

#[test]
fn ssh_remote_pod_delete() {
    if !matches!(
        crate::executor::executor_mode(),
        crate::executor::ExecutorMode::Docker
    ) {
        crate::executor::skip_test();
        return;
    }
    let repo = TestRepo::new();
    let home = TestHome::new();
    let executor = ExecutorResources::ssh(&home);
    let daemon = TestDaemon::start(&home);
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create a pod on the remote
    let pod_name = "delete-test-remote";
    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", pod_name, "--", "true"])
        .output()
        .expect("rumpel enter failed to execute");
    assert!(
        output.status.success(),
        "rumpel enter failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify it exists in list
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("rumpel list failed to execute");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(pod_name),
        "pod should exist before delete"
    );

    // Delete the pod (--wait so it's fully gone before checking list)
    let output = pod_command(&repo, &daemon)
        .args(["delete", "--wait", pod_name])
        .output()
        .expect("rumpel delete failed to execute");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "rumpel delete failed: stdout={}, stderr={}",
        stdout,
        stderr
    );

    // Verify it is gone from list
    let output = pod_command(&repo, &daemon)
        .arg("list")
        .output()
        .expect("rumpel list failed to execute");

    let stdout_list = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout_list.contains(pod_name),
        "pod should not exist after delete in list output: {}",
        stdout_list
    );
}
