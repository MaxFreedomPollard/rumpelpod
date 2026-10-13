// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for SSH agent forwarding in pods.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use indoc::{formatdoc, indoc};
use rumpelpod::daemon::protocol::{ClientContext, PodReconnectRequest};
use rumpelpod::CommandExt;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::common::{
    pod_command, write_test_devcontainer, TestDaemon, TestHome, TestRepo, TEST_REPO_PATH, TEST_USER,
};
use crate::executor::{ExecutorMode, ExecutorResources};

struct TestSshAgent {
    child: Option<Child>,
    socket_path: PathBuf,
    comment: String,
}

impl TestSshAgent {
    fn start(home: &TestHome, name: &str) -> Self {
        let comment = format!("rumpelpod-{name}");
        let key_path = generate_key(home, name, &comment);
        let socket_path = home.path().join(format!("{name}.sock"));
        let child = Command::new("ssh-agent")
            .args(["-D", "-a"])
            .arg(&socket_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start test ssh-agent");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let output = Command::new("ssh-add")
                .arg(&key_path)
                .env("SSH_AUTH_SOCK", &socket_path)
                .output()
                .expect("run ssh-add for test agent");
            if output.status.success() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "test ssh-agent did not become ready: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        Self {
            child: Some(child),
            socket_path,
            comment,
        }
    }

    fn stop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        child.kill().expect("kill test ssh-agent");
        child.wait().expect("wait for test ssh-agent");
    }
}

impl Drop for TestSshAgent {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Err(error) = child.kill() {
                eprintln!("failed to kill test ssh-agent: {error}");
            }
            if let Err(error) = child.wait() {
                eprintln!("failed to wait for test ssh-agent: {error}");
            }
        }
    }
}

fn generate_key(home: &TestHome, name: &str, comment: &str) -> PathBuf {
    let key_path = home.path().join(format!("{name}_ed25519"));
    Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path)
        .args(["-N", "", "-C", comment, "-q"])
        .success()
        .expect("ssh-keygen failed");
    key_path
}

fn write_ssh_agent_config(repo: &TestRepo, executor: &ExecutorResources, ssh_agent: Value) {
    let mut config: Value = serde_json::from_str(&executor.json).expect("parse executor config");
    config
        .as_object_mut()
        .expect("executor config is an object")
        .insert("sshAgent".to_string(), ssh_agent);
    fs::write(
        repo.path().join(".rumpelpod.json"),
        serde_json::to_vec_pretty(&config).expect("serialize rumpelpod config"),
    )
    .expect("write .rumpelpod.json");
}

fn assert_agent_comment(
    repo: &TestRepo,
    daemon: &TestDaemon,
    pod_name: &str,
    socket_path: Option<&Path>,
    comment: &str,
) {
    let mut command = pod_command(repo, daemon);
    match socket_path {
        Some(socket_path) => {
            command.env("SSH_AUTH_SOCK", socket_path);
        }
        None => {
            command.env_remove("SSH_AUTH_SOCK");
        }
    }
    let output = command
        .args(["enter", "--create", pod_name, "--", "ssh-add", "-l"])
        .output()
        .expect("list keys through pod agent");
    assert!(
        output.status.success(),
        "listing pod keys failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(comment),
        "expected agent key {comment}, got: {stdout}"
    );
}

fn register_reconnect_context(
    daemon: &TestDaemon,
    repo: &TestRepo,
    pod_name: &str,
    socket_path: &Path,
) {
    let body = serde_json::to_vec(&PodReconnectRequest {
        repo_path: repo.path().to_path_buf(),
        pod_name: pod_name.to_string(),
        client_context: ClientContext {
            ssh_auth_sock: Some(socket_path.to_path_buf()),
        },
    })
    .expect("serialize reconnect request");
    let body_len = body.len();
    let headers = format!(
        "POST /pod/reconnect-events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {body_len}\r\n\r\n"
    );
    let mut stream = UnixStream::connect(&daemon.socket_path).expect("connect to restarted daemon");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set reconnect response timeout");
    stream
        .write_all(headers.as_bytes())
        .expect("write reconnect headers");
    stream.write_all(&body).expect("write reconnect body");

    let mut response = [0u8; 1024];
    let read = stream.read(&mut response).expect("read reconnect response");
    let response = String::from_utf8_lossy(&response[..read]);
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "unexpected reconnect response: {response}"
    );
}

fn register_codex_attach_context(daemon: &TestDaemon, repo: &TestRepo, socket_path: &Path) {
    let repo_path = repo.path().to_string_lossy();
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("repo_path", &repo_path)
        .append_pair("no_dangerously_bypass_approvals_and_sandbox", "false")
        .finish();
    let url = format!("ws://localhost/pod/codex/missing?{query}");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build WebSocket test runtime");
    runtime.block_on(async {
        let stream = tokio::net::UnixStream::connect(&daemon.socket_path)
            .await
            .expect("connect Codex WebSocket to daemon");
        let (mut websocket, _) = tokio_tungstenite::client_async(url, stream)
            .await
            .expect("open Codex WebSocket");
        let attach = serde_json::json!({
            "type": "attach",
            "cols": 80,
            "rows": 24,
            "create": false,
            "extra_args": [],
            "client_context": {
                "ssh_auth_sock": socket_path,
            },
        });
        websocket
            .send(Message::Text(
                serde_json::to_string(&attach)
                    .expect("serialize Codex attach")
                    .into(),
            ))
            .await
            .expect("send Codex attach");
        let response = websocket
            .next()
            .await
            .expect("Codex WebSocket closed without response")
            .expect("read Codex WebSocket response");
        let Message::Text(response) = response else {
            panic!("unexpected Codex WebSocket response: {response:?}");
        };
        let response_json: Value =
            serde_json::from_str(&response).expect("parse Codex attach response");
        assert_eq!(response_json["type"], "session_ended");
    });
}

/// Add a key to a pod's ssh-agent and verify it is usable from inside the
/// container via the relayed socket.
#[test]
fn ssh_add_and_list() {
    let home = TestHome::new();
    // The daemon spawns `ssh-agent` when handling `rumpel ssh-add`, and
    // the CLI itself execs `ssh-add`, so both need to be reachable from
    // the narrowed PATH.
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();

    // The base test image installs openssh-client, giving us ssh-add.
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();

    // Create the pod.
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "ssh-test", "--", "true"])
        .success()
        .expect("failed to create pod");

    // Generate a throwaway ed25519 key on the local machine.
    let key_path = home.path().join("test_ed25519");
    std::process::Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-f"])
        .arg(&key_path)
        .args(["-N", "", "-q"])
        .success()
        .expect("ssh-keygen failed");

    // Add the key via `rumpel ssh-add <pod> <key>`.  ssh-add prints
    // its confirmation to stderr, so check the combined output there.
    let output = pod_command(&repo, &daemon)
        .args(["ssh-add", "ssh-test"])
        .arg(&key_path)
        .output()
        .expect("rumpel ssh-add failed to execute");
    assert!(
        output.status.success(),
        "rumpel ssh-add failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Identity added") || stderr.contains("identity added"),
        "unexpected ssh-add stderr: {stderr}"
    );

    // List keys from the local machine via `rumpel ssh-add <pod> -l`.
    let stdout = pod_command(&repo, &daemon)
        .args(["ssh-add", "ssh-test", "-l"])
        .success()
        .expect("rumpel ssh-add -l failed");
    let list_output = String::from_utf8_lossy(&stdout);
    assert!(
        list_output.contains("ssh-ed25519") || list_output.contains("ED25519"),
        "expected key in local machine's list: {list_output}"
    );

    // Verify the key is reachable from inside the container through the
    // relayed agent socket.
    let stdout = pod_command(&repo, &daemon)
        .args(["enter", "--create", "ssh-test", "--", "ssh-add", "-l"])
        .success()
        .expect("ssh-add -l inside container failed");
    let container_output = String::from_utf8_lossy(&stdout);
    assert!(
        container_output.contains("ssh-ed25519") || container_output.contains("ED25519"),
        "expected key visible inside container: {container_output}"
    );
}

#[test]
fn ssh_agent_configured_key_is_added_automatically() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let mut daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");

    let comment = "rumpelpod-configured-key";
    generate_key(&home, "configured", comment);
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["~/configured_ed25519"]}),
    );

    assert_agent_comment(&repo, &daemon, "configured", None, comment);

    // Warm, stopped, and recreated connections retain the same configured
    // agent rather than invoking ssh-add again.
    fs::remove_file(daemon.bin_dir.join("ssh-add")).expect("remove host ssh-add binary");
    assert_agent_comment(&repo, &daemon, "configured", None, comment);

    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "configured"])
        .success()
        .expect("stop pod with a configured SSH agent");
    assert_agent_comment(&repo, &daemon, "configured", None, comment);

    pod_command(&repo, &daemon)
        .args(["recreate", "configured"])
        .success()
        .expect("recreate pod with an already-prepared SSH agent");
    assert_agent_comment(&repo, &daemon, "configured", None, comment);

    home.link_local_bin("ssh-add");
    daemon.kill();
    drop(daemon);
    let daemon = TestDaemon::start(&home);
    assert_agent_comment(&repo, &daemon, "configured", None, comment);
}

#[test]
fn ssh_agent_missing_configured_key_prevents_pod_creation() {
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["missing-key"]}),
    );

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "missing-key", "--", "true"])
        .output()
        .expect("run rumpel enter with a missing configured key");
    assert!(
        !output.status.success(),
        "pod creation unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("reading configured SSH key") && stderr.contains("missing-key"),
        "unexpected error: {stderr}"
    );

    let stdout = pod_command(&repo, &daemon)
        .arg("list")
        .success()
        .expect("list pods after rejected creation");
    assert!(
        !String::from_utf8_lossy(&stdout).contains("missing-key"),
        "rejected pod was persisted"
    );
}

#[test]
fn ssh_agent_malformed_configured_key_prevents_pod_creation() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    fs::write(home.path().join("malformed-key"), "not an SSH private key")
        .expect("write malformed SSH key");
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["~/malformed-key"]}),
    );

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "malformed-key", "--", "true"])
        .output()
        .expect("run rumpel enter with a malformed configured key");
    assert!(
        !output.status.success(),
        "pod creation unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ssh-add failed while loading configured SSH keys"),
        "unexpected error: {stderr}"
    );

    let stdout = pod_command(&repo, &daemon)
        .arg("list")
        .success()
        .expect("list pods after rejected creation");
    assert!(
        !String::from_utf8_lossy(&stdout).contains("malformed-key"),
        "rejected pod was persisted"
    );
}

#[test]
fn ssh_agent_malformed_configured_key_prevents_recreate() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json)
        .expect("write initial .rumpelpod.json");

    pod_command(&repo, &daemon)
        .args([
            "enter",
            "--create",
            "malformed-recreate",
            "--",
            "touch",
            "/tmp/original-container",
        ])
        .success()
        .expect("create pod before rejected recreate");

    fs::write(
        home.path().join("malformed-recreate-key"),
        "not an SSH private key",
    )
    .expect("write malformed SSH key");
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["~/malformed-recreate-key"]}),
    );
    let output = pod_command(&repo, &daemon)
        .args(["recreate", "malformed-recreate"])
        .output()
        .expect("run rumpel recreate with a malformed configured key");
    assert!(
        !output.status.success(),
        "pod recreate unexpectedly succeeded"
    );

    fs::write(repo.path().join(".rumpelpod.json"), &executor.json)
        .expect("restore .rumpelpod.json");
    pod_command(&repo, &daemon)
        .args([
            "enter",
            "malformed-recreate",
            "--",
            "test",
            "-f",
            "/tmp/original-container",
        ])
        .success()
        .expect("rejected recreate replaced the original container");
}

#[test]
fn ssh_agent_start_failure_prevents_pod_creation() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    generate_key(&home, "unstartable", "rumpelpod-unstartable");
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["~/unstartable_ed25519"]}),
    );
    fs::remove_file(daemon.bin_dir.join("ssh-agent")).expect("remove daemon ssh-agent binary");

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "unstartable", "--", "true"])
        .output()
        .expect("run rumpel enter without daemon ssh-agent");
    assert!(
        !output.status.success(),
        "pod creation unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to start ssh-agent"),
        "unexpected error: {stderr}"
    );

    let stdout = pod_command(&repo, &daemon)
        .arg("list")
        .success()
        .expect("list pods after rejected creation");
    assert!(
        !String::from_utf8_lossy(&stdout).contains("unstartable"),
        "rejected pod was persisted"
    );
}

#[test]
fn ssh_agent_malformed_configured_key_prevents_fork() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json)
        .expect("write initial .rumpelpod.json");
    pod_command(&repo, &daemon)
        .args(["enter", "--create", "fork-source", "--", "true"])
        .success()
        .expect("create fork source");

    fs::write(
        home.path().join("malformed-fork-key"),
        "not an SSH private key",
    )
    .expect("write malformed SSH key");
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["~/malformed-fork-key"]}),
    );
    let output = pod_command(&repo, &daemon)
        .args(["fork", "fork-source", "fork-destination"])
        .output()
        .expect("run rumpel fork with a malformed configured key");
    assert!(!output.status.success(), "pod fork unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ssh-add failed while loading configured SSH keys"),
        "unexpected error: {stderr}"
    );

    let stdout = pod_command(&repo, &daemon)
        .arg("list")
        .success()
        .expect("list pods after rejected fork");
    assert!(
        !String::from_utf8_lossy(&stdout).contains("fork-destination"),
        "rejected fork was persisted"
    );
}

#[test]
fn ssh_agent_ambient_uses_latest_live_socket() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"ambient": true}));

    let first = TestSshAgent::start(&home, "ambient-first");
    assert_agent_comment(
        &repo,
        &daemon,
        "ambient",
        Some(&first.socket_path),
        &first.comment,
    );

    let mut second = TestSshAgent::start(&home, "ambient-second");
    assert_agent_comment(
        &repo,
        &daemon,
        "ambient",
        Some(&second.socket_path),
        &second.comment,
    );

    second.stop();
    assert_agent_comment(&repo, &daemon, "ambient", None, &first.comment);
}

#[test]
fn ssh_add_rejects_ambient_pods() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"ambient": true}));

    let agent = TestSshAgent::start(&home, "ambient-reject");
    assert_agent_comment(
        &repo,
        &daemon,
        "ambient-reject",
        Some(&agent.socket_path),
        &agent.comment,
    );

    let additional_comment = "rumpelpod-ambient-additional";
    let additional_key = generate_key(&home, "ambient-additional", additional_comment);
    let output = pod_command(&repo, &daemon)
        .env("SSH_AUTH_SOCK", &agent.socket_path)
        .args(["ssh-add", "ambient-reject"])
        .arg(&additional_key)
        .output()
        .expect("run rumpel ssh-add for ambient pod");
    assert!(
        !output.status.success(),
        "rumpel ssh-add unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("uses ambient SSH agent forwarding"),
        "unexpected error: {stderr}"
    );

    let output = Command::new("ssh-add")
        .arg("-l")
        .env("SSH_AUTH_SOCK", &agent.socket_path)
        .output()
        .expect("list ambient agent keys");
    assert!(output.status.success(), "failed to list ambient agent keys");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains(additional_comment),
        "rejected key was added to the ambient agent: {stdout}"
    );
}

#[test]
fn ssh_agent_reconnect_registers_ambient_socket_after_daemon_restart() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let mut daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"ambient": true}));

    let agent = TestSshAgent::start(&home, "ambient-reconnect");
    assert_agent_comment(
        &repo,
        &daemon,
        "ambient-reconnect",
        Some(&agent.socket_path),
        &agent.comment,
    );

    daemon.kill();
    drop(daemon);
    let daemon = TestDaemon::start(&home);
    register_reconnect_context(&daemon, &repo, "ambient-reconnect", &agent.socket_path);

    assert_agent_comment(&repo, &daemon, "ambient-reconnect", None, &agent.comment);
}

#[test]
fn ssh_agent_codex_attach_registers_ambient_socket_in_first_message() {
    let home = TestHome::new();
    home.link_local_bins(&["ssh-agent", "ssh-add"]);
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"ambient": true}));

    let agent = TestSshAgent::start(&home, "ambient-codex");
    register_codex_attach_context(&daemon, &repo, &agent.socket_path);

    assert_agent_comment(&repo, &daemon, "ambient-codex", None, &agent.comment);
}

#[test]
fn ssh_agent_ambient_missing_socket_reports_broken_agent_without_eager_probe() {
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"ambient": true}));

    let socket_path = home.path().join("unprobed-agent.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind fake SSH agent socket");
    pod_command(&repo, &daemon)
        .env("SSH_AUTH_SOCK", &socket_path)
        .args(["enter", "--create", "unprobed", "--", "true"])
        .success()
        .expect("create ambient pod without using its agent");

    listener
        .set_nonblocking(true)
        .expect("make fake SSH agent nonblocking");
    let error = listener
        .accept()
        .expect_err("ambient registration unexpectedly probed the socket");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    drop(listener);
    fs::remove_file(&socket_path).expect("remove fake SSH agent socket");
    let output = pod_command(&repo, &daemon)
        .env_remove("SSH_AUTH_SOCK")
        .args(["enter", "--create", "unprobed", "--", "ssh-add", "-l"])
        .output()
        .expect("query unavailable ambient agent");
    assert!(
        !output.status.success(),
        "unavailable ambient agent unexpectedly worked: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.contains("communication with agent failed"),
        "expected a broken agent error, got: {diagnostic}"
    );
}

#[test]
fn ssh_agent_keys_and_ambient_are_rejected_together() {
    let home = TestHome::new();
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "", "");
    fs::write(
        repo.path().join(".rumpelpod.json"),
        r#"{"sshAgent":{"keys":["key"],"ambient":true}}"#,
    )
    .expect("write invalid .rumpelpod.json");

    let output = pod_command(&repo, &daemon)
        .args(["enter", "--create", "invalid", "--", "true"])
        .output()
        .expect("run rumpel enter");
    assert!(!output.status.success(), "invalid SSH config succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("sshAgent.keys and sshAgent.ambient are mutually exclusive"),
        "unexpected error: {stderr}"
    );
}

fn enable_build_ssh(repo: &TestRepo, options: Value) {
    let path = repo.path().join(".devcontainer/devcontainer.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["build"]["options"] = options;
    fs::write(path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
}

fn require_build_transport_agent(home: &TestHome, socket: &Path) {
    let docker = home.bin_dir().join("docker");
    let real_docker = fs::read_link(&docker).expect("resolve real Docker CLI");
    let real_docker = real_docker.display();
    let socket = socket.display();
    fs::remove_file(&docker).unwrap();
    fs::write(
        &docker,
        formatdoc! {r#"
        #!/bin/sh
        for arg in "$@"; do
            if [ "$arg" = build ] && [ "$SSH_AUTH_SOCK" != '{socket}' ]; then
                echo 'build replaced the Docker transport agent' >&2
                exit 1
            fi
        done
        exec '{real_docker}' "$@"
    "#},
    )
    .unwrap();
    fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).unwrap();
}

fn record_loaded_agent_sockets(home: &TestHome) -> PathBuf {
    let ssh_add = home.bin_dir().join("ssh-add");
    let real_ssh_add = fs::read_link(&ssh_add).unwrap();
    let real_ssh_add = real_ssh_add.display();
    let log = home.path().join("loaded-agent-sockets");
    let log_path = log.display();
    fs::remove_file(&ssh_add).unwrap();
    fs::write(
        &ssh_add,
        formatdoc! {r#"
        #!/bin/sh
        printf '%s\n' "$SSH_AUTH_SOCK" >> '{log_path}'
        exec '{real_ssh_add}' "$@"
    "#},
    )
    .unwrap();
    fs::set_permissions(&ssh_add, fs::Permissions::from_mode(0o755)).unwrap();
    log
}

#[test]
fn ssh_agent_build_uses_only_configured_keys_separately_from_pod_agent() {
    let home = TestHome::new();
    home.link_local_bin("ssh-add");
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let loaded_sockets = record_loaded_agent_sockets(&home);
    let ambient = TestSshAgent::start(&home, "ambient-build");
    if matches!(
        crate::executor::executor_mode(),
        ExecutorMode::Docker | ExecutorMode::Ssh
    ) {
        require_build_transport_agent(&home, &ambient.socket_path);
    }
    generate_key(&home, "first", "first-build-key");
    let second = generate_key(&home, "second", "second-build-key");
    let repo = TestRepo::new();
    fs::copy(second, repo.path().join("second-key")).unwrap();
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["~/first_ed25519", "second-key"]}),
    );
    write_test_devcontainer(
        &repo,
        indoc! {r#"
        RUN --mount=type=ssh --mount=type=ssh,id=deploy,target=/tmp/deploy.sock ssh-add -L > /tmp/build-keys && test "$(SSH_AUTH_SOCK=/tmp/deploy.sock ssh-add -L)" = "$(cat /tmp/build-keys)"
    "#},
        "",
    );
    let ambient_socket = ambient.socket_path.display();
    enable_build_ssh(
        &repo,
        serde_json::json!([
            "--ssh",
            format!("default={ambient_socket}"),
            format!("--ssh=deploy={ambient_socket}")
        ]),
    );

    let output = pod_command(&repo, &daemon)
        .env("SSH_AUTH_SOCK", &ambient.socket_path)
        .args([
            "enter",
            "--create",
            "configured-build",
            "--",
            "cat",
            "/tmp/build-keys",
        ])
        .success()
        .expect("build with configured SSH keys");
    let keys = String::from_utf8(output).unwrap();
    assert_eq!(keys.lines().count(), 2, "unexpected build keys: {keys}");
    assert!(keys.contains("first-build-key"), "{keys}");
    assert!(keys.contains("second-build-key"), "{keys}");
    assert!(!keys.contains(&ambient.comment), "{keys}");
    assert_agent_comment(&repo, &daemon, "configured-build", None, "first-build-key");
    assert_agent_comment(&repo, &daemon, "configured-build", None, "second-build-key");
    let sockets = fs::read_to_string(loaded_sockets).unwrap();
    let sockets: Vec<_> = sockets.lines().collect();
    assert_eq!(
        sockets.len(),
        3,
        "pod, base build and preparation each need an agent"
    );
    assert!(
        Path::new(sockets[0]).exists(),
        "pod agent must remain alive"
    );
    assert_ne!(sockets[1], sockets[2], "builds must use separate agents");
    for socket in &sockets[1..] {
        assert!(
            !Path::new(socket).exists(),
            "build socket left behind: {socket}"
        );
    }
}

#[test]
fn ssh_agent_image_build_gets_fresh_configured_agent_without_ambient_socket() {
    let home = TestHome::new();
    home.link_local_bin("ssh-add");
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let loaded_sockets = record_loaded_agent_sockets(&home);
    let key = generate_key(&home, "build", "configured-build-key");
    let public_key = fs::read_to_string(key.with_extension("pub")).unwrap();
    let public_key = public_key.trim();
    let repo = TestRepo::new();
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"keys": [key]}));
    write_test_devcontainer(
        &repo,
        &formatdoc! {r#"
        RUN --mount=type=ssh test "$(ssh-add -L)" = '{public_key}'
    "#},
        "",
    );
    enable_build_ssh(&repo, serde_json::json!(["--ssh", "default"]));

    for _ in 0..2 {
        pod_command(&repo, &daemon)
            .env_remove("SSH_AUTH_SOCK")
            .args(["image", "build", "--no-cache"])
            .success()
            .expect("build with a fresh configured agent");
    }
    let sockets = fs::read_to_string(loaded_sockets).unwrap();
    let sockets: Vec<_> = sockets.lines().collect();
    assert_eq!(sockets.len(), 2);
    assert_ne!(
        sockets[0], sockets[1],
        "each build must create a fresh agent"
    );
    for socket in sockets {
        assert!(
            !Path::new(socket).exists(),
            "build socket left behind: {socket}"
        );
    }
    assert_agent_comment(
        &repo,
        &daemon,
        "build-without-ambient",
        None,
        "configured-build-key",
    );
}

#[test]
fn ssh_agent_image_build_preserves_unset_and_ambient_forwarding() {
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let ambient = TestSshAgent::start(&home, "ambient-image-build");
    let repo = TestRepo::new();
    fs::write(repo.path().join(".rumpelpod.json"), &executor.json).unwrap();
    let comment = &ambient.comment;
    write_test_devcontainer(
        &repo,
        &formatdoc! {r#"
        RUN --mount=type=ssh ssh-add -L | grep '{comment}'
    "#},
        "",
    );
    enable_build_ssh(&repo, serde_json::json!(["--ssh=default"]));
    // With no ssh-agent executable, success also proves no managed build
    // agent was started for either ambient configuration.
    fs::remove_file(home.bin_dir().join("ssh-agent")).unwrap();
    pod_command(&repo, &daemon)
        .env("SSH_AUTH_SOCK", &ambient.socket_path)
        .args(["image", "build", "--no-cache"])
        .success()
        .expect("build with unset sshAgent");
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"ambient": true}));
    pod_command(&repo, &daemon)
        .env("SSH_AUTH_SOCK", &ambient.socket_path)
        .args(["image", "build", "--no-cache"])
        .success()
        .expect("build with ambient sshAgent");
}

#[test]
fn ssh_agent_image_build_without_ssh_does_not_load_configured_keys() {
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_ssh_agent_config(
        &repo,
        &executor,
        serde_json::json!({"keys": ["missing-key"]}),
    );
    write_test_devcontainer(&repo, "RUN test -z \"$SSH_AUTH_SOCK\"", "");
    fs::remove_file(home.bin_dir().join("ssh-agent")).unwrap();
    pod_command(&repo, &daemon)
        .args(["image", "build"])
        .success()
        .expect("build without SSH must not start an agent or read its keys");
}

#[test]
fn ssh_agent_image_build_rejects_invalid_configured_keys() {
    let home = TestHome::new();
    home.link_local_bin("ssh-add");
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let repo = TestRepo::new();
    write_test_devcontainer(&repo, "RUN --mount=type=ssh ssh-add -l", "");
    enable_build_ssh(&repo, serde_json::json!(["--ssh=default"]));
    for (name, expected) in [
        ("missing-key", "reading configured SSH key"),
        ("malformed-key", "ssh-add failed"),
    ] {
        fs::write(repo.path().join("malformed-key"), "not a private key").unwrap();
        write_ssh_agent_config(&repo, &executor, serde_json::json!({"keys": [name]}));
        let output = pod_command(&repo, &daemon)
            .args(["image", "build"])
            .output()
            .expect("run build with invalid configured key");
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{stderr}");
    }
}

#[test]
fn ssh_agent_image_build_explicit_empty_keys_excludes_ambient_keys() {
    let home = TestHome::new();
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let ambient = TestSshAgent::start(&home, "excluded-build");
    let repo = TestRepo::new();
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"keys": []}));
    write_test_devcontainer(
        &repo,
        indoc! {r#"
        RUN --mount=type=ssh ssh-add -l > /tmp/keys; test "$?" = 1 && grep 'no identities' /tmp/keys
    "#},
        "",
    );
    enable_build_ssh(&repo, serde_json::json!(["--ssh=default"]));
    pod_command(&repo, &daemon)
        .env("SSH_AUTH_SOCK", &ambient.socket_path)
        .args(["image", "build"])
        .success()
        .expect("explicit empty build agent");
}

#[test]
fn ssh_agent_compose_build_uses_only_configured_keys() {
    match crate::executor::executor_mode() {
        ExecutorMode::Docker => {}
        ExecutorMode::Podman | ExecutorMode::Ssh | ExecutorMode::K8s => {
            crate::executor::skip_test();
            return;
        }
    }
    let home = TestHome::new();
    home.link_local_bin("ssh-add");
    let executor = ExecutorResources::setup(&home);
    let daemon = TestDaemon::start(&home);
    let ambient = TestSshAgent::start(&home, "ambient-compose-build");
    require_build_transport_agent(&home, &ambient.socket_path);
    let key = generate_key(&home, "compose", "configured-compose-key");
    let public_key = fs::read_to_string(key.with_extension("pub")).unwrap();
    let public_key = public_key.trim();
    let repo = TestRepo::new();
    write_ssh_agent_config(&repo, &executor, serde_json::json!({"keys": [key]}));
    write_test_devcontainer(
        &repo,
        &formatdoc! {r#"
        RUN --mount=type=ssh test "$(ssh-add -L)" = '{public_key}'
    "#},
        "",
    );
    fs::write(
        repo.path().join(".devcontainer/compose.yaml"),
        indoc! {r#"
        services:
          agent:
            build:
              context: ..
              dockerfile: .devcontainer/Dockerfile
              ssh:
                - default
            command: ["sleep", "infinity"]
    "#},
    )
    .unwrap();
    fs::write(
        repo.path().join(".devcontainer/devcontainer.json"),
        serde_json::to_vec(&serde_json::json!({
            "dockerComposeFile": "compose.yaml",
            "service": "agent",
            "workspaceFolder": TEST_REPO_PATH,
            "containerUser": TEST_USER,
        }))
        .unwrap(),
    )
    .unwrap();
    assert_agent_comment(
        &repo,
        &daemon,
        "compose-build",
        Some(&ambient.socket_path),
        "configured-compose-key",
    );
    pod_command(&repo, &daemon)
        .args(["delete", "--force", "--wait", "compose-build"])
        .success()
        .expect("delete Compose SSH-build project");
}
