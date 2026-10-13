// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::thread;
use std::time::{Duration, Instant};

use rumpelpod::daemon::protocol::{ClientContext, ConnectPodRequest, Daemon, DaemonClient};
use rumpelpod::CommandExt;

use crate::common::{pod_command, write_test_devcontainer, TestDaemon, TestHome, TestRepo};
use crate::executor::{executor_supports_stop, ExecutorResources};

fn wait_for_prepared_connection(repo: &TestRepo, daemon: &TestDaemon) {
    let client = DaemonClient::new_unix(&daemon.socket_path);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .prepared_pod_connection(ConnectPodRequest {
                pod_name: "test".into(),
                repo_path: repo.path().to_path_buf(),
                client_context: ClientContext::default(),
            })
            .expect("query prepared connection")
            .is_some()
        {
            return;
        }
        assert!(Instant::now() < deadline, "pod readiness was not observed");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn enter_prepared_connection_restarts_stopped_pod() {
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
        .args(["enter", "--create", "test", "--", "touch", "/tmp/old-pod"])
        .success()
        .unwrap();
    wait_for_prepared_connection(&repo, &daemon);
    pod_command(&repo, &daemon)
        .args(["enter", "test", "--", "test", "-f", "/tmp/old-pod"])
        .success()
        .expect("enter prepared pod");
    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .unwrap();
    let client = DaemonClient::new_unix(&daemon.socket_path);
    assert!(client
        .prepared_pod_connection(ConnectPodRequest {
            pod_name: "test".into(),
            repo_path: repo.path().to_path_buf(),
            client_context: ClientContext::default(),
        })
        .unwrap()
        .is_none());
    pod_command(&repo, &daemon)
        .args(["enter", "test", "--", "test", "-f", "/tmp/old-pod"])
        .success()
        .expect("restart stopped pod");
}

#[test]
fn cp_prepared_connection_restarts_stopped_pod() {
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
        .args(["enter", "--create", "test", "--", "true"])
        .success()
        .unwrap();
    wait_for_prepared_connection(&repo, &daemon);
    let source = home.path().join("source");
    let destination = home.path().join("destination");
    fs::write(&source, "warm copy\n").unwrap();
    pod_command(&repo, &daemon)
        .args(["cp", source.to_str().unwrap(), "test:restarted-file"])
        .success()
        .expect("upload to prepared pod");
    pod_command(&repo, &daemon)
        .args(["cp", "test:restarted-file", destination.to_str().unwrap()])
        .success()
        .expect("download from prepared pod");
    assert_eq!(fs::read(&source).unwrap(), fs::read(&destination).unwrap());

    fs::write(&source, "survives pod restart\n").unwrap();
    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .unwrap();
    pod_command(&repo, &daemon)
        .args(["cp", source.to_str().unwrap(), "test:restarted-file"])
        .success()
        .expect("restart pod before upload");
    pod_command(&repo, &daemon)
        .args(["stop", "--wait", "test"])
        .success()
        .unwrap();
    pod_command(&repo, &daemon)
        .args(["cp", "test:restarted-file", destination.to_str().unwrap()])
        .success()
        .expect("restart pod before download");
    assert_eq!(fs::read(source).unwrap(), fs::read(destination).unwrap());
}
