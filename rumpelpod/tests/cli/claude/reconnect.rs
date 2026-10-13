// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::thread;
use std::time::{Duration, Instant};

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use rumpelpod::daemon::protocol::ClaudeConnection;

use crate::common::{TestHome, TestRepo};

#[test]
fn claude_stale_connection_falls_back_to_preparation() {
    check_stale_connection(false);
}

#[test]
fn claude_stalled_handshake_falls_back_to_preparation() {
    check_stale_connection(true);
}

// Keep the daemon's cached response deterministic: real event supervision
// can notice a deliberately broken pod before the CLI attempts attachment.
fn check_stale_connection(stall: bool) {
    let repo = TestRepo::new();
    let home = TestHome::new();
    let socket_dir = tempfile::tempdir_in("/tmp").expect("short Unix socket directory");
    let socket = socket_dir.path().join("daemon.sock");
    let listener = UnixListener::bind(&socket).expect("bind fake daemon");
    let pod_listener = TcpListener::bind("127.0.0.1:0").expect("bind stale pod endpoint");
    let pod_addr = pod_listener.local_addr().expect("pod endpoint address");
    let connection = ClaudeConnection {
        container_url: format!("http://{pod_addr}"),
        container_token: "test-token".into(),
        container_repo_path: "/workspace".into(),
    };
    let pod = thread::spawn(move || {
        let (mut stream, _) = pod_listener.accept().expect("accept terminal connection");
        if stall {
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .expect("client closes stalled handshake");
            assert!(request.starts_with(b"GET /claude "));
        }
    });
    let daemon = thread::spawn(move || {
        let responses = [
            (
                "POST /pod/claude-connection ",
                serde_json::to_string(&connection).unwrap(),
            ),
            (
                "GET /pod ",
                "event: result\ndata: {\"pods\":[]}\n\n".to_string(),
            ),
            (
                "PUT /pod ",
                "event: error\ndata: {\"error\":\"fallback-launch-reached\"}\n\n".to_string(),
            ),
        ];
        for (expected, body) in responses {
            let (mut stream, _) = listener.accept().expect("accept daemon request");
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            assert!(
                request_line.starts_with(expected),
                "unexpected request: {request_line}"
            );
            let mut content_length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header == "\r\n" {
                    break;
                }
                assert!(!header.is_empty(), "incomplete request headers");
                let (name, value) = header.split_once(':').expect("HTTP header");
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut request_body = vec![0; content_length];
            reader.read_exact(&mut request_body).unwrap();
            let length = body.len();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}"
            )
            .unwrap();
        }
    });

    let pair = native_pty_system()
        .openpty(PtySize::default())
        .expect("create client PTY");
    let mut command = CommandBuilder::new("rumpel");
    command.cwd(repo.path());
    command.env("HOME", home.path());
    command.env("PATH", home.bin_dir());
    command.env("RUMPELPOD_DAEMON_SOCKET", &socket);
    command.args(["claude", "--create", "test"]);
    let started = Instant::now();
    let mut child = pair
        .slave
        .spawn_command(command)
        .expect("start Claude client");
    drop(pair.slave);
    let mut reader = pair.master.try_clone_reader().unwrap();
    let mut output = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => output.extend_from_slice(&buffer[..count]),
            Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
            Err(error) => panic!("read client terminal: {error}"),
        }
    }
    assert!(!child.wait().unwrap().success());
    let elapsed = started.elapsed();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("fallback-launch-reached"), "{output}");
    if stall {
        assert!(elapsed >= Duration::from_secs(9), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(30), "{elapsed:?}");
    }
    pod.join().expect("pod thread");
    daemon.join().expect("daemon thread");
}
