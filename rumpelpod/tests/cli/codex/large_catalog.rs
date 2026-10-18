// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::process::Command;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rumpelpod::CommandExt;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::common::{setup_codex_test_repo, CodexSession};
use crate::common::launch_pod_via_daemon;

#[test]
fn codex_large_plugin_catalog_stays_connected() {
    let (home, repo, _executor, daemon) = setup_codex_test_repo();
    let marketplace_dir = repo.path().join(".agents/plugins");
    let plugin_dir = repo.path().join("plugins/large-catalog/.codex-plugin");
    fs::create_dir_all(&marketplace_dir).expect("create marketplace directory");
    fs::create_dir_all(&plugin_dir).expect("create plugin directory");

    // A local catalog makes the real app-server exceed tungstenite's former
    // 16 MiB frame limit without depending on a remote catalog or inference.
    let description = "x".repeat(17 * 1024 * 1024);
    fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::to_vec(&json!({
            "name": "large-catalog",
            "interface": {
                "displayName": "Large Catalog Regression",
                "shortDescription": "Exercise large remote catalogs",
                "longDescription": description,
            },
        }))
        .expect("serialize plugin"),
    )
    .expect("write plugin");
    fs::write(
        marketplace_dir.join("marketplace.json"),
        serde_json::to_vec(&json!({
            "name": "frame-size-regression",
            "plugins": [{
                "name": "large-catalog",
                "source": {"source": "local", "path": "./plugins/large-catalog"},
                "policy": {"installation": "AVAILABLE", "authentication": "ON_INSTALL"},
                "category": "Productivity",
            }],
        }))
        .expect("serialize marketplace"),
    )
    .expect("write marketplace");
    Command::new("git")
        .current_dir(repo.path())
        .args(["add", ".agents", "plugins"])
        .success()
        .expect("stage catalog fixture");
    Command::new("git")
        .current_dir(repo.path())
        .args(["commit", "-s", "-m", "Exercise oversized catalog responses"])
        .success()
        .expect("commit catalog fixture");

    let mut session = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    session.dismiss_dialogs_with_timeout(Duration::from_secs(30));
    let launch = launch_pod_via_daemon(&repo, &daemon, "test");

    let runtime = tokio::runtime::Runtime::new().expect("create WebSocket runtime");
    runtime.block_on(async {
        // Inspect the real response so a truncated or ignored fixture cannot
        // silently turn the TUI check below into an ordinary small-catalog test.
        let mut url = url::Url::parse(&launch.container_url).expect("parse pod URL");
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            other => panic!("unexpected pod URL scheme: {other}"),
        };
        url.set_scheme(scheme).expect("set WebSocket scheme");
        url.set_path("/codex");
        url.set_query(Some("bypass=true"));
        let mut request = url
            .as_str()
            .into_client_request()
            .expect("build pod request");
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", launch.container_token)
                .parse()
                .expect("auth header"),
        );
        let config = WebSocketConfig::default()
            .max_frame_size(Some(128 * 1024 * 1024))
            .max_message_size(Some(128 * 1024 * 1024));
        let (mut ws, _) =
            tokio_tungstenite::connect_async_with_config(request, Some(config), false)
                .await
                .expect("connect to pod proxy");
        rpc(
            &mut ws,
            1,
            "initialize",
            json!({
                "clientInfo": {"name": "catalog-regression", "version": "1"},
                "capabilities": {"experimentalApi": true},
            }),
        )
        .await;
        ws.send(Message::Text(
            json!({"method": "initialized"}).to_string().into(),
        ))
        .await
        .expect("send initialized notification");
        let (catalog, wire_size) = rpc(
            &mut ws,
            2,
            "plugin/list",
            json!({
                "cwds": [launch.container_repo_path],
                "marketplaceKinds": ["local"],
            }),
        )
        .await;
        assert!(
            wire_size > 16 * 1024 * 1024,
            "catalog was only {wire_size} bytes"
        );
        let marketplaces = catalog["marketplaces"].as_array().expect("marketplaces");
        let marketplace = marketplaces
            .iter()
            .find(|entry| entry["name"] == "frame-size-regression")
            .expect("fixture marketplace in response");
        let returned_description = marketplace["plugins"][0]["interface"]["longDescription"]
            .as_str()
            .expect("plugin description");
        assert_eq!(returned_description.len(), description.len());
        assert!(
            returned_description == description,
            "catalog description changed in transit"
        );
        eprintln!("real Codex plugin/list response: {wire_size} bytes");

        // A subsequent response on the same socket rules out a successful
        // catalog delivery followed by a dropped connection.
        let (models, _) = rpc(&mut ws, 3, "model/list", json!({})).await;
        assert!(!models["data"].as_array().expect("models").is_empty());
        ws.close(None).await.expect("close protocol client");
    });

    session.send_with_timeout("/plugins", Duration::from_secs(10));
    session.wait_for_with_timeout("Large Catalog Regression", Duration::from_secs(30));
}

async fn rpc(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    id: u64,
    method: &str,
    params: Value,
) -> (Value, usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
        ws.send(Message::Text(
            json!({"id": id, "method": method, "params": params})
                .to_string()
                .into(),
        ))
        .await
        .unwrap_or_else(|error| panic!("send {method}: {error}"));
        loop {
            let message = ws
                .next()
                .await
                .unwrap_or_else(|| panic!("connection closed waiting for {method}"))
                .unwrap_or_else(|error| panic!("read {method}: {error}"));
            match message {
                Message::Text(text) => {
                    let mut response: Value =
                        serde_json::from_str(&text).expect("parse RPC message");
                    if response["id"] == id {
                        assert!(
                            response.get("error").is_none(),
                            "{method}: {}",
                            response["error"]
                        );
                        return (response["result"].take(), text.len());
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Close(frame) => {
                    panic!("connection closed waiting for {method}: {frame:?}")
                }
                Message::Binary(_) | Message::Frame(_) => {
                    panic!("unexpected frame waiting for {method}")
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {method}"))
}
