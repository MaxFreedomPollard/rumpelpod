// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use axum::extract::ws::WebSocketUpgrade;
use axum::routing::any;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::task::AbortOnDropHandle;

use super::proxy_to_app_server;
use crate::codex::run_codex_proxy;

#[tokio::test]
async fn codex_proxy_forwards_large_catalog_frames() {
    timeout(Duration::from_secs(10), async {
        // Real plugin catalogs exceed Tungstenite's 16 MiB frame limit even
        // though they fit within its 64 MiB message limit.
        let catalog = serde_json::json!({
            "id": 1,
            "result": {"catalog": "x".repeat(17 * 1024 * 1024)},
        })
        .to_string();
        let expected_catalog = catalog.clone();
        let app_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app_port = app_listener.local_addr().unwrap().port();
        let app_server = AbortOnDropHandle::new(tokio::spawn(async move {
            let (stream, _) = app_listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let request = ws.next().await.unwrap().unwrap();
            assert!(request.to_text().unwrap().contains("plugin/list"));
            ws.send(Message::Text(catalog.into())).await.unwrap();
            let request = ws.next().await.unwrap().unwrap();
            assert!(request.to_text().unwrap().contains("thread/list"));
            ws.send(Message::Text(r#"{"id":2,"result":{"data":[]}}"#.into()))
                .await
                .unwrap();
            ws.close(None).await.unwrap();
            assert!(matches!(
                ws.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
        }));

        let pod_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pod_url = format!("http://{}", pod_listener.local_addr().unwrap());
        let router = Router::new().route(
            "/codex",
            any(move |ws: WebSocketUpgrade| async move {
                ws.on_upgrade(move |socket| async move {
                    proxy_to_app_server(socket, app_port, false).await.unwrap();
                })
            }),
        );
        let _pod_server = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(pod_listener, router).await.unwrap();
        }));

        let daemon_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let daemon_url = format!("ws://{}", daemon_listener.local_addr().unwrap());
        let (ready_tx, _ready_rx) = mpsc::sync_channel(1);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let _daemon_proxy = AbortOnDropHandle::new(tokio::spawn(run_codex_proxy(
            daemon_listener,
            pod_url,
            "pod-test-token".to_string(),
            "frontend-test-token".to_string(),
            Arc::new(AtomicBool::new(false)),
            ready_tx,
            cancel_rx,
        )));

        let mut request = daemon_url.into_client_request().unwrap();
        request.headers_mut().insert(
            "Authorization",
            "Bearer frontend-test-token".parse().unwrap(),
        );
        let config = WebSocketConfig::default().max_frame_size(Some(64 * 1024 * 1024));
        let (mut frontend, _) =
            tokio_tungstenite::connect_async_with_config(request, Some(config), false)
                .await
                .unwrap();
        frontend
            .send(Message::Text(r#"{"id":1,"method":"plugin/list"}"#.into()))
            .await
            .unwrap();
        let response = frontend.next().await.unwrap().unwrap();
        assert!(response.to_text().unwrap() == expected_catalog);

        // A follow-up must use this same connection; reconnecting would hide
        // the repeated startup failure seen by the real frontend.
        frontend
            .send(Message::Text(r#"{"id":2,"method":"thread/list"}"#.into()))
            .await
            .unwrap();
        let response = frontend.next().await.unwrap().unwrap();
        assert_eq!(
            response.to_text().unwrap(),
            r#"{"id":2,"result":{"data":[]}}"#
        );
        assert!(matches!(
            frontend.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        frontend.flush().await.unwrap();
        app_server.await.unwrap();
    })
    .await
    .expect("Codex proxies must keep the connection usable after a large catalog");
}
