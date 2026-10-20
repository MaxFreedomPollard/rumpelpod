// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Host-side exec proxy for reaching container-serve via the
//! executor's streaming exec primitive.
//!
//! Instead of routing through bridge IPs, SSH port forwards, or
//! kubectl port-forward, we exec `rumpel tcp-proxy --port <port>` into
//! the pod for each TCP connection.  The proxy bridges the
//! connection's bytes over the exec's stdin/stdout.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::executor::{Executor, PodId};

#[derive(Clone, Copy)]
enum ConnectionKind {
    PodHttp,
    ForwardedTcp,
}

/// Handle for an active exec proxy listener.  Dropping cancels the
/// listener and its active connections.
pub struct ExecProxyHandle {
    /// Local port the proxy listener is bound to.
    pub port: u16,
    alive: Arc<AtomicBool>,
    target_container: String,
    _cancel_tx: tokio::sync::watch::Sender<bool>,
    reset_tx: tokio::sync::watch::Sender<()>,
}

impl ExecProxyHandle {
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    pub fn target_container(&self) -> &str {
        &self.target_container
    }

    // Preserve the listener address while cancelling exec sessions that used
    // a lost host transport. Their pipe reads may never observe TCP closure.
    pub(crate) fn reset_connections(&self) {
        self.reset_tx.send_replace(());
    }
}

/// Start a listener for pod-server HTTP requests via `rumpel tcp-proxy`.
/// HTTP framing delimits requests, so client EOF cancels the owned exec.
///
/// Works on both backends: the `Executor` picks between docker and
/// kubectl subprocesses.
pub async fn start_pod_http_proxy(
    executor: Executor,
    pod_id: PodId,
    container_port: u16,
) -> Result<ExecProxyHandle> {
    start_pod_http_proxy_in_container(executor, pod_id.as_str().to_string(), container_port).await
}

pub async fn start_pod_http_proxy_in_container(
    executor: Executor,
    container: String,
    container_port: u16,
) -> Result<ExecProxyHandle> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding exec proxy listener")?;
    start_proxy(
        listener,
        executor,
        container,
        container_port,
        ConnectionKind::PodHttp,
    )
}

/// Proxy a forwarded TCP service on a caller-supplied listener.
/// Preserve write half-closes so services can reply after request EOF.
///
/// Lets callers pick the host port up front (e.g. rebinding a port
/// recorded in the database so URLs stay stable across daemon
/// restarts).  Safe to call from sync code: the accept loop is
/// spawned on the shared daemon runtime rather than the current
/// thread's (possibly absent) tokio context.
pub fn start_exec_proxy_on_listener_in_container(
    listener: TcpListener,
    executor: Executor,
    container: String,
    container_port: u16,
) -> Result<ExecProxyHandle> {
    start_proxy(
        listener,
        executor,
        container,
        container_port,
        ConnectionKind::ForwardedTcp,
    )
}

fn start_proxy(
    listener: TcpListener,
    executor: Executor,
    container: String,
    container_port: u16,
    kind: ConnectionKind,
) -> Result<ExecProxyHandle> {
    let port = listener.local_addr()?.port();

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let (reset_tx, reset_rx) = tokio::sync::watch::channel(());
    let alive = Arc::new(AtomicBool::new(true));
    let alive2 = alive.clone();
    let target_container = container.clone();

    crate::async_runtime::RUNTIME.spawn(async move {
        accept_loop(
            listener,
            executor,
            container,
            container_port,
            kind,
            cancel_rx,
            reset_rx,
            alive2,
        )
        .await;
    });

    Ok(ExecProxyHandle {
        port,
        alive,
        target_container,
        _cancel_tx: cancel_tx,
        reset_tx,
    })
}

#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    listener: TcpListener,
    executor: Executor,
    container: String,
    container_port: u16,
    kind: ConnectionKind,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    mut reset_rx: tokio::sync::watch::Receiver<()>,
    alive: Arc<AtomicBool>,
) {
    let mut bridges = tokio::task::JoinSet::new();
    loop {
        let stream = tokio::select! {
            biased;
            _ = cancel_rx.changed() => break,
            result = reset_rx.changed() => {
                if result.is_err() { break; }
                bridges.abort_all();
                continue;
            }
            Some(result) = bridges.join_next() => {
                if let Err(error) = result {
                    if !error.is_cancelled() { log::error!("exec proxy bridge task failed: {error}"); }
                }
                continue;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        log::error!("exec proxy accept error: {e}");
                        break;
                    }
                }
            }
        };

        let executor = executor.clone();
        let container = container.clone();

        bridges.spawn(async move {
            if let Err(e) =
                bridge_connection(stream, &executor, &container, container_port, kind).await
            {
                log::debug!("exec proxy bridge ended: {e:#}");
            }
        });
    }

    alive.store(false, Ordering::Relaxed);
}

async fn bridge_connection(
    tcp_stream: tokio::net::TcpStream,
    executor: &Executor,
    container: &str,
    container_port: u16,
    kind: ConnectionKind,
) -> Result<()> {
    let streams = executor
        .exec_streaming(
            container,
            vec![
                crate::daemon::RUMPEL_CONTAINER_BIN.to_string(),
                "tcp-proxy".to_string(),
                "--port".to_string(),
                container_port.to_string(),
            ],
        )
        .await
        .context("exec-streaming tcp-proxy in pod")?;

    let crate::executor::ExecStreams {
        stdin,
        stdout,
        mut stderr,
        keepalive,
    } = streams;

    // Forward stderr to debug log so tcp-proxy diagnostics surface
    // without corrupting the tunneled byte stream.
    let _stderr = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            match stderr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(s) = std::str::from_utf8(&buf[..n]) {
                        for line in s.lines() {
                            log::debug!("tcp-proxy stderr: {line}");
                        }
                    }
                }
            }
        }
    }));

    forward_streams(tcp_stream, stdin, stdout, kind).await;
    drop(keepalive);
    Ok(())
}

async fn forward_streams(
    tcp_stream: tokio::net::TcpStream,
    mut stdin: impl AsyncWriteExt + Unpin,
    mut stdout: impl AsyncReadExt + Unpin,
    kind: ConnectionKind,
) {
    let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();

    // exec stdout -> tcp write
    let h1 = async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match stdout.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tcp_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = tcp_write.shutdown().await;
    };

    // tcp read -> exec stdin
    let h2 = async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if stdin.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = stdin.shutdown().await;
    };

    match kind {
        // Pod HTTP clients delimit requests with HTTP framing. EOF means
        // the request was abandoned, including event-stream read timeouts.
        ConnectionKind::PodHttp => tokio::select! {
            () = h1 => {},
            () = h2 => {},
        },
        // Forwarded services can use a write half-close to request a reply.
        ConnectionKind::ForwardedTcp => {
            tokio::join!(h1, h2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn forwarded_tcp_preserves_replies_after_write_half_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        let (to_pod, mut pod_input) = tokio::io::duplex(1024);
        let (mut from_pod, pod_output) = tokio::io::duplex(1024);
        let bridge = tokio::spawn(forward_streams(
            accepted,
            to_pod,
            pod_output,
            ConnectionKind::ForwardedTcp,
        ));
        client.write_all(b"request").await.unwrap();
        client.shutdown().await.unwrap();
        let mut request = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), pod_input.read_to_end(&mut request))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request, b"request");
        assert!(!bridge.is_finished());
        from_pod.write_all(b"reply").await.unwrap();
        from_pod.shutdown().await.unwrap();
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply, b"reply");
        bridge.await.unwrap();
    }
}
