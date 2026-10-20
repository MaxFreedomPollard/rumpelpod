// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::env::VarError;
use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::task::AbortOnDropHandle;

use crate::async_runtime::block_on;
use crate::cli::ExecCommand;
use crate::daemon::protocol::{ClientContext, ConnectPodRequest};
use crate::daemon::{self, DaemonServer};
use crate::exec_pty::RawTerminal;
use crate::exec_stream::{
    self, ExecSignal, ExecSpec, ExecutionId, Frame, SignalRequest, TerminalSize, TerminalSpec,
    CHUNK_SIZE, CONTENT_TYPE, UNKNOWN_OUTCOME,
};
use crate::git::get_repo_root;

#[derive(Serialize, Deserialize)]
struct ExecRequest {
    connection: ConnectPodRequest,
    spec: ExecSpec,
}

pub(crate) fn exec(command: &ExecCommand) -> Result<u8> {
    let interactive = command.tty && io::stdin().is_terminal();
    let terminal = if command.tty {
        Some(TerminalSpec {
            size: if interactive {
                TerminalSize::from_stdin()?
            } else {
                TerminalSize { rows: 24, cols: 80 }
            },
            term: match std::env::var("TERM") {
                Ok(term) => Some(term),
                Err(VarError::NotPresent) => None,
                Err(error @ VarError::NotUnicode(_)) => return Err(error.into()),
            },
        })
    } else {
        None
    };
    let repo_path = get_repo_root()?;
    let relative_workdir = std::env::current_dir()?
        .strip_prefix(&repo_path)
        .context("current directory is outside the repository")?
        .to_path_buf();
    let request = ExecRequest {
        connection: ConnectPodRequest {
            pod_name: command.name.clone(),
            repo_path,
            client_context: ClientContext::current(),
        },
        spec: ExecSpec {
            id: ExecutionId(rand::random()),
            command: command.command.clone(),
            relative_workdir,
            terminal,
        },
    };
    let initial = exec_stream::encode_request(&request)?;
    let socket = daemon::socket_path()?;
    block_on(async {
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        let mut hangup = signal(SignalKind::hangup())?;
        let window_change = if interactive {
            Some(signal(SignalKind::window_change())?)
        } else {
            None
        };
        let client = exec_stream::client_builder().unix_socket(socket).build()?;
        let (tx, rx) = mpsc::channel::<io::Result<Vec<u8>>>(4);
        tx.send(Ok(initial)).await.context("sending exec request")?;

        let stdin_tx = tx;
        let _stdin = AbortOnDropHandle::new(tokio::spawn(async move {
            if let Err(error) = forward_stdin(&stdin_tx, window_change).await {
                if let Err(send_error) = stdin_tx.send(Err(error)).await {
                    log::debug!("exec input closed after stdin error: {send_error}");
                }
            }
        }));
        let send = client
            .post("http://localhost/pod/exec")
            .header("content-type", CONTENT_TYPE)
            .body(reqwest::Body::wrap_stream(ReceiverStream::new(rx)))
            .send();
        let response = tokio::select! {
            response = send => response.context(UNKNOWN_OUTCOME)?,
            _ = interrupt.recv() => return Err(anyhow::anyhow!("exec interrupted before startup was acknowledged; command outcome is unknown")),
            _ = terminate.recv() => return Err(anyhow::anyhow!("exec terminated before startup was acknowledged; command outcome is unknown")),
            _ = hangup.recv() => return Err(anyhow::anyhow!("exec disconnected before startup was acknowledged; command outcome is unknown")),
        };
        if !response.status().is_success() {
            let status = response.status();
            let error = response.text().await.context(UNKNOWN_OUTCOME)?;
            return Err(anyhow::anyhow!("exec failed ({status}): {error}"));
        }
        // Keep local Ctrl-C available during startup. Switch before displaying
        // any prompt so password input cannot be echoed by the local terminal.
        let _terminal = if interactive {
            Some(RawTerminal::new()?)
        } else {
            None
        };
        // Headers acknowledge that the execution ID has been registered. A
        // separate, bodyless HTTP/2 request bypasses both stream and connection
        // DATA flow control when the command stops consuming stdin.
        let signals = async {
            loop {
                let signal = tokio::select! {
                    _ = interrupt.recv() => ExecSignal::Interrupt,
                    _ = terminate.recv() => ExecSignal::Terminate,
                    _ = hangup.recv() => ExecSignal::Hangup,
                };
                let response = client
                    .post("http://localhost/pod/exec/signal")
                    .query(&ExecSignalRequest {
                        repo_path: request.connection.repo_path.clone(),
                        pod_name: request.connection.pod_name.clone(),
                        id: request.spec.id,
                        signal,
                    })
                    .send()
                    .await
                    .context("sending exec signal")?;
                if !response.status().is_success() {
                    let error = response.text().await?;
                    return Err(anyhow::anyhow!("forwarding exec signal: {error}"));
                }
            }
        };
        tokio::select! {
            result = read_output(response) => result,
            result = signals => result,
        }
    })
}

async fn read_output(response: reqwest::Response) -> Result<u8> {
    let mut reader = StreamReader::new(response.bytes_stream().map_err(exec_stream::io_error));
    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    loop {
        match exec_stream::read_frame(&mut reader)
            .await
            .context(UNKNOWN_OUTCOME)?
        {
            Some(Frame::Stdout(data)) => {
                stdout.write_all(&data).await?;
                stdout.flush().await?;
            }
            Some(Frame::Stderr(data)) => {
                stderr.write_all(&data).await?;
                stderr.flush().await?;
            }
            Some(Frame::Exit(code)) => return Ok(code),
            Some(Frame::Error(error)) => return Err(anyhow::anyhow!("{error}")),
            Some(Frame::Stdin(_) | Frame::StdinEof | Frame::Resize(_)) => {
                return Err(anyhow::anyhow!("invalid exec response; {UNKNOWN_OUTCOME}"));
            }
            None => return Err(anyhow::anyhow!("{UNKNOWN_OUTCOME}")),
        }
    }
}

async fn forward_stdin(
    tx: &mpsc::Sender<io::Result<Vec<u8>>>,
    mut window_change: Option<Signal>,
) -> io::Result<()> {
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0; CHUNK_SIZE];
    loop {
        let frame = tokio::select! {
            count = stdin.read(&mut buffer) => {
                let count = count?;
                if count == 0 {
                    Frame::StdinEof
                } else {
                    Frame::Stdin(buffer[..count].to_vec())
                }
            }
            _ = async {
                match &mut window_change {
                    Some(signal) => signal.recv().await,
                    None => std::future::pending().await,
                }
            } => Frame::Resize(TerminalSize::from_stdin().map_err(exec_stream::io_error)?),
        };
        let eof = matches!(frame, Frame::StdinEof);
        if tx.send(Ok(frame.encode())).await.is_err() || eof {
            return Ok(());
        }
    }
}

pub(crate) fn daemon_routes(daemon: Arc<DaemonServer>) -> Router {
    Router::new()
        .route("/pod/exec", post(daemon_exec))
        .route("/pod/exec/signal", post(daemon_signal))
        .with_state(daemon)
}

async fn daemon_exec(
    State(daemon): State<Arc<DaemonServer>>,
    body: Body,
) -> Result<Response, (StatusCode, String)> {
    let mut reader = StreamReader::new(body.into_data_stream().map_err(exec_stream::io_error));
    let request: ExecRequest = exec_stream::read_request(&mut reader)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("{error:#}")))?;
    let (client, endpoint) = daemon
        .exec_connection(&request.connection)
        .map_err(|error| (StatusCode::CONFLICT, format!("{error:#}")))?;
    let initial = exec_stream::encode_request(&request.spec)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("{error:#}")))?;
    let input = futures_util::stream::once(async { Ok::<_, io::Error>(initial.into()) })
        .chain(ReaderStream::new(reader));
    let response = client
        .post(format!("{}/exec", endpoint.url))
        .bearer_auth(&endpoint.token)
        .header("content-type", CONTENT_TYPE)
        .body(reqwest::Body::wrap_stream(input))
        .send()
        .await
        .map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("{UNKNOWN_OUTCOME}: {error}"),
            )
        })?;
    Ok(Response::builder()
        .status(response.status())
        .header("content-type", CONTENT_TYPE)
        .body(Body::from_stream(response.bytes_stream()))
        .expect("valid exec response"))
}

#[derive(Serialize, Deserialize)]
struct ExecSignalRequest {
    repo_path: PathBuf,
    pod_name: String,
    id: ExecutionId,
    signal: ExecSignal,
}

async fn daemon_signal(
    State(daemon): State<Arc<DaemonServer>>,
    Query(request): Query<ExecSignalRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let (client, endpoint) = daemon
        .exec_connection(&ConnectPodRequest {
            repo_path: request.repo_path,
            pod_name: request.pod_name,
            client_context: ClientContext::default(),
        })
        .map_err(|error| (StatusCode::CONFLICT, format!("{error:#}")))?;
    let response = client
        .post(format!("{}/exec/signal", endpoint.url))
        .bearer_auth(&endpoint.token)
        .query(&SignalRequest {
            id: request.id,
            signal: request.signal,
        })
        .send()
        .await
        .map_err(|error| (StatusCode::BAD_GATEWAY, format!("{error:#}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let message = response
            .text()
            .await
            .map_err(|error| (StatusCode::BAD_GATEWAY, format!("{error:#}")))?;
        return Err((status, message));
    }
    Ok(StatusCode::NO_CONTENT)
}
