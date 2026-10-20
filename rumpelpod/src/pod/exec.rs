// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Component;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures_util::TryStreamExt;
use nix::errno::Errno;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

use super::server::PodServerState;
use crate::async_command::ProcessGroup;
use crate::exec_pty::{Pty, TerminalSession};
use crate::exec_stream::{
    self, ExecSignal, ExecSpec, ExecutionId, Frame, SignalRequest, CHUNK_SIZE, CONTENT_TYPE,
};

pub(crate) type Executions = Arc<Mutex<HashMap<ExecutionId, mpsc::Sender<Signal>>>>;

struct RegisteredExecution {
    executions: Executions,
    id: ExecutionId,
}

impl Drop for RegisteredExecution {
    fn drop(&mut self) {
        self.executions.lock().unwrap().remove(&self.id);
    }
}

pub(super) async fn signal_handler(
    State(state): State<PodServerState>,
    Query(request): Query<SignalRequest>,
) -> StatusCode {
    let signal = match request.signal {
        ExecSignal::Interrupt => Signal::SIGINT,
        ExecSignal::Terminate => Signal::SIGTERM,
        ExecSignal::Hangup => Signal::SIGHUP,
    };
    let sender = state.executions.lock().unwrap().get(&request.id).cloned();
    if let Some(sender) = sender {
        if let Err(error) = sender.send(signal).await {
            log::debug!("exec finished before signal delivery: {error}");
        }
    }
    // The command can finish between the client sending a signal and its
    // arrival. Execution IDs keep that race from signaling a reused PID.
    StatusCode::NO_CONTENT
}

pub(super) async fn exec_handler(
    State(state): State<PodServerState>,
    body: Body,
) -> Result<Response, (StatusCode, String)> {
    let mut reader = StreamReader::new(body.into_data_stream().map_err(exec_stream::io_error));
    let spec: ExecSpec = exec_stream::read_request(&mut reader)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("{error:#}")))?;
    if spec.command.is_empty()
        || spec
            .relative_workdir
            .components()
            .any(|component| match component {
                Component::Normal(_) | Component::CurDir => false,
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => true,
            })
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid exec command or relative directory".into(),
        ));
    }
    if !*state.setup_done.borrow() {
        return Err((StatusCode::CONFLICT, "pod setup is not complete".into()));
    }
    let repo_path = state
        .repo_path
        .lock()
        .await
        .clone()
        .ok_or_else(|| (StatusCode::CONFLICT, "pod repository is unavailable".into()))?;
    let env = state.resolved_env.lock().unwrap().clone();
    let mut command = Command::new(&spec.command[0]);
    command
        .args(&spec.command[1..])
        .current_dir(repo_path.join(&spec.relative_workdir))
        .env_clear()
        .envs(env)
        .kill_on_drop(true);
    let terminal = match &spec.terminal {
        Some(terminal) => Some(
            Pty::prepare(&mut command, terminal)
                .map_err(|error| (StatusCode::BAD_REQUEST, format!("{error:#}")))?,
        ),
        None => {
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            command.as_std_mut().process_group(0);
            None
        }
    };
    let mut executions = state.executions.lock().unwrap();
    if executions.contains_key(&spec.id) {
        return Err((
            StatusCode::CONFLICT,
            "execution ID is already active".into(),
        ));
    }
    let child = command.spawn().map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("starting exec program: {error}"),
        )
    })?;
    // Command retains the configured slave descriptors after spawn.
    // Release them so PTY EOF follows the last child closing its terminal.
    drop(command);
    let (signal_tx, signals) = mpsc::channel(4);
    executions.insert(spec.id, signal_tx);
    drop(executions);
    let registration = RegisteredExecution {
        executions: state.executions,
        id: spec.id,
    };
    let (tx, rx) = mpsc::channel::<io::Result<Vec<u8>>>(4);
    tokio::spawn(async move {
        if let Err(error) = run_process(child, reader, &tx, signals, registration, terminal).await {
            if let Err(send_error) = tx
                .send(Ok(Frame::Error(format!("{error:#}")).encode()))
                .await
            {
                log::debug!("exec response closed after process error: {send_error}");
            }
        }
    });
    Ok(Response::builder()
        .header("content-type", CONTENT_TYPE)
        .body(Body::from_stream(ReceiverStream::new(rx)))
        .expect("valid exec response"))
}

async fn run_process(
    mut child: Child,
    reader: impl AsyncRead + Unpin,
    tx: &mpsc::Sender<io::Result<Vec<u8>>>,
    mut signals: mpsc::Receiver<Signal>,
    registration: RegisteredExecution,
    terminal: Option<Pty>,
) -> Result<()> {
    let mut group = ProcessGroup::new(&child);
    let pid = Pid::from_raw(child.id().context("exec process has no PID")? as i32);
    let mut session = if terminal.is_some() {
        Some(TerminalSession::new(pid)?)
    } else {
        None
    };
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let completed = async {
        match &terminal {
            Some(terminal) => forward_terminal_output(terminal, tx).await?,
            None => {
                tokio::try_join!(
                    forward_output(stdout.context("exec stdout missing")?, tx, Frame::Stdout),
                    forward_output(stderr.context("exec stderr missing")?, tx, Frame::Stderr),
                )?;
            }
        }
        // Keep the leader unreaped while descendants hold output open. Its
        // reserved PID lets signals safely reach the process group even after
        // the leader exits, without risking a newly reused PID.
        child.wait().await.context("waiting for exec process")
    };
    let input = forward_input(reader, stdin, terminal.as_ref());
    tokio::pin!(completed, input);
    let status = loop {
        tokio::select! {
            result = &mut completed => break result?,
            result = &mut input => {
                return result.context("exec input failed; command outcome is unknown");
            }
            Some(signal) = signals.recv() => {
                if let Some(terminal) = &terminal {
                    if let Some(foreground) = terminal.foreground_group()? {
                        if foreground != pid {
                            forward_signal(foreground, signal)?;
                        }
                    }
                }
                forward_signal(pid, signal)?;
            }
            _ = tx.closed() => {
                return Err(anyhow::anyhow!("exec client disconnected; terminating command"));
            }
        }
    };
    if let Some(session) = &mut session {
        session.completed();
    }
    drop(registration);
    let code = match (status.code(), status.signal()) {
        (Some(code), None) => u8::try_from(code).context("invalid exec exit code")?,
        (None, Some(signal)) => u8::try_from(128 + signal).context("invalid exec signal")?,
        (None, None) | (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!("invalid exec exit status"))
        }
    };
    group.completed(status);
    tx.send(Ok(Frame::Exit(code).encode()))
        .await
        .context("sending exec exit status")?;
    Ok(())
}

enum Input<'a> {
    Pipe(ChildStdin),
    Terminal(&'a Pty),
}

async fn forward_input(
    mut reader: impl AsyncRead + Unpin,
    stdin: Option<ChildStdin>,
    terminal: Option<&Pty>,
) -> Result<()> {
    let mut stdin = Some(match terminal {
        Some(terminal) => Input::Terminal(terminal),
        None => Input::Pipe(stdin.context("exec stdin missing")?),
    });
    let mut eof = false;
    loop {
        match exec_stream::read_frame(&mut reader).await? {
            Some(Frame::Stdin(data)) => {
                if eof {
                    return Err(anyhow::anyhow!("exec stdin data after EOF"));
                }
                if let Some(writer) = &mut stdin {
                    let result = match writer {
                        Input::Pipe(writer) => writer.write_all(&data).await,
                        Input::Terminal(terminal) => terminal.write_all(&data).await,
                    };
                    match result {
                        Ok(()) => {}
                        // A command may finish reading before the producer finishes.
                        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => stdin = None,
                        Err(error)
                            if terminal.is_some() && error.raw_os_error() == Some(libc::EIO) =>
                        {
                            stdin = None;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            Some(Frame::StdinEof) => {
                if eof {
                    return Err(anyhow::anyhow!("duplicate exec stdin EOF"));
                }
                eof = true;
                // A PTY cannot be half-closed. Leave it alive for output;
                // terminal EOF is the caller's Ctrl-D byte, not transport EOF.
                stdin = None;
            }
            Some(Frame::Resize(size)) => {
                terminal
                    .context("terminal resize for exec without a terminal")?
                    .resize(size)?;
            }
            Some(Frame::Stdout(_) | Frame::Stderr(_) | Frame::Exit(_) | Frame::Error(_)) => {
                return Err(anyhow::anyhow!("invalid exec input frame"));
            }
            None => {
                if !eof {
                    return Err(anyhow::anyhow!("exec input closed without stdin EOF"));
                }
                // Request completion is distinct from cancellation of the
                // response, which run_process observes through tx.closed().
                return std::future::pending().await;
            }
        }
    }
}

async fn forward_terminal_output(
    terminal: &Pty,
    tx: &mpsc::Sender<io::Result<Vec<u8>>>,
) -> Result<()> {
    let mut buffer = vec![0; CHUNK_SIZE];
    loop {
        let count = terminal.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        tx.send(Ok(Frame::Stdout(buffer[..count].to_vec()).encode()))
            .await
            .context("streaming exec terminal output")?;
    }
}

fn forward_signal(pid: Pid, signal: Signal) -> Result<()> {
    match killpg(pid, signal) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(error).context("forwarding exec signal"),
    }
}

async fn forward_output(
    mut reader: impl AsyncRead + Unpin,
    tx: &mpsc::Sender<io::Result<Vec<u8>>>,
    frame: fn(Vec<u8>) -> Frame,
) -> Result<()> {
    let mut buffer = vec![0; CHUNK_SIZE];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        tx.send(Ok(frame(buffer[..count].to_vec()).encode()))
            .await
            .context("streaming exec output")?;
    }
}
