// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generic screen-style PTY session manager.
//!
//! Holds child PTY sessions open across client disconnections so a
//! WebSocket client can detach and reattach.  A virtual terminal buffer
//! (vt100) tracks the screen state so the full TUI is replayed
//! immediately on reattach, like screen/tmux.
//!
//! Used both by the pod server (for the in-container `claude` session)
//! and by the host-side daemon (for the host-side `codex` TUI).  The
//! generic bridge does not resolve cmd[0]; a per-use-case
//! `cmd_transform` callback can rewrite the first SessionParams before
//! the child is spawned -- the pod side uses this to substitute the
//! container's claude binary path.

use std::collections::HashMap;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use axum::extract::ws::{Message, WebSocket};
use nix::pty::{forkpty, ForkptyResult, Winsize};
use nix::sys::signal::{kill, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use tokio::io::unix::AsyncFd;
use tokio::sync::Mutex;

use crate::daemon::protocol::ClientContext;

// ---------------------------------------------------------------------------
// Session types
// ---------------------------------------------------------------------------

struct PtySession {
    session_id: u64,
    master_fd: OwnedFd,
    child_pid: Pid,
    /// Virtual terminal buffer tracking the screen state for replay
    /// on reattach, like screen/tmux.
    screen: Arc<Mutex<vt100::Parser>>,
    /// Attached clients follow output and child replacement on this channel.
    output_tx: tokio::sync::broadcast::Sender<PtyOutput>,
}

#[derive(Clone)]
enum PtyOutput {
    Data(Vec<u8>),
    Replaced,
}

type AttachedSession = (
    u64,
    Arc<Mutex<vt100::Parser>>,
    tokio::sync::broadcast::Receiver<PtyOutput>,
);

/// A monitor belongs to one child, even when its name is reused by a later
/// session. Replacement and lifecycle cleanup share the same creation lock.
pub(crate) struct SessionWatch {
    name: String,
    session_id: u64,
    sessions: PtySessions,
    pub(crate) screen: Arc<Mutex<vt100::Parser>>,
    output: tokio::sync::broadcast::Receiver<PtyOutput>,
}

impl SessionWatch {
    pub(crate) async fn changed(&mut self) -> bool {
        match self.output.recv().await {
            Ok(PtyOutput::Data(_)) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                true
            }
            Ok(PtyOutput::Replaced) | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                false
            }
        }
    }

    pub(crate) async fn is_current(&self) -> bool {
        self.sessions
            .inner
            .lock()
            .await
            .get(&self.name)
            .is_some_and(|session| session.session_id == self.session_id)
    }

    pub(crate) async fn replace<F>(&self, build_spec: F) -> Result<bool>
    where
        F: FnOnce() -> Result<Option<SessionSpec>> + Send,
    {
        let creation_lock = self.sessions.creation_lock(&self.name).await;
        let _creation_guard = creation_lock.lock().await;
        if !self.is_current().await {
            return Ok(false);
        }
        let Some(spec) = build_spec()? else {
            return Ok(false);
        };
        if spec.name != self.name {
            return Err(anyhow::anyhow!("replacement changed the PTY session name"));
        }
        let (rows, cols) = self.screen.lock().await.screen().size();
        let mut sessions = self.sessions.inner.lock().await;
        let Some(old) = sessions.get(&self.name) else {
            return Ok(false);
        };
        if old.session_id != self.session_id || !child_is_alive(old.child_pid) {
            return Ok(false);
        }
        // Prepare the replacement before retiring the failed frontend so a
        // spawn error leaves an inspectable terminal instead of an empty slot.
        let replacement = spawn_session(
            &self.name,
            &spec.cmd,
            spec.workdir,
            &spec.env,
            cols,
            rows,
            &self.sessions,
        )?;
        let watch = replacement.watch(&self.name, &self.sessions);
        let old = sessions
            .insert(self.name.clone(), replacement)
            .expect("replacement has an existing session");
        drop(sessions);
        // Publish the new generation before notifying attached clients so input
        // already targets it when they switch output subscriptions.
        let _ = old.output_tx.send(PtyOutput::Replaced);
        match kill(old.child_pid, Signal::SIGTERM) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => return Err(error).context("terminating replaced PTY child"),
        }
        if let Some(on_spawn) = spec.on_spawn {
            on_spawn(watch);
        }
        Ok(true)
    }
}

impl PtySession {
    fn watch(&self, name: &str, sessions: &PtySessions) -> SessionWatch {
        SessionWatch {
            name: name.to_string(),
            session_id: self.session_id,
            sessions: sessions.clone(),
            screen: self.screen.clone(),
            output: self.output_tx.subscribe(),
        }
    }
}

type SessionCreationLocks = HashMap<String, Weak<Mutex<()>>>;

enum SessionAcquire {
    Attached(AttachedSession),
    Missing,
    Unavailable,
}

#[derive(Clone)]
pub struct PtySessions {
    inner: Arc<Mutex<HashMap<String, PtySession>>>,
    creation_locks: Arc<Mutex<SessionCreationLocks>>,
    next_session_id: Arc<AtomicU64>,
}

impl Default for PtySessions {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            creation_locks: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl PtySessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach without constructing spawn parameters when a live child exists.
    /// Cold preparation is serialized per session so it cannot delay warm
    /// attachments to unrelated sessions.
    async fn spawn_or_attach<F>(
        &self,
        name: String,
        cols: u16,
        rows: u16,
        create: bool,
        build_spec: F,
    ) -> Result<SessionAcquire>
    where
        F: FnOnce() -> Result<Option<SessionSpec>> + Send,
    {
        if let Some(attached) = self.attach_existing(&name, cols, rows).await? {
            return Ok(SessionAcquire::Attached(attached));
        }
        if !create {
            return Ok(SessionAcquire::Missing);
        }

        let creation_lock = self.creation_lock(&name).await;
        let _creation_guard = creation_lock.lock().await;

        // Another request may have created the session while this one waited.
        if let Some(attached) = self.attach_existing(&name, cols, rows).await? {
            return Ok(SessionAcquire::Attached(attached));
        }

        let Some(spec) = build_spec()? else {
            return Ok(SessionAcquire::Unavailable);
        };
        if spec.name != name {
            return Err(anyhow::anyhow!(
                "session spec name {:?} does not match requested name {name:?}",
                spec.name
            ));
        }

        let session = spawn_session(&name, &spec.cmd, spec.workdir, &spec.env, cols, rows, self)?;
        let watch = session.watch(&name, self);
        self.inner.lock().await.insert(name.clone(), session);
        if let Some(on_spawn) = spec.on_spawn {
            on_spawn(watch);
        }

        match self.attach_existing(&name, cols, rows).await? {
            Some(attached) => Ok(SessionAcquire::Attached(attached)),
            None => Ok(SessionAcquire::Missing),
        }
    }

    async fn attach_existing(
        &self,
        name: &str,
        cols: u16,
        rows: u16,
    ) -> Result<Option<AttachedSession>> {
        let mut sessions = self.inner.lock().await;

        // Reap dead sessions so a later create request starts fresh.
        if let Some(session) = sessions.get(name) {
            if !child_is_alive(session.child_pid) {
                sessions.remove(name);
            }
        }

        let Some(session) = sessions.get(name) else {
            return Ok(None);
        };

        // Resize to match the attaching client's terminal, so
        // reattaching after a window resize (or reconnection) shows
        // the right dimensions.
        set_winsize(session.master_fd.as_raw_fd(), cols, rows)?;
        let screen = Arc::clone(&session.screen);
        let rx = session.output_tx.subscribe();
        let session_id = session.session_id;
        drop(sessions);

        screen.lock().await.screen_mut().set_size(rows, cols);
        Ok(Some((session_id, screen, rx)))
    }

    async fn creation_lock(&self, name: &str) -> Arc<Mutex<()>> {
        let mut locks = self.creation_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(name).and_then(Weak::upgrade) {
            return lock;
        }

        let lock = Arc::new(Mutex::new(()));
        locks.insert(name.to_string(), Arc::downgrade(&lock));
        lock
    }

    fn next_session_id(&self) -> u64 {
        self.next_session_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("PTY session IDs exhausted")
    }

    async fn resize(&self, name: &str, cols: u16, rows: u16) -> Result<()> {
        let sessions = self.inner.lock().await;
        let session = sessions
            .get(name)
            .with_context(|| format!("no such pty session: {name}"))?;
        set_winsize(session.master_fd.as_raw_fd(), cols, rows)?;
        session
            .screen
            .lock()
            .await
            .screen_mut()
            .set_size(rows, cols);
        Ok(())
    }

    async fn write_input(&self, name: &str, input: &[u8]) -> Result<()> {
        let sessions = self.inner.lock().await;
        let session = sessions
            .get(name)
            .with_context(|| format!("no such pty session: {name}"))?;
        // A client may receive input before consuming its Replaced event.
        // Resolve the current fd under the replacement lock instead of writing
        // to an exited child and disconnecting an otherwise healthy attachment.
        nix::unistd::write(&session.master_fd, input).context("writing to PTY child")?;
        Ok(())
    }

    /// Remove a session only while its generation still owns the name.
    async fn remove_session(&self, name: &str, session_id: u64) {
        let mut sessions = self.inner.lock().await;
        if sessions
            .get(name)
            .is_some_and(|session| session.session_id == session_id)
        {
            sessions.remove(name);
        }
    }

    /// Run use-case cleanup and terminate the named child without allowing a
    /// cold builder to publish resources between those operations.
    pub(crate) async fn terminate_with_cleanup<F>(&self, name: &str, cleanup: F) -> Result<()>
    where
        F: FnOnce() + Send,
    {
        // Serialize with cold preparation so lifecycle cleanup cannot return
        // while a frontend for the old pod is still being spawned.
        let creation_lock = self.creation_lock(name).await;
        let _creation_guard = creation_lock.lock().await;
        cleanup();
        let session = self.inner.lock().await.remove(name);
        let Some(session) = session else {
            return Ok(());
        };

        match kill(session.child_pid, Signal::SIGTERM) {
            Ok(()) => Ok(()),
            Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("terminating pty session '{name}': {e}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Session spawning
// ---------------------------------------------------------------------------

fn spawn_session(
    name: &str,
    cmd: &[String],
    workdir: Option<PathBuf>,
    env: &[String],
    cols: u16,
    rows: u16,
    sessions: &PtySessions,
) -> Result<PtySession> {
    let winsize = Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };

    if cmd.is_empty() {
        return Err(anyhow::anyhow!("empty command"));
    }

    // Safety: we only call async-signal-safe functions (chdir, execvp)
    // in the child before exec. The mutex is held in the parent, so
    // the child (which has its own address space after fork) never
    // touches it.
    let result = unsafe { forkpty(&winsize, None) }.context("forkpty")?;

    match result {
        ForkptyResult::Child => {
            // -- child process --
            // These calls do not return on success (exec replaces the process).

            if let Some(ref dir) = workdir {
                if let Err(e) = std::env::set_current_dir(dir) {
                    let dir = dir.display();
                    eprintln!("pty: chdir to {dir} failed: {e}");
                    std::process::exit(1);
                }
            }

            for entry in env {
                if let Some((key, value)) = entry.split_once('=') {
                    std::env::set_var(key, value);
                }
            }

            crate::ensure_tui_terminal_env();

            let err = std::process::Command::new(&cmd[0]).args(&cmd[1..]).exec();
            eprintln!("pty: exec failed: {err}");
            std::process::exit(1);
        }
        ForkptyResult::Parent { child, master } => {
            // Set master fd to non-blocking for async I/O
            let flags = nix::fcntl::fcntl(&master, nix::fcntl::FcntlArg::F_GETFL)
                .context("fcntl F_GETFL")?;
            let mut oflags = nix::fcntl::OFlag::from_bits_truncate(flags);
            oflags.insert(nix::fcntl::OFlag::O_NONBLOCK);
            nix::fcntl::fcntl(&master, nix::fcntl::FcntlArg::F_SETFL(oflags))
                .context("fcntl F_SETFL O_NONBLOCK")?;

            let screen = Arc::new(Mutex::new(vt100::Parser::new(rows, cols, 0)));
            let (output_tx, _) = tokio::sync::broadcast::channel(256);
            let session_id = sessions.next_session_id();

            // Spawn persistent reader: continuously drains PTY output,
            // updates the screen buffer, and broadcasts to clients.
            // Runs even when no client is attached so the child never
            // blocks on write() and the screen buffer stays current.
            let reader_fd = nix::unistd::dup(&master).context("dup master for reader")?;
            spawn_persistent_reader(
                reader_fd,
                Arc::clone(&screen),
                output_tx.clone(),
                sessions.clone(),
                name.to_string(),
                session_id,
                child,
            );

            Ok(PtySession {
                session_id,
                master_fd: master,
                child_pid: child,
                screen,
                output_tx,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Persistent PTY reader
// ---------------------------------------------------------------------------

/// Background task that continuously reads PTY output, updates the
/// screen buffer, and broadcasts to any connected WebSocket clients.
fn spawn_persistent_reader(
    fd: OwnedFd,
    screen: Arc<Mutex<vt100::Parser>>,
    tx: tokio::sync::broadcast::Sender<PtyOutput>,
    sessions: PtySessions,
    name: String,
    session_id: u64,
    child_pid: Pid,
) {
    tokio::spawn(async move {
        let async_fd = match AsyncFd::new(fd) {
            Ok(fd) => fd,
            Err(e) => {
                eprintln!("pty: persistent reader AsyncFd::new failed: {e}");
                sessions.remove_session(&name, session_id).await;
                return;
            }
        };

        let mut buf = vec![0u8; 4096];
        loop {
            let mut ready = match async_fd.readable().await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("pty: persistent reader readiness failed: {e}");
                    break;
                }
            };

            match ready.try_io(|inner| {
                nix::unistd::read(inner, &mut buf)
                    .map_err(std::io::Error::from)
                    .and_then(|n| {
                        if n == 0 {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "pty eof",
                            ))
                        } else {
                            Ok(n)
                        }
                    })
            }) {
                Ok(Ok(n)) => {
                    let data = &buf[..n];
                    screen.lock().await.process(data);
                    // Send returns Err only when there are no receivers,
                    // which is normal when no client is attached.
                    let _ = tx.send(PtyOutput::Data(data.to_vec()));
                }
                // EOF (read returned 0) or EIO (PTY slave closed on
                // Linux) -- the session is over.
                Ok(Err(_)) => break,
                // WouldBlock -- AsyncFd will re-poll
                Err(_) => continue,
            }
        }
        sessions.remove_session(&name, session_id).await;
        tokio::task::spawn_blocking(move || match waitpid(child_pid, None) {
            Ok(_) | Err(nix::errno::Errno::ECHILD) => {}
            Err(error) => eprintln!("pty: reaping child {child_pid}: {error}"),
        });
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn child_is_alive(pid: Pid) -> bool {
    match waitpid(pid, Some(WaitPidFlag::WNOHANG)) {
        Ok(WaitStatus::StillAlive) => true,
        // Any terminal status means the child is gone
        Ok(_) => false,
        // ECHILD means the child was already reaped
        Err(nix::errno::Errno::ECHILD) => false,
        // Other errors are unexpected; treat as dead to avoid leaking
        Err(_) => false,
    }
}

/// Build the escape sequence stream that reconstructs the full terminal
/// state for a new client: alternate screen mode, screen contents with
/// formatting, input modes, cursor position, and cursor visibility.
fn build_screen_replay(screen: &vt100::Screen) -> Vec<u8> {
    let mut out = Vec::new();

    // Switch to alternate screen first so the content lands in the
    // right buffer (TUI apps like ink use alternate screen).
    if screen.alternate_screen() {
        out.extend_from_slice(b"\x1b[?1049h");
    }

    // state_formatted() = contents (clear + cells with colors) + input
    // modes (application keypad/cursor, bracketed paste, mouse protocol).
    out.extend_from_slice(&screen.state_formatted());

    // cursor_state_formatted() = cursor visibility + cursor position.
    out.extend_from_slice(&screen.cursor_state_formatted());

    out
}

fn set_winsize(fd: RawFd, cols: u16, rows: u16) -> Result<()> {
    let ws = Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCSWINSZ is a well-defined ioctl for setting terminal
    // window size, and ws is a valid Winsize struct on the stack.
    let ret = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &ws as *const Winsize) };
    if ret < 0 {
        return Err(std::io::Error::last_os_error()).context("ioctl TIOCSWINSZ");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire protocol
// ---------------------------------------------------------------------------

/// Text messages carry JSON control messages. Binary messages carry
/// raw PTY data (no prefix, no framing beyond WebSocket itself).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PtyControl {
    /// Client-owned session: client tells the server everything about
    /// the child process to spawn.  Used by pod-side claude, where the
    /// in-container handler runs whatever cmd the host sent (subject
    /// to a server-side cmd transform that resolves the binary path).
    Session {
        name: String,
        cmd: Vec<String>,
        workdir: Option<PathBuf>,
        env: Vec<String>,
        cols: u16,
        rows: u16,
        /// When false, the server will not spawn a new session if the
        /// named one does not exist (used for reconnection).
        create: bool,
    },
    /// Server-owned session: the server already knows what to spawn
    /// (name, cmd, workdir, env), and the client only contributes
    /// terminal dimensions, the create flag, and any user-supplied
    /// extra args that should be appended to the server-controlled
    /// cmd.  Used by daemon-side codex, where the daemon owns the
    /// codex binary path and the proxy URL.
    Attach {
        cols: u16,
        rows: u16,
        create: bool,
        extra_args: Vec<String>,
        client_context: ClientContext,
    },
    /// Terminal resize (client -> server).
    Resize { cols: u16, rows: u16 },
    /// Sent by the server when the PTY child process has exited.
    SessionEnded,
    /// The session is absent and its server-owned spawn parameters cannot be
    /// constructed from current cached state.
    SessionUnavailable,
    /// The server failed while preparing or spawning the session.
    SessionError { error: String },
}

// ---------------------------------------------------------------------------
// Generic WebSocket <-> PTY bridge
// ---------------------------------------------------------------------------

/// Optional callback to rewrite the cmd before spawning the child.
/// Used by the pod side to substitute the in-container claude binary
/// path; the daemon side does not need this and passes `None`.
pub type CmdTransform = Box<dyn FnOnce(&mut Vec<String>) -> Result<()> + Send>;

/// Server-controlled session shape (name + cmd + workdir + env).
/// Used by `serve_ws_session_with_params` callers (currently the
/// daemon's codex handler) to fully own the child process spec.
pub struct SessionSpec {
    pub name: String,
    pub cmd: Vec<String>,
    pub workdir: Option<PathBuf>,
    pub env: Vec<String>,
    pub(crate) on_spawn: Option<Box<dyn FnOnce(SessionWatch) + Send>>,
}

/// Client-owned entry point: read `PtyControl::Session` from the wire,
/// optionally rewrite the cmd via `cmd_transform`, then drive the
/// bridge.  Used by the pod's `/claude` route.
pub async fn serve_ws_session(
    mut socket: WebSocket,
    sessions: PtySessions,
    cmd_transform: Option<CmdTransform>,
) {
    let (name, mut cmd, workdir, env, cols, rows, create) = match socket.recv().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<PtyControl>(&text) {
            Ok(PtyControl::Session {
                name,
                cmd,
                workdir,
                env,
                cols,
                rows,
                create,
            }) => (name, cmd, workdir, env, cols, rows, create),
            Ok(other) => {
                eprintln!("pty: expected session message, got {other:?}");
                return;
            }
            Err(e) => {
                eprintln!("pty: invalid session request: {e}");
                return;
            }
        },
        Some(Ok(_)) => {
            eprintln!("pty: expected text message with session params, got binary");
            return;
        }
        Some(Err(e)) => {
            eprintln!("pty: WebSocket error reading session request: {e}");
            return;
        }
        None => return,
    };

    // Preserve the client-owned protocol's existing transform timing. Codex
    // uses the server-owned path below for lazy preparation.
    if create {
        if let Some(transform) = cmd_transform {
            if let Err(error) = transform(&mut cmd) {
                eprintln!("pty: cmd transform failed: {error:#}");
                return;
            }
        }
    }

    let requested_name = name.clone();
    run_bridge(socket, sessions, name, cols, rows, create, move || {
        Ok(Some(SessionSpec {
            name: requested_name,
            cmd,
            workdir,
            env,
            on_spawn: None,
        }))
    })
    .await
}

/// Server-owned entry point: read `PtyControl::Attach` from the wire,
/// then construct spawn parameters only if the requested session is absent.
/// Used by the daemon's `/pod/codex/{name}` route.
pub async fn serve_ws_session_with_params<F, C>(
    mut socket: WebSocket,
    sessions: PtySessions,
    session_name: String,
    remember_client_context: C,
    build_spec: F,
) where
    F: FnOnce(Vec<String>) -> Result<Option<SessionSpec>> + Send,
    C: FnOnce(&ClientContext) + Send,
{
    let (cols, rows, create, extra_args, client_context) = match socket.recv().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<PtyControl>(&text) {
            Ok(PtyControl::Attach {
                cols,
                rows,
                create,
                extra_args,
                client_context,
            }) => (cols, rows, create, extra_args, client_context),
            Ok(other) => {
                eprintln!("pty: expected attach message, got {other:?}");
                return;
            }
            Err(e) => {
                eprintln!("pty: invalid attach request: {e}");
                return;
            }
        },
        Some(Ok(_)) => {
            eprintln!("pty: expected text message with attach params, got binary");
            return;
        }
        Some(Err(e)) => {
            eprintln!("pty: WebSocket error reading attach request: {e}");
            return;
        }
        None => return,
    };

    remember_client_context(&client_context);

    run_bridge(
        socket,
        sessions,
        session_name,
        cols,
        rows,
        create,
        move || build_spec(extra_args),
    )
    .await
}

/// Spawn or attach the screen session, replay its current screen state
/// to the new client, then bridge stdin/stdout until the client
/// disconnects or the session ends.
async fn run_bridge<F>(
    mut socket: WebSocket,
    sessions: PtySessions,
    name: String,
    mut cols: u16,
    mut rows: u16,
    create: bool,
    build_spec: F,
) where
    F: FnOnce() -> Result<Option<SessionSpec>> + Send,
{
    let (mut session_id, screen, mut output_rx) = match sessions
        .spawn_or_attach(name.clone(), cols, rows, create, build_spec)
        .await
    {
        Ok(SessionAcquire::Attached(tuple)) => tuple,
        Ok(SessionAcquire::Missing) => {
            // Session does not exist and create=false (reconnection
            // to a session that already exited).
            let msg = serde_json::to_string(&PtyControl::SessionEnded)
                .expect("SessionEnded is always serializable");
            let _ = socket.send(Message::Text(msg.into())).await;
            return;
        }
        Ok(SessionAcquire::Unavailable) => {
            let msg = serde_json::to_string(&PtyControl::SessionUnavailable)
                .expect("SessionUnavailable is always serializable");
            let _ = socket.send(Message::Text(msg.into())).await;
            return;
        }
        Err(e) => {
            eprintln!("pty session failed for '{name}': {e:#}");
            let msg = serde_json::to_string(&PtyControl::SessionError {
                error: format!("{e:#}"),
            })
            .expect("SessionError is always serializable");
            let _ = socket.send(Message::Text(msg.into())).await;
            return;
        }
    };

    // Replay the current screen state so the client sees the full TUI
    // immediately, like screen/tmux do on reattach.
    {
        let parser = screen.lock().await;
        let replay = build_screen_replay(parser.screen());
        if !replay.is_empty() && socket.send(Message::Binary(replay.into())).await.is_err() {
            return;
        }
    }

    // Main loop: multiplex between broadcast output and WebSocket input.
    loop {
        tokio::select! {
            // Persistent reader produced output -> forward as binary
            output = output_rx.recv() => {
                match output {
                    Ok(PtyOutput::Data(data)) => {
                        if socket.send(Message::Binary(data.into())).await.is_err() {
                            break;
                        }
                    }
                    // Lagged: we missed some output, not fatal for a terminal
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Ok(PtyOutput::Replaced)
                    | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // A lagging client can miss Replaced, so also check for
                        // a successor when the old output channel closes.
                        match sessions.attach_existing(&name, cols, rows).await {
                            Ok(Some((new_id, new_screen, new_output))) if new_id != session_id => {
                                session_id = new_id;
                                output_rx = new_output;
                                let parser = new_screen.lock().await;
                                let mut replay = b"\x1b[?1049l\x1b[0m\x1b[2J\x1b[H".to_vec();
                                replay.extend(build_screen_replay(parser.screen()));
                                if socket.send(Message::Binary(replay.into())).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            Ok(Some(_)) | Ok(None) => {}
                            Err(error) => {
                                eprintln!("pty: attaching replacement for '{name}': {error:#}");
                            }
                        }
                        let msg = serde_json::to_string(&PtyControl::SessionEnded)
                            .expect("SessionEnded is always serializable");
                        let _ = socket.send(Message::Text(msg.into())).await;
                        break;
                    }
                }
            }

            // WebSocket received a message -> handle it
            ws_msg = socket.recv() => {
                match ws_msg {
                    // Binary = raw PTY input from client
                    Some(Ok(Message::Binary(data))) => {
                        if !data.is_empty() {
                            if let Err(error) = sessions.write_input(&name, &data).await {
                                eprintln!("pty: input for '{name}': {error:#}");
                                break;
                            }
                        }
                    }
                    // Text = JSON control message
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<PtyControl>(&text) {
                            Ok(PtyControl::Resize { cols: new_cols, rows: new_rows }) => {
                                cols = new_cols;
                                rows = new_rows;
                                let _ = sessions.resize(&name, cols, rows).await;
                            }
                            Ok(PtyControl::Session { .. }) | Ok(PtyControl::Attach { .. }) => {
                                // Handshake messages after handshake are nonsensical
                            }
                            Ok(PtyControl::SessionEnded)
                            | Ok(PtyControl::SessionUnavailable)
                            | Ok(PtyControl::SessionError { .. }) => {
                                // Server-to-client only, ignore if received
                            }
                            Err(e) => {
                                eprintln!("pty: invalid control message: {e}");
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => continue,
                    Some(Err(_)) => break,
                    None => break,
                }
            }
        }
    }

    // The session's master and persistent reader keep the PTY alive for
    // reattachment after this client's output subscription is dropped.
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    fn test_session(session_id: u64) -> PtySession {
        let master_fd = File::open("/dev/null").expect("open /dev/null").into();
        let (output_tx, _) = tokio::sync::broadcast::channel(1);
        PtySession {
            session_id,
            master_fd,
            child_pid: Pid::from_raw(1),
            screen: Arc::new(Mutex::new(vt100::Parser::new(1, 1, 0))),
            output_tx,
        }
    }

    #[tokio::test]
    async fn stale_reader_does_not_remove_replacement_session() {
        let sessions = PtySessions::new();
        sessions
            .inner
            .lock()
            .await
            .insert("same-name".to_string(), test_session(2));

        sessions.remove_session("same-name", 1).await;

        let registered_id = sessions
            .inner
            .lock()
            .await
            .get("same-name")
            .map(|session| session.session_id);
        assert_eq!(registered_id, Some(2));
    }

    #[tokio::test]
    async fn lifecycle_cleanup_holds_session_creation_lock() {
        let sessions = PtySessions::new();
        let creation_lock = sessions.creation_lock("locked-cleanup").await;

        sessions
            .terminate_with_cleanup("locked-cleanup", || {
                assert!(creation_lock.try_lock().is_err());
            })
            .await
            .unwrap();
    }
}
