// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Client-side terminal bridge for attaching to a remote PTY session
//! over WebSocket.
//!
//! Replaces `docker exec -it ... screen ...` with a direct WebSocket
//! connection to the in-container PTY server.  Automatically reconnects
//! when the connection is lost (e.g. after laptop suspend).

use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::signal::unix::{signal, SignalKind};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use url::Url;

use crate::daemon::protocol::{ClientContext, DaemonClient};
use crate::daemon::reconnect::ReconnectEvent;
use crate::pty_session::PtyControl;

/// Trait alias so TCP and Unix streams can flow through the same
/// `WebSocketStream<Box<dyn _>>` type.
trait WsTransport: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> WsTransport for T {}

type WsStream = WebSocketStream<Box<dyn WsTransport>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

/// How often the write loop sends a WebSocket ping.  The connection
/// goes through docker exec or kubectl port-forward tunnels, so TCP
/// keepalive on the loopback socket is useless.  Periodic pings force
/// data through the tunnel and cause it to detect a dead underlying
/// connection (e.g. after laptop suspend).
const PING_INTERVAL: Duration = Duration::from_secs(30);

/// Ctrl-a (0x01) is the first byte of the detach sequence (same as
/// GNU screen).
const DETACH_PREFIX: u8 = 0x01;
/// 'd' (0x64) or Ctrl-d (0x04) completes the detach sequence after
/// Ctrl-a, so it works whether the user releases Ctrl or not.
const DETACH_SUFFIX: u8 = b'd';
const DETACH_SUFFIX_CTRL: u8 = 0x04;

pub enum AttachOutcome {
    /// User pressed the detach sequence; session still running.
    Detached,
    /// The remote session ended (child exited).
    SessionEnded,
}

/// The terminal bridge has not started, so a caller using cached
/// connection details can prepare the pod and retry.
#[derive(Debug)]
pub struct InitialConnectionFailed(anyhow::Error);

impl std::fmt::Display for InitialConnectionFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "initial PTY connection failed: {:#}", self.0)
    }
}

impl std::error::Error for InitialConnectionFailed {}

/// The daemon has no live local session and cannot construct one from its
/// current cached pod connection. Callers may perform targeted preparation
/// and retry the same endpoint.
#[derive(Debug)]
pub struct SessionUnavailable;

impl std::fmt::Display for SessionUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PTY session is unavailable from cached state")
    }
}

impl std::error::Error for SessionUnavailable {}

/// RAII guard that restores the original terminal settings on drop,
/// ensuring cleanup even on panic or early return.
struct TerminalGuard {
    original: nix::sys::termios::Termios,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if let Err(e) = nix::sys::termios::tcsetattr(
            io::stdin(),
            nix::sys::termios::SetArg::TCSANOW,
            &self.original,
        ) {
            eprintln!("warning: failed to restore terminal: {e}");
        }
    }
}

fn apply_termios(termios: &nix::sys::termios::Termios) {
    if let Err(e) =
        nix::sys::termios::tcsetattr(io::stdin(), nix::sys::termios::SetArg::TCSANOW, termios)
    {
        eprintln!("warning: failed to set terminal attributes: {e}");
    }
}

/// Strip kernel echo but leave SIGINT delivery, line mode, and
/// output processing untouched.  Used between rumpel start and
/// Claude's first render: any bytes typed (or emitted by the
/// terminal itself, like focus-change or mouse reports from a
/// lingering other session) reach the tty while stdin forwarding
/// is still gated off, and without this they would be echoed to
/// the screen as "^[[O" / "^[j" garbage.  ISIG stays on so Ctrl-C
/// during this window still kills the client.
fn make_no_echo(termios: &mut nix::sys::termios::Termios) {
    termios
        .local_flags
        .remove(nix::sys::termios::LocalFlags::ECHO | nix::sys::termios::LocalFlags::ECHONL);
}

/// Install a SIGINT/SIGTERM handler that restores `original`
/// termios and exits.  `TerminalGuard::drop` would do the same,
/// but the default signal disposition terminates without
/// unwinding -- leaving the tty in no-echo mode for whatever runs
/// next if the user Ctrl-C's during the pre-render window.
fn install_sigint_restore_handler(original: nix::sys::termios::Termios) -> Result<()> {
    static INSTALL_RESULT: OnceLock<Result<(), String>> = OnceLock::new();
    match INSTALL_RESULT.get_or_init(move || {
        ctrlc::set_handler(move || {
            let _ = nix::sys::termios::tcsetattr(
                io::stdin(),
                nix::sys::termios::SetArg::TCSANOW,
                &original,
            );
            std::process::exit(130);
        })
        .map_err(|error| error.to_string())
    }) {
        Ok(()) => Ok(()),
        Err(error) => Err(anyhow::anyhow!("installing SIGINT handler: {error}")),
    }
}

nix::ioctl_read_bad!(tiocgwinsz, libc::TIOCGWINSZ, libc::winsize);

/// Get the current terminal dimensions via ioctl.
pub fn get_terminal_size() -> Result<(u16, u16)> {
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    unsafe {
        tiocgwinsz(io::stdout().as_raw_fd(), &mut ws).context("TIOCGWINSZ ioctl failed")?;
    }
    Ok((ws.ws_col, ws.ws_row))
}

/// Client-owned session parameters sent as the first WebSocket
/// message in `WireParams::Session` mode.  The server spawns the
/// session if it does not exist, otherwise reuses it and replays the
/// screen state.
pub struct SessionParams {
    pub name: String,
    pub cmd: Vec<String>,
    pub workdir: Option<String>,
    pub env: Vec<String>,
}

/// Underlying transport for the WebSocket: TCP through a tunnel/proxy
/// (used by claude against the in-pod server) or a Unix domain socket
/// (used by codex against the local daemon).
pub enum PtyTransport {
    /// HTTP(S) URL.  We currently only use plain `http://` against
    /// loopback; TLS is not wired up.
    Tcp { url: String },
    /// Path to a Unix domain socket (the daemon's main RPC socket).
    Unix { socket: PathBuf },
}

/// First-message variant the client sends to the server.
///
/// The two variants correspond to the two server-side handlers in
/// `pty_session`: `serve_ws_session` (client owns the session shape)
/// vs `serve_ws_session_with_params` (server owns it).
pub enum WireParams {
    /// Send `PtyControl::Session` with full params -- claude-style.
    Session(SessionParams),
    /// Send `PtyControl::Attach` with only dimensions and any user
    /// extra args -- codex-style.  The daemon's handler already knows
    /// the binary path and proxy URL.
    Attach {
        extra_args: Vec<String>,
        client_context: ClientContext,
    },
}

/// How to coordinate reconnection with the daemon.
///
/// When provided, the attach loop waits for the daemon's SSE endpoint
/// instead of polling blindly.  The daemon manages remote host and
/// pod event endpoint reconnection internally.
pub struct ReconnectConfig {
    pub daemon_socket: PathBuf,
    pub repo_path: PathBuf,
    pub pod_name: String,
}

/// Connect to or launch a screen-style PTY session over WebSocket.
///
/// `transport` is either a TCP URL (pod-side claude through the
/// container exec proxy) or a Unix socket path (daemon-side codex).
/// `path` is the WebSocket route on the server.  `token` is the bearer
/// token sent in the Authorization header (loopback Unix sockets do
/// not validate it; an empty string is fine there).
///
/// Sends the params as the first message and bridges local
/// stdin/stdout until the user detaches (Ctrl-a d) or the session
/// ends.  Raw mode is deferred until the first output arrives from
/// the remote so ctrl-c works during startup.  Automatically
/// reconnects if the WebSocket connection drops.
pub fn attach(
    transport: PtyTransport,
    path: &str,
    token: &str,
    params: WireParams,
    reconnect: Option<ReconnectConfig>,
) -> Result<AttachOutcome> {
    eprintln!("[Ctrl-a d to detach]");

    // -- Terminal setup -------------------------------------------------

    let original_termios =
        nix::sys::termios::tcgetattr(io::stdin()).context("reading terminal attributes")?;

    // Install before touching termios: once we switch to no-echo, a
    // Ctrl-C that goes through the default signal disposition would
    // terminate the process without unwinding, leaving the tty
    // without echo for whatever runs next.
    install_sigint_restore_handler(original_termios.clone())?;

    let _guard = TerminalGuard {
        original: original_termios.clone(),
    };

    let mut no_echo = original_termios.clone();
    make_no_echo(&mut no_echo);

    let mut raw = original_termios.clone();
    nix::sys::termios::cfmakeraw(&mut raw);

    apply_termios(&no_echo);

    // -- Run async bridge -----------------------------------------------

    let path = path.to_string();
    let token = token.to_string();
    let outcome = crate::async_runtime::block_on(attach_async(
        &transport, &path, &token, params, &no_echo, &raw, reconnect,
    ))?;

    // Reset terminal emulator state that escape sequences from the remote
    // may have changed (termios is restored by TerminalGuard, but modes
    // like alternate screen or hidden cursor need explicit cleanup).
    // Scroll the TUI content into scrollback so the shell prompt appears
    // on a clean visible area while the output remains accessible.
    let (_, rows) = get_terminal_size().unwrap_or((80, 24));
    io::stdout()
        .write_all(
            format!(
                "\x1b[?1049l\
                 \x1b[?1006l\
                 \x1b[?1003l\
                 \x1b[?1002l\
                 \x1b[?1000l\
                 \x1b[?1004l\
                 \x1b[?2004l\
                 \x1b[0m\
                 \x1b[?25h\
                 \x1b[r\
                 \x1b[{rows}S\
                 \x1b[H"
            )
            .as_bytes(),
        )
        .and_then(|()| io::stdout().flush())
        .context("resetting terminal emulator state")?;

    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Internal bridge outcome (distinguishes connection loss from session end)
// ---------------------------------------------------------------------------

enum BridgeOutcome {
    Detached,
    SessionEnded,
    SessionUnavailable,
    SessionError(String),
    ConnectionLost,
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Build the WebSocket handshake request.  The URL host part is
/// cosmetic for the loopback transports we use (server ignores Host),
/// so we just put `localhost` for Unix.
fn build_ws_request(
    transport: &PtyTransport,
    path: &str,
    token: &str,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let (uri, host) = match transport {
        PtyTransport::Tcp { url } => {
            let mut ws_url = Url::parse(url).context("parsing container URL")?;
            let scheme = match ws_url.scheme() {
                "http" => "ws",
                "https" => "wss",
                other => return Err(anyhow::anyhow!("unexpected scheme: {other}")),
            };
            ws_url
                .set_scheme(scheme)
                .expect("ws/wss are always valid schemes");
            ws_url.set_path(path);

            let host = match ws_url.port() {
                Some(port) => format!("{}:{port}", ws_url.host_str().unwrap_or("localhost")),
                None => ws_url.host_str().unwrap_or("localhost").to_string(),
            };
            (ws_url.into(), host)
        }
        PtyTransport::Unix { .. } => (format!("ws://localhost{path}"), "localhost".to_string()),
    };

    tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(uri)
        .header("Host", &host)
        .header("Authorization", format!("Bearer {token}"))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .body(())
        .context("building WebSocket request")
}

/// Open the underlying byte stream.  Returns a boxed trait object so
/// the WS code path is uniform across TCP and Unix transports.
async fn open_transport(transport: &PtyTransport) -> Result<Box<dyn WsTransport>> {
    match transport {
        PtyTransport::Tcp { url } => {
            let url = Url::parse(url).context("parsing TCP URL")?;
            let host = url
                .host_str()
                .context("missing host in TCP URL")?
                .to_string();
            let port = url
                .port_or_known_default()
                .context("missing port in TCP URL")?;
            let stream = TcpStream::connect((host.as_str(), port))
                .await
                .with_context(|| format!("connecting to {host}:{port}"))?;
            Ok(Box::new(stream))
        }
        PtyTransport::Unix { socket } => {
            let stream = UnixStream::connect(socket)
                .await
                .with_context(|| format!("connecting to {}", socket.display()))?;
            Ok(Box::new(stream))
        }
    }
}

/// Connect a WebSocket and send the first control message based on
/// `params`.  Used both for the initial connect and for reconnection
/// (with `create=false`).
async fn connect_ws(
    transport: &PtyTransport,
    path: &str,
    token: &str,
    params: &WireParams,
    create: bool,
) -> Result<WsStream> {
    // A cached proxy can still be listening after its remote transport
    // fails. Bound the handshake so callers can reach pod recovery.
    let (mut ws, _response) = tokio::time::timeout(Duration::from_secs(10), async {
        let stream = open_transport(transport).await?;
        let request = build_ws_request(transport, path, token)?;
        tokio_tungstenite::client_async(request, stream)
            .await
            .context("WebSocket handshake failed")
    })
    .await
    .context("timed out connecting to PTY session")??;

    let (cols, rows) = get_terminal_size().unwrap_or((80, 24));
    let first_msg = match params {
        WireParams::Session(p) => PtyControl::Session {
            name: p.name.clone(),
            cmd: p.cmd.clone(),
            workdir: p.workdir.clone().map(Into::into),
            env: p.env.clone(),
            cols,
            rows,
            create,
        },
        WireParams::Attach {
            extra_args,
            client_context,
        } => PtyControl::Attach {
            cols,
            rows,
            create,
            extra_args: extra_args.clone(),
            client_context: client_context.clone(),
        },
    };
    let json = serde_json::to_string(&first_msg).context("serializing session params")?;
    ws.send(Message::Text(json.into()))
        .await
        .context("sending session parameters")?;

    Ok(ws)
}

// ---------------------------------------------------------------------------
// Async entry point with reconnection
// ---------------------------------------------------------------------------

async fn attach_async(
    transport: &PtyTransport,
    path: &str,
    token: &str,
    params: WireParams,
    no_echo_termios: &nix::sys::termios::Termios,
    raw_termios: &nix::sys::termios::Termios,
    reconnect: Option<ReconnectConfig>,
) -> Result<AttachOutcome> {
    // First connection: propagate errors (invalid URL, unreachable pod).
    let ws = connect_ws(transport, path, token, &params, true)
        .await
        .map_err(InitialConnectionFailed)?;

    // Defer raw mode until the remote sends its first output, so ctrl-c
    // works while waiting for the session to start.
    match bridge(ws, Some(raw_termios)).await {
        BridgeOutcome::Detached => return Ok(AttachOutcome::Detached),
        BridgeOutcome::SessionEnded => return Ok(AttachOutcome::SessionEnded),
        BridgeOutcome::SessionUnavailable => {
            return Err(anyhow::Error::new(SessionUnavailable));
        }
        BridgeOutcome::SessionError(error) => {
            return Err(anyhow::anyhow!(error));
        }
        BridgeOutcome::ConnectionLost => {}
    }

    // Reconnection loop.  Uses create=false so the server does not
    // spawn a fresh session if the original child already exited.
    loop {
        // Leave raw mode while disconnected so ctrl-c delivers SIGINT
        // and the user can kill the process normally.  Stay in no-echo
        // so keystrokes and terminal-generated sequences aren't echoed
        // to the screen while we print reconnection status.
        apply_termios(no_echo_termios);
        eprintln!("[connection lost]");

        if let Some(ref rc) = reconnect {
            // Let the daemon coordinate remote host and pod
            // reconnection with exponential backoff.
            match wait_for_pod_reconnect(rc).await {
                Ok(ReconnectOutcome::Connected) => {}
                Ok(ReconnectOutcome::Stopped) => {
                    eprintln!("[pod stopped]");
                    return Ok(AttachOutcome::SessionEnded);
                }
                Err(e) => {
                    eprintln!("[daemon reconnect failed: {e:#}, retrying directly]");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        } else {
            eprintln!("[reconnecting...]");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let ws = match connect_ws(transport, path, token, &params, false).await {
            Ok(ws) => ws,
            Err(_) => continue,
        };

        // Reconnection replays screen state immediately, so enter raw
        // mode before the bridge starts.
        apply_termios(raw_termios);
        write_status("[reconnected]\r\n").await;

        match bridge(ws, None).await {
            BridgeOutcome::Detached => return Ok(AttachOutcome::Detached),
            BridgeOutcome::SessionEnded => return Ok(AttachOutcome::SessionEnded),
            BridgeOutcome::SessionUnavailable => {
                return Err(anyhow::Error::new(SessionUnavailable));
            }
            BridgeOutcome::SessionError(error) => {
                return Err(anyhow::anyhow!(error));
            }
            BridgeOutcome::ConnectionLost => continue,
        }
    }
}

enum ReconnectOutcome {
    Connected,
    Stopped,
}

/// Wait for the daemon to re-establish the connection to a pod.
///
/// Subscribes to the daemon's SSE endpoint and prints status messages
/// as the daemon retries.  Returns `Connected` when the pod is
/// reachable, or `Stopped` if the pod was intentionally stopped.
async fn wait_for_pod_reconnect(config: &ReconnectConfig) -> Result<ReconnectOutcome> {
    let daemon_socket = config.daemon_socket.clone();
    let repo_path = config.repo_path.clone();
    let pod_name = config.pod_name.clone();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ReconnectEvent>(16);

    tokio::task::spawn_blocking(move || -> Result<()> {
        let client = DaemonClient::new_unix(&daemon_socket);
        for event in client.pod_reconnect_events(&repo_path, &pod_name)? {
            let event = event?;
            let is_connected = matches!(event, ReconnectEvent::Connected);
            if tx.blocking_send(event).is_err() {
                break;
            }
            if is_connected {
                break;
            }
        }
        Ok(())
    });

    while let Some(event) = rx.recv().await {
        match event {
            ReconnectEvent::Attempting => {
                eprintln!("[reconnecting to host...]");
            }
            ReconnectEvent::HostConnected => {
                eprintln!("[host connected, reconnecting to pod...]");
            }
            ReconnectEvent::Connected => {
                return Ok(ReconnectOutcome::Connected);
            }
            ReconnectEvent::Failed { error } => {
                eprintln!("[reconnect failed: {error}, retrying...]");
            }
            ReconnectEvent::Stopped => {
                return Ok(ReconnectOutcome::Stopped);
            }
        }
    }

    Err(anyhow::anyhow!(
        "reconnect stream ended without connected event"
    ))
}

/// Write a status message to stdout.  Uses \r\n because the terminal
/// is in raw mode.
async fn write_status(msg: &str) {
    let mut stdout = tokio::io::stdout();
    let _ = stdout.write_all(msg.as_bytes()).await;
    let _ = stdout.flush().await;
}

// ---------------------------------------------------------------------------
// WebSocket <-> terminal bridge
// ---------------------------------------------------------------------------

/// When `raw_termios` is `Some`, the terminal is still in cooked mode
/// and raw mode is deferred until the first output arrives from the
/// remote.  When `None`, the caller already set raw mode.
async fn bridge(ws: WsStream, raw_termios: Option<&nix::sys::termios::Termios>) -> BridgeOutcome {
    let (ws_write, ws_read) = ws.split();

    // Shared flag: set by read_loop on every received message, cleared
    // by write_loop after each ping.  If write_loop sees the flag
    // still clear after READ_TIMEOUT, the connection is dead.
    let received = Arc::new(AtomicBool::new(true));

    // Whether raw mode is active.  write_loop gates stdin forwarding
    // on this so keystrokes aren't forwarded in cooked mode.
    let raw_active = Arc::new(AtomicBool::new(raw_termios.is_none()));
    // Wakes write_loop when read_loop activates raw mode, so the
    // select! re-evaluates the stdin branch condition immediately
    // instead of waiting for the next ping or signal.
    let raw_notify = Arc::new(tokio::sync::Notify::new());

    tokio::select! {
        result = write_loop(ws_write, Arc::clone(&received), Arc::clone(&raw_active), Arc::clone(&raw_notify)) => {
            match result {
                Ok(BridgeOutcome::Detached) => BridgeOutcome::Detached,
                Ok(BridgeOutcome::SessionEnded) => BridgeOutcome::SessionEnded,
                Ok(BridgeOutcome::SessionUnavailable) => BridgeOutcome::SessionUnavailable,
                Ok(BridgeOutcome::SessionError(error)) => BridgeOutcome::SessionError(error),
                // WebSocket send error = connection lost
                Ok(BridgeOutcome::ConnectionLost) | Err(_) => BridgeOutcome::ConnectionLost,
            }
        }
        outcome = read_loop(ws_read, Arc::clone(&received), raw_termios, raw_active, raw_notify) => outcome,
    }
}

/// Forward stdin, window-resize signals, and periodic keepalive pings
/// to the WebSocket.
async fn write_loop(
    mut ws_write: WsWriter,
    received: Arc<AtomicBool>,
    raw_active: Arc<AtomicBool>,
    raw_notify: Arc<tokio::sync::Notify>,
) -> Result<BridgeOutcome> {
    let mut stdin = tokio::io::stdin();
    let mut stdin_buf = [0u8; 4096];
    let mut saw_ctrl_a = false;
    let mut sigwinch =
        signal(SignalKind::window_change()).context("registering SIGWINCH handler")?;
    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    // First tick fires immediately; skip it.
    ping_interval.tick().await;

    loop {
        let is_raw = raw_active.load(Ordering::Relaxed);
        tokio::select! {
            // Only forward stdin once raw mode is active, otherwise
            // the cooked-mode line discipline would echo AND we'd
            // forward -- and ctrl-c should stay a signal, not data.
            n = stdin.read(&mut stdin_buf), if is_raw => {
                let n = match n {
                    Ok(0) | Err(_) => return Ok(BridgeOutcome::SessionEnded),
                    Ok(n) => n,
                };

                match process_stdin(&stdin_buf[..n], &mut saw_ctrl_a) {
                    StdinAction::Detach => return Ok(BridgeOutcome::Detached),
                    StdinAction::Send(data) => {
                        ws_write.send(Message::Binary(data.into())).await
                            .context("sending to WebSocket")?;
                    }
                    StdinAction::Nothing => {}
                }
            }
            _ = sigwinch.recv() => {
                if let Ok((cols, rows)) = get_terminal_size() {
                    let msg = PtyControl::Resize { cols, rows };
                    let json = serde_json::to_string(&msg)
                        .expect("Resize is always serializable");
                    ws_write
                        .send(Message::Text(json.into()))
                        .await
                        .context("sending resize")?;
                }
            }
            _ = ping_interval.tick() => {
                // If nothing was received since the last ping,
                // the tunnel is likely dead.
                if !received.swap(false, Ordering::Relaxed) {
                    return Ok(BridgeOutcome::ConnectionLost);
                }
                ws_write.send(Message::Ping(vec![].into())).await
                    .context("sending keepalive ping")?;
            }
            // Restart the loop so the stdin branch condition is
            // re-evaluated now that raw mode is active.
            _ = raw_notify.notified(), if !is_raw => {}
        }
    }
}

/// Forward WebSocket output to stdout.  Returns `SessionEnded` if the
/// server sends a `SessionEnded` control message, `ConnectionLost` if
/// the WebSocket closes or errors without one.
async fn read_loop(
    mut ws_read: WsReader,
    received: Arc<AtomicBool>,
    raw_termios: Option<&nix::sys::termios::Termios>,
    raw_active: Arc<AtomicBool>,
    raw_notify: Arc<tokio::sync::Notify>,
) -> BridgeOutcome {
    let mut stdout = tokio::io::stdout();
    let mut entered_raw = raw_termios.is_none();
    while let Some(Ok(msg)) = ws_read.next().await {
        received.store(true, Ordering::Relaxed);
        match msg {
            Message::Binary(data) => {
                if !entered_raw {
                    if let Some(termios) = raw_termios {
                        // TCSAFLUSH drops anything the kernel has
                        // buffered in the tty input queue during the
                        // no-echo window, so stray keystrokes the user
                        // typed before Claude rendered aren't
                        // forwarded as input.
                        if let Err(e) = nix::sys::termios::tcsetattr(
                            io::stdin(),
                            nix::sys::termios::SetArg::TCSAFLUSH,
                            termios,
                        ) {
                            eprintln!("warning: failed to enter raw mode: {e}");
                        }
                    }
                    entered_raw = true;
                    raw_active.store(true, Ordering::Relaxed);
                    raw_notify.notify_one();
                }
                if stdout.write_all(&data).await.is_err() || stdout.flush().await.is_err() {
                    break;
                }
            }
            Message::Text(text) => match serde_json::from_str(&text) {
                Ok(PtyControl::SessionEnded) => return BridgeOutcome::SessionEnded,
                Ok(PtyControl::SessionUnavailable) => return BridgeOutcome::SessionUnavailable,
                Ok(PtyControl::SessionError { error }) => {
                    return BridgeOutcome::SessionError(error);
                }
                Ok(PtyControl::Session { .. })
                | Ok(PtyControl::Attach { .. })
                | Ok(PtyControl::Resize { .. })
                | Err(_) => {}
            },
            Message::Close(_) => break,
            // tungstenite handles Ping internally (auto-pongs before
            // returning a message to the caller).
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    // WebSocket closed or errored without a SessionEnded message.
    BridgeOutcome::ConnectionLost
}

// ---------------------------------------------------------------------------
// Stdin processing
// ---------------------------------------------------------------------------

enum StdinAction {
    Detach,
    Send(Vec<u8>),
    Nothing,
}

fn process_stdin(data: &[u8], saw_ctrl_a: &mut bool) -> StdinAction {
    let mut to_send = Vec::with_capacity(data.len() + 1);
    for &byte in data {
        if *saw_ctrl_a {
            *saw_ctrl_a = false;
            if byte == DETACH_SUFFIX || byte == DETACH_SUFFIX_CTRL {
                return StdinAction::Detach;
            } else if byte == DETACH_PREFIX {
                // Ctrl-a Ctrl-a -> send one literal Ctrl-a, stay alert
                to_send.push(DETACH_PREFIX);
                *saw_ctrl_a = true;
            } else {
                // Not a detach sequence -- flush the buffered Ctrl-a
                to_send.push(DETACH_PREFIX);
                to_send.push(byte);
            }
        } else if byte == DETACH_PREFIX {
            *saw_ctrl_a = true;
        } else {
            to_send.push(byte);
        }
    }
    if to_send.is_empty() {
        StdinAction::Nothing
    } else {
        StdinAction::Send(to_send)
    }
}
