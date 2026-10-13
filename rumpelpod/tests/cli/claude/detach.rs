// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Test detach (Ctrl-a d) and reattach: verify the PTY session
//! survives across client disconnections and the screen is replayed.
//! Also tests that the client exits cleanly when the remote session ends.

use std::time::Instant;

use super::common::{setup_claude_test_repo, ClaudeSession};

/// Ctrl-a (0x01) followed by 'd' triggers detach.
const CTRL_A: u8 = 0x01;

#[test]
fn claude_detach_reattach() {
    let (fake_home, repo, _executor, daemon) = setup_claude_test_repo();

    // -- First session: start Claude, then detach ---------------------

    let mut session =
        ClaudeSession::spawn(&repo, &daemon, fake_home.path(), "claude-haiku-4-5", &[]);

    session.wait_for("~/workspace");
    let replay_marker = "claude-reattach-keeps-pending-input";
    session.write_raw(replay_marker.as_bytes());
    session.wait_for(replay_marker);

    // Send the detach sequence: Ctrl-a then d.
    session.write_raw(&[CTRL_A, b'd']);

    // The rumpel process should exit after detaching.
    session.wait_for_exit();

    // Preparation would parse this file. A running session must remain
    // reachable while the host's next devcontainer config is being edited.
    std::fs::write(
        repo.path().join(".devcontainer/devcontainer.json"),
        "invalid json",
    )
    .expect("invalidate host devcontainer config");

    // -- Second session: reattach and verify screen replay -------------
    //
    // The server maintains a virtual terminal buffer (like screen/tmux)
    // and replays the screen contents on attach, so the client sees the
    // full TUI immediately without the app needing to re-render.

    let started = Instant::now();
    let mut session2 = ClaudeSession::spawn_for_pod(
        &repo,
        &daemon,
        fake_home.path(),
        "test",
        false,
        "claude-haiku-4-5",
        &[],
    );

    session2.wait_for(replay_marker);
    let elapsed = started.elapsed();
    eprintln!("Claude warm reattach screen replay: {elapsed:?}");
}

/// After Ctrl-a d the terminal must be fully restored: mouse tracking
/// off, bracketed paste off, TUI content scrolled into scrollback,
/// and the detach message visible with nothing below it.
#[test]
fn claude_detach_restores_terminal() {
    let (fake_home, repo, _executor, daemon) = setup_claude_test_repo();

    let mut session =
        ClaudeSession::spawn(&repo, &daemon, fake_home.path(), "claude-haiku-4-5", &[]);

    session.wait_for("~/workspace");

    // Detach and let the rumpel process exit.
    session.write_raw(&[CTRL_A, b'd']);
    session.wait_for_exit();

    let screen = session.screen();

    // Mouse tracking must be off so the local shell does not receive
    // garbage escape sequences from mouse movement.
    assert_eq!(
        screen.mouse_protocol_mode(),
        vt100::MouseProtocolMode::None,
        "mouse tracking should be disabled after detach"
    );

    // Bracketed paste must be off so the local shell's own paste
    // handling is not confused.
    assert!(
        !screen.bracketed_paste(),
        "bracketed paste should be disabled after detach"
    );

    // The detach message must be on the visible screen.
    let contents = screen.contents();
    assert!(
        contents.contains("[detached from session]"),
        "detach message not found on screen:\n{contents}"
    );

    // Everything below the detach message should be empty -- the TUI
    // content was scrolled into scrollback, not left on screen.
    let (detach_row, _) = screen.cursor_position();
    let (rows, cols) = screen.size();
    for row in detach_row..rows {
        let line: String = screen.rows(0, cols).nth(row as usize).unwrap_or_default();
        assert!(
            line.trim().is_empty(),
            "row {row} below detach message should be empty, got: {line:?}"
        );
    }
}

/// When Claude exits inside the pod (via Ctrl-D), the client must
/// detect it and exit cleanly instead of hanging.  Restarting must
/// launch a fresh session, not try to reattach to the dead one.
#[test]
fn claude_session_exit() {
    let (home, repo, _executor, daemon) = setup_claude_test_repo();

    let mut session = ClaudeSession::spawn(&repo, &daemon, home.path(), "claude-haiku-4-5", &[]);

    session.wait_for("~/workspace");

    // Ctrl-D (0x04) at an empty prompt causes Claude Code to exit.
    // Send it twice: the first triggers the exit intent, the second
    // confirms in case the TUI consumed the first one.
    session.write_raw(&[0x04]);
    std::thread::sleep(std::time::Duration::from_millis(500));
    session.write_raw(&[0x04]);

    // The rumpel process must exit, not hang.  wait_for_exit panics
    // after 10s if the child is still alive.
    session.wait_for_exit();

    // A replacement session must use the running pod's original workspace,
    // without needing the host devcontainer configuration again.
    std::fs::write(
        repo.path().join(".devcontainer/devcontainer.json"),
        "invalid json",
    )
    .expect("invalidate host devcontainer config");

    // Restarting must launch a fresh Claude session rather than
    // reattaching to the now-dead one.
    let mut session2 = ClaudeSession::spawn(&repo, &daemon, home.path(), "claude-haiku-4-5", &[]);

    session2.wait_for("~/workspace");
}
