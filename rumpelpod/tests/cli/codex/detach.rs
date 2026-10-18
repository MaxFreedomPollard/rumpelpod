// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Test detach (Ctrl-a d) and reattach for the host-side codex screen
//! session: type into the prompt, detach, reattach, and verify the
//! screen replay restores the pending input immediately without starting
//! another frontend or waiting for initialization again.

use super::common::{setup_codex_test_repo, CodexSession};

/// Ctrl-a (0x01) followed by 'd' triggers detach in pty_attach.
const CTRL_A: u8 = 0x01;

#[test]
fn codex_detach_reattach() {
    let (home, repo, _executor, daemon) = setup_codex_test_repo();

    // -- First session: type a marker, then detach ----------------------

    let mut session = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    session.wait_for_ready();
    let replay_marker = "warm-reattach-replay-marker";
    session.write_raw(replay_marker.as_bytes());
    session.wait_for(replay_marker);

    session.write_raw(&[CTRL_A, b'd']);
    session.wait_for_exit();

    // A live daemon-owned frontend must reattach before configuration or
    // credentials needed only for spawning a new frontend are consulted.
    std::fs::write(repo.path().join(".rumpelpod.json"), "not valid json")
        .expect("invalidate rumpelpod config");
    std::fs::remove_file(home.path().join(".codex/auth.json"))
        .expect("remove host Codex credentials");
    std::fs::remove_file(daemon.bin_dir.join("codex")).expect("remove host Codex binary");

    // -- Second session: reattach and verify screen replay --------------
    //
    // The daemon-managed screen session keeps the codex TUI process
    // alive between invocations.  Reattaching replays the vt100 screen
    // buffer so the previous question and answer are visible
    // immediately, with no wait_for_ready() needed.

    let mut session2 = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    session2.wait_for(replay_marker);
}
