// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Smoke test: verify that the codex TUI on the local machine can communicate
//! with the codex app-server inside the container through the pod
//! server's WebSocket proxy and execute a shell tool through code mode,
//! with API requests cached via the LLM cache proxy.

use std::time::Duration;

use super::common::{setup_codex_test_repo, CodexSession};

#[test]
fn codex_smoke() {
    let (_home, repo, _executor, daemon) = setup_codex_test_repo();

    // Exercise the app-server's default permission bypass and Codex's own model.
    let mut session = CodexSession::spawn(&repo, &daemon, _home.path(), &[]);

    session.wait_for_ready();
    let contents = session.screen().contents();
    assert!(contents.contains("Full Access"), "{contents}");

    session.send("Run `printf %x 3735928559`.");
    session.wait_for_with_timeout("deadbeef", Duration::from_secs(30));
}

#[test]
fn codex_wait_accepts_the_current_screen() {
    let (home, repo, _executor, daemon) = setup_codex_test_repo();
    let mut session = CodexSession::spawn(&repo, &daemon, home.path(), &[]);
    session.wait_for_ready();

    // Readiness has already consumed the status response. Matching it again
    // must not depend on Codex emitting another terminal update.
    session.wait_for_with_timeout("Permissions:", Duration::ZERO);
    session.wait_for_absent_with_timeout("Waiting for startup", Duration::ZERO);
}
