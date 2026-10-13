// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the `rumpel claude` subcommand.
//!
//! Tests verify that the claude CLI running inside a container can
//! communicate with the local machine via the LLM cache proxy routed
//! through the pod server and git HTTP server tunnel, producing
//! deterministic, offline-reproducible results.

pub(super) mod common;

mod detach;
mod fork;
mod host_session;
mod install;
mod lifecycle;
mod reauth;
mod reconnect;
mod smoke;
mod system_prompt;
mod tool_use;
