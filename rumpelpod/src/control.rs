// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;

tokio::task_local! {
    static OPERATION: Operation;
}

#[derive(Clone)]
pub(crate) struct Operation {
    name: String,
    started: Instant,
    phase: Arc<Mutex<(&'static str, Instant)>>,
}

impl Operation {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            started: Instant::now(),
            phase: Arc::new(Mutex::new(("queued", Instant::now()))),
        }
    }

    pub(crate) fn describe(&self) -> String {
        let (phase, since) = *self.phase.lock().unwrap();
        let name = &self.name;
        let age = self.started.elapsed().as_secs();
        let phase_age = since.elapsed().as_secs();
        format!("{name}, age {age}s, phase {phase} for {phase_age}s")
    }

    pub(crate) async fn scope<F: Future>(&self, future: F) -> F::Output {
        *self.phase.lock().unwrap() = ("running", Instant::now());
        OPERATION.scope(self.clone(), future).await
    }
}

struct Phase {
    operation: Operation,
    previous: (&'static str, Instant),
}

impl Drop for Phase {
    fn drop(&mut self) {
        *self.operation.phase.lock().unwrap() = self.previous;
    }
}

pub(crate) async fn phase<F: Future>(name: &'static str, future: F) -> F::Output {
    let _phase = OPERATION
        .try_with(|operation| Phase {
            operation: operation.clone(),
            previous: std::mem::replace(
                &mut *operation.phase.lock().unwrap(),
                (name, Instant::now()),
            ),
        })
        .ok();
    future.await
}

// Only use for queries and transport setup that are safe to abandon. A local
// child exiting does not establish whether a remote lifecycle mutation finished.
// Builds, uploads, hooks, and arbitrary execs must retain their own semantics.
pub(crate) async fn bounded<T>(
    name: &'static str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let deadline = timeout();
    phase(name, async {
        match tokio::time::timeout(deadline, future).await {
            Ok(result) => result,
            Err(_) => {
                let operation = OPERATION
                    .try_with(Operation::describe)
                    .unwrap_or_else(|_| name.to_string());
                log::warn!(
                    "{operation}: timed out after {deadline:?}; cancelled local control work"
                );
                Err(anyhow::anyhow!(
                    "{name} timed out after {deadline:?}; check the host connection and retry"
                ))
            }
        }
    })
    .await
}

fn timeout() -> Duration {
    // Integration tests need to exercise expiry without laptop-scale delays.
    match std::env::var("RUMPELPOD_CONTROL_TIMEOUT_MS") {
        Ok(raw) => {
            let ms: u64 = raw
                .parse()
                .expect("RUMPELPOD_CONTROL_TIMEOUT_MS must be a positive integer");
            assert!(ms > 0, "RUMPELPOD_CONTROL_TIMEOUT_MS must be positive");
            Duration::from_millis(ms)
        }
        Err(std::env::VarError::NotPresent) => Duration::from_secs(30),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("RUMPELPOD_CONTROL_TIMEOUT_MS must be valid Unicode")
        }
    }
}
