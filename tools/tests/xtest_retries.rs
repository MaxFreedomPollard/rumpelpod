// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Barrier;
use std::thread;

use tools::take_xtest_retry;

#[test]
fn retry_budget_is_shared_between_workers() {
    let budget = AtomicU64::new(20);
    let barrier = Barrier::new(32);
    let granted = thread::scope(|scope| {
        let workers: Vec<_> = (0..32)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    take_xtest_retry(&budget)
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| usize::from(worker.join().expect("retry worker panicked")))
            .sum::<usize>()
    });

    assert_eq!(granted, 20);
    assert_eq!(budget.load(Relaxed), 0);
    assert!(!take_xtest_retry(&budget));
    assert_eq!(budget.load(Relaxed), 0);
}

#[test]
fn retry_budget_zero_disables_retries() {
    let budget = AtomicU64::new(0);
    assert!(!take_xtest_retry(&budget));
    assert_eq!(budget.load(Relaxed), 0);
}
