// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#[cfg(target_os = "linux")]
use std::collections::HashSet;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

#[cfg(target_os = "linux")]
use anyhow::Context;
use anyhow::Result;
#[cfg(target_os = "linux")]
use nix::errno::Errno;
#[cfg(target_os = "linux")]
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;

// Interactive shells give each job its own process group. The leader must
// remain unreaped until cancellation cleanup has found those groups.
pub(crate) struct TerminalSession(Option<Pid>);

impl TerminalSession {
    pub fn new(pid: Pid) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Ok(Self(Some(pid)))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            Err(anyhow::anyhow!("terminal exec requires a Linux pod server"))
        }
    }

    pub fn completed(&mut self) {
        self.0 = None;
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let Some(pid) = self.0 else {
            return;
        };
        #[cfg(target_os = "linux")]
        if let Err(error) = kill_jobs(pid.as_raw()) {
            eprintln!("cleaning up exec terminal jobs: {error:#}");
        }
        #[cfg(not(target_os = "linux"))]
        eprintln!("cannot clean up terminal session {pid} outside a Linux pod server");
    }
}

#[cfg(target_os = "linux")]
fn in_session(pid: i32, session: i32) -> Result<bool> {
    let stat = match fs::read(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(false),
        Err(error) => return Err(error).context("reading exec job status"),
    };
    // comm can contain spaces, parentheses, or non-UTF-8 bytes. The numeric
    // fields begin after its last closing parenthesis.
    let end = stat
        .iter()
        .rposition(|byte| *byte == b')')
        .context("process status has no command name")?;
    let fields: Vec<_> = std::str::from_utf8(&stat[end + 1..])?
        .split_ascii_whitespace()
        .collect();
    let state = fields.first().context("process status has no state")?;
    let sid = fields
        .get(3)
        .context("process status has no session")?
        .parse::<i32>()?;
    Ok(sid == session && *state != "Z" && *state != "X")
}

#[cfg(target_os = "linux")]
fn kill_jobs(session: i32) -> Result<()> {
    // Stop the shell from launching more jobs while we enumerate. Its Child
    // handle stays unreaped, so the session ID cannot be reused.
    match killpg(Pid::from_raw(session), Signal::SIGKILL) {
        Ok(()) | Err(Errno::ESRCH) => {}
        Err(error) => return Err(error).context("killing exec terminal leader"),
    }
    let mut signaled = HashSet::new();
    loop {
        let mut found = false;
        for entry in fs::read_dir("/proc").context("listing exec terminal jobs")? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(pid) = name.to_str().and_then(|name| name.parse::<i32>().ok()) else {
                continue;
            };
            if pid == session || signaled.contains(&pid) || !in_session(pid, session)? {
                continue;
            }
            // SAFETY: pidfd_open takes a PID and flags, returning an owned fd.
            let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            if raw == -1 {
                if Errno::last() == Errno::ESRCH {
                    continue;
                }
                return Err(io::Error::last_os_error()).context("opening exec job pidfd");
            }
            // SAFETY: a successful pidfd_open returns a new owned descriptor.
            let fd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
            // Recheck after opening the pidfd: the PID may have been reused
            // between enumeration and open. The fd pins signal delivery.
            if !in_session(pid, session)? {
                continue;
            }
            // SAFETY: fd is a live pidfd and null siginfo requests SIGKILL.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    fd.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            };
            if result == -1 && Errno::last() != Errno::ESRCH {
                return Err(io::Error::last_os_error()).context("killing exec terminal job");
            }
            signaled.insert(pid);
            found = true;
        }
        if !found {
            return Ok(());
        }
        // Catch jobs forked during enumeration. Already-signaled processes
        // can remain visible while the kernel finishes their teardown.
    }
}
