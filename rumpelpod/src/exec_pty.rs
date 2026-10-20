// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::process::Stdio;

use anyhow::{Context, Result};
use nix::errno::Errno;
use nix::fcntl::{fcntl, FcntlArg, FdFlag, OFlag};
use nix::pty::{openpty, Winsize};
use nix::sys::termios::{cfmakeraw, tcgetattr, tcsetattr, SetArg, Termios};
use nix::unistd::{read, setsid, tcgetpgrp, write, Pid};
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::process::Command;

use crate::exec_stream::{TerminalSize, TerminalSpec};

mod session;
pub(crate) use session::TerminalSession;

pub(crate) struct RawTerminal(Termios);

impl RawTerminal {
    pub fn new() -> Result<Self> {
        let original = tcgetattr(io::stdin()).context("reading terminal attributes")?;
        let mut raw = original.clone();
        cfmakeraw(&mut raw);
        tcsetattr(io::stdin(), SetArg::TCSANOW, &raw).context("setting terminal raw mode")?;
        Ok(Self(original))
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) {
        if let Err(error) = tcsetattr(io::stdin(), SetArg::TCSANOW, &self.0) {
            eprintln!("failed to restore terminal attributes: {error}");
        }
    }
}

impl TerminalSize {
    pub fn from_stdin() -> Result<Self> {
        let mut size = Winsize {
            ws_row: 0,
            ws_col: 0,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: stdin is a live descriptor and size is writable for the ioctl.
        if unsafe { libc::ioctl(io::stdin().as_raw_fd(), libc::TIOCGWINSZ, &mut size) } == -1 {
            return Err(io::Error::last_os_error()).context("reading terminal size");
        }
        Ok(Self {
            rows: size.ws_row,
            cols: size.ws_col,
        })
    }

    fn winsize(self) -> Winsize {
        Winsize {
            ws_row: self.rows,
            ws_col: self.cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        }
    }
}

pub(crate) struct Pty(AsyncFd<OwnedFd>);

impl Pty {
    pub fn prepare(command: &mut Command, terminal: &TerminalSpec) -> Result<Self> {
        let pair = openpty(&terminal.size.winsize(), None).context("allocating exec terminal")?;
        // A leaked slave keeps output open after the command exits. A leaked
        // master keeps the terminal alive after its client disconnects.
        for fd in [&pair.master, &pair.slave] {
            fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
        }
        let flags = OFlag::from_bits_retain(fcntl(&pair.master, FcntlArg::F_GETFL)?);
        fcntl(&pair.master, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
        let master = AsyncFd::new(pair.master)?;
        command
            .stdin(Stdio::from(pair.slave.try_clone()?))
            .stdout(Stdio::from(pair.slave.try_clone()?))
            .stderr(Stdio::from(pair.slave));
        if let Some(term) = &terminal.term {
            command.env("TERM", term);
        }
        // SAFETY: the child only calls async-signal-safe syscalls here.
        // Stdio is installed before pre_exec, so fd 0 is the terminal slave.
        unsafe {
            command.as_std_mut().pre_exec(|| {
                setsid().map_err(io::Error::from)?;
                // SSH opens /dev/tty for passwords; terminal stdio alone does
                // not give the process a controlling terminal.
                if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(Self(master))
    }

    pub fn resize(&self, size: TerminalSize) -> Result<()> {
        let size = size.winsize();
        // SAFETY: the master descriptor and window-size pointer are valid.
        if unsafe { libc::ioctl(self.0.as_raw_fd(), libc::TIOCSWINSZ, &size) } == -1 {
            return Err(io::Error::last_os_error()).context("resizing exec terminal");
        }
        Ok(())
    }

    pub fn foreground_group(&self) -> Result<Option<Pid>> {
        match tcgetpgrp(self.0.get_ref()) {
            Ok(pid) => Ok((pid.as_raw() > 0).then_some(pid)),
            // The terminal can hang up before its final output is drained.
            Err(Errno::ENOTTY) => Ok(None),
            Err(error) => Err(error).context("reading exec terminal foreground group"),
        }
    }

    pub async fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0
            .async_io(Interest::READABLE, |fd| match read(fd, buffer) {
                // Linux reports terminal EOF as EIO when the last slave closes.
                Ok(count) => Ok(count),
                Err(Errno::EIO) => Ok(0),
                Err(error) => Err(error.into()),
            })
            .await
    }

    pub async fn write_all(&self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let count = self
                .0
                .async_io(Interest::WRITABLE, |fd| {
                    write(fd, data).map_err(io::Error::from)
                })
                .await?;
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            data = &data[count..];
        }
        Ok(())
    }
}
