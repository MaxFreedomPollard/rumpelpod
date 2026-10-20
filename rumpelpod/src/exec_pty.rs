// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::Stdio;

use anyhow::{Context, Result};
use nix::errno::Errno;
#[cfg(target_os = "linux")]
use nix::fcntl::OFlag;
use nix::pty::Winsize;
#[cfg(target_os = "linux")]
use nix::pty::{grantpt, posix_openpt, unlockpt};
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
    pub(crate) fn from_fd(fd: RawFd) -> Result<Self> {
        let mut size = Winsize {
            ws_row: 0,
            ws_col: 0,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: size is writable for the ioctl; invalid descriptors return an error.
        if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) } == -1 {
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

fn allocate_terminal() -> Result<(OwnedFd, OwnedFd)> {
    #[cfg(target_os = "linux")]
    {
        // Atomic CLOEXEC prevents concurrent spawns from inheriting a slave
        // that delays EOF or a master that keeps a disconnected session alive.
        let master =
            posix_openpt(OFlag::O_RDWR | OFlag::O_NOCTTY | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK)
                .context("allocating exec terminal master")?;
        grantpt(&master).context("granting exec terminal access")?;
        unlockpt(&master).context("unlocking exec terminal slave")?;
        // SAFETY: master is a live PTY descriptor. TIOCGPTPEER takes flags and
        // returns a new descriptor without reading or writing userspace memory.
        let slave = unsafe {
            libc::ioctl(
                master.as_raw_fd(),
                libc::TIOCGPTPEER,
                libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
            )
        };
        if slave == -1 {
            return Err(io::Error::last_os_error()).context("allocating exec terminal slave");
        }
        // SAFETY: a successful TIOCGPTPEER returns a new owned descriptor.
        let slave = unsafe { OwnedFd::from_raw_fd(slave) };
        Ok((master.into(), slave))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(anyhow::anyhow!("terminal exec requires a Linux pod server"))
    }
}

impl Pty {
    pub fn prepare(command: &mut Command, terminal: &TerminalSpec) -> Result<Self> {
        let (master, slave) = allocate_terminal()?;
        let master = Self(AsyncFd::new(master)?);
        master.resize(terminal.size)?;
        command
            .stdin(Stdio::from(slave.try_clone()?))
            .stdout(Stdio::from(slave.try_clone()?))
            .stderr(Stdio::from(slave));
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
        Ok(master)
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
            let mut ready = self.0.writable().await?;
            // A hung-up PTY with a full input buffer can keep returning EAGAIN.
            // Closed readiness stays set, so retrying it would spin forever
            // and prevent the command's final output and exit from being read.
            if ready.ready().is_write_closed() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            let count = match ready.try_io(|fd| write(fd.get_ref(), data).map_err(io::Error::from))
            {
                Ok(result) => result?,
                Err(_) => continue,
            };
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            data = &data[count..];
        }
        Ok(())
    }
}
