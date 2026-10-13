// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use log::error;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command as TokioCommand};

use crate::async_command::AsyncCommandExt;
use crate::config::load_json_config;

const SSH_AGENT_START_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct ManagedSshAgent {
    child: Child,
    agent_dir: PathBuf,
    pub(crate) socket_path: PathBuf,
    temporary_dir: Option<tempfile::TempDir>,
    pub(crate) configured_keys_hash: Option<String>,
}

impl ManagedSshAgent {
    pub(crate) async fn for_build(repo_root: &Path) -> Result<Option<Self>> {
        let config = load_json_config(repo_root)?.ssh_agent;
        if config.ambient {
            return Ok(None);
        }
        let Some(keys) = config.keys else {
            return Ok(None);
        };
        let keys = resolve_ssh_key_paths(repo_root, &keys)?;
        // macOS's default temporary directory can exceed the Unix socket path limit.
        let directory = tempfile::Builder::new()
            .prefix("rumpel-build-agent-")
            .tempdir_in("/tmp")
            .context("creating build ssh-agent directory")?;
        let mut agent = Self::configured(directory.path().join("agent"), &keys).await?;
        agent.temporary_dir = Some(directory);
        Ok(Some(agent))
    }

    pub(crate) fn build_source(&self, source: &str) -> String {
        let id = source.split_once('=').map_or(source, |(id, _)| id);
        let socket = self.socket_path.display();
        format!("{id}={socket}")
    }

    pub(crate) async fn configured(agent_dir: PathBuf, keys: &[PathBuf]) -> Result<Self> {
        let configured_keys_hash = Self::configured_keys_hash(keys)?;
        let mut agent = Self::start(agent_dir).await?;
        agent.add_configured_keys(keys).await?;
        agent.configured_keys_hash = Some(configured_keys_hash);
        Ok(agent)
    }

    pub(crate) fn configured_keys_hash(keys: &[PathBuf]) -> Result<String> {
        let mut hasher = Sha256::new();
        for key in keys {
            hasher.update(key.as_os_str().as_encoded_bytes());
            hasher.update([0]);
            hasher.update(std::fs::read(key).with_context(|| {
                let key = key.display();
                format!("reading configured SSH key {key}")
            })?);
        }
        Ok(hex::encode(hasher.finalize()))
    }

    pub(crate) async fn start(agent_dir: PathBuf) -> Result<Self> {
        match std::fs::remove_dir_all(&agent_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                let agent_dir = agent_dir.display();
                return Err(error)
                    .with_context(|| format!("removing stale ssh-agent directory {agent_dir}"));
            }
        }
        std::fs::create_dir_all(&agent_dir).with_context(|| {
            let agent_dir = agent_dir.display();
            format!("creating ssh-agent directory {agent_dir}")
        })?;

        let socket_path = agent_dir.join("agent.sock");
        let child = match TokioCommand::new("ssh-agent")
            .kill_on_drop(true)
            .args(["-D", "-a"])
            .arg(&socket_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                if let Err(cleanup_error) = std::fs::remove_dir_all(&agent_dir) {
                    let agent_dir = agent_dir.display();
                    error!(
                        "failed to remove ssh-agent directory {agent_dir} after startup failure: {cleanup_error}"
                    );
                }
                return Err(error).context("failed to start ssh-agent");
            }
        };
        let mut agent = Self {
            child,
            agent_dir,
            socket_path,
            configured_keys_hash: None,
            temporary_dir: None,
        };
        let deadline = Instant::now() + SSH_AGENT_START_TIMEOUT;
        while !agent.socket_path.exists() {
            match agent.child.try_wait() {
                Ok(Some(status)) => {
                    let mut stderr = String::new();
                    if let Some(mut pipe) = agent.child.stderr.take() {
                        pipe.read_to_string(&mut stderr)
                            .await
                            .context("reading ssh-agent error")?;
                    }
                    let stderr = stderr.trim();
                    return Err(anyhow::anyhow!("ssh-agent exited with {status}: {stderr}"));
                }
                Ok(None) => {}
                Err(error) => return Err(error).context("checking ssh-agent startup"),
            }
            if Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "ssh-agent did not create its socket within {SSH_AGENT_START_TIMEOUT:?}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(agent)
    }

    pub(crate) fn is_alive(&mut self) -> Result<bool> {
        match self.child.try_wait() {
            Ok(Some(_)) => Ok(false),
            Ok(None) => Ok(true),
            Err(error) => Err(error).context("checking ssh-agent status"),
        }
    }

    async fn add_configured_keys(&mut self, keys: &[PathBuf]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let output = Command::new("ssh-add")
            .args(keys)
            .env("SSH_AUTH_SOCK", &self.socket_path)
            .env("SSH_ASKPASS_REQUIRE", "never")
            .stdin(Stdio::null())
            .output_async()
            .await
            .context("running ssh-add for configured SSH keys")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stderr = stderr.trim();
            return Err(anyhow::anyhow!(
                "ssh-add failed while loading configured SSH keys with status {}: {stderr}",
                output.status
            ));
        }
        Ok(())
    }
}

impl Drop for ManagedSshAgent {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = self.child.start_kill() {
                    error!("failed to kill ssh-agent: {error}");
                }
            }
            Err(error) => error!("failed to check ssh-agent before cleanup: {error}"),
        }
        match std::fs::remove_dir_all(&self.agent_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                let agent_dir = self.agent_dir.display();
                error!("failed to remove ssh-agent directory {agent_dir}: {error}");
            }
        }
    }
}

pub(crate) fn resolve_ssh_key_paths(repo_root: &Path, keys: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut resolved = Vec::with_capacity(keys.len());
    for key in keys {
        let path = match key.to_str() {
            Some("~") => dirs::home_dir().context("could not determine home directory")?,
            Some(value) if value.starts_with("~/") => dirs::home_dir()
                .context("could not determine home directory")?
                .join(&value[2..]),
            Some(value) if value.starts_with('~') => {
                return Err(anyhow::anyhow!(
                    "SSH key path '{value}' uses unsupported user-home expansion"
                ));
            }
            Some(_) | None if key.is_absolute() => key.clone(),
            Some(_) | None => repo_root.join(key),
        };
        let metadata = std::fs::metadata(&path).with_context(|| {
            let path = path.display();
            format!("reading configured SSH key {path}")
        })?;
        if !metadata.is_file() {
            let path = path.display();
            return Err(anyhow::anyhow!("configured SSH key {path} is not a file"));
        }
        resolved.push(path);
    }
    Ok(resolved)
}

pub(crate) async fn configure_build_ssh(
    repo_root: &Path,
    options: &mut [String],
) -> Result<Option<ManagedSshAgent>> {
    if !options
        .iter()
        .any(|option| option == "--ssh" || option.starts_with("--ssh="))
    {
        return Ok(None);
    }
    let Some(agent) = ManagedSshAgent::for_build(repo_root).await? else {
        return Ok(None);
    };
    // Docker's ssh:// transport may need different identities from the build.
    // Explicit sources restrict build mounts without replacing SSH_AUTH_SOCK.
    let mut options = options.iter_mut();
    while let Some(option) = options.next() {
        if option == "--ssh" {
            let source = options
                .next()
                .context("--ssh requires a build SSH source")?;
            *source = agent.build_source(source);
        } else if let Some(source) = option.strip_prefix("--ssh=") {
            *option = format!("--ssh={}", agent.build_source(source));
        }
    }
    Ok(Some(agent))
}
