// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transfer conversations and settings without cloning a running daemon.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{backup::Backup, Connection, OpenFlags};
use walkdir::WalkDir;

// Keep transfer provenance inside the opaque archive so the host needs no
// Codex-specific fork/recreate protocol. This entry is consumed on restore.
pub(in crate::pod) const SNAPSHOT_SOURCE_FILE: &str = ".codex/.rumpelpod-snapshot-source";

pub(in crate::pod) fn archive<W: Write>(
    archive: &mut tar::Builder<W>,
    home: &Path,
    pod_name: &str,
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(pod_name.len() as u64);
    header.set_mode(0o600);
    header.set_cksum();
    archive.append_data(&mut header, SNAPSHOT_SOURCE_FILE, pod_name.as_bytes())?;
    let codex_home = home.join(".codex");
    let entries = WalkDir::new(&codex_home).into_iter().filter_entry(|entry| {
        let rel = entry.path().strip_prefix(&codex_home).unwrap();
        if entry.path() == home.join(SNAPSHOT_SOURCE_FILE)
            || rel.starts_with("app-server-control")
            || rel.starts_with("packages/app-server-daemon/install.lock.d")
            || rel.starts_with(".tmp")
            || rel.starts_with("tmp")
            || entry.file_type().is_socket()
        {
            return false;
        }
        if entry.depth() == 1 {
            let name = entry.file_name().to_string_lossy();
            if name.ends_with(".sqlite-wal") || name.ends_with(".sqlite-shm") {
                return false;
            }
        }
        // Native daemon state contains process identities, sockets, locks,
        // logs and thread recovery records; only settings survive transfer.
        !rel.starts_with("app-server-daemon")
            || rel == Path::new("app-server-daemon")
            || rel == Path::new("app-server-daemon/settings.json")
    });
    for entry in entries {
        let entry = entry.context("walking Codex state")?;
        let rel = entry.path().strip_prefix(home)?;
        if entry.depth() == 1 && entry.path().extension().is_some_and(|ext| ext == "sqlite") {
            // Copying a live DB and its WAL separately races with checkpoints.
            // SQLite's backup includes committed WAL rows in one coherent DB.
            let snapshot = tempfile::NamedTempFile::new().context("staging Codex database")?;
            let source =
                Connection::open_with_flags(entry.path(), OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .context("opening Codex database for snapshot")?;
            let mut destination = Connection::open(snapshot.path())?;
            Backup::new(&source, &mut destination)?
                .run_to_completion(128, Duration::from_millis(10), None)
                .context("snapshotting Codex database")?;
            drop(destination);
            archive.append_path_with_name(snapshot.path(), rel)?;
            continue;
        }
        if entry.file_type().is_file() {
            let file = fs::File::open(entry.path()).context("opening Codex state file")?;
            let metadata = file
                .metadata()
                .context("reading Codex state file metadata")?;
            let mut header = tar::Header::new_gnu();
            header.set_metadata(&metadata);
            // Codex keeps appending logs and rollouts while we copy. Reading
            // past the header's recorded size would corrupt the next entry.
            let mut reader = file.take(metadata.len());
            archive.append_data(&mut header, rel, &mut reader)?;
            if reader.limit() != 0 {
                return Err(anyhow::anyhow!(
                    "Codex state file shrank during snapshot: {}",
                    entry.path().display()
                ));
            }
            continue;
        }
        archive
            .append_path_with_name(entry.path(), rel)
            .with_context(|| format!("archiving {}", entry.path().display()))?;
    }
    Ok(())
}

/// Codex 0.163 has no clone/reset command. A fork must not impersonate its
/// source's mobile host, but should retain ordinary auth and conversation state.
pub(in crate::pod) fn reset_fork_identity(home: &Path) -> Result<()> {
    let codex_home = home.join(".codex");
    match fs::remove_file(codex_home.join("installation_id")) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("resetting forked Codex installation identity"),
    }

    let settings_path = codex_home.join("app-server-daemon/settings.json");
    let settings = match fs::read(&settings_path) {
        Ok(settings) => Some(settings),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).context("reading forked Codex daemon settings"),
    };
    if let Some(settings) = settings {
        let mut settings: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&settings).context("parsing forked Codex daemon settings")?;
        settings.insert("remoteControlEnabled".to_string(), false.into());
        fs::write(settings_path, serde_json::to_vec_pretty(&settings)?)
            .context("disabling remote control in forked Codex settings")?;
    }

    // All conversation tables stay untouched in the restored database.
    for entry in fs::read_dir(&codex_home).context("reading forked Codex home")? {
        let entry = entry.context("reading forked Codex state entry")?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("state_") || !name.ends_with(".sqlite") {
            continue;
        }
        let db = Connection::open_with_flags(entry.path(), OpenFlags::SQLITE_OPEN_READ_WRITE)
            .context("opening forked Codex state database")?;
        let has_enrollments: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'remote_control_enrollments')",
            [],
            |row| row.get(0),
        )?;
        if has_enrollments {
            db.execute("DELETE FROM remote_control_enrollments", [])
                .context("clearing forked Codex remote-control enrollment")?;
        }
    }
    Ok(())
}
