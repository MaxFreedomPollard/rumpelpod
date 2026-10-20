// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Binary framing keeps command output lossless while sharing an HTTP stream
//! with EOF, errors, and exit status.

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) const CHUNK_SIZE: usize = 64 * 1024;
const MAX_REQUEST_SIZE: usize = 1024 * 1024;
pub(crate) const CONTENT_TYPE: &str = "application/vnd.rumpelpod.exec";
pub(crate) const UNKNOWN_OUTCOME: &str =
    "exec connection lost; command outcome is unknown (the command was not retried)";

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ExecSpec {
    pub id: ExecutionId,
    pub command: Vec<String>,
    pub relative_workdir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub(crate) struct ExecutionId(pub [u8; 16]);

impl From<ExecutionId> for String {
    fn from(id: ExecutionId) -> Self {
        hex::encode(id.0)
    }
}

impl TryFrom<String> for ExecutionId {
    type Error = hex::FromHexError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut bytes = [0; 16];
        hex::decode_to_slice(value, &mut bytes)?;
        Ok(Self(bytes))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecSignal {
    Interrupt,
    Terminate,
    Hangup,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SignalRequest {
    pub id: ExecutionId,
    pub signal: ExecSignal,
}

pub(crate) enum Frame {
    Stdin(Vec<u8>),
    StdinEof,
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(u8),
    Error(String),
}

impl Frame {
    pub fn encode(self) -> Vec<u8> {
        let (tag, payload) = match self {
            Self::Stdin(data) => (1, data),
            Self::StdinEof => (2, Vec::new()),
            Self::Stdout(data) => (3, data),
            Self::Stderr(data) => (4, data),
            Self::Exit(code) => (5, vec![code]),
            Self::Error(error) => (6, error.into_bytes()),
        };
        let mut bytes = Vec::with_capacity(5 + payload.len());
        bytes.push(tag);
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes
    }
}

pub(crate) async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> Result<Option<Frame>> {
    let mut tag = [0];
    if reader.read(&mut tag).await? == 0 {
        return Ok(None);
    }
    let payload = read_payload(reader, CHUNK_SIZE).await?;
    let frame = match (tag[0], payload.as_slice()) {
        (1, _) => Frame::Stdin(payload),
        (2, []) => Frame::StdinEof,
        (3, _) => Frame::Stdout(payload),
        (4, _) => Frame::Stderr(payload),
        (5, [code]) => Frame::Exit(*code),
        (6, _) => Frame::Error(String::from_utf8(payload).context("exec error is not UTF-8")?),
        (tag, _) => return Err(anyhow::anyhow!("invalid exec frame {tag}")),
    };
    Ok(Some(frame))
}

async fn read_payload(reader: &mut (impl AsyncRead + Unpin), limit: usize) -> Result<Vec<u8>> {
    let len = reader.read_u32().await? as usize;
    if len > limit {
        return Err(anyhow::anyhow!("exec payload exceeds {limit} bytes"));
    }
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

pub(crate) fn encode_request(request: &impl Serialize) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(request)?;
    if payload.len() > MAX_REQUEST_SIZE {
        return Err(anyhow::anyhow!("exec request is too large"));
    }
    let mut bytes = Vec::with_capacity(4 + payload.len());
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(crate) async fn read_request<T: DeserializeOwned>(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<T> {
    let payload = read_payload(reader, MAX_REQUEST_SIZE).await?;
    serde_json::from_slice(&payload).context("invalid exec request")
}

pub(crate) fn io_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::other(error)
}

/// Events keep the HTTP/2 connection open so commands need no remote handshake.
/// Neither redirects nor transport retries may replay a side-effecting request.
pub(crate) fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .http2_prior_knowledge()
        // Compression can delay small writes until a compressor fills its
        // buffer, which breaks programs that exchange prompts over pipes.
        .gzip(false)
        .http2_keep_alive_interval(Duration::from_secs(5))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
}
