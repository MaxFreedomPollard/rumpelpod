#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Build the devcontainer image, boot it privileged, check out the
# commit under test, and run the release test pipeline inside it. This
# keeps the CI workflow thin and makes the exact CI sequence
# reproducible locally: run it with docker and the same
# REPO_URL/COMMIT to mirror what the runners do.
#
# The commit under test is fetched straight from its origin over the
# network (REPO_URL) rather than bind-mounted, so a fork's own repo
# works by pointing REPO_URL at it. GitHub serves fetches of a bare
# commit id as long as it is reachable from a ref, which a pushed
# branch tip always is.
#
# When STAGING_DIR is set, the release rumpel binary the pipeline built
# and tested is copied there afterwards so CI can publish it -- the
# published binary is exactly the one the tests ran against.

set -euo pipefail

# git URL to fetch the commit under test from. On a fork this is the
# fork's own repo, so its pushed commits are reachable.
REPO_URL=${REPO_URL:?REPO_URL must be set}
# Commit id or ref to check out and test.
COMMIT=${COMMIT:?COMMIT must be set}
# Optional: directory on the host to copy the tested release binary to.
STAGING_DIR=${STAGING_DIR:-}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

# The base image is pinned by digest in the Dockerfile itself, so the
# build needs no --build-arg to stay reproducible.
docker build --tag rumpelpod-dev "$repo_root/.devcontainer"

# --privileged lets the container run systemd and nested containers
# (docker, podman, k3d), the same as local development
# (devcontainer.json). Nested container storage must not live on the
# container's own overlayfs root -- the kernel refuses
# overlay-upon-overlay -- so both engines get named volumes, mirroring
# the devcontainer.json mounts. The runner is fresh per job, so fixed
# volume names are fine here.
docker run --detach --name devcontainer \
  --privileged \
  --volume rumpelpod-podman-storage:/var/lib/containers \
  --volume rumpelpod-docker-storage:/var/lib/docker \
  rumpelpod-dev

cleanup() {
  docker rm --force devcontainer >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Right after boot systemctl reports "offline" (the bus is not up yet)
# and is-system-running --wait errors out instead of waiting; poll for
# a real manager state before waiting on it. Report the entrypoint log
# instead of hiding the cause when the container exits during startup.
manager_ready=0
for _ in $(seq 60); do
  running=$(docker inspect --format '{{.State.Running}}' devcontainer)
  if [ "$running" != true ]; then
    echo "devcontainer exited during startup" >&2
    docker logs devcontainer >&2 || true
    exit 1
  fi
  state=$(docker exec devcontainer systemctl is-system-running 2>/dev/null || true)
  case "$state" in
    initializing | starting | running | degraded)
      manager_ready=1
      break
      ;;
  esac
  sleep 1
done
if [ "$manager_ready" -ne 1 ]; then
  echo "devcontainer systemd did not start within 60 seconds" >&2
  docker logs devcontainer >&2 || true
  exit 1
fi
timeout 300 docker exec devcontainer systemctl is-system-running --wait || true

# The image ships a clone of the upstream repo with a prebuilt target/
# for incremental builds. Fetch the commit under test into that clone
# and check it out. --tags because build.rs derives the version from
# git describe.
docker exec --user user --workdir /workspaces/rumpelpod devcontainer \
  git fetch --tags "$REPO_URL" "$COMMIT"
docker exec --user user --workdir /workspaces/rumpelpod devcontainer \
  git checkout --detach FETCH_HEAD

# A login shell because claude lives in ~/.local/bin, which only
# .profile puts on PATH; the ENV docker exec uses does not include it.
# The timeout accommodates the LLM CLI replay tests, which exceed the
# default 120s on the runners' four cores.
# Allow transient failures more chances on shared CI runners while
# retaining xtest's configured ten-retry suite budget.
docker exec --user user --workdir /workspaces/rumpelpod devcontainer \
  bash -lc 'cargo pipeline --release --timeout 300 --retries 3'

# Copy out the tested release binary for the host's own architecture.
# The pipeline cross-builds both linux targets, so pick the native one.
# Stream it out with `docker exec ... cat` rather than `docker cp`:
# cp resolves paths through the storage driver and has failed to find
# this runtime-written, cargo hard-linked file, while a normal exec
# reads it through the live mount regardless of runtime.
if [ -n "$STAGING_DIR" ]; then
  case "$(dpkg --print-architecture)" in
    amd64) triple=x86_64-unknown-linux-musl; name=rumpel-linux-amd64 ;;
    arm64) triple=aarch64-unknown-linux-musl; name=rumpel-linux-arm64 ;;
    *)
      echo "unsupported host architecture: $(dpkg --print-architecture)" >&2
      exit 1
      ;;
  esac
  mkdir -p "$STAGING_DIR"
  docker exec --user user --workdir /workspaces/rumpelpod devcontainer \
    cat "target/$triple/release/rumpel" >"$STAGING_DIR/$name"
  chmod +x "$STAGING_DIR/$name"
fi
