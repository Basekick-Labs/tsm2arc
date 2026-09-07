#!/bin/sh
# Rebuilds the embedded WAL decoder blob. Requires the Rust toolchain pinned
# by rust-toolchain.toml (rustup installs it automatically) with the
# wasm32-wasip1 target.
set -e
cd "$(dirname "$0")"
cargo build --release --target wasm32-wasip1
cp target/wasm32-wasip1/release/waldecoder.wasm ../decoder.wasm
ls -la ../decoder.wasm
