#!/usr/bin/env bash

set -e

TARGET="aarch64-apple-darwin"

BUILDVARIANT="${1:-debug}"

if [[ "$BUILDVARIANT" != "debug" && "$BUILDVARIANT" != "release" ]]; then
    echo "Usage: $0 [debug|release]"
    exit 1
fi

RELFLAG=""

if [[ "$BUILDVARIANT" == "release" ]]; then
    RELFLAG="--release"
fi

if ! rustup target list --installed | grep -q "^${TARGET}$"; then
    rustup target add "${TARGET}"
fi

$HOME/.cargo/bin/cargo rustc \
    -p byteshare-bridge \
    --lib \
    --crate-type staticlib \
    $RELFLAG \
    --target ${TARGET} \
    --target-dir target

cp target/${TARGET}/${BUILDVARIANT}/libbyteshare_bridge.a target/generated/

$HOME/.cargo/bin/cargo run \
    -p uniffi-bindgen \
    -- \
    generate \
    --library target/${TARGET}/${BUILDVARIANT}/libbyteshare_bridge.a \
    --language swift \
    --out-dir target/generated 
