#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

dependency_tree="$(cargo tree --workspace --all-features --prefix none)"
for forbidden in openssl openssl-sys native-tls tokio-native-tls rustls-openssl; do
  if grep -Eq "^${forbidden} v" <<<"$dependency_tree"; then
    echo "forbidden TLS dependency in all-features graph: ${forbidden}" >&2
    exit 1
  fi
done

echo "TLS dependency policy passed: Rustls only; no OpenSSL/native-tls runtime"
