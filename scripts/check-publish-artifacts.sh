#!/usr/bin/env bash
# Verify that publishable crates work from their packaged crates.io archives.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

pkg_version() {
  cargo pkgid -p "$1" | sed 's/.*#//'
}

package_crate() {
  local package="$1"
  shift
  cargo package -p "$package" --locked --allow-dirty "$@"
}

package_crate_with_local_shardmap_patch() {
  local package="$1"
  shift
  cargo package -p "$package" --locked --allow-dirty \
    --config "patch.crates-io.shardmap.path=\"$root/crates/shardmap\"" \
    "$@"
}

package_shardmap_with_local_client_patch() {
  cargo package -p shardmap --locked --allow-dirty --all-features \
    --config "patch.crates-io.shardcache-client-rs.path=\"$root/crates/shardcache-client-rs\""
}

unpack_crate() {
  local package="$1"
  local version="$2"
  local crate_file="$root/target/package/${package}-${version}.crate"

  if [[ ! -f "$crate_file" ]]; then
    echo "missing packaged crate: $crate_file" >&2
    exit 1
  fi

  tar -xzf "$crate_file" -C "$unpacked"
}

write_consumer_main() {
  local dir="$1"
  mkdir -p "$dir/src"
  printf 'fn main() {}\n' >"$dir/src/main.rs"
}

write_patch_table() {
  cat <<EOF

[patch.crates-io]
shardmap = { path = "$unpacked/shardmap-$shardmap_version" }
shardcache-client-rs = { path = "$unpacked/shardcache-client-rs-$client_version" }
EOF
}

write_shardcache_patch_table() {
  cat <<EOF

[patch.crates-io]
shardmap = { path = "$unpacked/shardmap-$shardmap_version" }
shardcache-client-rs = { path = "$unpacked/shardcache-client-rs-$client_version" }
EOF
}

check_consumer() {
  local name="$1"
  local manifest="$tmp/$name/Cargo.toml"

  echo "checking packaged consumer: $name"
  cargo check --manifest-path "$manifest"
}

check_shardcache_binary() {
  local manifest="$unpacked/shardcache-$shardcache_version/Cargo.toml"

  write_shardcache_patch_table >>"$manifest"

  echo "checking packaged binary: shardcache default"
  cargo check --manifest-path "$manifest"

  echo "checking packaged binary: shardcache redis-server"
  cargo check --manifest-path "$manifest" \
    --no-default-features \
    --features redis-server,redis-functions,redis-modules-all

  echo "checking packaged binary: shardcache SCNP TLS overflow"
  cargo check --manifest-path "$manifest" \
    --no-default-features \
    --features redis-server,kv-overflow,scnp-tls
}

shardmap_version="$(pkg_version shardmap)"
shardcache_version="$(pkg_version shardcache)"
client_version="$(pkg_version shardcache-client-rs)"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/shard-kv-publish-artifacts.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
unpacked="$tmp/unpacked"
mkdir -p "$unpacked"

package_crate shardcache-client-rs
package_shardmap_with_local_client_patch

# This crate depends on the workspace shardmap version. During a PR for a new
# shardmap release, that exact version is not indexed on crates.io yet. Use a
# temporary Cargo patch only while creating the dependent archive so CI can
# still validate the packaged source before the publish-order handoff.
package_crate_with_local_shardmap_patch shardcache --no-verify

unpack_crate shardmap "$shardmap_version"
unpack_crate shardcache "$shardcache_version"
unpack_crate shardcache-client-rs "$client_version"

check_shardcache_binary

default_consumer="$tmp/default-consumer"
mkdir -p "$default_consumer"
write_consumer_main "$default_consumer"
cat >"$default_consumer/Cargo.toml" <<EOF
[package]
name = "shard-kv-publish-default-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
shardmap = "$shardmap_version"
shardcache-client-rs = "$client_version"
EOF
write_patch_table >>"$default_consumer/Cargo.toml"

redis_consumer="$tmp/redis-consumer"
mkdir -p "$redis_consumer"
write_consumer_main "$redis_consumer"
cat >"$redis_consumer/Cargo.toml" <<EOF
[package]
name = "shard-kv-publish-redis-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
shardmap = { version = "$shardmap_version", default-features = false, features = ["redis-server", "redis-functions", "redis-modules-all"] }
shardcache-client-rs = "$client_version"
EOF
write_patch_table >>"$redis_consumer/Cargo.toml"

check_consumer default-consumer
check_consumer redis-consumer

kv_overflow_consumer="$tmp/kv-overflow-consumer"
mkdir -p "$kv_overflow_consumer"
write_consumer_main "$kv_overflow_consumer"
cat >"$kv_overflow_consumer/Cargo.toml" <<EOF
[package]
name = "shard-kv-publish-kv-overflow-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
shardmap = { version = "$shardmap_version", features = ["kv-overflow-redis"] }
EOF
write_patch_table >>"$kv_overflow_consumer/Cargo.toml"

check_consumer kv-overflow-consumer

scnp_tls_consumer="$tmp/scnp-tls-consumer"
mkdir -p "$scnp_tls_consumer"
write_consumer_main "$scnp_tls_consumer"
cat >"$scnp_tls_consumer/Cargo.toml" <<EOF
[package]
name = "shard-kv-publish-scnp-tls-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
shardmap = { version = "$shardmap_version", default-features = false, features = ["scnp-tls"] }
EOF
write_patch_table >>"$scnp_tls_consumer/Cargo.toml"

check_consumer scnp-tls-consumer

active_sync_consumer="$tmp/active-sync-consumer"
mkdir -p "$active_sync_consumer"
write_consumer_main "$active_sync_consumer"
cat >"$active_sync_consumer/Cargo.toml" <<EOF
[package]
name = "shard-kv-publish-active-sync-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
shardmap = { version = "$shardmap_version", default-features = false, features = ["active-sync-tls"] }
EOF
write_patch_table >>"$active_sync_consumer/Cargo.toml"

check_consumer active-sync-consumer
