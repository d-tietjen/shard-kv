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
  cargo package -p "$package" --locked "$@"
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
shardcache-redis = { path = "$unpacked/shardcache-redis-$redis_version" }
shardcache-client-rs = { path = "$unpacked/shardcache-client-rs-$client_version" }
EOF
}

write_shardcache_patch_table() {
  cat <<EOF

[patch.crates-io]
shardmap = { path = "$unpacked/shardmap-$shardmap_version" }
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
}

shardmap_version="$(pkg_version shardmap)"
shardcache_version="$(pkg_version shardcache)"
redis_version="$(pkg_version shardcache-redis)"
client_version="$(pkg_version shardcache-client-rs)"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/shard-kv-publish-artifacts.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
unpacked="$tmp/unpacked"
mkdir -p "$unpacked"

package_crate shardmap --all-features
package_crate shardcache-client-rs

# These crates depend on the workspace shardmap version. Package them without
# Cargo's built-in verify step, then verify them below from the generated
# archives with a local crates.io patch for the packaged shardmap archive.
package_crate shardcache-redis --no-verify
package_crate shardcache --no-verify

unpack_crate shardmap "$shardmap_version"
unpack_crate shardcache "$shardcache_version"
unpack_crate shardcache-redis "$redis_version"
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
shardcache-redis = "$redis_version"
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
shardcache-redis = { version = "$redis_version", default-features = false, features = ["redis-server", "redis-functions", "redis-modules-all"] }
shardcache-client-rs = "$client_version"
EOF
write_patch_table >>"$redis_consumer/Cargo.toml"

check_consumer default-consumer
check_consumer redis-consumer
