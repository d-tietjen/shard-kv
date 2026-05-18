# Shared helpers for bench scripts. Source-only; not executable on its own.

# Wrap a command with `taskset -c <cpuset>` on Linux when SERVER_CPUSET is
# set. On macOS or when SERVER_CPUSET is empty, runs the command directly.
#
# Usage:
#   pinned_exec ./target/release/fast-cache-server ...
pinned_exec() {
  if [[ -n "${SERVER_CPUSET:-}" ]] && command -v taskset >/dev/null 2>&1; then
    taskset -c "$SERVER_CPUSET" "$@"
  else
    "$@"
  fi
}

# Echo the resolved cpuset and pinning tool, or "none" if not active.
report_pinning() {
  if [[ -n "${SERVER_CPUSET:-}" ]]; then
    if command -v taskset >/dev/null 2>&1; then
      echo "pinning: taskset -c $SERVER_CPUSET"
    else
      echo "pinning: SERVER_CPUSET=$SERVER_CPUSET set but taskset is unavailable; pinning skipped"
    fi
  else
    echo "pinning: none (set SERVER_CPUSET=0-3 to pin server-side cores)"
  fi
}

# Resolve a server's host PID. Works for direct binaries (PID is $!) and
# Docker containers (look up via docker inspect).
resolve_container_pid() {
  local container="$1"
  docker inspect --format '{{.State.Pid}}' "$container" 2>/dev/null || echo ""
}
