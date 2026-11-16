#!/usr/bin/env python3
"""Build source-pinned qualification artifacts; time them only on the target host.

This temporary experiment changes only native.rs between sequential builds and
restores its original bytes. It does not select or publish an optimization.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess

ROOT = Path(__file__).resolve().parents[1]
NATIVE = ROOT / "crates/shardmap/src/succession/native.rs"

def compact(s):
    s=s.replace("    key_len: usize,\n    value_len: usize,\n    deadline: Option<u64>,\n    slot: u32,", "    key_len: u32,\n    value_len_flags: u32,")
    s=s.replace("type Table =", """const HAS_TTL: u32 = 1 << 31;
    const _: () = assert!(size_of::<Row>() == 32);
    impl Row {
        /// Live slot headers hold absolute deadlines; free slots hold list links.
        #[inline(always)]
        fn deadline(&self) -> Option<u64> {
            if self.value_len_flags & HAS_TTL == 0 { return None; }
            // SAFETY: key follows its aligned eight-byte slot header; shard is held.
            Some(unsafe { self.key.as_ptr().sub(SLOT_HEADER).cast::<u64>().read() })
        }
        /// Keep TTL presence separate so zero and the full u64 domain remain valid.
        fn set_deadline(&mut self, deadline: Option<u64>, value_len: usize) {
            // SAFETY: same exclusively held live slot header, disjoint from payload.
            unsafe { self.key.as_ptr().sub(SLOT_HEADER).cast::<u64>().write(deadline.unwrap_or(0)); }
            self.value_len_flags = value_len as u32 | if deadline.is_some() { HAS_TTL } else { 0 };
        }
    }
    type Table =""")
    s=s.replace("    fn release(&self,", """    /// Derive a slot number only for deletion/reclamation, never ordinary reads.
        fn row_slot(&self, shard: usize, row: &Row) -> u32 {
            let offset = row.key.as_ptr() as usize - self.mapping.ptr.as_ptr() as usize - SLOT_HEADER;
            ((offset - self.slot_offset(shard, 0)) / self.geometry.stride) as u32
        }
        fn release(&self,""")
    s=s.replace("self.inner.mapping.bytes(row.key, row.key_len)","self.inner.mapping.bytes(row.key, row.key_len as usize)")
    s=s.replace("            .deadline\n", "            .deadline()\n")
    s=s.replace("row.deadline.is_some_and", "row.deadline().is_some_and")
    s=s.replace("row.value, row.value_len)","row.value, (row.value_len_flags & !HAS_TTL) as usize)")
    s=s.replace("            row.value_len = value.len();\n            row.deadline = deadline;", "            row.set_deadline(deadline, value.len());")
    s=s.replace("                    let offset = self.inner.slot_offset(shard, row.slot);", "                    let slot = self.inner.row_slot(shard, row);\n                    let offset = self.inner.slot_offset(shard, slot);")
    s=s.replace("                    free = row.slot;", "                    free = slot;")
    s=s.replace("            state.free = base.add(offset).cast::<u32>().read();", "            state.free = base.add(offset).cast::<u32>().read();\n            base.add(offset).cast::<u64>().write(deadline.unwrap_or(0));")
    s=s.replace("                key_len: key.len(),\n                value_len: value.len(),\n                deadline,\n                slot,", "                key_len: key.len() as u32,\n                value_len_flags: value.len() as u32 | if deadline.is_some() { HAS_TTL } else { 0 },")
    s=s.replace("        self.inner.release(shard, state, row.slot);\n        Ok(row\n            .deadline()\n            .is_none_or(|deadline| deadline > crate::storage::ttl_now_millis()))", "        let live = row.deadline().is_none_or(|deadline| deadline > crate::storage::ttl_now_millis());\n        self.inner.release(shard, state, self.inner.row_slot(shard, &row));\n        Ok(live)")
    s=s.replace("/// Offset diagnostics are calculated explicitly, outside ordinary reads.", "/// Lengths fit u32 under the checked 1 GiB backing limit. TTL presence uses the\n/// value-length high bit; absolute deadlines occupy the existing slot header.")
    s += """
    #[cfg(test)]
    #[test]
    fn compact_deadline_keeps_full_u64_domain() {
        let mut slot = [0u64; 4];
        // SAFETY: pointers into the live aligned test slot; header is eight bytes.
        let key = unsafe { NonNull::new_unchecked(slot.as_mut_ptr().cast::<u8>().add(SLOT_HEADER)) };
        let mut row = Row { hash: 0, key, value: key, key_len: 0, value_len_flags: 0 };
        for deadline in [None, Some(0), Some(u64::MAX), None] {
            row.set_deadline(deadline, 7);
            assert_eq!(row.deadline(), deadline);
            assert_eq!(row.value_len_flags & !HAS_TTL, 7);
            assert_eq!(row.key, key);
        }
    }
    """
    return s

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    original = NATIVE.read_bytes()
    assert b"key_len: usize" in original and b"    #[inline]\n    pub fn get" in original
    env = os.environ.copy()
    env["CARGO_CACHE_AUTO_CLEAN_FREQUENCY"] = "never"
    config = args.output / "rustfmt.toml"
    config.write_text('edition = "2024"\n')
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupted)
    paths = sorted((ROOT / "crates/shardmap/src").rglob("*.rs")) + [
        ROOT / "crates/shardmap/build.rs", ROOT / "crates/shardmap/Cargo.toml",
        ROOT / "Cargo.lock", ROOT / "crates/shardmap/examples/cache_succession_bench.rs"]
    def sources():
        return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    try:
        for name in ("baseline", "inline", "compact", "both"):
            text = original.decode()
            if name in ("compact", "both"):
                text = compact(text)
            if name in ("inline", "both"):
                text = text.replace("    #[inline]\n    pub fn get", "    #[inline(always)]\n    pub fn get")
            NATIVE.write_text(text)
            subprocess.run(["rustfmt", "--edition", "2024", "--config-path", str(config),
                            "--config", "skip_children=true", str(NATIVE)], check=True, cwd=ROOT, env=env)
            before = sources()
            patch = subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=ROOT)
            (args.output / (name + "-source.patch")).write_bytes(patch)
            with (args.output / (name + "-tests.log")).open("w") as log:
                subprocess.run(["cargo", "test", "--locked", "-p", "shardmap", "--features", "succession", "--lib"],
                               check=True, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            subprocess.run(["cargo", "build", "--locked", "--release", "-p", "shardmap", "--features", "succession",
                            "--example", "cache_succession_bench"], check=True, cwd=ROOT, env=env)
            assert sources() == before, "source changed during build"
            target = Path(env.get("CARGO_TARGET_DIR", str(ROOT / "target")))
            binary = args.output / (name + "-bench")
            shutil.copy2(target / "release/examples/cache_succession_bench", binary)
            manifest = {"candidate": name, "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                        "source_sha256": before, "patch_sha256": hashlib.sha256(patch).hexdigest(),
                        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "bytes": binary.stat().st_size,
                        "compiler": subprocess.check_output(["rustc", "--version", "--verbose"], text=True),
                        "build_profile": "release, succession, default features; workspace fat LTO/codegen1",
                        "compile_host": subprocess.check_output(["uname", "-srmo"], text=True).strip(),
                        "tested": "public lib regressions; plus full-u64 compact deadline representation for compact/both"}
            (args.output / (name + "-artifact.json")).write_text(json.dumps(manifest, indent=2) + "\n")
            print(f"built and tested {name}", flush=True)
    finally:
        NATIVE.write_bytes(original)
    assert NATIVE.read_bytes() == original


if __name__ == "__main__":
    main()
