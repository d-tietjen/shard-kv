# fast-cache fuzz harnesses

This directory contains `cargo-fuzz` targets plus reusable support code that is
also exercised by normal `cargo test` integration tests.

Run the embedded command-sequence fuzzer against the safe default build:

```sh
cd crates/fast-cache
cargo fuzz run embedded_command_sequence
```

Run the same fuzzer against the unsafe fast-cache feature:

```sh
cd crates/fast-cache
cargo fuzz run embedded_command_sequence --features unsafe
```

The command-sequence harness drives strings, hashes, lists, sets, and sorted sets
against an independent in-memory model. It checks value results, wrong-type
behavior, key deletion when containers become empty, batch string reads/writes,
and the store-wide key index.
