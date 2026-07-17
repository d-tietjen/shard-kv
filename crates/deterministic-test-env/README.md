# Deterministic Test Environment

This non-published test-support crate is vendored from Blossom revision
`46750a97a70fd301e3e6f3255316c1d7e837a9dd`. It keeps ShardMap's deterministic
active-sync fault tests reproducible without requiring private Git credentials
during normal workspace builds or crates.io packaging.

The upstream source is MIT licensed. Update this directory only by replacing it
from a reviewed Blossom revision, then run the full workspace test and license
inventory gates.
