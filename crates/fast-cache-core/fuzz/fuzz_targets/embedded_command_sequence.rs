#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../support/embedded_command_sequence.rs"]
mod embedded_command_sequence;

fuzz_target!(|data: &[u8]| {
    embedded_command_sequence::run(data);
});
