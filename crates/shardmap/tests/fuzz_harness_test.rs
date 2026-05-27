#[path = "../fuzz/support/embedded_command_sequence.rs"]
mod embedded_command_sequence;

#[test]
fn deterministic_embedded_command_sequences_match_model() {
    const SEEDS: [&[u8]; 8] = [
        b"",
        b"\x00",
        b"\xff",
        b"shardmap-fuzz",
        b"mixed strings hashes lists sets zsets",
        b"\x01\x10\x20\x30\x40\x50\x60\x70\x80\x90",
        b"\x17\x00\x00\x05\x23\x42\x99\xff\x7f\x80",
        b"overwrite-types-delete-empty-containers",
    ];

    for seed in SEEDS {
        embedded_command_sequence::run(seed);
    }
}

#[test]
fn generated_embedded_command_sequences_match_model() {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for case_id in 0..128_u64 {
        state ^= case_id.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let len = 1 + (next_u64(&mut state) as usize % 512);
        let mut data = Vec::with_capacity(len);
        for _ in 0..len {
            data.push(next_u64(&mut state) as u8);
        }
        embedded_command_sequence::run(&data);
    }
}

fn next_u64(state: &mut u64) -> u64 {
    let mut value = *state;
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    *state = value;
    value.wrapping_mul(0x2545_f491_4f6c_dd1d)
}
