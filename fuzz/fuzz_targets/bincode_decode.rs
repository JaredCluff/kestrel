// fuzz/fuzz_targets/bincode_decode.rs
//
// Fuzz target for the bincode KestrelMessage decoder. The agent and
// hub both feed network bytes into this decoder; if a malformed frame
// can panic, an attacker on the WS path can DoS either side.
//
// Property: `decode_from_slice` must NEVER panic on arbitrary input.
// Errors are fine (and expected for most inputs); panics are not.
//
// Run:
//   cd fuzz && cargo +nightly fuzz run bincode_decode

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = bincode::serde::decode_from_slice::<kestrel_proto::KestrelMessage, _>(
        data,
        bincode::config::standard(),
    );
    // No assertions — test passes by completing without panic.
});
