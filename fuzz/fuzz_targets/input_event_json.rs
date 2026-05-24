// fuzz/fuzz_targets/input_event_json.rs
//
// Fuzz target for the InputEvent JSON parser. Browsers ship these
// over the WebRTC data channel; the parser is in a security-critical
// position (operator-trusted but pages-untrusted).
//
// Property: `serde_json::from_str::<InputEvent>` must NEVER panic.
//
// Run:
//   cd fuzz && cargo +nightly fuzz run input_event_json

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The fuzzer can supply non-UTF8 bytes; the JSON parser only
    // sees a &str, so we round-trip via lossy decode. Lossy decoding
    // can't itself panic (it inserts U+FFFD for invalid sequences).
    let s = String::from_utf8_lossy(data);
    let _ = serde_json::from_str::<
        kestrel_agent::capabilities::webrtc_session::InputEvent,
    >(&s);
});
