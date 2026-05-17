use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Compute HMAC-SHA256(psk, nonce) — the hub's proof-of-key response.
pub fn hmac_response(psk: &[u8], nonce: &[u8; 32]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(psk).expect("HMAC accepts any key length");
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

/// Constant-time verification of an auth response MAC.
pub fn verify_response(psk: &[u8], nonce: &[u8; 32], mac: &[u8; 32]) -> bool {
    let expected = hmac_response(psk, nonce);
    constant_time_eq(&expected, mac)
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_verify_roundtrip() {
        let psk = b"super-secret-key-for-testing-only";
        let nonce = [0xABu8; 32];
        let mac = hmac_response(psk, &nonce);
        assert!(verify_response(psk, &nonce, &mac));
    }

    #[test]
    fn wrong_key_fails() {
        let nonce = [1u8; 32];
        let mac = hmac_response(b"correct-key", &nonce);
        assert!(!verify_response(b"wrong-key", &nonce, &mac));
    }

    #[test]
    fn wrong_nonce_fails() {
        let psk = b"some-psk";
        let mac = hmac_response(psk, &[1u8; 32]);
        assert!(!verify_response(psk, &[2u8; 32], &mac));
    }
}
