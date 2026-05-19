use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Domain-separation label for the TLS exporter input. Change this string to
/// invalidate all existing auth tokens (e.g., if the auth construction is ever
/// substantively revised).
pub const AUTH_EXPORTER_LABEL: &[u8] = b"kestrel auth v1";

/// Compute `HMAC-SHA256(psk, nonce || tls_exporter)`. The `tls_exporter` is
/// 32 bytes of keying material derived from the TLS session via
/// `ConnectionCommon::export_keying_material(out, AUTH_EXPORTER_LABEL, None)`.
///
/// Binding the MAC to the TLS exporter prevents a LAN MITM with a self-signed
/// cert from proxying the handshake: the exporter is unique per TLS session,
/// so a MITM that terminates TLS on each leg sees a different exporter than
/// the legitimate endpoint, and the MAC won't verify on the far side.
pub fn hmac_response(psk: &[u8], nonce: &[u8; 32], tls_exporter: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(psk).expect("PSK must be non-empty");
    mac.update(nonce);
    mac.update(tls_exporter);
    mac.finalize().into_bytes().into()
}

/// Constant-time verification of an auth response MAC. See `hmac_response`.
pub fn verify_response(
    psk: &[u8],
    nonce: &[u8; 32],
    tls_exporter: &[u8],
    mac: &[u8; 32],
) -> bool {
    let mut m = HmacSha256::new_from_slice(psk).expect("PSK must be non-empty");
    m.update(nonce);
    m.update(tls_exporter);
    m.verify_slice(mac).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPORTER_A: &[u8; 32] = &[0xEE; 32];
    const EXPORTER_B: &[u8; 32] = &[0xCC; 32];

    #[test]
    fn hmac_verify_roundtrip() {
        let psk = b"super-secret-key-for-testing-only";
        let nonce = [0xABu8; 32];
        let mac = hmac_response(psk, &nonce, EXPORTER_A);
        assert!(verify_response(psk, &nonce, EXPORTER_A, &mac));
    }

    #[test]
    fn wrong_key_fails() {
        let nonce = [1u8; 32];
        let mac = hmac_response(b"correct-key", &nonce, EXPORTER_A);
        assert!(!verify_response(b"wrong-key", &nonce, EXPORTER_A, &mac));
    }

    #[test]
    fn wrong_nonce_fails() {
        let psk = b"some-psk";
        let mac = hmac_response(psk, &[1u8; 32], EXPORTER_A);
        assert!(!verify_response(psk, &[2u8; 32], EXPORTER_A, &mac));
    }

    #[test]
    fn mismatched_exporter_fails() {
        // This is the MITM defense: if the two endpoints see different TLS
        // sessions (because an attacker terminates TLS on each leg), their
        // exporter material differs and the MAC can't be reused.
        let psk = b"some-psk";
        let nonce = [7u8; 32];
        let mac = hmac_response(psk, &nonce, EXPORTER_A);
        assert!(!verify_response(psk, &nonce, EXPORTER_B, &mac));
    }

    #[test]
    fn empty_exporter_still_works_but_changes_the_mac() {
        // Sanity: passing an empty exporter (legacy / unbound mode) is a
        // distinct MAC from any non-empty exporter. We don't expose an
        // "empty exporter" code path in production — this is just confirming
        // the function is total.
        let psk = b"k";
        let nonce = [0u8; 32];
        let mac_empty = hmac_response(psk, &nonce, &[]);
        let mac_with = hmac_response(psk, &nonce, EXPORTER_A);
        assert_ne!(mac_empty, mac_with);
    }
}
