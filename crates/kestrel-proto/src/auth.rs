use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Domain-separation label for the TLS exporter input. Change this string to
/// invalidate all existing auth tokens (e.g., if the auth construction is ever
/// substantively revised).
pub const AUTH_EXPORTER_LABEL: &[u8] = b"kestrel auth v1";

/// Domain-separation prefix for HKDF-derived per-node PSKs. The HKDF info
/// parameter is this prefix concatenated with the UTF-8 bytes of the
/// node_id. The `v1` suffix lets us rotate derivation later without
/// silently colliding with an old install's derived keys.
pub const NODE_PSK_INFO_PREFIX: &[u8] = b"kestrel-node-psk-v1:";

/// Derive a per-node PSK from a hub-side master secret and a node identifier.
///
/// Uses HKDF-SHA256 with the master_secret as IKM, an empty salt, and an
/// info parameter that combines `NODE_PSK_INFO_PREFIX` with the node_id
/// bytes. The 32-byte output is suitable as a direct PSK for the HMAC
/// challenge-response in [`hmac_response`] / [`verify_response`].
///
/// Properties this gives us:
/// - Each node gets a distinct PSK. A leaked agent PSK does not compromise
///   any other node nor the master secret (HKDF is a one-way KDF).
/// - The hub only needs to remember the master_secret; per-node PSKs are
///   re-derived on each connect from the configured node_id.
/// - Derivation is deterministic, so re-enrolling a node from the same
///   master_secret produces the same PSK.
pub fn derive_per_node_psk(master_secret: &[u8], node_id: &str) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(None, master_secret);
    let mut info: Vec<u8> = Vec::with_capacity(NODE_PSK_INFO_PREFIX.len() + node_id.len());
    info.extend_from_slice(NODE_PSK_INFO_PREFIX);
    info.extend_from_slice(node_id.as_bytes());
    let mut out = [0u8; 32];
    hk.expand(&info, &mut out)
        .expect("32-byte output is well under HKDF-SHA256's L*HashLen=255*32 cap");
    out
}

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

    #[test]
    fn derive_per_node_psk_is_deterministic() {
        // Same master + same node_id => same PSK. This is what lets the hub
        // re-derive a node's PSK on every connect without storing it.
        let master = b"hub-master-secret-32bytes-test!!";
        let k1 = derive_per_node_psk(master, "alpha");
        let k2 = derive_per_node_psk(master, "alpha");
        assert_eq!(k1, k2);
    }

    #[test]
    fn derive_per_node_psk_differs_per_node() {
        // Different node_ids under the same master MUST produce distinct
        // PSKs — otherwise the whole point of per-node PSKs is defeated.
        let master = b"hub-master-secret-32bytes-test!!";
        let alpha = derive_per_node_psk(master, "alpha");
        let beta = derive_per_node_psk(master, "beta");
        assert_ne!(alpha, beta);
    }

    #[test]
    fn derive_per_node_psk_differs_per_master() {
        // Re-running `kestrel-hub init` with a fresh master MUST yield a
        // different per-node PSK; otherwise rotating the master wouldn't
        // actually rotate anything.
        let m1 = b"master-one--------32bytes-test!!";
        let m2 = b"master-two--------32bytes-test!!";
        let k1 = derive_per_node_psk(m1, "alpha");
        let k2 = derive_per_node_psk(m2, "alpha");
        assert_ne!(k1, k2);
    }

    #[test]
    fn derive_per_node_psk_handles_empty_node_id() {
        // Defensive: empty node_id is a degenerate input. HKDF must still
        // produce a 32-byte output without panicking. It's the caller's
        // job to reject empty node_ids at the config layer; this just
        // proves the primitive is total.
        let master = b"some-master";
        let k = derive_per_node_psk(master, "");
        assert_eq!(k.len(), 32);
    }

    #[test]
    fn derive_per_node_psk_handles_unicode_node_id() {
        // node_ids are UTF-8 strings in our config schema, so the derivation
        // must work with any valid str — not just ASCII.
        let master = b"master";
        let a = derive_per_node_psk(master, "ノード-α");
        let b = derive_per_node_psk(master, "node-alpha");
        assert_eq!(a.len(), 32);
        assert_ne!(a, b);
    }

    #[test]
    fn derive_per_node_psk_full_handshake_roundtrip() {
        // End-to-end check: a hub that derives the per-node PSK can
        // authenticate to an agent that has the same derived PSK stored.
        // This is the property the real handshake will rely on.
        let master = b"hub-master";
        let node_id = "alpha";
        let hub_view = derive_per_node_psk(master, node_id);
        let agent_view = derive_per_node_psk(master, node_id); // simulates the agent's stored PSK

        let nonce = [0x7Au8; 32];
        let mac = hmac_response(&hub_view, &nonce, EXPORTER_A);
        assert!(verify_response(&agent_view, &nonce, EXPORTER_A, &mac));
    }

    #[test]
    fn cross_node_psk_does_not_authenticate() {
        // The point of per-node PSKs: a leaked agent PSK cannot authenticate
        // as a different node. Concretely, if node beta's PSK is used to
        // generate a MAC, it must NOT verify against node alpha's derived
        // PSK.
        let master = b"hub-master";
        let alpha = derive_per_node_psk(master, "alpha");
        let beta = derive_per_node_psk(master, "beta");

        let nonce = [0x33u8; 32];
        let mac_with_beta = hmac_response(&beta, &nonce, EXPORTER_A);
        assert!(!verify_response(&alpha, &nonce, EXPORTER_A, &mac_with_beta));
    }
}
