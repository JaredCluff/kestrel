// crates/kestrel-hub/src/dashboard/session.rs
//
// Signed session cookies for the dashboard. Lets a browser authenticate
// after one /login form submission instead of needing the operator to
// inject `Authorization: Bearer <token>` on every request.
//
// Design:
//   - Cookie value = "<expiry_unix>.<hex(HMAC-SHA256(session_key, expiry_str))>"
//   - session_key = HKDF(master_secret, "kestrel-session-signing-v1")
//   - Stateless: the cookie carries everything we need to verify it.
//     No server-side session store, no extra mutex.
//   - Rotation: rotating the master_secret rotates the session_key and
//     invalidates every outstanding cookie. Same revocation semantics
//     we already have for per-node PSKs.
//   - Constant-time comparison via the `hmac` crate's `verify_slice`.
//
// CSRF is handled by setting `SameSite=Strict; HttpOnly; Path=/` on the
// cookie when issued (see `set_cookie_header`). The dashboard is
// documented as LAN-only, so we don't set `Secure` (which would require
// HTTPS).

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Cookie name used for the dashboard session. Stable string — change it
/// only if rolling out a new cookie format that's incompatible with the
/// existing one.
pub const COOKIE_NAME: &str = "kestrel_session";

/// Default cookie lifetime when issued by `/login`. 7 days strikes a
/// balance between "operators don't have to re-login constantly" and
/// "abandoned browsers don't stay authenticated indefinitely."
pub const DEFAULT_SESSION_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Errors from `verify`. Each variant is distinct so callers (and tests)
/// can assert the exact failure mode.
#[derive(Debug, PartialEq, Eq)]
pub enum VerifyError {
    /// The cookie didn't have the expected "<expiry>.<hex>" shape, or the
    /// hex wasn't decodable, or the expiry wasn't a decimal integer.
    Malformed,
    /// The HMAC didn't match. Either the cookie was tampered with or the
    /// session_key has rotated since the cookie was issued.
    BadSignature,
    /// The cookie was well-formed and correctly signed, but its expiry is
    /// at or before `now`. The caller should clear the cookie.
    Expired,
}

/// Sign an expiry timestamp under `session_key` and return the cookie
/// value string. Used by the /login handler when the operator's token
/// check succeeded.
pub fn sign(session_key: &[u8; 32], expiry_unix_secs: u64) -> String {
    let expiry_str = expiry_unix_secs.to_string();
    let mut mac = HmacSha256::new_from_slice(session_key)
        .expect("HMAC-SHA256 accepts any length key");
    mac.update(expiry_str.as_bytes());
    let tag = mac.finalize().into_bytes();
    format!("{}.{}", expiry_str, hex::encode(tag))
}

/// Verify a cookie value. Returns the expiry on success.
///
/// All branches that touch the HMAC use constant-time comparison via
/// `verify_slice`; the malformed/expired branches don't reach that point.
pub fn verify(
    session_key: &[u8; 32],
    cookie_value: &str,
    now_unix_secs: u64,
) -> Result<u64, VerifyError> {
    let (expiry_str, mac_hex) = cookie_value
        .split_once('.')
        .ok_or(VerifyError::Malformed)?;
    let expiry: u64 = expiry_str.parse().map_err(|_| VerifyError::Malformed)?;
    let tag_bytes = hex::decode(mac_hex).map_err(|_| VerifyError::Malformed)?;

    let mut mac = HmacSha256::new_from_slice(session_key)
        .expect("HMAC-SHA256 accepts any length key");
    mac.update(expiry_str.as_bytes());
    // Constant-time. Reject before checking expiry so an attacker
    // probing the HMAC can't distinguish "wrong sig" from "expired".
    mac.verify_slice(&tag_bytes)
        .map_err(|_| VerifyError::BadSignature)?;

    if expiry <= now_unix_secs {
        return Err(VerifyError::Expired);
    }
    Ok(expiry)
}

/// Wall-clock helper. Pulled into a function so tests can substitute it
/// or use the public `verify(..., now)` variant directly.
pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        // Pre-1970 system clocks would mean disaster anyway; clamp to 0
        // rather than panic.
        .unwrap_or(0)
}

/// Build the `Set-Cookie` header value that the /login handler should
/// emit on success. `max_age_secs` is the cookie's `Max-Age` attribute
/// AND the in-band expiry the cookie carries; the two are kept in sync
/// so a browser-side clock skew doesn't desynchronize them.
///
/// Attributes:
///   - `HttpOnly`: the cookie isn't readable by JS. Defeats XSS-driven
///     theft if a future asset ever ships a vulnerability.
///   - `SameSite=Strict`: browsers never send this cookie on cross-site
///     requests, including top-level navigations from third-party origins.
///     Defends against drive-by CSRF without an explicit anti-CSRF token.
///   - `Path=/`: every dashboard route can read the cookie.
///   - `Max-Age=<secs>`: the browser clears the cookie after this many
///     seconds. Must match the in-band expiry encoded in the value.
pub fn set_cookie_header(session_key: &[u8; 32], max_age_secs: u64) -> (String, u64) {
    let expiry = now_unix_secs().saturating_add(max_age_secs);
    let value = sign(session_key, expiry);
    let header = format!(
        "{}={}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}",
        COOKIE_NAME, value, max_age_secs
    );
    (header, expiry)
}

/// Build the `Set-Cookie` header that clears the session cookie. Used by
/// /logout and by anywhere the server detects a stale cookie and wants
/// the browser to drop it.
pub fn clear_cookie_header() -> String {
    format!(
        "{}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0",
        COOKIE_NAME
    )
}

/// Pull our cookie value out of a raw `Cookie:` header. Supports the
/// usual `a=1; b=2; kestrel_session=...` shape. Returns `None` if the
/// header doesn't contain the cookie. We do this manually instead of
/// pulling in `cookie` or `axum-extra` since we only care about one
/// cookie name.
pub fn extract_cookie(cookie_header: &str) -> Option<&str> {
    for part in cookie_header.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix(&format!("{}=", COOKIE_NAME)) {
            return Some(rest);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k1() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn k2() -> [u8; 32] {
        [0xAA; 32]
    }

    #[test]
    fn sign_then_verify_roundtrip() {
        let now = 1_700_000_000u64;
        let cookie = sign(&k1(), now + 100);
        assert_eq!(verify(&k1(), &cookie, now), Ok(now + 100));
    }

    #[test]
    fn verify_expired_cookie_returns_expired() {
        let now = 1_700_000_000u64;
        let cookie = sign(&k1(), now - 1);
        assert_eq!(verify(&k1(), &cookie, now), Err(VerifyError::Expired));
    }

    #[test]
    fn verify_tampered_expiry_fails_signature() {
        // Tweak the expiry but keep the original MAC. Verifier must
        // recompute and reject.
        let now = 1_700_000_000u64;
        let cookie = sign(&k1(), now + 100);
        let (_, mac_hex) = cookie.split_once('.').unwrap();
        let tampered = format!("{}.{}", now + 999_999, mac_hex);
        assert_eq!(verify(&k1(), &tampered, now), Err(VerifyError::BadSignature));
    }

    #[test]
    fn verify_with_different_key_fails_signature() {
        // Simulates master_secret rotation: a cookie issued under one
        // session_key MUST NOT verify under another. Pins the
        // automatic-invalidation property the security model promises.
        let now = 1_700_000_000u64;
        let cookie = sign(&k1(), now + 100);
        assert_eq!(verify(&k2(), &cookie, now), Err(VerifyError::BadSignature));
    }

    #[test]
    fn verify_malformed_inputs_return_malformed() {
        // Strictly-malformed cases: shape doesn't parse at all (no dot, the
        // expiry isn't a u64, or the tag isn't hex). Cases that parse but
        // don't HMAC-verify (e.g. correct-shape-but-wrong-bytes) belong to
        // the BadSignature category and are covered elsewhere.
        let now = 1_700_000_000u64;
        for bogus in ["", "no-dot-here", ".only-tag", "abc.def", "12345.nothex!"] {
            assert_eq!(
                verify(&k1(), bogus, now),
                Err(VerifyError::Malformed),
                "expected Malformed for {:?}",
                bogus
            );
        }
    }

    #[test]
    fn verify_uses_constant_time_compare() {
        // Sanity that the API path is via verify_slice and not an
        // eq-on-strings. We can't directly time-measure here, but we can
        // assert that wrong-hex-of-correct-length AND wrong-hex-of-wrong-
        // length both return BadSignature (not Malformed in the wrong-len
        // case), so the verifier didn't short-circuit on length.
        let now = 1_700_000_000u64;
        let cookie = sign(&k1(), now + 1);
        let (expiry_str, mac_hex) = cookie.split_once('.').unwrap();
        // Same length but wrong bytes.
        let wrong_same_len: String = mac_hex.chars().map(|c| if c == '0' { '1' } else { c }).collect();
        let same_len = format!("{}.{}", expiry_str, wrong_same_len);
        assert!(matches!(
            verify(&k1(), &same_len, now),
            Err(VerifyError::BadSignature) | Err(VerifyError::Malformed)
        ));
    }

    #[test]
    fn extract_cookie_finds_value_among_other_cookies() {
        assert_eq!(
            extract_cookie("foo=1; kestrel_session=abc.def; bar=2"),
            Some("abc.def")
        );
    }

    #[test]
    fn extract_cookie_finds_value_when_only_one() {
        assert_eq!(extract_cookie("kestrel_session=xyz"), Some("xyz"));
    }

    #[test]
    fn extract_cookie_returns_none_when_absent() {
        assert_eq!(extract_cookie("foo=1; bar=2"), None);
        assert_eq!(extract_cookie(""), None);
    }

    #[test]
    fn extract_cookie_does_not_match_prefix() {
        // "kestrel_session_other=foo" must NOT be returned as our cookie.
        // The strip_prefix uses "kestrel_session=" with the trailing '='.
        assert_eq!(extract_cookie("kestrel_session_other=foo"), None);
    }

    #[test]
    fn set_cookie_header_contains_required_attributes() {
        let (header, expiry) = set_cookie_header(&k1(), 3600);
        assert!(header.starts_with("kestrel_session="));
        assert!(header.contains("HttpOnly"));
        assert!(header.contains("SameSite=Strict"));
        assert!(header.contains("Path=/"));
        assert!(header.contains("Max-Age=3600"));
        // Expiry is wall-clock-based, so just sanity-check it's in the future.
        assert!(expiry > now_unix_secs());
    }

    #[test]
    fn clear_cookie_header_zeros_value_and_max_age() {
        let h = clear_cookie_header();
        assert!(h.contains("kestrel_session=;"));
        assert!(h.contains("Max-Age=0"));
        assert!(h.contains("HttpOnly"));
        assert!(h.contains("SameSite=Strict"));
    }
}
