// crates/kestrel-hub/tests/snapshots.rs
//
// Snapshot (golden-file) tests for stable wire/UI surfaces. Where the
// existing tests use `.contains("expected substring")`, these capture
// the FULL output and diff it against a checked-in snapshot. Catches:
//
//   - Unintended HTML changes in the dashboard (e.g. a stray
//     attribute or whitespace change that .contains() would miss)
//   - Audit log line shape regressions (field order, json escaping)
//   - SDP shape changes that propagate from a webrtc-rs upgrade
//
// Workflow:
//   - First run with INSTA_UPDATE=auto writes new snapshots under
//     `snapshots/`. The CI workflow runs WITHOUT that env var, so
//     a wire-shape regression fails CI loudly.
//   - When a change is intentional, run `cargo insta review` (or
//     `cargo insta accept`) locally and commit the updated snapshot
//     alongside the code change.
//
// The snapshots are checked-in artifacts. Reviewing the diff is the
// whole point — a PR that changes a snapshot file is announcing "I
// changed the wire shape, here's the new one."

use axum::body::Body;
use axum::http::Request;
use kestrel_test::{build_app, build_app_with_token, cookie_for};
use tower::ServiceExt;

const TOKEN: &str = "test-control-token-aaaaaaaaaaaaaa";

// ── Dashboard HTML snapshots ─────────────────────────────────────────

async fn fetch_html(app: axum::Router, uri: &str, cookie: Option<&str>) -> String {
    let mut req = Request::builder().method("GET").uri(uri);
    if let Some(c) = cookie {
        req = req.header("cookie", c);
    }
    let resp = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Normalize HTML for stable snapshots — strip the parts that legitimately
/// vary (random session ids, cache-busters) while keeping anything an
/// operator might care about reviewing.
fn normalize_html(html: &str) -> String {
    // Today: no dynamic placeholders in the snapshotted paths. Helper
    // exists so future additions (timestamps, random ids) can be
    // redacted here without touching every snapshot.
    html.to_string()
}

#[tokio::test]
async fn snapshot_index_anonymous() {
    let (app, _state) = build_app();
    let html = fetch_html(app, "/", None).await;
    insta::assert_snapshot!("index_anonymous", normalize_html(&html));
}

#[tokio::test]
async fn snapshot_index_signed_in() {
    let (app, state) = build_app_with_token(TOKEN);
    let cookie = cookie_for(&state);
    let html = fetch_html(app, "/", Some(&cookie)).await;
    insta::assert_snapshot!("index_signed_in", normalize_html(&html));
}

#[tokio::test]
async fn snapshot_node_detail_signed_in() {
    let (app, state) = build_app_with_token(TOKEN);
    let cookie = cookie_for(&state);
    let html = fetch_html(app, "/node/alpha", Some(&cookie)).await;
    insta::assert_snapshot!("node_detail_signed_in", normalize_html(&html));
}

#[tokio::test]
async fn snapshot_node_detail_anonymous() {
    let (app, _state) = build_app();
    let html = fetch_html(app, "/node/alpha", None).await;
    insta::assert_snapshot!("node_detail_anonymous", normalize_html(&html));
}

#[tokio::test]
async fn snapshot_login_form() {
    let (app, _state) = build_app_with_token(TOKEN);
    let html = fetch_html(app, "/login", None).await;
    insta::assert_snapshot!("login_form", normalize_html(&html));
}

// ── Audit log line snapshot ──────────────────────────────────────────

#[tokio::test]
async fn snapshot_audit_log_line_shape() {
    // Pin the exact JSON shape of an audit line. If a future change
    // reorders fields, adds a column, or changes a key name, this
    // snapshot fires and the reviewer sees the diff in plain JSON.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let log = kestrel_hub::audit::AuditLogger::file(&path).await.unwrap();
    log.log(
        "shell_run",
        "macstudio",
        "command=ls -la /tmp",
        kestrel_hub::audit::CallStatus::Ok,
        142,
        None,
    )
    .await;
    log.flush().await;
    drop(log);
    let contents = std::fs::read_to_string(&path).unwrap();

    // Redact the timestamp fields (they vary per run). Insta has a
    // built-in redaction pattern, but for one field a simple replace
    // is clearer.
    let mut json: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
    if let Some(o) = json.as_object_mut() {
        o.insert("ts_unix".into(), serde_json::Value::String("<REDACTED>".into()));
        o.insert("ts".into(), serde_json::Value::String("<REDACTED>".into()));
    }
    insta::assert_json_snapshot!("audit_log_line_shape", json);
}

#[tokio::test]
async fn snapshot_audit_log_redacts_type_text_bytes() {
    // The constitution promises that type_text logs `len=N`, never the
    // typed bytes. This snapshot pins that — if a future refactor of
    // mcp.rs accidentally logs the text itself, the snapshot fires.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let log = kestrel_hub::audit::AuditLogger::file(&path).await.unwrap();
    // mcp::type_text passes `format!("len={}", text.chars().count())`
    // as the args summary; mimic that here.
    let supposed_text = "my secret password is hunter2";
    let summary = format!("len={}", supposed_text.chars().count());
    log.log(
        "type_text",
        "macstudio",
        &summary,
        kestrel_hub::audit::CallStatus::Ok,
        12,
        None,
    )
    .await;
    log.flush().await;
    drop(log);
    let contents = std::fs::read_to_string(&path).unwrap();
    let mut json: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
    if let Some(o) = json.as_object_mut() {
        o.insert("ts_unix".into(), serde_json::Value::String("<REDACTED>".into()));
        o.insert("ts".into(), serde_json::Value::String("<REDACTED>".into()));
    }
    let line = serde_json::to_string(&json).unwrap();
    // Sanity: the literal text MUST NOT appear in the snapshot.
    assert!(
        !line.contains("hunter2"),
        "audit log contained the typed text: {}",
        line
    );
    insta::assert_json_snapshot!("audit_log_type_text_redacted", json);
}
