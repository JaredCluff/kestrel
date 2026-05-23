// crates/kestrel-hub/src/audit.rs
//
// Append-only audit log for MCP tool calls. Every operator-initiated
// action against an agent (screenshot, type_text, shell_run, ...) is
// recorded as a single JSON Lines entry with timestamp, op name,
// node_id, an args summary string, result status, duration, and the
// error message on failure.
//
// Schema (one JSON object per line):
//   {
//     "ts_unix": 1734567890,
//     "ts": "2026-05-23T14:24:50Z",       // RFC-3339 UTC
//     "op": "screenshot",
//     "node_id": "macstudio",
//     "args": "display=0",
//     "status": "ok",                      // or "error"
//     "duration_ms": 142,
//     "error": null                        // string when status="error"
//   }
//
// Why JSONL: it's append-friendly, line-oriented (works with `tail -f`),
// trivially grep/jq-able, and tolerates concurrent appenders on POSIX
// (writes of less than PIPE_BUF are atomic).
//
// IMPORTANT: the audit log MUST NOT be stdout. The MCP server speaks
// JSON-RPC over stdio; writing audit lines to stdout would corrupt the
// protocol stream. Callers must pass a real path or use `disabled()`.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Status of a single MCP tool call.
#[derive(Debug, Clone, Copy)]
pub enum CallStatus {
    Ok,
    Error,
}

impl CallStatus {
    fn as_str(self) -> &'static str {
        match self {
            CallStatus::Ok => "ok",
            CallStatus::Error => "error",
        }
    }
}

/// Audit logger that appends JSON Lines entries to a file. Cloneable
/// (the underlying file lives behind an Arc<Mutex<>>), so it can be
/// freely shared across handlers.
///
/// A `disabled` variant short-circuits all writes — useful for tests
/// and for installs that don't want an audit log on disk.
#[derive(Clone)]
pub struct AuditLogger {
    inner: Arc<AuditInner>,
}

enum AuditInner {
    Disabled,
    File {
        // Mutex serializes concurrent appends. Tokio's async file API
        // doesn't guarantee atomicity for multiple parallel write_all
        // calls; a brief lock around format + write is simpler than
        // wrangling pwrite/pwrite_at across platforms.
        file: Mutex<File>,
    },
}

impl AuditLogger {
    /// Audit-disabled logger. All `log` calls become no-ops.
    pub fn disabled() -> Self {
        AuditLogger {
            inner: Arc::new(AuditInner::Disabled),
        }
    }

    /// Open `path` in append mode (creating if absent). Returns Err if
    /// the file can't be opened; callers can fall back to `disabled()`
    /// with a warning rather than failing hub startup.
    pub async fn file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())
            .await
            .map_err(|e| {
                anyhow::anyhow!("open audit log {:?}: {}", path.as_ref(), e)
            })?;
        Ok(AuditLogger {
            inner: Arc::new(AuditInner::File {
                file: Mutex::new(file),
            }),
        })
    }

    /// Append one entry. Best-effort: write errors are logged via
    /// `tracing::warn!` but never propagated — an unwriteable audit
    /// log must not break MCP tool calls.
    pub async fn log_with_user(
        &self,
        user_id: Option<&str>,
        op: &str,
        node_id: &str,
        args_summary: &str,
        status: CallStatus,
        duration_ms: u64,
        error: Option<&str>,
    ) {
        let AuditInner::File { file } = &*self.inner else {
            return;
        };
        let ts_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let ts_human = format_rfc3339_utc(ts_unix);
        let user_field = match user_id {
            Some(u) => format!("\"{}\"", json_escape(u)),
            None => "null".to_string(),
        };
        let line = format!(
            "{{\"ts_unix\":{},\"ts\":\"{}\",\"user_id\":{},\"op\":\"{}\",\"node_id\":\"{}\",\"args\":\"{}\",\"status\":\"{}\",\"duration_ms\":{},\"error\":{}}}\n",
            ts_unix, ts_human, user_field,
            json_escape(op), json_escape(node_id), json_escape(args_summary),
            status.as_str(), duration_ms,
            match error {
                Some(e) => format!("\"{}\"", json_escape(e)),
                None => "null".to_string(),
            }
        );
        let mut f = file.lock().await;
        if let Err(e) = f.write_all(line.as_bytes()).await {
            tracing::warn!("audit log write failed: {}", e);
        }
    }

    pub async fn log(
        &self,
        op: &str,
        node_id: &str,
        args_summary: &str,
        status: CallStatus,
        duration_ms: u64,
        error: Option<&str>,
    ) {
        let AuditInner::File { file } = &*self.inner else {
            return;
        };
        let ts_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let ts_human = format_rfc3339_utc(ts_unix);
        let line = format!(
            "{{\"ts_unix\":{},\"ts\":\"{}\",\"op\":\"{}\",\"node_id\":\"{}\",\"args\":\"{}\",\"status\":\"{}\",\"duration_ms\":{},\"error\":{}}}\n",
            ts_unix,
            ts_human,
            json_escape(op),
            json_escape(node_id),
            json_escape(args_summary),
            status.as_str(),
            duration_ms,
            match error {
                Some(e) => format!("\"{}\"", json_escape(e)),
                None => "null".to_string(),
            }
        );
        let mut f = file.lock().await;
        if let Err(e) = f.write_all(line.as_bytes()).await {
            tracing::warn!("audit log write failed: {}", e);
        }
        // Flush is intentionally NOT called per-write — the OS buffer
        // flushes on file drop, on close, or on explicit fsync. We
        // accept the small loss window in exchange for higher throughput
        // under bursts of tool calls.
    }
}

/// Convert a Unix timestamp to RFC-3339 UTC ("YYYY-MM-DDTHH:MM:SSZ").
/// No external date crate; the math is straightforward for our needs
/// and we already pull `chrono`-shaped formatting nowhere else in the
/// hub. Years 1970-9999 supported; not Y10K-safe.
fn format_rfc3339_utc(ts_unix: u64) -> String {
    let secs = ts_unix;
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let mut days = secs / 86400;

    let mut year: u64 = 1970;
    loop {
        let dy = if is_leap(year) { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let days_in_month: [u64; 12] = [
        31,
        if is_leap(year) { 29 } else { 28 },
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ];
    let mut month: usize = 0;
    while month < 12 && days >= days_in_month[month] {
        days -= days_in_month[month];
        month += 1;
    }
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month + 1,
        days + 1,
        h,
        m,
        s
    )
}

fn is_leap(y: u64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Minimal JSON-string escape. We only have to escape backslash, quote,
/// and control characters; everything else is passed through. UTF-8 is
/// fine inside JSON strings.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_known_timestamps() {
        // Epoch.
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00Z");
        // 2025-05-23 14:24:50 UTC. Confirmed via `date -u -r 1748010290`.
        assert_eq!(format_rfc3339_utc(1748010290), "2025-05-23T14:24:50Z");
        // Leap day inside a leap year. 2024-02-29 00:00:00 UTC = 1709164800.
        // Confirmed via `date -u -r 1709164800`.
        assert_eq!(format_rfc3339_utc(1709164800), "2024-02-29T00:00:00Z");
    }

    #[test]
    fn json_escape_quotes_backslashes_and_newlines() {
        assert_eq!(json_escape("hello"), "hello");
        assert_eq!(json_escape("a\"b"), "a\\\"b");
        assert_eq!(json_escape("a\\b"), "a\\\\b");
        assert_eq!(json_escape("a\nb"), "a\\nb");
        // Control char (0x07 = BEL) is \u-escaped.
        assert_eq!(json_escape("a\u{0007}b"), "a\\u0007b");
        // High codepoints pass through as UTF-8 inside the string.
        assert_eq!(json_escape("héllo-α"), "héllo-α");
    }

    #[tokio::test]
    async fn disabled_logger_is_a_noop() {
        let log = AuditLogger::disabled();
        // No file involved, no panic. The point is that `log` can be
        // called from a tool handler without the handler caring whether
        // audit is on.
        log.log("screenshot", "n", "display=0", CallStatus::Ok, 5, None)
            .await;
    }

    #[tokio::test]
    async fn file_logger_appends_jsonl_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let log = AuditLogger::file(&path).await.unwrap();

        log.log("screenshot", "macstudio", "display=0", CallStatus::Ok, 142, None)
            .await;
        log.log(
            "shell_run",
            "macstudio",
            "command=echo hi",
            CallStatus::Error,
            18,
            Some("node 'macstudio' not connected"),
        )
        .await;
        // Drop the logger so the underlying file flushes on close.
        drop(log);

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "expected 2 lines, got: {:?}", contents);
        // Round-trip each line through serde_json to confirm valid JSON.
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("invalid JSON: {} (line: {})", e, line));
        }
        assert!(lines[0].contains("\"op\":\"screenshot\""));
        assert!(lines[0].contains("\"status\":\"ok\""));
        assert!(lines[0].contains("\"duration_ms\":142"));
        assert!(lines[0].contains("\"error\":null"));
        assert!(lines[1].contains("\"status\":\"error\""));
        assert!(lines[1].contains("not connected"));
    }

    #[tokio::test]
    async fn file_logger_handles_unicode_and_quotes_in_fields() {
        // node_ids and args summaries are operator-controlled — they
        // can contain quotes, backslashes, unicode. The serializer
        // must produce valid JSON in every case.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let log = AuditLogger::file(&path).await.unwrap();
        log.log(
            "type_text",
            "node-α",
            r#"text="hi \n there""#,
            CallStatus::Ok,
            5,
            None,
        )
        .await;
        drop(log);
        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(parsed["node_id"], "node-α");
        assert_eq!(parsed["args"], r#"text="hi \n there""#);
    }

    #[tokio::test]
    async fn file_logger_concurrent_appends_dont_interleave() {
        // Two concurrent logs MUST produce two complete lines, not one
        // interleaved garbage line. The internal Mutex around the file
        // write is what gets us that. We don't try to assert ordering —
        // either order is fine — just that both lines round-trip.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let log = AuditLogger::file(&path).await.unwrap();
        let a = log.clone();
        let b = log.clone();
        let h1 = tokio::spawn(async move {
            a.log("op_a", "n1", "args_a", CallStatus::Ok, 1, None).await;
        });
        let h2 = tokio::spawn(async move {
            b.log("op_b", "n2", "args_b", CallStatus::Ok, 2, None).await;
        });
        let _ = tokio::join!(h1, h2);
        drop(log);
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }
}
