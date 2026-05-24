// crates/kestrel-hub/tests/load.rs
//
// Concurrency / load tests for the caps and throughput properties we
// rely on in production. These aren't speed benchmarks — they're
// SHAPE pins: "does the WebRTC session cap engage at exactly the
// configured limit when 20 sessions try to create at once" and so on.
//
// Each test runs in well under a second. We're not stress-testing
// performance; we're verifying that the cap-and-cleanup contracts
// hold under concurrent pressure, which `.contains()` tests can't
// observe.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kestrel_hub::audit::{AuditLogger, CallStatus};
use kestrel_hub::sandbox::SandboxRegistry;
use kestrel_hub::webrtc::SessionRegistry;

// ── WebRTC session cap under concurrent create ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webrtc_session_cap_engages_under_concurrent_create() {
    // 20 concurrent create() calls against a cap of 5. Exactly 5
    // must succeed; 15 must observe `None` (cap reached). Closed
    // sessions don't count, so the count is deterministic.
    let reg = SessionRegistry::new().with_max_concurrent(5);
    let mut handles = Vec::with_capacity(20);
    for i in 0..20 {
        let r = reg.clone();
        handles.push(tokio::spawn(async move {
            r.create(format!("node-{}", i)).await.is_some()
        }));
    }
    let mut accepted = 0;
    for h in handles {
        if h.await.unwrap() {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, 5,
        "exactly cap-many sessions should be created (got {} of 20 with cap=5)",
        accepted
    );
    // And the registry should hold exactly 5 entries.
    assert_eq!(reg.list().await.len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webrtc_session_cap_releases_after_close() {
    // Fill the cap, close every session, fill it again. Verifies the
    // "Closed entries don't count" branch holds under churn (not just
    // the one-shot test we already have).
    let reg = SessionRegistry::new().with_max_concurrent(3);
    let mut ids = Vec::new();
    for i in 0..3 {
        ids.push(reg.create(format!("a-{}", i)).await.unwrap());
    }
    // Over cap.
    assert!(reg.create("overflow".into()).await.is_none());
    // Close them all.
    for id in &ids {
        assert!(reg.mark_closed(id).await);
    }
    // Now 3 fresh sessions should fit.
    for i in 0..3 {
        assert!(
            reg.create(format!("b-{}", i)).await.is_some(),
            "session {} should have fit after closes",
            i
        );
    }
}

// ── Sandbox cap under concurrent spawn ──────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_cap_engages_under_concurrent_spawn() {
    let reg = SandboxRegistry::new().with_max_concurrent(3);
    let mut handles = Vec::with_capacity(10);
    for _ in 0..10 {
        let r = reg.clone();
        handles.push(tokio::spawn(async move {
            r.spawn("test-image", 3600).await.is_ok()
        }));
    }
    let mut accepted = 0;
    for h in handles {
        if h.await.unwrap() {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, 3,
        "expected exactly cap=3 successful spawns out of 10 concurrent (got {})",
        accepted
    );
}

// ── Audit log throughput ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_log_handles_thousand_concurrent_writes_without_loss() {
    // Spray 1000 audit lines across 16 concurrent writers; verify
    // we end up with exactly 1000 well-formed JSON lines. This pins
    // both the no-interleave property AND the no-drop property —
    // a regression in the inner Mutex would break either.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let log = AuditLogger::file(&path).await.unwrap();

    const TOTAL: usize = 1000;
    const WRITERS: usize = 16;
    let per_writer = TOTAL / WRITERS;
    let mut handles = Vec::with_capacity(WRITERS);
    for w in 0..WRITERS {
        let log = log.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per_writer {
                log.log(
                    "shell_run",
                    "macstudio",
                    &format!("writer={},seq={}", w, i),
                    CallStatus::Ok,
                    1,
                    None,
                )
                .await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    log.flush().await;
    drop(log);

    let contents = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    // We schedule per_writer * WRITERS = 992 writes (TOTAL is 1000
    // but per_writer rounds down). Assert against the actual scheduled
    // count, not TOTAL.
    assert_eq!(
        lines.len(),
        per_writer * WRITERS,
        "expected exactly {} audit lines, got {}",
        per_writer * WRITERS,
        lines.len()
    );
    // Every line must be valid JSON — no partial / interleaved writes.
    for (idx, line) in lines.iter().enumerate() {
        serde_json::from_str::<serde_json::Value>(line).unwrap_or_else(|e| {
            panic!("line {} is not valid JSON: {} (line: {:?})", idx, e, line)
        });
    }
}

#[tokio::test]
async fn audit_log_throughput_baseline() {
    // Not a perf test in the gating sense — just a smoke check that
    // we can sustain >=1000 writes/sec on the harness's tempdir.
    // If a future change introduces a per-write fsync this drops
    // by 100× and the test catches it before merge.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let log = AuditLogger::file(&path).await.unwrap();

    const N: usize = 1000;
    let start = Instant::now();
    for i in 0..N {
        log.log(
            "shell_run",
            "macstudio",
            &format!("seq={}", i),
            CallStatus::Ok,
            1,
            None,
        )
        .await;
    }
    let elapsed = start.elapsed();
    let writes_per_sec = (N as f64) / elapsed.as_secs_f64();
    assert!(
        writes_per_sec >= 1000.0,
        "audit log throughput dropped below 1000/sec (got {:.0}/sec, elapsed={:?}). \
         A per-write fsync was likely added accidentally.",
        writes_per_sec,
        elapsed
    );
}

// ── Supervisor jitter spread under concurrent reconnects ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_jitter_spreads_concurrent_reconnects() {
    // Sample backoff_with_jitter many times in parallel and verify
    // the distribution actually spreads (which is the whole point —
    // see the README's "no thundering herd"). Without jitter every
    // sample would equal the base; with jitter we should see a
    // healthy spread across the ±25% window.
    //
    // We can't import `backoff_with_jitter` directly (private), so
    // exercise it via the public `spawn`'s observable behavior:
    // spawn 8 supervisors all aimed at the same closed port and time
    // their first Disconnected. The first reconnect (attempt=1) sleeps
    // backoff_with_jitter(0) = 1s ±25%. Without jitter all 8 would
    // fire simultaneously; with jitter, the first and last should
    // differ by a measurable amount.
    use kestrel_hub::config::NodeConfig;
    use kestrel_hub::events::NodeEvent;
    use kestrel_hub::router::NodeRegistry;
    use kestrel_hub::supervisor;

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let spawn_time = Instant::now();
    // supervisor::spawn returns a JoinHandle, not a Future, so the
    // task starts as soon as we call it. We collect the handles to
    // satisfy clippy's "non-binding let on a future" lint AND so the
    // test owns the lifetime — when handles drop at end of scope,
    // tokio aborts the supervisor tasks. Otherwise they'd leak into
    // other tests sharing the runtime.
    let mut sup_handles = Vec::with_capacity(8);
    for i in 0..8 {
        sup_handles.push(supervisor::spawn(
            NodeConfig {
                node_id: format!("doomed-{}", i),
                address: "127.0.0.1:1".parse().unwrap(),
            },
            registry.clone(),
            zeroize::Zeroizing::new(vec![0u8; 32]),
        ));
    }

    // Look for Reconnecting events where the supervisor has already
    // slept through at least one jittered backoff. The very first
    // Reconnecting (attempt=0) is emitted synchronously at spawn,
    // and attempt=1 fires right after the fail-fast TCP refusal —
    // neither reflects jitter. Anything with attempt >= 2 has the
    // first jittered sleep baked into its arrival time.
    //
    // 4s deadline lets the 1s base backoff + 25% slack happen for
    // every supervisor.
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut firsts: std::collections::HashMap<String, Instant> = std::collections::HashMap::new();
    while Instant::now() < deadline && firsts.len() < 8 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(NodeEvent::Reconnecting { node_id, attempt })) if attempt >= 2 => {
                firsts.entry(node_id).or_insert_with(Instant::now);
            }
            Ok(Ok(_)) => continue,
            // Lagged broadcast or recv error: stop polling but
            // evaluate what we have.
            _ => break,
        }
    }
    assert!(
        firsts.len() >= 4,
        "only {} supervisors emitted attempt≥2 Reconnecting within deadline (spawn → now = {:?})",
        firsts.len(),
        spawn_time.elapsed(),
    );
    let timestamps: Vec<Instant> = firsts.values().copied().collect();
    let min = *timestamps.iter().min().unwrap();
    let max = *timestamps.iter().max().unwrap();
    let spread = max.duration_since(min);
    // Without jitter, spread should be sub-millisecond (all supervisors
    // sleep the same backoff). With ±25% jitter on a 1s base, expect
    // spread well over 50ms.
    assert!(
        spread.as_millis() >= 50,
        "expected jittered supervisors to spread by ≥50ms; got {:?} (n={})",
        spread,
        timestamps.len()
    );
    // Explicitly abort supervisors so they don't continue retrying
    // after the test ends (would slow down sibling tests in the
    // same binary).
    for h in sup_handles {
        h.abort();
    }
}
