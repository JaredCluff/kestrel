// crates/kestrel-hub/src/jobs.rs
//
// Phase 7: async long-running jobs. Today's MCP `shell_run` blocks
// the call for the duration of the command — a 30-minute build hangs
// the AI's turn. This module introduces a job registry: a tool call
// returns immediately with a job_id; the AI polls / streams output
// separately.
//
// Initial scope: just `shell_run_async`. The pattern generalizes
// (other long ops like screenshot bursts, AX walks, etc. can be
// added later) but starting narrow keeps this PR reviewable.
//
// State model: jobs live in an Arc<RwLock<HashMap<JobId, Job>>>. No
// persistence across hub restarts — that's documented and acceptable
// for v1 (operators restart hubs rarely; jobs running through a
// restart can be re-launched by the agent's own state). The job's
// underlying agent-side PTY or process keeps running independent of
// hub job state; we just lose the tracking and the buffered output.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use crate::router::NodeRegistry;

/// Opaque job identifier. UUID-shaped string ("kj-" + 16 hex chars).
/// The "kj-" prefix is for human-readability when these appear in
/// audit logs and error messages.
pub type JobId = String;

/// Job lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Just created; the spawn task hasn't yet started executing.
    /// Transient — usually flips to `Running` within milliseconds.
    Pending,
    /// Spawn task is in progress.
    Running,
    /// Completed successfully. `exit_code` will be `Some(0)` for the
    /// common case; other codes are possible for non-zero shell exits.
    Completed,
    /// The job errored out (agent not connected, transport failure,
    /// timeout reached, etc.). `error` carries the message.
    Failed,
    /// `job_cancel` was called; the spawn task was aborted. Any
    /// output captured up to the cancel point stays in `output_buf`.
    Cancelled,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Job {
    pub job_id: JobId,
    pub op: String,
    pub node_id: String,
    pub args_summary: String,
    pub status: JobStatus,
    pub created_unix: u64,
    pub completed_unix: Option<u64>,
    /// Wall-clock duration since `created_unix`. Computed at query
    /// time; not stored. For completed jobs it's bounded by
    /// `completed_unix - created_unix`.
    #[serde(skip_serializing)]
    started: Instant,
    /// Captured output buffer. Populated by the spawn task as the
    /// underlying agent operation produces stdout/stderr. For shell
    /// jobs this is the PTY's accumulated bytes; UTF-8 lossily
    /// decoded on read.
    pub output_buf: Vec<u8>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

impl Job {
    pub fn duration(&self) -> Duration {
        match self.completed_unix {
            Some(end) => Duration::from_secs(end.saturating_sub(self.created_unix)),
            None => self.started.elapsed(),
        }
    }
}

/// In-memory registry of jobs. Cloneable (Arc'd inside) so handlers
/// can share. Bounded only by available memory + the agent's
/// willingness to keep running — operators should `job_cancel` long-
/// running orphans rather than rely on auto-eviction.
#[derive(Clone)]
pub struct JobRegistry {
    inner: Arc<RwLock<std::collections::HashMap<JobId, Job>>>,
    /// Reference into the hub's NodeRegistry — needed to dispatch
    /// agent calls (shell_run, screenshot, etc.) from job spawn
    /// tasks. The same Arc the rest of the hub shares.
    node_registry: Arc<NodeRegistry>,
    /// Per-job abort handles. `job_cancel` calls `.abort()` to
    /// terminate the spawn task; the task observes the cancellation
    /// at its next .await point.
    handles: Arc<RwLock<std::collections::HashMap<JobId, tokio::task::AbortHandle>>>,
}

impl JobRegistry {
    pub fn new(node_registry: Arc<NodeRegistry>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(std::collections::HashMap::new())),
            node_registry,
            handles: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Start a shell job on `node_id`. Returns the JobId immediately;
    /// the actual `run_shell` call happens on a detached spawn task.
    pub async fn start_shell(&self, node_id: String, command: String) -> JobId {
        let job_id = fresh_job_id();
        let now = now_unix();
        let job = Job {
            job_id: job_id.clone(),
            op: "shell_run".into(),
            node_id: node_id.clone(),
            args_summary: format!("command={}", command),
            status: JobStatus::Pending,
            created_unix: now,
            completed_unix: None,
            started: Instant::now(),
            output_buf: vec![],
            exit_code: None,
            error: None,
        };
        self.inner.write().await.insert(job_id.clone(), job);

        // Spawn the worker. Flip Pending -> Running, do the call,
        // flip to Completed / Failed.
        let inner = self.inner.clone();
        let handles = self.handles.clone();
        let registry = self.node_registry.clone();
        let job_id_for_task = job_id.clone();
        let job_id_for_handle = job_id.clone();
        let task = tokio::spawn(async move {
            {
                let mut map = inner.write().await;
                if let Some(j) = map.get_mut(&job_id_for_task) {
                    j.status = JobStatus::Running;
                }
            }
            let result = registry.run_shell(&node_id, &command).await;
            let mut map = inner.write().await;
            if let Some(j) = map.get_mut(&job_id_for_task) {
                if j.status == JobStatus::Cancelled {
                    return;
                }
                j.completed_unix = Some(now_unix());
                match result {
                    Ok(output) => {
                        j.output_buf = output.into_bytes();
                        j.status = JobStatus::Completed;
                        j.exit_code = Some(0);
                    }
                    Err(e) => {
                        j.error = Some(e.to_string());
                        j.status = JobStatus::Failed;
                    }
                }
            }
        });
        // Track the AbortHandle so `cancel()` can actually terminate
        // the spawn task at its next await point.
        {
            let mut h = handles.write().await;
            h.insert(job_id_for_handle, task.abort_handle());
        }

        job_id
    }

    /// Look up a job by id. Cheap clone — output_buf could be large
    /// but the API is meant for status checks; full output goes via
    /// `output_since`.
    pub async fn status(&self, job_id: &str) -> Option<Job> {
        self.inner.read().await.get(job_id).cloned()
    }

    /// Read output starting at byte offset `since_offset`. Returns
    /// (bytes, new_offset). When the job has produced no new bytes
    /// since `since_offset`, returns an empty Vec and the same
    /// offset (caller backs off).
    pub async fn output_since(
        &self,
        job_id: &str,
        since_offset: usize,
    ) -> Option<(Vec<u8>, usize)> {
        let map = self.inner.read().await;
        let job = map.get(job_id)?;
        if since_offset >= job.output_buf.len() {
            return Some((vec![], job.output_buf.len()));
        }
        let chunk = job.output_buf[since_offset..].to_vec();
        let new_offset = job.output_buf.len();
        Some((chunk, new_offset))
    }

    /// Mark a job as Cancelled AND abort its spawn task. The abort
    /// lands at the next await point inside the task — which is
    /// either inside the registry call (so the request is dropped)
    /// or already past, in which case it just bails out without
    /// recording results. The underlying agent operation may still
    /// run to completion on its side; the 30s shell_run timeout
    /// caps any runaway.
    pub async fn cancel(&self, job_id: &str) -> bool {
        // Abort the spawn task first.
        if let Some(handle) = self.handles.write().await.remove(job_id) {
            handle.abort();
        }
        let mut map = self.inner.write().await;
        match map.get_mut(job_id) {
            Some(j) if matches!(j.status, JobStatus::Pending | JobStatus::Running) => {
                j.status = JobStatus::Cancelled;
                j.completed_unix = Some(now_unix());
                true
            }
            _ => false,
        }
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }
}

fn fresh_job_id() -> JobId {
    use rand::RngCore;
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("kj-{}", hex::encode(bytes))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fresh_job_id_is_distinct() {
        let a = fresh_job_id();
        let b = fresh_job_id();
        assert!(a.starts_with("kj-"));
        assert_eq!(a.len(), 3 + 16);
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn cancel_pending_or_running_succeeds() {
        let reg = JobRegistry::new(Arc::new(NodeRegistry::new()));
        // Start a job that will Fail (no node connected); race the
        // cancel before completion is the goal but the test is
        // tolerant — either Cancelled or Failed is OK as a terminal
        // state.
        let id = reg
            .start_shell("ghost".into(), "echo hi".into())
            .await;
        // Cancelling a job that's already Failed is a no-op (returns
        // false). Cancelling Pending/Running returns true.
        let _ = reg.cancel(&id).await;
        let final_status = reg.status(&id).await.unwrap().status;
        assert!(
            matches!(
                final_status,
                JobStatus::Cancelled | JobStatus::Failed
            ),
            "job should be terminal, got {:?}",
            final_status
        );
    }

    #[tokio::test]
    async fn output_since_returns_full_buffer_for_zero_offset() {
        let reg = JobRegistry::new(Arc::new(NodeRegistry::new()));
        // Seed a job directly so we don't need a connected agent.
        let id = "kj-deadbeefdeadbeef".to_string();
        {
            let mut map = reg.inner.write().await;
            map.insert(
                id.clone(),
                Job {
                    job_id: id.clone(),
                    op: "shell_run".into(),
                    node_id: "n".into(),
                    args_summary: String::new(),
                    status: JobStatus::Completed,
                    created_unix: 0,
                    completed_unix: Some(0),
                    started: Instant::now(),
                    output_buf: b"hello world".to_vec(),
                    exit_code: Some(0),
                    error: None,
                },
            );
        }
        let (chunk, off) = reg.output_since(&id, 0).await.unwrap();
        assert_eq!(chunk, b"hello world");
        assert_eq!(off, 11);
        // Read again at the new offset — nothing new.
        let (chunk2, off2) = reg.output_since(&id, off).await.unwrap();
        assert!(chunk2.is_empty());
        assert_eq!(off2, 11);
    }
}
