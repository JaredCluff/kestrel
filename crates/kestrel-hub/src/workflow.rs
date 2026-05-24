// crates/kestrel-hub/src/workflow.rs
//
// Phase 9: declarative cross-machine workflows. The AI specifies a
// sequence of steps; each step targets a node (by id or by capability
// predicate) and invokes an op (shell_run, screenshot, world_state, ...).
// The hub executes the steps in order; later steps can reference
// earlier steps' captured outputs via simple `${step_name.output}`
// substitution.
//
// Scope for v1:
//   - Linear sequence (no fan-out, no parallel steps)
//   - Simple var substitution (no Turing-complete templating)
//   - Per-step on_error: continue | fail (default fail)
//   - Whole-workflow timeout (default 5min)
//
// Out of scope for v1:
//   - DAG (parallel branches) — linear is enough for most agent-driven flows
//   - Conditional steps based on a previous step's result — agents can
//     chain workflow_run calls themselves
//   - Rollback on failure — the agent decides recovery
//
// Steps reuse the hub's existing primitives:
//   - shell_run / shell_run_async (Phase 7) for shell commands
//   - screenshot / world_state / describe for observation
//   - fleet_find (Phase 8) used INSIDE the workflow runner when a
//     step targets `{ needs: {...} }` instead of an explicit node_id

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::router::{CapabilityNeeds, NodeRegistry};

/// One step in a workflow.
#[derive(Debug, Clone, serde::Deserialize, schemars::JsonSchema)]
pub struct WorkflowStep {
    /// Human-readable name. Used for capture lookups and audit trail.
    /// Must be unique within the workflow.
    pub name: String,
    /// Either an explicit node id or a capability predicate. When
    /// `needs` is supplied, the runner uses `fleet_find` to pick the
    /// first matching node; if there are multiple, picks the first
    /// alphabetically (deterministic). If both are supplied, `node`
    /// wins.
    #[serde(default)]
    pub node: Option<String>,
    #[serde(default)]
    pub needs: Option<CapabilityNeeds>,
    /// Op to invoke. Supported ops: "shell_run", "screenshot",
    /// "world_state", "describe", "type_text", "key_combo",
    /// "clipboard_read", "clipboard_write".
    pub op: String,
    /// Op-specific args. For `shell_run`, expects {"command": "..."}.
    #[serde(default)]
    pub args: serde_json::Value,
    /// On error: "continue" (record the error in the step result and
    /// move on), "fail" (abort the whole workflow), or "rollback"
    /// (abort + run each previously-Ok step's rollback shell command
    /// in reverse order). Default: fail.
    #[serde(default = "default_on_error")]
    pub on_error: OnError,
    /// Phase 9 follow-up: conditional execution. If set, the step is
    /// skipped unless the named earlier step's status matches `when`.
    /// Format: "step_name == ok", "step_name == error",
    /// "step_name != ok". Comparison is on the literal status string.
    #[serde(default)]
    pub when: Option<String>,
    /// Phase 9 follow-up: rollback shell command run on this step's
    /// configured node when a later step's `on_error=rollback`
    /// triggers. Steps without a rollback are simply skipped during
    /// rollback. Only `shell_run`-style commands are supported; this
    /// is intentionally narrower than the main `op` surface to keep
    /// rollback predictable.
    #[serde(default)]
    pub rollback: Option<String>,
    /// Phase 9 follow-up: run this step in parallel with the
    /// immediately-preceding steps that also have `parallel: true`.
    /// Workflow execution alternates between sequential blocks and
    /// parallel blocks; a parallel block ends at the next step
    /// without `parallel: true`. Captures from parallel-block steps
    /// are visible to subsequent (sequential or parallel) steps the
    /// same way sequential captures are; within a parallel block
    /// they're NOT visible to siblings (race-free semantics).
    #[serde(default)]
    pub parallel: bool,
}

fn default_on_error() -> OnError { OnError::Fail }

#[derive(Debug, Clone, Copy, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    Continue,
    Fail,
    Rollback,
}

/// Result of a single step.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StepResult {
    pub name: String,
    pub status: StepStatus,
    pub output: Option<String>,
    pub error: Option<String>,
    pub node_id: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus { Ok, Error, Skipped }

/// What `workflow_run` returns.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkflowResult {
    pub steps: Vec<StepResult>,
    pub total_duration_ms: u64,
    pub status: WorkflowStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus { Ok, PartialError, Aborted, TimedOut, RolledBack }

/// Execute a workflow against a registry. Total wall-clock budget is
/// `timeout`. Steps with `parallel: true` form contiguous batches
/// that execute concurrently; otherwise execution is sequential.
/// `on_error: rollback` triggers per-step rollback shell commands
/// in reverse order across all previously-Ok steps before aborting.
pub async fn run(
    registry: &Arc<NodeRegistry>,
    steps: Vec<WorkflowStep>,
    timeout: Duration,
) -> WorkflowResult {
    let started = std::time::Instant::now();
    let mut captured: HashMap<String, String> = HashMap::new();
    let mut results: Vec<StepResult> = Vec::with_capacity(steps.len());
    let mut step_statuses: HashMap<String, StepStatus> = HashMap::new();
    let mut overall = WorkflowStatus::Ok;
    let mut completed_steps: Vec<WorkflowStep> = vec![];

    let fut = async {
        let mut i = 0;
        while i < steps.len() {
            // Group consecutive parallel:true steps into one block.
            let block_end = if steps[i].parallel {
                let mut j = i;
                while j < steps.len() && steps[j].parallel { j += 1; }
                j
            } else {
                i + 1
            };
            let block: Vec<WorkflowStep> = steps[i..block_end].to_vec();
            i = block_end;

            if block.len() == 1 {
                // Sequential single step.
                let step = block.into_iter().next().unwrap();
                let outcome = execute_step(registry, &step, &captured, &step_statuses).await;
                apply_outcome(
                    outcome,
                    step,
                    &mut captured,
                    &mut step_statuses,
                    &mut results,
                    &mut completed_steps,
                    &mut overall,
                )
                .await;
                if overall == WorkflowStatus::Aborted || overall == WorkflowStatus::RolledBack {
                    return;
                }
            } else {
                // Parallel batch. Run all concurrently. Captures from
                // within the batch are NOT visible to siblings —
                // they snapshot `captured` as-of block start.
                let snapshot_captured = captured.clone();
                let snapshot_statuses = step_statuses.clone();
                let futures: Vec<_> = block
                    .iter()
                    .map(|s| execute_step(registry, s, &snapshot_captured, &snapshot_statuses))
                    .collect();
                let outcomes = futures::future::join_all(futures).await;
                for (step, outcome) in block.into_iter().zip(outcomes.into_iter()) {
                    apply_outcome(
                        outcome,
                        step,
                        &mut captured,
                        &mut step_statuses,
                        &mut results,
                        &mut completed_steps,
                        &mut overall,
                    )
                    .await;
                    if overall == WorkflowStatus::Aborted || overall == WorkflowStatus::RolledBack {
                        return;
                    }
                }
            }
        }
    };

    match tokio::time::timeout(timeout, fut).await {
        Ok(()) => {}
        Err(_) => {
            overall = WorkflowStatus::TimedOut;
        }
    }

    // Rollback path: if any step's on_error=Rollback triggered, we
    // already marked overall=RolledBack and bailed; now walk back
    // through completed Ok steps and run each one's rollback
    // command. Failures during rollback are recorded as step
    // results but don't propagate.
    if overall == WorkflowStatus::RolledBack {
        run_rollbacks(registry, &completed_steps, &mut results).await;
    }

    WorkflowResult {
        steps: results,
        total_duration_ms: started.elapsed().as_millis() as u64,
        status: overall,
    }
}

/// Outcome of a single step execution. The runner converts this to
/// a StepResult + status-map entry + captured map mutation.
enum StepOutcome {
    Ok { output: String, node_id: String, duration_ms: u64 },
    Skipped,
    Err { error: String, node_id: Option<String>, duration_ms: u64, on_error: OnError },
}

async fn execute_step(
    registry: &Arc<NodeRegistry>,
    step: &WorkflowStep,
    captured: &HashMap<String, String>,
    step_statuses: &HashMap<String, StepStatus>,
) -> StepOutcome {
    // Conditional skip.
    if let Some(cond) = &step.when {
        if !eval_when(cond, step_statuses) {
            return StepOutcome::Skipped;
        }
    }
    let args_str = step.args.to_string();
    let substituted = substitute(&args_str, captured);
    // Substitution failures (typo'd ${step.output}, mismatched braces)
    // would silently fall back to Null and run the step with empty args
    // — easy to miss in a multi-step workflow. Surface the parse error
    // up-front so operators see the typo, not a downstream "missing
    // required arg" error.
    let step_started = std::time::Instant::now();
    let args: serde_json::Value = match serde_json::from_str(&substituted) {
        Ok(v) => v,
        Err(e) => {
            return StepOutcome::Err {
                error: format!(
                    "step '{}' args failed to parse after substitution \
                     (typo in `${{...}}` reference?): {}; substituted text: {}",
                    step.name, e, substituted
                ),
                node_id: None,
                duration_ms: step_started.elapsed().as_millis() as u64,
                on_error: step.on_error,
            };
        }
    };

    let node_id = match (&step.node, &step.needs) {
        (Some(n), _) => Some(n.clone()),
        (None, Some(needs)) => registry.find_nodes_with(needs).await.into_iter().next(),
        (None, None) => None,
    };
    let Some(node_id) = node_id else {
        return StepOutcome::Err {
            error: format!(
                "step '{}' has no target node (provide `node` or matching `needs`)",
                step.name
            ),
            node_id: None,
            duration_ms: step_started.elapsed().as_millis() as u64,
            on_error: step.on_error,
        };
    };
    let dispatch = run_step(registry, &step.op, &node_id, &args).await;
    let duration_ms = step_started.elapsed().as_millis() as u64;
    match dispatch {
        Ok(out) => StepOutcome::Ok { output: out, node_id, duration_ms },
        Err(e) => StepOutcome::Err {
            error: e.to_string(),
            node_id: Some(node_id),
            duration_ms,
            on_error: step.on_error,
        },
    }
}

async fn apply_outcome(
    outcome: StepOutcome,
    step: WorkflowStep,
    captured: &mut HashMap<String, String>,
    step_statuses: &mut HashMap<String, StepStatus>,
    results: &mut Vec<StepResult>,
    completed_steps: &mut Vec<WorkflowStep>,
    overall: &mut WorkflowStatus,
) {
    match outcome {
        StepOutcome::Skipped => {
            let result = StepResult {
                name: step.name.clone(),
                status: StepStatus::Skipped,
                output: None,
                error: None,
                node_id: None,
                duration_ms: 0,
            };
            step_statuses.insert(step.name.clone(), StepStatus::Skipped);
            results.push(result);
        }
        StepOutcome::Ok { output, node_id, duration_ms } => {
            captured.insert(format!("{}.output", step.name), output.clone());
            let result = StepResult {
                name: step.name.clone(),
                status: StepStatus::Ok,
                output: Some(output),
                error: None,
                node_id: Some(node_id),
                duration_ms,
            };
            step_statuses.insert(step.name.clone(), StepStatus::Ok);
            completed_steps.push(step);
            results.push(result);
        }
        StepOutcome::Err { error, node_id, duration_ms, on_error } => {
            let result = StepResult {
                name: step.name.clone(),
                status: StepStatus::Error,
                output: None,
                error: Some(error),
                node_id,
                duration_ms,
            };
            step_statuses.insert(step.name.clone(), StepStatus::Error);
            results.push(result);
            match on_error {
                OnError::Continue => {
                    if *overall == WorkflowStatus::Ok {
                        *overall = WorkflowStatus::PartialError;
                    }
                }
                OnError::Fail => {
                    *overall = WorkflowStatus::Aborted;
                }
                OnError::Rollback => {
                    *overall = WorkflowStatus::RolledBack;
                }
            }
        }
    }
}

/// Run each completed step's rollback shell command (if any) in
/// reverse order. Best-effort: rollback failures are appended to
/// results as Error steps named "rollback:<original-name>", but
/// don't change the overall status (we've already RolledBack).
async fn run_rollbacks(
    registry: &Arc<NodeRegistry>,
    completed: &[WorkflowStep],
    results: &mut Vec<StepResult>,
) {
    for step in completed.iter().rev() {
        let Some(cmd) = &step.rollback else { continue };
        let Some(node_id) = step.node.as_deref() else {
            // Capability-routed steps lack a concrete node by the
            // time we rollback; we'd have to re-find or capture
            // the node_id at execution time. v1 limitation:
            // rollback supported only for steps with explicit node.
            continue;
        };
        let started = std::time::Instant::now();
        let outcome = registry.run_shell(node_id, cmd).await;
        let duration_ms = started.elapsed().as_millis() as u64;
        let (status, error) = match outcome {
            Ok(_) => (StepStatus::Ok, None),
            Err(e) => (StepStatus::Error, Some(e.to_string())),
        };
        results.push(StepResult {
            name: format!("rollback:{}", step.name),
            status,
            output: None,
            error,
            node_id: Some(node_id.into()),
            duration_ms,
        });
    }
}

/// Dispatch one step. Returns the captured string output on success.
async fn run_step(
    registry: &Arc<NodeRegistry>,
    op: &str,
    node_id: &str,
    args: &serde_json::Value,
) -> anyhow::Result<String> {
    match op {
        "shell_run" => {
            let cmd = args
                .get("command")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("shell_run: missing string `command` arg"))?;
            registry.run_shell(node_id, cmd).await
        }
        "screenshot" => {
            // Return a tag so substitutions in later steps can know a
            // screenshot was taken; the PNG bytes themselves aren't
            // useful as text. Operators wanting the image should call
            // `screenshot` directly.
            let _png = registry.screenshot(node_id, 0, None).await?;
            Ok("[screenshot ok]".into())
        }
        "world_state" => {
            let state = registry
                .world_state_for(node_id)
                .await
                .ok_or_else(|| anyhow::anyhow!("no world state for '{}'", node_id))?;
            Ok(serde_json::to_string(&state).unwrap_or_default())
        }
        "describe" => {
            let tree = registry.describe(node_id, 0).await?;
            Ok(serde_json::to_string(&tree).unwrap_or_default())
        }
        "type_text" => {
            let text = args
                .get("text")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("type_text: missing string `text` arg"))?;
            registry.type_text(node_id, text.into()).await?;
            Ok("ok".into())
        }
        "clipboard_read" => {
            let content = registry.clipboard_read(node_id).await?;
            Ok(serde_json::to_string(&content).unwrap_or_default())
        }
        "clipboard_write" => {
            let text = args
                .get("text")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("clipboard_write: missing string `text` arg"))?;
            registry
                .clipboard_write(node_id, kestrel_proto::ClipboardContent::Text(text.into()))
                .await?;
            Ok("ok".into())
        }
        other => anyhow::bail!("workflow op '{}' not supported", other),
    }
}

/// Evaluate a `when` predicate against the recorded step statuses.
/// Format: `<step_name> <op> <status>` where op is `==` or `!=` and
/// status is one of `ok`, `error`, `skipped`. Anything malformed
/// evaluates to false (step skipped) — fail-closed semantics.
fn eval_when(cond: &str, statuses: &HashMap<String, StepStatus>) -> bool {
    let cond = cond.trim();
    let (lhs, op, rhs) = if let Some((l, r)) = cond.split_once("==") {
        (l.trim(), "==", r.trim())
    } else if let Some((l, r)) = cond.split_once("!=") {
        (l.trim(), "!=", r.trim())
    } else {
        return false;
    };
    let status = match statuses.get(lhs) {
        Some(s) => s,
        None => return false, // referenced step didn't run
    };
    let want = match rhs {
        "ok" => StepStatus::Ok,
        "error" => StepStatus::Error,
        "skipped" => StepStatus::Skipped,
        _ => return false,
    };
    match op {
        "==" => *status == want,
        "!=" => *status != want,
        _ => false,
    }
}

/// Replace `${name.output}` tokens with the captured values. Naïve
/// string replace — no escape handling. v1 is intentionally tiny;
/// production callers needing templating should pre-render the args.
fn substitute(input: &str, captured: &HashMap<String, String>) -> String {
    let mut out = input.to_string();
    for (key, val) in captured {
        let token = format!("${{{}}}", key);
        out = out.replace(&token, val);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_replaces_known_tokens() {
        let mut m = HashMap::new();
        m.insert("step1.output".into(), "hello".into());
        assert_eq!(substitute("echo ${step1.output}", &m), "echo hello");
        // Unknown tokens left intact (no panic).
        assert_eq!(substitute("echo ${nope.output}", &m), "echo ${nope.output}");
    }

    #[tokio::test]
    async fn empty_workflow_returns_ok() {
        let reg = Arc::new(NodeRegistry::new());
        let res = run(&reg, vec![], Duration::from_secs(5)).await;
        assert_eq!(res.status, WorkflowStatus::Ok);
        assert!(res.steps.is_empty());
    }

    #[tokio::test]
    async fn step_with_malformed_substitution_errors_loudly() {
        // Regression: previously a JSON parse failure after substitution
        // was silently swallowed into Value::Null and the step ran with
        // empty args. The realistic trigger is a previous step's output
        // containing a `"` that, when interpolated into a JSON string,
        // closes it early and breaks the surrounding JSON. Without the
        // explicit error, that became a confusing "missing required arg"
        // error from a downstream op.
        let reg = Arc::new(NodeRegistry::new());
        let mut captured = HashMap::new();
        // Previous step's output contains a quote — exactly what would
        // break the JSON when substituted naively into a quoted string.
        captured.insert("prev.output".into(), r#"oops"break"#.into());
        let statuses = HashMap::new();
        let step = WorkflowStep {
            name: "bad".into(),
            node: Some("any".into()),
            needs: None,
            op: "shell_run".into(),
            args: serde_json::json!({ "command": "${prev.output}" }),
            on_error: OnError::Continue,
            when: None,
            rollback: None,
            parallel: false,
        };
        let outcome = execute_step(&reg, &step, &captured, &statuses).await;
        match outcome {
            StepOutcome::Err { error, .. } => {
                assert!(
                    error.contains("failed to parse after substitution"),
                    "expected substitution parse error, got: {}",
                    error
                );
            }
            other => panic!(
                "expected Err outcome from malformed substitution, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[tokio::test]
    async fn step_without_node_or_needs_errors() {
        let reg = Arc::new(NodeRegistry::new());
        let step = WorkflowStep {
            name: "noop".into(),
            node: None,
            needs: None,
            op: "world_state".into(),
            args: serde_json::json!({}),
            on_error: OnError::Continue,
            when: None,
            rollback: None,
            parallel: false,
        };
        let res = run(&reg, vec![step], Duration::from_secs(5)).await;
        assert_eq!(res.status, WorkflowStatus::PartialError);
        assert_eq!(res.steps[0].status, StepStatus::Error);
        assert!(res.steps[0].error.as_ref().unwrap().contains("no target node"));
    }

    #[tokio::test]
    async fn parallel_steps_form_a_block_that_completes_atomically() {
        // Three parallel steps, all targeting a node that doesn't
        // exist. All should error (on_error=Continue) and the
        // workflow should end as PartialError, not aborted.
        let reg = Arc::new(NodeRegistry::new());
        let mk = |name: &str| WorkflowStep {
            name: name.into(),
            node: Some("ghost".into()),
            needs: None,
            op: "world_state".into(),
            args: serde_json::json!({}),
            on_error: OnError::Continue,
            when: None,
            rollback: None,
            parallel: true,
        };
        let res = run(&reg, vec![mk("a"), mk("b"), mk("c")], Duration::from_secs(5)).await;
        assert_eq!(res.steps.len(), 3);
        assert_eq!(res.status, WorkflowStatus::PartialError);
        for s in &res.steps {
            assert_eq!(s.status, StepStatus::Error);
        }
    }

    #[tokio::test]
    async fn rollback_on_error_runs_in_reverse_for_completed_steps() {
        // First two steps would succeed against a real node — we
        // can't easily make them succeed without an agent, so this
        // test pins the RolledBack status when the first error
        // step has on_error=Rollback. The rollback walker only
        // operates on previously-Ok steps; in this empty-cache test
        // there are none, so we just verify the status code.
        let reg = Arc::new(NodeRegistry::new());
        let step = WorkflowStep {
            name: "bad".into(),
            node: Some("ghost".into()),
            needs: None,
            op: "world_state".into(),
            args: serde_json::json!({}),
            on_error: OnError::Rollback,
            when: None,
            rollback: None,
            parallel: false,
        };
        let res = run(&reg, vec![step], Duration::from_secs(5)).await;
        assert_eq!(res.status, WorkflowStatus::RolledBack);
    }

    #[tokio::test]
    async fn fail_on_error_aborts_remaining_steps() {
        let reg = Arc::new(NodeRegistry::new());
        let steps = vec![
            WorkflowStep {
                name: "doomed".into(),
                node: None,
                needs: None,
                op: "world_state".into(),
                args: serde_json::json!({}),
                on_error: OnError::Fail,
                when: None,
                rollback: None,
                parallel: false,
            },
            WorkflowStep {
                name: "never_runs".into(),
                node: Some("x".into()),
                needs: None,
                op: "world_state".into(),
                args: serde_json::json!({}),
                on_error: OnError::Continue,
                when: None,
                rollback: None,
                parallel: false,
            },
        ];
        let res = run(&reg, steps, Duration::from_secs(5)).await;
        assert_eq!(res.status, WorkflowStatus::Aborted);
        assert_eq!(res.steps.len(), 1, "second step must not have run");
    }
}
