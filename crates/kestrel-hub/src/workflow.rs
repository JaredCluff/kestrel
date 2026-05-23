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
    /// move on) or "fail" (abort the whole workflow). Default: fail.
    #[serde(default = "default_on_error")]
    pub on_error: OnError,
    /// Phase 9 follow-up: conditional execution. If set, the step is
    /// skipped unless the named earlier step's status matches `when`.
    /// Format: "step_name == ok", "step_name == error",
    /// "step_name != ok". Comparison is on the literal status string.
    #[serde(default)]
    pub when: Option<String>,
}

fn default_on_error() -> OnError { OnError::Fail }

#[derive(Debug, Clone, Copy, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    Continue,
    Fail,
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
pub enum WorkflowStatus { Ok, PartialError, Aborted, TimedOut }

/// Execute a workflow against a registry. Total wall-clock budget is
/// `timeout`; steps that share a node are sequential (we don't try to
/// parallelize same-node ops because they might interact).
pub async fn run(
    registry: &Arc<NodeRegistry>,
    steps: Vec<WorkflowStep>,
    timeout: Duration,
) -> WorkflowResult {
    let started = std::time::Instant::now();
    let mut captured: HashMap<String, String> = HashMap::new();
    let mut results: Vec<StepResult> = Vec::with_capacity(steps.len());
    let mut overall = WorkflowStatus::Ok;

    let mut step_statuses: HashMap<String, StepStatus> = HashMap::new();
    let fut = async {
        for step in steps {
            // Evaluate `when` predicate against earlier step statuses.
            if let Some(cond) = &step.when {
                if !eval_when(cond, &step_statuses) {
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
                    continue;
                }
            }
            // Substitute ${name.output} references using captured map.
            let args_str = step.args.to_string();
            let substituted = substitute(&args_str, &captured);
            let args: serde_json::Value =
                serde_json::from_str(&substituted).unwrap_or(serde_json::Value::Null);

            // Resolve target node — explicit id wins, else capability-routed.
            let node_id = match (&step.node, &step.needs) {
                (Some(n), _) => Some(n.clone()),
                (None, Some(needs)) => {
                    let candidates = registry.find_nodes_with(needs).await;
                    candidates.into_iter().next()
                }
                (None, None) => None,
            };

            let step_started = std::time::Instant::now();
            let mut result = StepResult {
                name: step.name.clone(),
                status: StepStatus::Ok,
                output: None,
                error: None,
                node_id: node_id.clone(),
                duration_ms: 0,
            };

            let node_id = match node_id {
                Some(n) => n,
                None => {
                    result.status = StepStatus::Error;
                    result.error = Some(format!(
                        "step '{}' has no target node (provide `node` or matching `needs`)",
                        step.name
                    ));
                    result.duration_ms = step_started.elapsed().as_millis() as u64;
                    let abort = matches!(step.on_error, OnError::Fail);
                    results.push(result);
                    if abort {
                        overall = WorkflowStatus::Aborted;
                        return;
                    } else if overall == WorkflowStatus::Ok {
                        overall = WorkflowStatus::PartialError;
                    }
                    continue;
                }
            };

            // Dispatch by op.
            let dispatch = run_step(registry, &step.op, &node_id, &args).await;
            result.duration_ms = step_started.elapsed().as_millis() as u64;
            match dispatch {
                Ok(out) => {
                    captured.insert(format!("{}.output", step.name), out.clone());
                    result.output = Some(out);
                }
                Err(e) => {
                    result.status = StepStatus::Error;
                    result.error = Some(e.to_string());
                    let abort = matches!(step.on_error, OnError::Fail);
                    results.push(result);
                    if abort {
                        overall = WorkflowStatus::Aborted;
                        return;
                    } else {
                        if overall == WorkflowStatus::Ok {
                            overall = WorkflowStatus::PartialError;
                        }
                        continue;
                    }
                }
            }
            step_statuses.insert(step.name.clone(), result.status);
            results.push(result);
        }
    };

    match tokio::time::timeout(timeout, fut).await {
        Ok(()) => {}
        Err(_) => {
            overall = WorkflowStatus::TimedOut;
        }
    }
    WorkflowResult {
        steps: results,
        total_duration_ms: started.elapsed().as_millis() as u64,
        status: overall,
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
        };
        let res = run(&reg, vec![step], Duration::from_secs(5)).await;
        assert_eq!(res.status, WorkflowStatus::PartialError);
        assert_eq!(res.steps[0].status, StepStatus::Error);
        assert!(res.steps[0].error.as_ref().unwrap().contains("no target node"));
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
            },
            WorkflowStep {
                name: "never_runs".into(),
                node: Some("x".into()),
                needs: None,
                op: "world_state".into(),
                args: serde_json::json!({}),
                on_error: OnError::Continue,
                when: None,
            },
        ];
        let res = run(&reg, steps, Duration::from_secs(5)).await;
        assert_eq!(res.status, WorkflowStatus::Aborted);
        assert_eq!(res.steps.len(), 1, "second step must not have run");
    }
}
