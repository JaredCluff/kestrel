// crates/kestrel-hub/src/policy.rs
//
// Phase 11: identity + per-user policy + approval gates. Today's hub
// has one control token, one operator, no granular permissions. This
// module adds:
//   - User identity (loaded from a config file; OIDC/SAML integration
//     is a follow-up — the data model is what matters most).
//   - Per-user policy (allow/deny by op + node tag).
//   - Approval gates: certain ops on certain nodes require a second
//     authenticated user to approve via the dashboard within a TTL.
//
// Scope for v1:
//   - File-backed user list (kestrel-policy.toml). OIDC providers are
//     a Phase 11b follow-up — wiring them up requires real provider
//     test environments we don't have access to from the dev machine.
//   - Approval flow tracked in-memory. Pending approvals expire on
//     hub restart; documented limitation.
//   - Audit log entries gain user_id (the actor) so per-user activity
//     is reconstructible.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{oneshot, RwLock};

/// One identifiable operator. Loaded from kestrel-policy.toml at hub
/// startup. The `bearer_token` field (if present) is the legacy
/// per-user equivalent of today's hub-wide `control_token`; OIDC
/// support (subject + issuer claims) is a v2 add.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct User {
    pub user_id: String,
    pub display_name: Option<String>,
    pub bearer_token: Option<String>,
    /// List of policy rules that this user matches.
    #[serde(default)]
    pub policies: Vec<PolicyRule>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PolicyRule {
    /// Glob-ish op name. "*" matches any; "shell_*" matches the shell
    /// family. v1 supports only prefix-wildcard (op ends with "*") and
    /// exact matches.
    pub op: String,
    /// Tag the policy applies to. "*" matches any node. Specific node
    /// tags can be set in kestrel.toml's `[[hub.nodes]]` entries (a
    /// follow-up wires the tag field into NodeConfig; for v1 we
    /// match against literal node_ids in the absence of tags).
    pub node: String,
    /// `allow` permits the op; `deny` blocks it (deny wins on conflict).
    /// `require_approval` permits the op only after another user in
    /// `approvers` clicks the dashboard's approval button within
    /// `ttl_secs` (default 60).
    pub action: PolicyAction,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PolicyAction {
    Allow,
    Deny,
    RequireApproval {
        /// User ids permitted to approve. Empty list means "any
        /// other user can approve" (typical for two-person review
        /// without designated reviewers).
        #[serde(default)]
        approvers: Vec<String>,
        /// Wait this long before timing out. Default 60.
        #[serde(default = "default_approval_ttl_secs")]
        ttl_secs: u64,
    },
}

fn default_approval_ttl_secs() -> u64 { 60 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
    NeedsApproval,
}

/// Container for the full policy doc.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct PolicyConfig {
    #[serde(default)]
    pub users: Vec<User>,
}

impl PolicyConfig {
    pub fn from_toml_str(s: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(s)?)
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_toml_str(&std::fs::read_to_string(path)?)
    }

    /// Look up the user identified by `bearer_token`. Constant-time
    /// compare so wrong-token probes can't time the lookup.
    pub fn user_for_bearer(&self, token: &str) -> Option<&User> {
        for u in &self.users {
            if let Some(t) = &u.bearer_token {
                if ct_eq(t.as_bytes(), token.as_bytes()) {
                    return Some(u);
                }
            }
        }
        None
    }

    /// Evaluate the policy: would `user` be allowed to perform `op`
    /// against `node`? Considers every rule for that user; deny wins
    /// over allow; require_approval is the next-strongest signal;
    /// no matching rule falls through to Deny.
    pub fn decide<'a>(&self, user: &'a User, op: &str, node: &str) -> (Decision, Option<&'a PolicyRule>) {
        let mut decision = Decision::Deny;
        let mut matched: Option<&'a PolicyRule> = None;
        for rule in &user.policies {
            if !op_matches(&rule.op, op) || !node_matches(&rule.node, node) {
                continue;
            }
            match &rule.action {
                PolicyAction::Deny => {
                    // Deny is a hard stop. Early return.
                    return (Decision::Deny, Some(rule));
                }
                PolicyAction::RequireApproval { .. } => {
                    decision = Decision::NeedsApproval;
                    matched = Some(rule);
                }
                PolicyAction::Allow => {
                    if decision != Decision::NeedsApproval {
                        decision = Decision::Allow;
                        matched = Some(rule);
                    }
                }
            }
        }
        (decision, matched)
    }
}

fn op_matches(pattern: &str, op: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return op.starts_with(prefix);
    }
    pattern == op
}

fn node_matches(pattern: &str, node: &str) -> bool {
    pattern == "*" || pattern == node
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ── Approval flow ────────────────────────────────────────────────────────

/// One pending approval. The actor is waiting on `responder`; the
/// dashboard's `/api/approvals/:id/approve` and `/api/approvals/:id/deny`
/// endpoints satisfy it.
pub struct PendingApproval {
    pub id: String,
    pub actor_user_id: String,
    pub op: String,
    pub node: String,
    pub approvers: Vec<String>,
    pub created_unix: u64,
    pub expires_at: Instant,
    responder: oneshot::Sender<ApprovalOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalOutcome {
    Approved,
    Denied,
    Timeout,
}

/// Snapshot of a pending approval suitable for dashboard rendering.
/// Excludes the responder channel (not Clone-able + meaningless to a
/// remote viewer).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ApprovalDto {
    pub id: String,
    pub actor_user_id: String,
    pub op: String,
    pub node: String,
    pub approvers: Vec<String>,
    pub created_unix: u64,
    pub expires_unix: u64,
}

#[derive(Clone, Default)]
pub struct ApprovalRegistry {
    inner: Arc<RwLock<HashMap<String, PendingApproval>>>,
}

impl ApprovalRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request approval for an action. Returns when an approver
    /// clicks approve/deny in the dashboard or when `ttl` elapses.
    pub async fn request(
        &self,
        actor_user_id: String,
        op: String,
        node: String,
        approvers: Vec<String>,
        ttl: Duration,
    ) -> ApprovalOutcome {
        let id = fresh_approval_id();
        let (responder, waiter) = oneshot::channel();
        let now = now_unix();
        let pending = PendingApproval {
            id: id.clone(),
            actor_user_id,
            op,
            node,
            approvers,
            created_unix: now,
            expires_at: Instant::now() + ttl,
            responder,
        };
        {
            let mut map = self.inner.write().await;
            map.insert(id.clone(), pending);
        }
        // Race waiter against the TTL. Either fires first.
        let outcome = tokio::select! {
            result = waiter => result.unwrap_or(ApprovalOutcome::Timeout),
            _ = tokio::time::sleep(ttl) => ApprovalOutcome::Timeout,
        };
        // Cleanup — the entry stays in the map after a sender-side
        // resolution because resolve() removes it; the timeout path
        // needs to remove explicitly.
        if outcome == ApprovalOutcome::Timeout {
            self.inner.write().await.remove(&id);
        }
        outcome
    }

    /// List currently pending approvals as DTOs for the dashboard.
    pub async fn pending(&self) -> Vec<ApprovalDto> {
        let now = now_unix();
        let map = self.inner.read().await;
        let mut list: Vec<ApprovalDto> = map
            .values()
            .map(|p| {
                let remaining = p.expires_at.saturating_duration_since(Instant::now());
                ApprovalDto {
                    id: p.id.clone(),
                    actor_user_id: p.actor_user_id.clone(),
                    op: p.op.clone(),
                    node: p.node.clone(),
                    approvers: p.approvers.clone(),
                    created_unix: p.created_unix,
                    expires_unix: now + remaining.as_secs(),
                }
            })
            .collect();
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
    }

    /// Resolve a pending approval (`Approved` or `Denied`). Returns
    /// true if the id existed and the resolution went through.
    pub async fn resolve(&self, id: &str, outcome: ApprovalOutcome) -> bool {
        let mut map = self.inner.write().await;
        match map.remove(id) {
            Some(p) => {
                let _ = p.responder.send(outcome);
                true
            }
            None => false,
        }
    }
}

fn fresh_approval_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 6];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("ap-{}", hex::encode(bytes))
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

    fn user_with_policies(id: &str, policies: Vec<PolicyRule>) -> User {
        User {
            user_id: id.into(),
            display_name: None,
            bearer_token: None,
            policies,
        }
    }

    #[test]
    fn deny_wins_over_allow() {
        let user = user_with_policies(
            "u",
            vec![
                PolicyRule { op: "*".into(), node: "*".into(), action: PolicyAction::Allow },
                PolicyRule {
                    op: "shell_*".into(),
                    node: "prod".into(),
                    action: PolicyAction::Deny,
                },
            ],
        );
        let cfg = PolicyConfig { users: vec![user.clone()] };
        let (d, _) = cfg.decide(&user, "shell_run", "prod");
        assert_eq!(d, Decision::Deny);
        let (d, _) = cfg.decide(&user, "shell_run", "dev");
        assert_eq!(d, Decision::Allow);
    }

    #[test]
    fn no_matching_rule_is_deny() {
        let cfg = PolicyConfig::default();
        let u = User {
            user_id: "u".into(),
            display_name: None,
            bearer_token: None,
            policies: vec![],
        };
        let (d, _) = cfg.decide(&u, "shell_run", "x");
        assert_eq!(d, Decision::Deny);
    }

    #[test]
    fn require_approval_beats_allow_but_loses_to_deny() {
        let user = user_with_policies(
            "u",
            vec![
                PolicyRule { op: "*".into(), node: "*".into(), action: PolicyAction::Allow },
                PolicyRule {
                    op: "shell_*".into(),
                    node: "*".into(),
                    action: PolicyAction::RequireApproval {
                        approvers: vec![],
                        ttl_secs: 60,
                    },
                },
                PolicyRule {
                    op: "shell_run".into(),
                    node: "nope".into(),
                    action: PolicyAction::Deny,
                },
            ],
        );
        let cfg = PolicyConfig { users: vec![user.clone()] };
        let (d, _) = cfg.decide(&user, "shell_run", "ok");
        assert_eq!(d, Decision::NeedsApproval);
        let (d, _) = cfg.decide(&user, "shell_run", "nope");
        assert_eq!(d, Decision::Deny);
        let (d, _) = cfg.decide(&user, "screenshot", "anywhere");
        assert_eq!(d, Decision::Allow);
    }

    #[test]
    fn user_for_bearer_constant_time_compare() {
        let cfg = PolicyConfig {
            users: vec![User {
                user_id: "alice".into(),
                display_name: None,
                bearer_token: Some("secret".into()),
                policies: vec![],
            }],
        };
        assert!(cfg.user_for_bearer("secret").is_some());
        assert!(cfg.user_for_bearer("wrong").is_none());
        // Wrong length still returns None (no panic).
        assert!(cfg.user_for_bearer("").is_none());
    }

    #[tokio::test]
    async fn approval_timeout_yields_timeout_outcome() {
        let reg = ApprovalRegistry::new();
        let outcome = reg
            .request(
                "alice".into(),
                "shell_run".into(),
                "prod".into(),
                vec![],
                Duration::from_millis(50),
            )
            .await;
        assert_eq!(outcome, ApprovalOutcome::Timeout);
    }

    #[tokio::test]
    async fn approval_resolved_returns_outcome() {
        let reg = Arc::new(ApprovalRegistry::new());
        let reg2 = reg.clone();
        let task = tokio::spawn(async move {
            reg2.request(
                "alice".into(),
                "shell_run".into(),
                "prod".into(),
                vec!["bob".into()],
                Duration::from_secs(5),
            )
            .await
        });
        // Wait briefly for the request to land in the map.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let pending = reg.pending().await;
        assert_eq!(pending.len(), 1);
        let id = pending[0].id.clone();
        let resolved = reg.resolve(&id, ApprovalOutcome::Approved).await;
        assert!(resolved);
        let outcome = task.await.unwrap();
        assert_eq!(outcome, ApprovalOutcome::Approved);
    }
}
