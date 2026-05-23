# Kestrel Next-Gen Roadmap

> **Status:** Living document. Updated as phases ship.
> **Drafted:** 2026-05-23

## Where we are

Phases 1–5 built the foundation: TLS WebSocket transport with PSK-HMAC + TLS-exporter binding (Phase 1), input/screen MCP tools (Phase 2), clipboard + PTY shell (Phase 3), accessibility tree (Phase 4), supervisor + dashboard + KVM (Phase 5). After Phase 5, we went through 17 deep-dive review passes (3 consecutive zero-finding required to exit), then a hardening series adding per-node PSKs, session-cookie dashboard auth + browser write UI, KVM hot-reload, PSK zeroization, MCP audit log, TUI tests, dashboard TLS, cross-platform AX, screenshot thumbnails, and a browser shell pane.

The MVP works. A signed-in operator can drive a fleet of machines from Claude Code, see them all on a dashboard, take actions in a browser, and audit every call. Per-node PSKs and session-cookie auth give us a defensible security story. The deep-dive review loop set a quality floor we can build on.

## Where we go

Today's Kestrel lets an AI *mechanically* operate machines: one keystroke, one click, one shell command at a time. Reasoning about *what's happening* on the fleet — across time, across machines, with appropriate guardrails — is left to the AI to manage in its own context. That's the next leap.

The thesis: **Kestrel becomes 10× more valuable when the abstractions match how an AI naturally reasons about a fleet — persistent state, change deltas, cross-machine workflows, capability matching, scoped permissions, ephemeral compute, extensible vendor plugins, real-time interaction.**

## The eight next-gen phases

Listed in **build order**. Ordering favors foundations first; later phases depend on earlier ones.

| # | Phase | Status |
|---|---|---|
| 6 | **Persistent World State + Diffs** | ✅ shipped (PRs #47–#51) |
| 7 | **Async Long-Running Jobs** | ✅ shipped (PR #52) |
| 8 | **Capability Advertisement + Smart Routing** | ✅ shipped (PR #53) |
| 9 | **Workflow Choreography** | ✅ shipped (PR #54) |
| 10 | **Sandbox Provisioning** | ✅ shipped registry + lifecycle (PR #55); backend bodies are stubs awaiting hardware verification |
| 11 | **Multi-Tenant Identity + Approval Gates** | ✅ shipped data model + approval primitives (PR #56); check_auth wiring + dashboard UI = Phase 11b |
| 12 | **Plugin Model for Capabilities** | ✅ shipped agent-side host (PR #57); hub-MCP surfacing = Phase 12b |
| 13 | **WebRTC Real-Time Streaming** | ✅ shipped signalling layer; RTP pipeline = Phase 13b |

Each phase ships in multiple PRs and is independently mergeable. Phases 6 and 7 are MUST-DO foundations; later phases assume them.

---

## Phase summaries

### Phase 6 — Persistent World State + Diffs

**Intent.** Maintain a structured, event-driven snapshot of each node's observable state so the AI queries cheap deltas instead of re-screenshotting and re-walking the AX tree every turn.

**Shape.** Agent runs a `WorldObserver` task that polls local state every ~2s (focused app, mouse position, clipboard metadata, open shell sessions, etc.) and emits a `Payload::WorldUpdate` event when something changed. Hub stores the latest per node, broadcasts changes on the existing event channel. New MCP tools `world_state` and `world_diff_since` give the AI cheap, structured queries.

**Non-goals here.** Pixel-level visual diffs (Phase 6b if demand surfaces). Full window enumeration (the existing AX walker covers it). Time-travel replay.

**Detailed plan.** `docs/superpowers/plans/2026-05-23-phase6-world-state.md`.

### Phase 7 — Async Long-Running Jobs

**Intent.** Decouple tool invocation from completion. Today `shell_run` blocks the MCP call for the duration of the command; a 30-minute build hangs the AI's turn. Replace with a job model.

**Shape.** New MCP tools: `job_start({op, args})` returns a `job_id` and an initial status; `job_status(job_id)` polls; `job_output(job_id, since_offset)` streams stdout; `job_cancel(job_id)` aborts. The current synchronous tools stay as thin wrappers for short ops. Job state lives in the hub (Arc<RwLock<HashMap<job_id, Job>>>); jobs survive reconnects to the underlying node, with bounded buffering.

**Non-goals here.** Cross-node jobs (Phase 9). Persistence across hub restarts (jobs are in-memory; that's fine for v1).

### Phase 8 — Capability Advertisement + Smart Routing

**Intent.** Stop hardcoding node IDs. Let the AI ask for *capability*: "give me a node with a GPU and a connected display."

**Shape.** Agents include a `Capabilities { os, has_gpu, has_display, has_sudo, has_docker, ... }` block in their `SystemInfo` on handshake. Hub aggregates. New MCP tool `fleet_find({needs: [...]})` returns matching `node_id`s ordered by reverse-current-load. Existing tools keep working with explicit `node_id`s.

**Non-goals here.** Continuous capability updates (we just snapshot on handshake; capabilities don't change often enough to be worth a watcher). Heterogeneous capability scoring (no ML).

### Phase 9 — Workflow Choreography

**Intent.** Cross-machine declarative workflows. The AI specifies the steps and constraints; the hub executes.

**Shape.** New MCP tool `workflow_run({steps: [...], on_error: ..., timeout: ...})`. A step is `{on: node_predicate, do: tool_call, capture: var_name}` and later steps can reference captured vars. Built on Phase 6 (world-state queries between steps), Phase 7 (long-running steps as jobs), Phase 8 (predicate-routed steps). Failures: per-step retry policies, `on_error: rollback` for transactions.

**Non-goals here.** A full DAG language (linear sequences with `if` are enough for v1). Visual workflow editor.

### Phase 10 — Sandbox Provisioning

**Intent.** Throwaway machines on demand. The AI can take risks ("yes try the dangerous migration") because there's a fresh VM behind the work.

**Shape.** New MCP tool `sandbox_spawn({image: "ubuntu-24.04" | "macos-15" | ..., ttl: 1h})` returns a `node_id` for a newly-provisioned VM with the agent already installed and enrolled. Backends: Tart on macOS, Lima/QEMU on Linux, optional cloud provider plugin. Auto-teardown on TTL expiry; explicit `sandbox_destroy(node_id)`.

**Non-goals here.** GPU passthrough (out of scope for v1). Persistent sandboxes (defeats the throwaway point). Cross-platform sandbox image catalog (we provide a few base images; vendors add more).

### Phase 11 — Multi-Tenant Identity + Approval Gates

**Intent.** Team-scale Kestrel. Multiple operators with distinct identities, per-action policy, dashboard-mediated approvals for sensitive operations.

**Shape.** OIDC integration (Google, GitHub, Okta). Per-user dashboard sessions (extending Phase-5 session-cookie infra). Audit log gains `user_id` field. New policy file: `policies.toml` maps user → allowed-ops; an "approval-required" op blocks the MCP call until an authorized operator clicks approve in the dashboard (60s timeout, configurable).

**Non-goals here.** Custom OIDC providers beyond a small set. Group memberships (RBAC v2). Fine-grained per-app-per-key permissions (this is a control plane, not a key management system).

### Phase 12 — Plugin Model for Capabilities

**Intent.** Vendor extensibility. Apps that have their own AX/automation hooks (Photoshop, Figma, an IDE) can ship Kestrel plugins exposing app-specific MCP tools.

**Shape.** Agent loads `.so`/`.dylib`/`.dll` plugins from `~/.kestrel/plugins/`. Plugin ABI: `extern "C" fn kestrel_plugin_info() -> PluginInfo` and `extern "C" fn kestrel_plugin_handle(req: &Request) -> Response`. Plugins register their tools at agent boot; tools surface through the hub's MCP automatically. Plugins are sandboxed via per-plugin process isolation (each plugin runs as a child process, not as a `dlopen` — safer ABI story).

**Non-goals here.** A plugin marketplace. Capability-permission system within plugins (plugins inherit the agent's privilege; operators choose plugins they trust).

### Phase 13 — WebRTC Real-Time Streaming

**Intent.** Sub-second interactive control. The AI flies a cursor across the screen in real time; humans watch live; both interact with the same desktop simultaneously.

**Shape.** Replace the polled screenshot path with a WebRTC pipeline: agent screen-captures continuously, encodes H.264/AV1, streams via SFU on the hub to dashboard browsers and to the AI (via a frame-extract proxy that turns the video into AccessibilityNode-paired keyframes). Input flows the other way: low-latency mouse/keyboard frames via the same WebRTC data channel.

**Non-goals here.** Audio streaming (Phase 13b). Multi-screen tiling. Recording (handled by browser-side or a separate recorder service).

---

## What ships under what name

Each phase produces 3–6 PRs. Branch naming: `feat/phase<N>-<slug>`. Plan docs: `docs/superpowers/plans/2026-05-23-phase<N>-<slug>.md`, written when the phase starts so the plan reflects current state of the codebase. The roadmap doc you're reading now is the master index.

## Order of operations for the implementer

1. Build Phase 6 in full (5 PRs). Required foundation for 7-9-11.
2. Build Phase 7 in full. Required foundation for 9-10.
3. Build Phase 8 in full. Cheap; unlocks Phase 9.
4. Build Phase 9 in full. The big-payoff phase.
5. Build Phase 10. Self-contained; can interleave.
6. Build Phase 11. Self-contained; needed before any team-scale deployment.
7. Build Phase 12. Architectural; can interleave with anything after Phase 7.
8. Build Phase 13. Heaviest; ship last so we don't block other phases on its uncertainty.

## What I can and can't verify

Where a phase involves OS-specific or external-service-specific code I can't runtime-verify from a macOS dev machine — sandbox provisioning on Linux, Windows AX walks, OIDC against real providers, WebRTC pipelines with real codecs — I'll write architecturally-correct skeletons with explicit caveats in the commit messages, same pattern as the cross-platform AX work in PR #43. Downstream operators on the right platform fill in any drift.
