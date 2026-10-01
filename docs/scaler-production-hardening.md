# Runner Scaler: Production Hardening Handoff

Status: implemented for internal validation and staged rollout. This document is the FR-012 deliverable from `specs/001-runner-scaler-vmss/spec.md`, updated for `specs/002-scaler-standalone-service/spec.md` (the scaler was extracted from `cmd/api` into its own binary, `cmd/scaler`) — known gaps and required work before the capacity scaler is relied on for full, unattended production traffic. Tracked under **CP-2690 (M3)**.

## Known gaps

### 1. No per-instance drain before scale-in (FR-010 partially satisfied)

`SetCapacity` asks Azure to resize the VMSS; Azure — not this service — chooses which instance(s) to remove. There is no linkage today between a VMSS instance ID and a runner ID in the runner registry (`internal/api/registry`, read by the standalone scaler's `internal/scaler.PostgresRunnerSource`), so the scaler cannot:

- identify which runner corresponds to the instance Azure is about to remove, or
- cordon that specific runner (stop directing new placements to it) and wait for its in-flight sandboxes to drain before requesting removal.

**Impact:** a scale-in can remove an instance that is still serving sandboxes. Those sandboxes fail or need relocation through whatever recovery path already exists for a runner disappearing unexpectedly (see `docs/architecture.md` § Recovering a Crashed Guest) — the same path as an unplanned runner loss, not a graceful handoff.

**What's needed:** the runner registration protocol (`proto/runner/v1/runner.proto`, `internal/api/grpc/runner_server.go`) would need to carry the Azure VM instance ID, and the scaler would need to move from `SetCapacity` (fleet-wide count) to `BeginDeleteInstances` (specific instance IDs) after cordoning and waiting for that runner's `CapacityUsed` to reach zero (or a timeout). This is a larger, cross-cutting change than this feature's scope and was not attempted here. Unaffected by the standalone-component extraction — the limitation is in the Azure/registry data model, not in which process runs the decision loop.

**Mitigation until fixed:** keep the scale-in threshold conservative (require a large, clearly-idle margin) and monitor sandbox failure/relocation rates after each scale-in during the rollout below.

### 2. No live-Azure end-to-end validation yet

Everything is verified against the `internal/azurescale.Fake` and unit/contract tests (`go test ./...`, all passing). `quickstart.md` Scenario 5 (manual validation against a real non-production VMSS) has not been run. Do this before any production reliance:

- Confirm `armcompute`/`azidentity` workload-identity auth actually succeeds against a real federated identity scoped per fleet (mirroring `runnerMetricsCollector` in `charts/firecracker-sandbox-service/values.yaml` in `n8n-cloud-infrastructure-next`).
- Confirm `SetCapacity`'s poller (`PollUntilDone`) behaves as expected under real Azure latency and doesn't block the evaluation loop for longer than `SANDBOX_SCALER_EVAL_INTERVAL`.

### 3. Fixed ±1 node step per evaluation cycle

Scale-out and scale-in each move the VMSS by exactly one node per cycle (see `internal/scaler/scaler.go`'s `decide`), regardless of how far free capacity is from the threshold. This is a deliberate, conservative default (the spec did not define a step-size formula), but it means a sudden large demand spike is corrected gradually — `EvalInterval` cycles times `1` node — not immediately. Combined with the 5-minute SC-001 reaction budget and a 1-minute default eval interval, a single missing node is corrected within budget; a demand spike requiring many additional nodes is not. Revisit if real traffic patterns show this is too slow.

### 4. In-memory state resets on pod restart/reschedule

`lastAction` (cooldown) and `scaleInConditionSince` (sustained-window tracking) live in process memory (`internal/scaler.Scaler`), not the store. A pod restart resets both to zero, which the code deliberately treats as "cooldown elapsed" / "no sustained window yet" (matching the spec's own first-run edge case) rather than erroring — but it also means a restart during a cooldown window silently lifts that cooldown early. Low risk given the standalone component is a small, focused binary that restarts rarely, but worth knowing. Unchanged by the `002` extraction; if anything, slightly more exposed now that the scaler can restart independently of the API (previously the two restarted together).

### 5. Single VMSS per scaler instance

By design (see `001`'s `plan.md` Scale/Scope, carried into `002`) — one scaler evaluates and controls exactly one VMSS, matching one `cmd/scaler` deployment per fleet. Multi-region/multi-VMSS coordination is out of scope; each fleet needs its own `cmd/scaler` deployment with its own `SANDBOX_SCALER_AZURE_*` configuration.

### 6. Not yet wired into the Helm chart

`charts/n8n-sandbox-service` (this repo) has no Deployment/ConfigMap/ServiceAccount entries for `cmd/scaler` yet — the idle sweeper's/API's equivalent wiring (`idleSweepInterval`, `SANDBOX_API_*` configmap entries) is the model to follow but was deliberately left out of `002`'s task list, same boundary `001` already drew. The Go binary (`make scaler`, `Dockerfile.scaler`) is fully functional and independently runnable via raw environment variables today; a deployment via this chart cannot run the standalone scaler until that wiring is added:
- A new Deployment (`replicas: 1` — see gap-adjacent note in §8 below) running the `cmd/scaler` image.
- A new ConfigMap/Secret for `SANDBOX_SCALER_*` (policy, Postgres connection, `SANDBOX_SCALER_API_TOKEN`).
- A corresponding `SANDBOX_API_SCALER_URL`/`SANDBOX_API_SCALER_TOKEN` entry on the existing API Deployment, pointed at the new scaler Service.
- The Azure workload-identity ServiceAccount/federated-credential wiring, which belongs in `n8n-cloud-infrastructure-next` (mirroring `runnerMetricsCollector` in `charts/firecracker-sandbox-service`) — see §8 for the identity itself.

### 7. No runtime policy mutation

`GET /admin/scaler` (proxied to the standalone component's `GET /policy`) is read-only. Changing thresholds/min/max/cooldown requires a config change and redeploy of `cmd/scaler`, consistent with every other `SANDBOX_*` setting in this service. Not a gap so much as a deliberate scope boundary (see `specs/002-scaler-standalone-service/contracts/scaler-internal-api.md`'s "Non-goals").

### 8. IAM separation: provisioning still required (infra-repo work)

`002-scaler-standalone-service` makes the **code** capable of least-privilege separation — `cmd/scaler` and `cmd/api` are now distinct binaries with distinct configuration, and `cmd/api` has zero remaining reference to `internal/azurescale` or any Azure/VMSS field (enforced by `internal/api/config.TestAPIConfigHasNoScalerOrAzureFields`). It does **not** itself provision any Azure identity — that's infra-repo work, same boundary `001` already drew (gap #6 there). Before relying on this in production:

- Provision a **new, separate** Azure user-assigned managed identity for `cmd/scaler` (do not reuse any identity the API or `runnerMetricsCollector` already holds), scoped to VMSS capacity write (e.g. a custom role limited to `Microsoft.Compute/virtualMachineScaleSets/write` and `/read` on the single target VMSS — narrower than the generic `Virtual Machine Contributor` role, which also grants power-off/delete/extension-management the scaler never needs).
- Federate that identity to `cmd/scaler`'s Kubernetes ServiceAccount (`system:serviceaccount:<namespace>:<scaler-service-account>`), mirroring `runnerMetricsCollector`'s federated-credential pattern in `terraform/environments/gwc/services-1/sandbox-firecracker.tf` (in `n8n-cloud-infrastructure-next`).
- Confirm (post-provisioning) that the **API's** identity — whatever it uses for its own purposes — carries no VMSS-write permission. Before `002`, the API's identity was used to construct the `azurescale.VMSSScaler` directly; after `002`, nothing in `cmd/api` constructs one at all, so there is no remaining code-level reason for the API to hold that permission.

### 9. Shared Postgres advisory-lock bug — resolved by this extraction, not by a fix

`001`'s embedded scaler reused `store.TryRun`'s hardcoded `idleSweepLockKey`, the same lock the idle sweeper uses — the two periodic jobs could silently contend for the same lock in Postgres/multi-pod mode. This is now moot: `internal/api/scaler.go` (the file that had the bug) was deleted outright as part of `002`'s extraction, and the standalone `cmd/scaler` runs as a single replica with no locking code at all (FR-008) — there is no lock to share. Noted here for the historical record, not as an open gap.

## What was explicitly descoped for this iteration (see spec.md Clarifications)

These are not bugs — they were deliberately cut from scope during `/speckit-clarify` and are noted here so they aren't mistaken for oversights during the M3 hardening pass:

- **Queue depth** as a second scaling signal — the sandbox service has no request queue today; the scaler uses aggregate registry free capacity alone.
- **Staged "internal + 1% rollout" traffic scoping** — removed entirely; the scaler applies to all production traffic from the start once enabled.

## Suggested rollout sequence

1. Add the Helm chart wiring (gap #6) and provision the standalone identity (gap #8) in a non-production environment.
2. Deploy `cmd/scaler` + updated `cmd/api` there; run `specs/002-scaler-standalone-service/quickstart.md` Scenarios 1–5 (decision-logic parity, proxy behavior, failure isolation).
3. Run `specs/001-runner-scaler-vmss/quickstart.md` Scenario 5 equivalent (manual, real Azure) against a non-production VMSS; record findings back into this document.
4. Enable in production with a conservative policy (wide thresholds, long cooldown) and watch `sandbox_scaler_*` metrics and `scaler evaluation` log lines for at least one full day before tightening.
5. Address gap #1 (per-instance drain) before scale-in is trusted at higher frequency/volume; until then, keep scale-in thresholds conservative per the mitigation above.
