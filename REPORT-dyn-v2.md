# REPORT-dyn-v2 — SPEC-dynamic v2a: inject loops over (server, tool) pairs

Branch `dynamic-loops` in `src/`, base `a24891e`. One code commit: `9ba2677`.

## What changed

`src/main.rs`:

- **Dynamic tool set = 5 dyn tools + 5 inject-loop tools.** `dyn_all_tools_json`
  = `dyn_tools_json` + `loop_tools_json(cfg, true)`; `tools/list` in dynamic
  mode now serves all ten. Static mode still calls `own_tools_json` =
  `loop_tools_json(cfg, false)`, whose create schema is the old literal
  byte-for-byte.
- **`create_tttt_inject_loop` (dynamic)** gained the extra required param
  `server` — the server must be in the Dyn map at create time (else a clear
  error quoting the name), and `tool` is a tool ON that server. All other loop
  options (`args` / `interval_secs` / `ignore` / `prefix` / `poll_timeout_secs`
  / filter options) keep the static LoopSpec semantics unchanged.
  `loop_server_arg` rejects a `server` param in STATIC mode with a clear error
  (unit- and integration-tested).
- **Per-tick resolution via `SubRef`** (`Static(Arc<Sub>)` |
  `Dynamic(Arc<Dyn>, String, gen)`): `run_loop` takes a SubRef and resolves the
  poll target at EVERY tick (`resolve_target` → pure decision `resolve_tick`).
  Server missing from the map = the loop ENDS after ONE final note inject
  (`loop <name>: server <server> not attached, ending — recreate to resume`),
  then removes itself from the loops map (and the servers side-map). A detach
  that was followed by a re-attach is caught too: each `DynEntry` carries an
  attach `gen` (from `Dyn::next_gen`) and the loop stamps the gen it was
  created against — a newer gen in the map also ENDS the loop, so **re-attach
  revives nothing** (explicit recreate only). **Call-level failures (timeout,
  write error) keep the static failure semantics** (skip/backlog/alarm) and do
  NOT end the loop — only a missing map entry / stale generation does
  (`resolve_tick` deliberately ignores `last_call_failed`). `detach` ends
  loops polling that server the same way: the removed entry makes the next
  tick resolve to Ends; `dyn_detach` additionally drains the dead sub's pending
  senders with an error response so an in-flight poll returns promptly (a
  call-level error, which per the rule does not itself end the loop — it just
  gets the loop to its ending tick fast).
- **`list_tttt_inject_loops` (dynamic)** shows `server.tool` per loop (side-map
  `LoopServers: loop name -> server`, so `LoopSpec`/`LoopHandle` and the
  existing tests that build them literally stay untouched); `delete`,
  `set_inject_filter`, `fetch_inject_backlog` stay loop-keyed and unchanged
  (dynamic delete just also cleans the side-map).

`tests/dynamic.rs`: v2a supersedes the content of two v1 assertions about the
DYNAMIC tool set (both are dynamic-mode tests, not static parity):
`dynamic_mode_exposes_only_the_five_dynamic_tools` (asserted exactly 5 names)
→ renamed `dynamic_mode_exposes_the_five_dyn_plus_five_inject_loop_tools`
(10 names), and the `missing_allowlist_...` tool-count `5` → `10`. All other
existing tests are untouched and green.

`tests/dyn_loops_v2a.rs` (new): 3 integration tests (own harness;
`--tttt-bin` true-no-op): full loop lifecycle (create requires attached server;
loop polls `fx.echo`; `list` shows `l1: poll fx.echo` with advancing
poll/inject counters; `set_inject_filter` + `fetch_inject_backlog` work on the
dynamic loop; detach ⇒ loop disappears from the list (final note went through
the no-op path); re-attach revives nothing; explicit recreate resumes),
loop-keyed delete (incl. cleanup of the server side-map), and the static-mode
`server`-param reject + unchanged static create/list/delete.

## Test outcome (recounted from two clean full runs)

- Baseline (base `a24891e` == the gate's `15af237` numbers): `cargo test` =
  **31 passed (23 unit + 8 integration), 0 failed** — confirmed by a real run
  before touching anything.
- Final: `cargo test` = **42 passed, 0 failed**
  (unit: **31** = 23 existing + **8 new** `v2a_loop_tests`;
  integration: **11** = 8 existing in `tests/dynamic.rs` (6 untouched,
  2 with the v2a-mandated dynamic-tool-set assertions) + **3 new** in
  `tests/dyn_loops_v2a.rs`).
  **New test functions: 11 (8 unit + 3 integration) ≥ 5.** Zero failures.
  Ran the full suite twice: identical results (no flakes in the timing-based
  integration test; the loop-lifecycle test finishes in ~6 s).
- New unit tests cover the spec list: loop-spec parse (`server` required in
  dynamic / rejected in static / must be a non-empty string), resolver
  transitions ok / missing-ends / detach+reattach-ends (revive nothing) /
  **call-failure does NOT end** (pure `resolve_tick` table + live `Dyn` map
  tests), static target always polls, `LoopSpec` field-set unchanged.

## /bin/true integration outcome

The loop integration test RAN (no pure-fn-only fallback needed) with one
adaptation: **this macOS host has no `/bin/true`** (only `/usr/bin/true`; the
first run against literal `/bin/true` failed silently because the inject
`spawn` error is swallowed — polls advanced, injects stayed 0). The test
helper `true_noop()` prefers `/bin/true` and falls back to `/usr/bin/true`.
`run_ctl_send` tolerates the true-no-op fully: `tttt ctl send <session> ...`
and `--enter` both exit 0 (verified with an argv-logging stub that recorded
`ctl send pty-1 [l1] {"text": "tick"}` + `ctl send pty-1 --enter` per inject),
so the loop's inject path executes and the `injects` counter advances while
nothing is really sent. The final end-note inject goes through the same no-op.

## Gates

- `cargo test` in `src/`: 42 passed / 0 failed (all 31 existing still pass;
  the 2 edited dynamic-mode assertions carry the new v2a tool-set expectation)
  — **PASS**
- `cargo build --release` in `src/`: **0 errors, 0 warnings** (grep of a fresh
  build: 0; test-profile build also 0) — **PASS**
- Static parity: existing static tests untouched and green, AND an empirical
  byte-parity check: built the base (`a24891e`) binary via `git stash` +
  `CARGO_TARGET_DIR=.../tmp/target-base` and diffed the full static
  `tools/list` response (fixture subordinate, default flags) between base and
  new binaries — **identical: True** — **PASS**
- `src/REPORT-dyn-v2.md` exists at this exact path and ends with `LANE-DONE`
  — **PASS**

Scope: v2a only. No v2b tool-promotion work, no auto-restart,
`docs/SPEC-dynamic.md` untouched, no unrelated reformatting, no new briefs.

## `git status --porcelain` (src/, captured after commit 9ba2677, before this
report file existed / was committed)

```
(empty — working tree clean)
```

## `git log --oneline -2` (src/)

```
9ba2677 feat: SPEC-dynamic v2a — inject loops over (server, tool) pairs in --dynamic mode
a24891e docs: SPEC-dynamic v2a amended — detach ends loops (explicit recreate), no auto-resume (ayourtch: too surprising)
```

LANE-DONE
