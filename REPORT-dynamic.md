# REPORT — mcp-absorb `--dynamic` v1 (SPEC-dynamic.md)

Branch: dynamic-mvp (base 12a8f62). Spec followed exactly; no non-goal was
implemented (no notification relay, no tool promotion, no auto-restart, no
inject loops in dynamic mode).

## What was built

All in `src/main.rs` (new code added in-place beside the existing code;
decision: **extended main.rs, no separate dyn.rs module** — the handlers need
private access to `Sub`/`result_text`/`now_secs`/`age_str` and the repo is a
single-file binary):

1. **parse_args**: gains `--dynamic` and `--allowlist PATH`. In dynamic mode a
   `--` subordinate argv is a usage error (`exit 2` with a message naming the
   flag and the attach path). Without the flag, behavior is unchanged
   (`-- <cmd>` still required).
2. **Cfg**: `+ dynamic: bool, allowlist: PathBuf`; default allowlist
   `~/.config/mcp-absorb/allowlist.json`, read ONLY in dynamic mode. Missing
   or unparseable file ⇒ dynamic mode starts with an empty registry + the
   reason, which `attach` quotes; the server still answers initialize /
   tools/list normally (no crash across CC's 30 s connect gate).
3. **Allowlist entries {name, command, args, env}**: parsed by the pure fn
   `parse_allowlist` (strict: bad JSON, non-array, missing name/command,
   non-string args, non-object env ⇒ `format!` String errors); argv built by
   the pure fn `build_argv` (command + args — argv never comes from the tool
   call). Spawn goes through `Sub::spawn_env`, a sibling of the existing
   `Sub::spawn` (which now delegates with an empty env — static path
   unchanged); the entry's `env` is applied to the child.
4. **DynEntry/Dyn state + 5 handlers** (`dyn_attach`/`dyn_detach`/`dyn_list`/
   `dyn_tools`/`dyn_call`): double-attach rejected ("already attached; detach
   it first"); unknown name errors list the available allowlist names; attach
   does initialize (10 s handshake timeout) + notifications/initialized +
   tools/list, caches the tool list, on handshake failure kills the child and
   errors; `call` takes `timeout_s` default 60 clamped 1–300, flattens via
   `result_text`; a call timeout or write error marks the entry **unhealthy**
   (recover = detach + attach); detach kills + waits the child, errors if not
   attached; `list` shows `name: ready|unhealthy, N tool(s), attached <age>`.
5. **own_tools_json / tools/list**: dynamic mode exposes ONLY the 5 dynamic
   tools (attach/detach/list/tools/call, schemas as specced); static mode
   output is byte-identical (subordinate tools + 5 inject-loop tools). Static
   mode's initialize proxying and tools/call passthrough are untouched (the
   only static-path change is threading `sub` as an Option; in static mode it
   is always Some and semantics are identical — verified by an end-to-end
   static-mode test driving a real subordinate through the binary).
6. **Shutdown**: dynamic mode kills all attached children on exit.

## Fixture choice

**Python** (`src/tests/fixture_stdio_mcp.py`): `python3` (homebrew 3.14.7)
spawns fine from cargo tests in this environment, so the specced Python stdio
fixture was kept — **no Rust fallback needed**. It answers initialize,
tools/list (2 fake tools: `echo`, `slow`) and tools/call (`echo` echoes the
arguments; `slow` sleeps). Integration tests (`src/tests/dynamic.rs`, 8 tests)
drive the real binary via `env!("CARGO_BIN_EXE_mcp-absorb") --dynamic
--allowlist <tmp file>`: five-tools-only listing, attach/tools/call/detach
round trip, double-attach reject (entry still usable after), timeout ⇒
unhealthy + detach/attach recovery, unknown name lists names, missing
allowlist ⇒ empty registry + reason, `--dynamic -- cmd` ⇒ exit 2, and a
static-mode parity test (fixture as `--` subordinate: its 2 tools + the 5
loop tools proxied, proxied call works, no dynamic tools present).

## Test counts (recounted from a clean `cargo test`)

- Unit (`unittests`, src/main.rs): **23 passed** = 19 existing (untouched, all
  still green) + **4 new** (`dyn_tests`: allowlist parse ok, empty-array ok,
  bad-shape rejections, argv build).
- Integration (`tests/dynamic.rs`): **8 passed** = 8 new.
- New total: **12** (≥ 6 required). Overall: **31 passed, 0 failed**.
- `cargo build --release`: 0 errors, 0 warnings (verified after touching the
  sources so both targets rebuilt from scratch of warnings).
- Static parity: existing tests untouched and green; the new static-mode
  integration test exercises the unchanged static path end to end.

## git status --porcelain (src/, captured after committing all code deliverables;
## REPORT-dynamic.md itself was the only untracked file at this moment and is
## committed immediately after this capture)

```
?? REPORT-dynamic.md
```

## git log --oneline -2 (src/, at capture time)

```
6d34302 dynamic v1: --dynamic + --allowlist, allowlist-gated attach/detach/list/tools/call
12a8f62 docs: SPEC-dynamic.md — attach/detach stdio MCP subordinates at runtime (his idea 11-07, --dynamic flag)
```

LANE-DONE
