# SPEC: mcp-absorb `--dynamic` — attach/detach MCP subordinates at runtime

Status: draft for ayourtch's review (his idea 2026-11-07 #mini 41943, steer: "behind the
flag --dynamic", 4196x). Grounded in src/main.rs as of 07e11b6.

## Problem

CC registers MCP servers once, at session start. A server that dies at spawn (or is not
yet registered) is gone for the whole session; recovery needs /mcp reconnect (registered
servers only) or a restart. The tttt MCP lived this way for weeks (raw JSON-RPC on
/tmp/tttt-mini.sock as the workaround). A multiplexer turns "attach a server mid-session"
into a tool call.

## Mode split

- Default (no flag): byte-identical to today. One config-fixed subordinate from the
  `--` argv, inject-loop tools, everything as shipped. The 4 running instances
  (irc/msgd/webex/email) are untouched by this work.
- `--dynamic`: no `--` subordinate argv (absent or error). The binary exposes the
  5 dynamic tools below INSTEAD OF the inject-loop tools (loops poll one fixed
  subordinate's tool; without a fixed sub they are meaningless — v1 removes them,
  see v2). `--session`/`--tttt-bin` keep their meaning.

    mcp-absorb --dynamic [--allowlist PATH] [--session pty-1] [--tttt-bin tttt]

## Allowlist (what may be attached)

Default path `~/.config/mcp-absorb/allowlist.json`, override `--allowlist PATH`.
JSON array of named entries; `attach` takes a NAME, never a command — an in-session
prompt (or an inject) can never spawn arbitrary code:

    [
      {"name": "tttt",   "command": "/Users/ayourtch/bin/tttt", "args": ["mcp"], "env": {"TTTT_MCP_SOCKET": "/tmp/tttt-mini.sock"}},
      {"name": "mqtt",   "command": "...", "args": [...]}
    ]

Missing/unreadable allowlist = dynamic mode starts with an empty registry and `attach`
fails with the reason (never a crash: CC holds the server alive across its 30 s
connect gate). Unknown name -> error listing available names.

## Tools (dynamic mode only)

- `attach(name)` -> spawns `command+args+env` via `Sub::spawn` (main.rs:34, reused as-is),
  then initialize handshake + tools/list, cache the result. Errors: spawn failure,
  handshake timeout (10 s), name already attached (rejected; detach first), name not in
  allowlist. Returns the tool count + names.
- `detach(name)` -> kill child (Child handle is held per entry), remove entry. Idempotent
  error if not attached.
- `list()` -> one line per entry: name, state (ready/unhealthy), tool count, attached-ago.
  Unhealthy = last call failed with write-error or timeout (no auto-restart in v1;
  recover = detach + attach).
- `tools(name)` -> cached tools/list as text (name, description, JSON schema). This is
  how the model discovers schemas; the registry does not re-emit them as first-class
  tools (see Open question).
- `call(name, tool, args)` -> `Sub::call("tools/call", ...)` passthrough, flattened text
  result (result_text, main.rs:101). Timeout: `timeout_s` param, default 60, clamp 1-300.
  On timeout the entry is marked unhealthy, the pending id is dropped (existing behavior,
  main.rs:87).

All five are plain JSON-in/text-out tools — no prompts/resources wiring needed.

## What is explicitly NOT in v1

- **Tool promotion to first-class CC tools.** Two gates block it today: (a) Sub's reader
  thread drops child notifications (main.rs:66-67 comment), so a child's
  `notifications/tools/list_changed` never reaches absorb, and (b) absorb would have to
  re-emit it upstream AND CC must honor list_changed by re-fetching — untested here.
  v1 contract: generic `call()` + `tools(name)` text. The cost is one extra hop
  (schema discovery via a tool call instead of the tool registry); the benefit is the
  whole feature works on today's CC without behavior bets.
- Inject-loop tools in dynamic mode (v2: loops over a `(server, tool)` pair).
- Auto-restart of crashed children; remote transports (stdio children only, matching
  the existing Sub).

## Open question (needs one experiment, not code)

Does THIS CC build re-fetch tools on `notifications/tools/list_changed`? Test: register
a tiny test server that sends list_changed after a delay, watch whether new tools appear
in the session without a restart. If yes, v2 relays child notifications and promotes
`name__tool` as real tools at attach time. If no, the v1 contract stands permanently.

## v2a: inject loops over dynamically attached servers (his steer 2026-11-07 13:4x: "implement v2 first with the inject loop")

Goal: in `--dynamic` mode the 5 inject-loop tools come back, with the loop's poll target
being a `(server, tool)` pair instead of the single static subordinate. The tool-promotion
experiment (old v2, renamed v2b) stays parked.

- Dynamic tool set = the 5 dyn tools + `create/delete/list_tttt_inject_loop` +
  `set_inject_filter` + `fetch_inject_backlog`. Static mode: byte-identical to today
  (and `create_tttt_inject_loop` with a `server` param there = clear reject).
- `create_tttt_inject_loop` (dynamic): extra required param `server` — must be attached
  at create time; `tool` is a tool ON that server. `args` / `interval_secs` / `ignore` /
  `prefix` / poll timeout / filter options keep the static semantics (LoopSpec fields,
  main.rs:127).
- **Resolution per tick**: the loop resolves `server -> Arc<Sub>` from the Dyn map at
  every poll. Server missing from the map (detach, or never attached) = the loop ENDS
  after injecting ONE final note (`loop <name>: server <server> not attached, ending —
  recreate to resume`). `detach` ends loops polling that server the same way; re-attach
  revives NOTHING — recreating a loop is one explicit tool call. Loop lifetime ⊆
  server-attached lifetime. (ayourtch 13:4x, twice: auto-resume is too surprising — the
  same allowlist name may point elsewhere after an edit, a resume injects a stale-delta
  burst, and silent wake-ups break predictability. "Perfect." on the end-on-detach form.)
  Call-level failures (timeout, write error) keep the STATIC failure semantics
  (skip/backlog) — they do not end the loop; only a missing map entry does.
- `list_tttt_inject_loop` (dynamic) shows `server` + `tool` per loop;
  delete / set_inject_filter / fetch_inject_backlog stay loop-keyed and unchanged.
- Plumbing: `run_loop` takes a `SubRef` (`Static(Arc<Sub>)` | `Dynamic(Arc<Dyn>, String)`)
  instead of a bare `Arc<Sub>`; the Static variant keeps today's behavior exactly.

Tests:
- Pure: loop-spec parse (server required in dynamic, rejected in static), resolver
  transitions ok / missing-ends (and call-failure does NOT end).
- Integration: attach the fixture, create a loop polling the fixture tool with a
  matching pattern, assert the loop advances and the inject path no-ops safely —
  `tttt_bin=/bin/true` makes `run_ctl_send` a no-op if it tolerates that; if it does
  not, cover the resolver + spec plumbing as pure fns and say so in the report.

## Implementation shape (~150-200 LOC + tests)

- `Cfg` += `dynamic: bool, allowlist: PathBuf` (parse_args, main.rs:839: new flags;
  dynamic mode must reject a `--` argv with a clear usage error).
- `struct DynEntry { sub: Arc<Sub>, child: Child, tools: Vec<Value>, attached_at: u64, healthy: bool }`
  + `Dyn { map: Mutex<HashMap<String, DynEntry>>, allowlist: Vec<AllowEntry> }` in main.
- 5 `dyn_*` handler fns beside the existing `tool_*` fns; `own_tools_json` (main.rs:590)
  switches on `cfg.dynamic`.
- Allowlist parse + argv build as pure fns with unit tests (the repo's test style).
- Integration test: `tests/fixture_stdio_mcp.py` — a 20-line stdio MCP that answers
  initialize/tools/list/tools/call; tests drive attach/tools/call/detach against it,
  including the double-attach reject and a timeout marking unhealthy.

## Gates (recountable)

- `cargo build --release` 0 errors/warnings
- `cargo test` — all existing tests pass + 6+ new (allowlist parse ok/bad, argv build,
  attach/detach/call round-trip vs fixture, double-attach reject, timeout->unhealthy)
- Static-mode parity: existing tests untouched and green; no change to `own_tools_json`
  output when `dynamic=false`
- E2E (manual, me): 5th instance in ~/.claude.json (`mcp-absorb --dynamic`), attach
  `tttt` from a scratch session, call `tttt_startup_done`-equivalent through `call()`,
  detach, verify the child process is gone.
