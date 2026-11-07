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
