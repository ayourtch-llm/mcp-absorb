# mcp-absorb

An MCP (Model Context Protocol) stdio proxy that "absorbs" a subordinate MCP
server: all of the subordinate's tools are exposed 1:1, plus three tools for
**tttt inject loops** — background pollers that turn a poll-style tool (like
an IRC `irc_recv`) into push notifications typed straight into a running
[tttt](https://github.com/ayourtch-llm/tttt) session's input.

Why a proxy: stdio MCP servers are one process per client, so a separate
babysitter process would be a *second* connection (a second IRC nick, etc.).
Absorb owns the single subordinate process, so the agent's own tool calls and
the loops share one connection and identity.

## Added tools

- `create_tttt_inject_loop` — poll `<tool>` with `<args>` every `interval_secs`;
  whenever the result is fresh (non-empty and ≠ the `ignore` string, default
  `"(no new messages)"`), inject it into the tttt session input in one batched,
  provenance-prefixed line via `tttt ctl send <session> <text> --enter`
- `delete_tttt_inject_loop` — stop a loop by name
- `list_tttt_inject_loops` — show loops with poll/inject counters

Keep polls short (e.g. `{"wait_secs": 5}`): subordinates may serve requests
serially, so a long poll delays the agent's own calls through the proxy.

## Install

```sh
cargo build --release
claude mcp add --scope user irc -- \
  /path/to/mcp-absorb/target/release/mcp-absorb -- \
  /path/to/mcp-irc-client/target/release/mcp-irc-client \
  --server irc.example.net:6667 --nick my-nick --channel "#my-channel"
```

Absorb's own flags go before the `--`: `--session pty-1` (tttt session to
inject into), `--tttt-bin tttt` (path to the tttt binary; the MCP socket is
auto-detected from `/tmp/tttt-mcp-*.sock` or `TTTT_MCP_SOCKET`).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
