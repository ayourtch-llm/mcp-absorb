//! SPEC-dynamic v2a integration: inject loops over (server, tool) pairs in
//! --dynamic mode, driven against the real binary + tests/fixture_stdio_mcp.py.
//! Injects go through a `--tttt-bin` true-no-op (`/bin/true`, or `/usr/bin/true`
//! on macOS which lacks /bin/true): run_ctl_send becomes a no-op (true accepts
//! any argv and exits 0), so the inject path runs and counts without touching
//! any real tttt session.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

static ALLOW_SEQ: AtomicU64 = AtomicU64::new(0);

fn python() -> String {
    std::env::var("PYTHON").unwrap_or_else(|_| "python3".to_string())
}

/// The specced `/bin/true` inject no-op: macOS has no /bin/true, so prefer it
/// and fall back to /usr/bin/true (same argv-blind exit-0 behavior:
/// run_ctl_send succeeds and the inject counter moves).
fn true_noop() -> String {
    for p in ["/bin/true", "/usr/bin/true"] {
        if Path::new(p).exists() {
            return p.to_string();
        }
    }
    panic!("no true binary found for the /bin/true no-op inject");
}

fn fixture_path() -> String {
    format!("{}/tests/fixture_stdio_mcp.py", env!("CARGO_MANIFEST_DIR"))
}

fn fx_allowlist(entry: &str) -> String {
    serde_json::to_string(&json!([
        {"name": entry, "command": python(), "args": [fixture_path()], "env": {"PYTHONUNBUFFERED": "1"}}
    ]))
    .unwrap()
}

struct Absorb {
    child: Child,
    stdin: ChildStdin,
    lines: mpsc::Receiver<String>,
    next_id: u64,
    allowlist_path: PathBuf,
}

impl Absorb {
    fn start_dynamic(allowlist_json: &str) -> Absorb {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "mcp-absorb-v2a-allowlist-{}-{}.json",
            std::process::id(),
            ALLOW_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, allowlist_json).unwrap();
        let true_bin = true_noop();
        let mut child = Command::new(env!("CARGO_BIN_EXE_mcp-absorb"))
            .args([
                "--dynamic",
                "--allowlist",
                path.to_str().unwrap(),
                "--tttt-bin",
                true_bin.as_str(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn mcp-absorb --dynamic");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(l) => {
                        if !l.trim().is_empty() && tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let mut a = Absorb { child, stdin, lines: rx, next_id: 0, allowlist_path: path };
        a.rpc("initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));
        a
    }

    fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(self.stdin, "{req}").unwrap();
        self.stdin.flush().unwrap();
        loop {
            let line = self
                .lines
                .recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|_| panic!("no response to '{method}' (id {id}) from mcp-absorb"));
            let v: Value = serde_json::from_str(&line).expect("non-JSON line from mcp-absorb");
            if v.get("id").and_then(|i| i.as_u64()) == Some(id) {
                return v;
            }
        }
    }

    /// Call one of absorb's own tools; returns (text, isError).
    fn tool(&mut self, name: &str, args: Value) -> (String, bool) {
        let r = self.rpc("tools/call", json!({"name": name, "arguments": args}));
        let result = &r["result"];
        let is_err = result["isError"].as_bool().unwrap_or(false);
        let text = result["content"]
            .as_array()
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|b| b["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        (text, is_err)
    }
}

impl Drop for Absorb {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.allowlist_path);
    }
}

/// The integer before " poll(s)" / " inject(s)" in a list line.
fn counter(line: &str, field: &str) -> u64 {
    let idx = line.find(field).unwrap_or_else(|| panic!("no '{field}' in: {line}"));
    let head = line[..idx].trim_end();
    let digits: String = head
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    digits.parse().unwrap_or_else(|_| panic!("no counter before '{field}' in: {line}"))
}

fn wait_until(mut f: impl FnMut(&mut Absorb) -> bool, a: &mut Absorb, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if f(a) {
            return;
        }
        thread::sleep(Duration::from_millis(200));
    }
    panic!("condition never held within 20 s: {what}");
}

#[test]
fn dynamic_loop_polls_server_tool_and_detach_ends_it() {
    let mut a = Absorb::start_dynamic(&fx_allowlist("fx"));

    // create requires an attached server (dynamic)
    let (text, is_err) = a.tool("create_tttt_inject_loop", json!({"name": "l0", "tool": "echo"}));
    assert!(is_err && text.contains("missing 'server'"), "no-server create: {text}");
    let (text, is_err) = a.tool(
        "create_tttt_inject_loop",
        json!({"name": "l0", "server": "nope", "tool": "echo"}),
    );
    assert!(is_err && text.contains("not attached"), "unattached-server create: {text}");

    let (text, is_err) = a.tool("attach", json!({"name": "fx"}));
    assert!(!is_err, "attach failed: {text}");

    // create a loop polling fx.echo every second (fixture echo answers JSON,
    // never the ignore string => every poll is "fresh" output)
    let (text, is_err) = a.tool(
        "create_tttt_inject_loop",
        json!({"name": "l1", "server": "fx", "tool": "echo", "args": {"text": "tick"}, "interval_secs": 1}),
    );
    assert!(!is_err && text.contains("polling fx.echo"), "create: {text}");

    // the loop advances and the inject path runs (tttt_bin=/bin/true no-ops
    // the actual send but the injects counter proves the path executed)
    wait_until(
        |a| {
            let (t, e) = a.tool("list_tttt_inject_loops", json!({}));
            !e && t.contains("l1: poll fx.echo") && counter(&t, " poll(s)") >= 2 && counter(&t, " inject(s)") >= 1
        },
        &mut a,
        "loop l1 polling fx.echo with >=2 polls and >=1 inject",
    );

    // the loop-keyed companions still work on a dynamic loop
    let (text, is_err) = a.tool("set_inject_filter", json!({"name": "l1", "patterns": ["tick"]}));
    assert!(!is_err && text.contains("filter ON"), "set_inject_filter: {text}");
    let (text, is_err) = a.tool("fetch_inject_backlog", json!({"name": "l1", "peek": true}));
    assert!(!is_err && text.contains("(backlog empty)"), "fetch_inject_backlog: {text}");

    // detach ends the loop: gone from the list after its final note
    let (text, is_err) = a.tool("detach", json!({"name": "fx"}));
    assert!(!is_err, "detach: {text}");
    wait_until(
        |a| {
            let (t, e) = a.tool("list_tttt_inject_loops", json!({}));
            !e && t.contains("(no inject loops)")
        },
        &mut a,
        "loop l1 ended after detach",
    );

    // re-attach revives NOTHING: the loop stays gone until recreated
    let (text, is_err) = a.tool("attach", json!({"name": "fx"}));
    assert!(!is_err, "re-attach: {text}");
    thread::sleep(Duration::from_millis(1500));
    let (text, is_err) = a.tool("list_tttt_inject_loops", json!({}));
    assert!(!is_err && text.contains("(no inject loops)"), "after re-attach: {text}");

    // an explicit recreate resumes polling the (re-attached) server
    let (text, is_err) = a.tool(
        "create_tttt_inject_loop",
        json!({"name": "l2", "server": "fx", "tool": "echo", "args": {"text": "tick2"}, "interval_secs": 1}),
    );
    assert!(!is_err && text.contains("polling fx.echo"), "recreate: {text}");
    wait_until(
        |a| {
            let (t, e) = a.tool("list_tttt_inject_loops", json!({}));
            !e && t.contains("l2: poll fx.echo") && counter(&t, " poll(s)") >= 1
        },
        &mut a,
        "recreated loop l2 polls",
    );
}

#[test]
fn dynamic_delete_is_loop_keyed_and_cleans_the_server_map() {
    let mut a = Absorb::start_dynamic(&fx_allowlist("fx"));
    a.tool("attach", json!({"name": "fx"}));
    let (_, is_err) = a.tool(
        "create_tttt_inject_loop",
        json!({"name": "d1", "server": "fx", "tool": "echo", "args": {"text": "x"}, "interval_secs": 1}),
    );
    assert!(!is_err);
    let (text, is_err) = a.tool("delete_tttt_inject_loop", json!({"name": "d1"}));
    assert!(!is_err && text.contains("stopped"), "delete: {text}");
    let (text, is_err) = a.tool("delete_tttt_inject_loop", json!({"name": "d1"}));
    assert!(is_err && text.contains("no loop named"), "double delete: {text}");
    let (text, _) = a.tool("list_tttt_inject_loops", json!({}));
    assert!(text.contains("(no inject loops)"), "list after delete: {text}");
}

#[test]
fn static_mode_rejects_the_server_param_and_creates_plain_loops() {
    // Static path: create with a `server` param = clear reject; without it,
    // the loop is created exactly as before (poll target shown without any
    // server). tttt_bin=/bin/true keeps injects inert.
    let true_bin = true_noop();
    let py = python();
    let fx = fixture_path();
    let mut child = Command::new(env!("CARGO_BIN_EXE_mcp-absorb"))
        .args(["--tttt-bin", true_bin.as_str(), "--", py.as_str(), fx.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn static mcp-absorb");
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(l) => {
                    if !l.trim().is_empty() && tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    let mut rpc = |id: u64, method: &str, params: Value| -> Value {
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(stdin, "{req}").unwrap();
        stdin.flush().unwrap();
        loop {
            let line = rx.recv_timeout(Duration::from_secs(30)).expect("no static response");
            let v: Value = serde_json::from_str(&line).unwrap();
            if v.get("id").and_then(|i| i.as_u64()) == Some(id) {
                return v;
            }
        }
    };
    rpc(1, "initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));
    let mut call = |id: u64, name: &str, args: Value| -> (String, bool) {
        let r = rpc(id, "tools/call", json!({"name": name, "arguments": args}));
        (
            r["result"]["content"][0]["text"].as_str().unwrap_or("").to_string(),
            r["result"]["isError"].as_bool().unwrap_or(false),
        )
    };

    let (text, is_err) = call(
        2,
        "create_tttt_inject_loop",
        json!({"name": "s1", "tool": "echo", "server": "fx"}),
    );
    assert!(is_err && text.contains("'server' is not accepted in static mode"), "static server reject: {text}");

    let (text, is_err) = call(
        3,
        "create_tttt_inject_loop",
        json!({"name": "s2", "tool": "echo", "args": {"text": "hi"}, "interval_secs": 60}),
    );
    assert!(!is_err && text.contains("polling echo every 60s"), "static create: {text}");
    let (text, is_err) = call(4, "list_tttt_inject_loops", json!({}));
    assert!(!is_err && text.contains("s2: poll echo ") && !text.contains("fx."), "static list: {text}");
    let (text, is_err) = call(5, "delete_tttt_inject_loop", json!({"name": "s2"}));
    assert!(!is_err && text.contains("stopped"), "static delete: {text}");

    let _ = child.kill();
    let _ = child.wait();
}
