//! Integration tests for `mcp-absorb --dynamic`, driving the real binary over
//! stdio JSON-RPC against tests/fixture_stdio_mcp.py (a minimal stdio MCP).
//! Fixture choice: python3 spawns reliably here, so the specced Python
//! fixture is used (no Rust fallback fixture needed).

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

static ALLOW_SEQ: AtomicU64 = AtomicU64::new(0);

fn python() -> String {
    std::env::var("PYTHON").unwrap_or_else(|_| "python3".to_string())
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
    fn start(allowlist_json: Option<&str>) -> Absorb {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "mcp-absorb-test-allowlist-{}-{}.json",
            std::process::id(),
            ALLOW_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        if let Some(text) = allowlist_json {
            std::fs::write(&path, text).unwrap();
        }
        let mut child = Command::new(env!("CARGO_BIN_EXE_mcp-absorb"))
            .args(["--dynamic", "--allowlist", path.to_str().unwrap()])
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
        Absorb { child, stdin, lines: rx, next_id: 0, allowlist_path: path }
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

#[test]
fn dynamic_mode_exposes_the_five_dyn_plus_five_inject_loop_tools() {
    // v2a updated this v1 assertion: dynamic mode now ALSO exposes the 5
    // inject-loop tools (loops poll a (server, tool) pair).
    let mut a = Absorb::start(Some(&fx_allowlist("fx")));
    a.rpc("initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));
    let mut names: Vec<String> = a.rpc("tools/list", json!({}))["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "attach",
            "call",
            "create_tttt_inject_loop",
            "delete_tttt_inject_loop",
            "detach",
            "fetch_inject_backlog",
            "list",
            "list_tttt_inject_loops",
            "set_inject_filter",
            "tools",
        ]
    );
}

#[test]
fn attach_tools_call_detach_round_trip() {
    let mut a = Absorb::start(Some(&fx_allowlist("fx")));
    a.rpc("initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));

    // not attached yet
    let (text, is_err) = a.tool("tools", json!({"name": "fx"}));
    assert!(is_err && text.contains("not attached"), "{text}");

    let (text, is_err) = a.tool("attach", json!({"name": "fx"}));
    assert!(!is_err, "attach failed: {text}");
    assert!(text.contains("2 tool(s)") && text.contains("echo") && text.contains("slow"), "{text}");

    let (text, is_err) = a.tool("list", json!({}));
    assert!(!is_err && text.contains("fx: ready, 2 tool(s)"), "{text}");

    let (text, is_err) = a.tool("tools", json!({"name": "fx"}));
    assert!(!is_err && text.contains("echo") && text.contains("inputSchema"), "{text}");

    let (text, is_err) = a.tool("call", json!({"name": "fx", "tool": "echo", "args": {"text": "hi"}}));
    assert!(!is_err && text.contains("hi"), "call: {text}");

    let (text, is_err) = a.tool("detach", json!({"name": "fx"}));
    assert!(!is_err && text.contains("detached"), "{text}");
    let (text, is_err) = a.tool("detach", json!({"name": "fx"}));
    assert!(is_err && text.contains("not attached"), "double detach: {text}");
    let (text, is_err) = a.tool("list", json!({}));
    assert!(!is_err && text.contains("(no servers attached)"), "{text}");
}

#[test]
fn double_attach_is_rejected() {
    let mut a = Absorb::start(Some(&fx_allowlist("fx")));
    a.rpc("initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));
    let (_, is_err) = a.tool("attach", json!({"name": "fx"}));
    assert!(!is_err);
    let (text, is_err) = a.tool("attach", json!({"name": "fx"}));
    assert!(is_err && text.contains("already attached"), "second attach: {text}");
    // still usable after the rejected second attach
    let (text, is_err) = a.tool("call", json!({"name": "fx", "tool": "echo", "args": {"k": 1}}));
    assert!(!is_err && text.contains("\"k\""), "{text}");
}

#[test]
fn timeout_marks_entry_unhealthy() {
    let mut a = Absorb::start(Some(&fx_allowlist("fx")));
    a.rpc("initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));
    let (_, is_err) = a.tool("attach", json!({"name": "fx"}));
    assert!(!is_err);
    // fixture 'slow' sleeps 5 s; our call timeout is 1 s -> unhealthy
    let (text, is_err) = a.tool("call", json!({"name": "fx", "tool": "slow", "args": {"timeout_s": 5}, "timeout_s": 1}));
    assert!(is_err && text.contains("did not answer"), "slow call: {text}");
    let (text, is_err) = a.tool("list", json!({}));
    assert!(!is_err && text.contains("fx: unhealthy, 2 tool(s)"), "{text}");
    // detach + attach recovers to ready (v1 recovery path)
    a.tool("detach", json!({"name": "fx"}));
    let (_, is_err) = a.tool("attach", json!({"name": "fx"}));
    assert!(!is_err);
    let (text, _) = a.tool("list", json!({}));
    assert!(text.contains("fx: ready"), "{text}");
}

#[test]
fn unknown_name_lists_available_names() {
    let mut a = Absorb::start(Some(&fx_allowlist("fx")));
    a.rpc("initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));
    let (text, is_err) = a.tool("attach", json!({"name": "nope"}));
    assert!(is_err, "unknown attach must fail");
    assert!(text.contains("not in the allowlist") && text.contains("fx"), "{text}");
}

#[test]
fn missing_allowlist_starts_fine_and_attach_quotes_the_reason() {
    let mut a = Absorb::start(None); // allowlist path points at a nonexistent file
    a.rpc("initialize", json!({"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}));
    // server started and answers normally:
    assert_eq!(a.rpc("tools/list", json!({}))["result"]["tools"].as_array().unwrap().len(), 10); // 5 dyn + 5 loop (v2a)
    let (text, is_err) = a.tool("attach", json!({"name": "anything"}));
    assert!(is_err && text.contains("cannot read allowlist"), "{text}");
    let (text, is_err) = a.tool("list", json!({}));
    assert!(!is_err && text.contains("(no servers attached)"), "{text}");
}

#[test]
fn dynamic_mode_rejects_a_subordinate_argv_with_exit_2() {
    let out = Command::new(env!("CARGO_BIN_EXE_mcp-absorb"))
        .args(["--dynamic", "--", "true"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--dynamic takes no subordinate argv"), "{err}");
}

#[test]
fn static_mode_still_proxies_subordinate_plus_loop_tools() {
    // Static-path parity: the fixture as a `--` subordinate must show its 2
    // tools plus absorb's 5 inject-loop tools, and none of the dynamic ones.
    let mut child = Command::new(env!("CARGO_BIN_EXE_mcp-absorb"))
        .args(["--", &python(), &fixture_path()])
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
    let names: Vec<String> = rpc(2, "tools/list", json!({}))["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
        .collect();
    assert!(names.contains(&"echo".to_string()) && names.contains(&"slow".to_string()), "{names:?}");
    assert!(names.contains(&"create_tttt_inject_loop".to_string()) && names.contains(&"fetch_inject_backlog".to_string()), "{names:?}");
    assert!(!names.contains(&"attach".to_string()) && !names.contains(&"detach".to_string()), "{names:?}");
    // proxied call works through the static path
    let r = rpc(3, "tools/call", json!({"name": "echo", "arguments": {"text": "via-static"}}));
    assert!(r["result"]["content"][0]["text"].as_str().unwrap().contains("via-static"), "{r}");
    let _ = child.kill();
    let _ = child.wait();
}
