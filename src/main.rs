//! mcp-absorb — an MCP stdio proxy that "absorbs" a subordinate MCP server.
//!
//! All of the subordinate's tools are proxied 1:1, and three tools are added:
//! create_tttt_inject_loop / delete_tttt_inject_loop / list_tttt_inject_loops.
//! An inject loop polls one subordinate tool on an interval and, whenever the
//! result is fresh (non-empty and not the configured "no news" string), injects
//! it into a running tttt session's input via `tttt ctl send <session> <text>
//! --enter` (socket auto-detected by tttt ctl, or TTTT_MCP_SOCKET).
//!
//! Because absorb owns the single subordinate process, the agent's own tool
//! calls and the loops share one connection/identity (e.g. one IRC nick).
//! The subordinate may handle requests serially, so loops should poll with
//! short waits — a long-poll would delay the agent's own calls.
//!
//! Usage: mcp-absorb [--session pty-1] [--tttt-bin tttt] -- <subordinate-cmd> [args...]

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

// ---- subordinate connection --------------------------------------------------

struct Sub {
    stdin: Mutex<std::process::ChildStdin>,
    pending: Mutex<HashMap<u64, mpsc::Sender<Value>>>,
    next_id: AtomicU64,
}

impl Sub {
    fn spawn(argv: &[String]) -> Result<(Arc<Sub>, Child), String> {
        let mut child = Command::new(&argv[0])
            .args(&argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("failed to spawn subordinate {}: {e}", argv[0]))?;
        let stdin = child.stdin.take().ok_or("no subordinate stdin")?;
        let stdout = child.stdout.take().ok_or("no subordinate stdout")?;
        let sub = Arc::new(Sub {
            stdin: Mutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        });
        let s2 = Arc::clone(&sub);
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(_) => break,
                };
                let msg: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Some(id) = msg.get("id").and_then(|i| i.as_u64()) {
                    if let Some(tx) = s2.pending.lock().unwrap().remove(&id) {
                        let _ = tx.send(msg);
                    }
                }
                // Subordinate notifications are dropped: absorb's stdout is
                // driven only by our own responses, keyed by request id.
            }
        });
        Ok((sub, child))
    }

    /// Send one JSON-RPC request to the subordinate and wait for its response.
    fn call(&self, method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        {
            let mut w = self.stdin.lock().unwrap();
            writeln!(w, "{req}").map_err(|e| format!("subordinate write failed: {e}"))?;
            w.flush().map_err(|e| e.to_string())?;
        }
        match rx.recv_timeout(timeout) {
            Ok(resp) => Ok(resp),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(format!("subordinate did not answer '{method}' within {timeout:?}"))
            }
        }
    }

    fn notify(&self, method: &str, params: Value) {
        let req = json!({"jsonrpc": "2.0", "method": method, "params": params});
        let mut w = self.stdin.lock().unwrap();
        let _ = writeln!(w, "{req}");
        let _ = w.flush();
    }
}

/// Flatten a tools/call response into its text content (all text blocks joined).
fn result_text(resp: &Value) -> (String, bool) {
    let result = &resp["result"];
    let is_err = result["isError"].as_bool().unwrap_or(false) || resp.get("error").is_some();
    let text = match result["content"].as_array() {
        Some(blocks) => blocks
            .iter()
            .filter_map(|b| b["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        None => resp["error"]["message"].as_str().unwrap_or("").to_string(),
    };
    (text, is_err)
}

// ---- inject loops ------------------------------------------------------------

struct LoopSpec {
    tool: String,
    args: Value,
    interval_secs: f64,
    ignore: String,
    prefix: String,
}

struct LoopHandle {
    spec: LoopSpec,
    stop: Arc<AtomicBool>,
    polls: Arc<AtomicU64>,
    injects: Arc<AtomicU64>,
}

struct Cfg {
    session: String,
    tttt_bin: String,
}

/// Break up `[TAG]` runs that tttt's send_keys would turn into keystrokes
/// ([ENTER], [CTRL+C], ...): polled output is untrusted, so a zero-width
/// space after '[' keeps the text readable but never matches a key token.
fn neutralize_key_tokens(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        out.push(chars[i]);
        if chars[i] == '[' {
            let tag: String = chars[i + 1..]
                .iter()
                .take_while(|c| **c != ']')
                .collect();
            let tagish = !tag.is_empty()
                && tag.len() <= 12
                && tag.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '+');
            if tagish && chars[i + 1..].iter().any(|c| *c == ']') {
                out.push('\u{200b}');
            }
        }
        i += 1;
    }
    out
}

fn run_ctl_send(cfg: &Cfg, extra: &[&str]) -> Result<(), String> {
    let mut args = vec!["ctl", "send", cfg.session.as_str()];
    args.extend_from_slice(extra);
    let out = Command::new(&cfg.tttt_bin)
        .args(&args)
        .output()
        .map_err(|e| format!("failed to run {} ctl send: {e}", cfg.tttt_bin))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "tttt ctl send failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

fn inject_into_tttt(cfg: &Cfg, text: &str) -> Result<(), String> {
    // One physical input line: raw '\n' keystrokes would submit mid-text.
    let line = neutralize_key_tokens(&text.replace(['\r', '\n'], " | "));
    // Text first, Enter second (older tttt `ctl send --enter` DROPPED the
    // text; two calls work with every version), with a pause so the TUI
    // finishes rendering the typed text before it is submitted.
    run_ctl_send(cfg, &[&line])?;
    std::thread::sleep(Duration::from_millis(300));
    run_ctl_send(cfg, &["--enter"])
}

fn run_loop(name: String, h: &LoopHandle, sub: Arc<Sub>, cfg: Arc<Cfg>) {
    let stop = Arc::clone(&h.stop);
    let polls = Arc::clone(&h.polls);
    let injects = Arc::clone(&h.injects);
    let tool = h.spec.tool.clone();
    let args = h.spec.args.clone();
    let interval = Duration::from_secs_f64(h.spec.interval_secs);
    let ignore = h.spec.ignore.clone();
    let prefix = h.spec.prefix.clone();
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            polls.fetch_add(1, Ordering::Relaxed);
            let resp = sub.call(
                "tools/call",
                json!({"name": tool, "arguments": args}),
                interval + Duration::from_secs(630),
            );
            match resp {
                Ok(r) => {
                    let (text, is_err) = result_text(&r);
                    let t = text.trim();
                    if !is_err && !t.is_empty() && t != ignore {
                        // One poll's output = one batched inject, with provenance.
                        let msg = format!("{prefix}{t}");
                        if inject_into_tttt(&cfg, &msg).is_ok() {
                            injects.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Err(_) => { /* subordinate busy or gone; try again next tick */ }
            }
            // Sleep in short slices so delete_tttt_inject_loop takes effect fast.
            let mut left = interval;
            while left > Duration::ZERO && !stop.load(Ordering::Relaxed) {
                let step = left.min(Duration::from_millis(500));
                std::thread::sleep(step);
                left = left.saturating_sub(step);
            }
        }
        let _ = name; // name lives as long as the thread for debuggers
    });
}

// ---- absorb's own tools ------------------------------------------------------

fn own_tools_json(cfg: &Cfg) -> Vec<Value> {
    vec![
        json!({
            "name": "create_tttt_inject_loop",
            "description": format!(
                "Create a background loop that polls one subordinate tool and injects fresh output into the tttt session '{}' input (via tttt ctl). Use SHORT waits in the polled tool's args (e.g. wait_secs 0-5): the subordinate may serve requests serially, so a long poll delays your own calls. One poll's output = one batched inject, prefixed for provenance.",
                cfg.session),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Loop name (unique; used for list/delete)"},
                    "tool": {"type": "string", "description": "Subordinate tool to poll, e.g. irc_recv"},
                    "args": {"type": "object", "description": "Arguments for the polled tool, e.g. {\"wait_secs\": 5}"},
                    "interval_secs": {"type": "number", "description": "Pause between polls (default 10, clamped 1-600)"},
                    "ignore": {"type": "string", "description": "Result text meaning 'no news', skipped (default: \"(no new messages)\")"},
                    "prefix": {"type": "string", "description": "Provenance prefix for injected text (default: \"[<name>] \")"}
                },
                "required": ["name", "tool"]
            }
        }),
        json!({
            "name": "delete_tttt_inject_loop",
            "description": "Stop and remove an inject loop by name.",
            "inputSchema": {
                "type": "object",
                "properties": {"name": {"type": "string", "description": "Loop name"}},
                "required": ["name"]
            }
        }),
        json!({
            "name": "list_tttt_inject_loops",
            "description": "List active inject loops with their poll/inject counters.",
            "inputSchema": {"type": "object", "properties": {}}
        }),
    ]
}

type Loops = Mutex<HashMap<String, LoopHandle>>;

fn tool_create_loop(loops: &Loops, sub: &Arc<Sub>, cfg: &Arc<Cfg>, a: &Value) -> Result<String, String> {
    let name = a["name"].as_str().ok_or("missing 'name'")?.to_string();
    let tool = a["tool"].as_str().ok_or("missing 'tool'")?.to_string();
    let args = if a["args"].is_object() { a["args"].clone() } else { json!({}) };
    let interval_secs = a["interval_secs"].as_f64().unwrap_or(10.0).clamp(1.0, 600.0);
    let ignore = a["ignore"].as_str().unwrap_or("(no new messages)").to_string();
    let prefix = a["prefix"].as_str().map(|s| s.to_string()).unwrap_or_else(|| format!("[{name}] "));

    let mut map = loops.lock().unwrap();
    if map.contains_key(&name) {
        return Err(format!("loop '{name}' already exists"));
    }
    let handle = LoopHandle {
        spec: LoopSpec { tool: tool.clone(), args, interval_secs, ignore, prefix },
        stop: Arc::new(AtomicBool::new(false)),
        polls: Arc::new(AtomicU64::new(0)),
        injects: Arc::new(AtomicU64::new(0)),
    };
    run_loop(name.clone(), &handle, Arc::clone(sub), Arc::clone(cfg));
    map.insert(name.clone(), handle);
    Ok(format!(
        "loop '{name}' created: polling {tool} every {interval_secs}s, injecting fresh output into session {}",
        cfg.session
    ))
}

fn tool_delete_loop(loops: &Loops, a: &Value) -> Result<String, String> {
    let name = a["name"].as_str().ok_or("missing 'name'")?;
    match loops.lock().unwrap().remove(name) {
        Some(h) => {
            h.stop.store(true, Ordering::Relaxed);
            Ok(format!("loop '{name}' stopped"))
        }
        None => Err(format!("no loop named '{name}'")),
    }
}

fn tool_list_loops(loops: &Loops) -> Result<String, String> {
    let map = loops.lock().unwrap();
    if map.is_empty() {
        return Ok("(no inject loops)".into());
    }
    let mut lines: Vec<String> = map
        .iter()
        .map(|(name, h)| {
            format!(
                "{name}: poll {} {} every {}s — {} poll(s), {} inject(s)",
                h.spec.tool,
                h.spec.args,
                h.spec.interval_secs,
                h.polls.load(Ordering::Relaxed),
                h.injects.load(Ordering::Relaxed),
            )
        })
        .collect();
    lines.sort();
    Ok(lines.join("\n"))
}

// ---- main --------------------------------------------------------------------

fn parse_args() -> Result<(Cfg, Vec<String>), String> {
    let mut session = "pty-1".to_string();
    let mut tttt_bin = "tttt".to_string();
    let mut it = std::env::args().skip(1);
    let mut sub_argv = Vec::new();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--session" => session = it.next().ok_or("missing value after --session")?,
            "--tttt-bin" => tttt_bin = it.next().ok_or("missing value after --tttt-bin")?,
            "--" => {
                sub_argv = it.collect();
                break;
            }
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    if sub_argv.is_empty() {
        return Err("usage: mcp-absorb [--session pty-1] [--tttt-bin tttt] -- <subordinate-cmd> [args...]".into());
    }
    Ok((Cfg { session, tttt_bin }, sub_argv))
}

fn main() {
    let (cfg, sub_argv) = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("mcp-absorb: {e}");
            std::process::exit(2);
        }
    };
    let cfg = Arc::new(cfg);
    let (sub, mut child) = match Sub::spawn(&sub_argv) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("mcp-absorb: {e}");
            std::process::exit(1);
        }
    };
    let loops: Loops = Mutex::new(HashMap::new());

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let id = msg.get("id").cloned();
        let method = msg["method"].as_str().unwrap_or("").to_string();
        let params = msg.get("params").cloned().unwrap_or(json!({}));
        let reply = |result: Value| -> Value {
            json!({"jsonrpc": "2.0", "id": id.clone().unwrap_or(Value::Null), "result": result})
        };
        let response: Option<Value> = match method.as_str() {
            "initialize" => {
                // Handshake the subordinate with the client's own params, but
                // answer for ourselves (capabilities stay plain tools).
                let _ = sub.call("initialize", params.clone(), Duration::from_secs(20));
                sub.notify("notifications/initialized", json!({}));
                let ver = params["protocolVersion"].as_str().unwrap_or("2024-11-05");
                Some(reply(json!({
                    "protocolVersion": ver,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "mcp-absorb", "version": env!("CARGO_PKG_VERSION")}
                })))
            }
            "ping" => Some(reply(json!({}))),
            "tools/list" => {
                let mut tools: Vec<Value> = sub
                    .call("tools/list", json!({}), Duration::from_secs(20))
                    .ok()
                    .and_then(|r| r["result"]["tools"].as_array().cloned())
                    .unwrap_or_default();
                tools.extend(own_tools_json(&cfg));
                Some(reply(json!({"tools": tools})))
            }
            "tools/call" => {
                let name = params["name"].as_str().unwrap_or("");
                let args = params["arguments"].clone();
                let own = match name {
                    "create_tttt_inject_loop" => Some(tool_create_loop(&loops, &sub, &cfg, &args)),
                    "delete_tttt_inject_loop" => Some(tool_delete_loop(&loops, &args)),
                    "list_tttt_inject_loops" => Some(tool_list_loops(&loops)),
                    _ => None,
                };
                match own {
                    Some(out) => {
                        let (text, is_err) = match out {
                            Ok(t) => (t, false),
                            Err(e) => (e, true),
                        };
                        Some(reply(json!({
                            "content": [{"type": "text", "text": text}],
                            "isError": is_err
                        })))
                    }
                    None => {
                        // Proxy to the subordinate. Generous timeout: the agent
                        // may deliberately long-poll (e.g. irc_recv wait 600).
                        match sub.call("tools/call", params.clone(), Duration::from_secs(660)) {
                            Ok(r) => Some(reply(r["result"].clone())),
                            Err(e) => Some(reply(json!({
                                "content": [{"type": "text", "text": e}],
                                "isError": true
                            }))),
                        }
                    }
                }
            }
            _ => {
                if id.is_some() && !method.starts_with("notifications/") {
                    Some(json!({"jsonrpc": "2.0", "id": id.clone().unwrap(),
                        "error": {"code": -32601, "message": format!("method not found: {method}")}}))
                } else {
                    None
                }
            }
        };
        if let Some(r) = response {
            let mut out = stdout.lock();
            let _ = writeln!(out, "{r}");
            let _ = out.flush();
        }
    }
    let _ = child.kill();
}
