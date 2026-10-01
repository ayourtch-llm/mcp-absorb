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
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    poll_timeout_secs: Option<f64>,
}

/// Lines held back by an inject filter, oldest first.
struct Backlog {
    entries: VecDeque<(u64, String)>, // (unix seconds, line)
    dropped: u64,
}

const BACKLOG_CAP: usize = 1000;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn age_str(then: u64) -> String {
    let d = now_secs().saturating_sub(then);
    if d < 60 {
        format!("{d}s ago")
    } else if d < 3600 {
        format!("{}m ago", d / 60)
    } else {
        format!("{}h{}m ago", d / 3600, (d % 3600) / 60)
    }
}

struct LoopHandle {
    spec: LoopSpec,
    stop: Arc<AtomicBool>,
    polls: Arc<AtomicU64>,
    injects: Arc<AtomicU64>,
    /// When Some, only lines containing one of these (lowercased)
    /// substrings are injected; the rest go to `backlog`.
    filter: Arc<Mutex<Option<Vec<String>>>>,
    backlog: Arc<Mutex<Backlog>>,
    /// Per-sender (last-line unix ts, was-it-injected) — drives the
    /// continuation-marker inherit and the follow window.
    follow: Arc<Mutex<HashMap<String, (u64, bool)>>>,
    /// (c) follow-window seconds: after an injected line from a sender,
    /// further lines from the same sender within this window also inject
    /// (ayourtch 2026-08-14: tagged-once means interested in the burst).
    /// 0 disables the window; the "…" continuation inherit (b) stays on.
    follow_window: Arc<AtomicU64>,
    debounce: Arc<Mutex<Debounce>>,
}

/// Extract the sender and body from "[#claude] <nick> text" or
/// "[msgd #rusty] rusty: text". Other senderless lines get plain filtering.
fn line_sender_and_body(line: &str) -> Option<(String, &str)> {
    if line.starts_with("[msgd ") {
        let (_, message) = line.split_once(']')?;
        let (sender, body) = message.trim_start().split_once(':')?;
        if sender.is_empty() || sender.chars().any(char::is_whitespace) {
            return None;
        }
        return Some((sender.to_lowercase(), body.trim_start()));
    }
    let lt = line.find('<')?;
    let gt = line[lt..].find('>')? + lt;
    let sender = line[lt + 1..gt].trim();
    if sender.is_empty() || sender.contains(' ') {
        return None;
    }
    let sender = sender.to_lowercase();
    Some((sender, line[gt + 1..].trim_start()))
}

/// Continuation fragments from the irc client's auto-split start with this
/// (see mcp-irc-client CONTINUATION_MARKER; agreed household-wide 2026-08-14).
const CONT_MARKER: char = '\u{2026}';
/// A "…" fragment inherits the previous line's decision within this cap.
const CONT_INHERIT_SECS: u64 = 600;

/// One line's inject decision under an active filter. Updates `follow`.
fn filter_decide(
    l: &str,
    pats: &[String],
    follow: &mut HashMap<String, (u64, bool)>,
    window: u64,
    now: u64,
) -> bool {
    let low = l.to_lowercase();
    let mut inject = pats.iter().any(|p| low.contains(p));
    if let Some((sender, body)) = line_sender_and_body(l) {
        let prev = follow.get(&sender).copied();
        if !inject {
            if body.starts_with(CONT_MARKER) {
                // (b) continuation inherits the previous line's decision
                if let Some((ts, was)) = prev {
                    if was && now.saturating_sub(ts) <= CONT_INHERIT_SECS {
                        inject = true;
                    }
                }
            } else if window > 0 {
                // (c) follow window after an injected line from this sender
                if let Some((ts, was)) = prev {
                    if was && now.saturating_sub(ts) <= window {
                        inject = true;
                    }
                }
            }
        }
        follow.insert(sender, (now, inject));
    }
    inject
}

const PROGRESS_PING_SECS: u64 = 30;

struct PendingEdit {
    line: String,
    first: u64,
    last: u64,
    edits: u64,
    last_ping: u64,
    pinged_edits: u64,
}

#[derive(Default)]
struct Debounce {
    window_secs: u64,
    pending: HashMap<String, PendingEdit>,
}

impl Debounce {
    /// Return lines released to the backlog when debounce is disabled.
    fn set_window(&mut self, secs: u64) -> Vec<String> {
        self.window_secs = secs.min(600);
        if self.window_secs == 0 {
            self.pending.drain().map(|(_, p)| p.line).collect()
        } else {
            Vec::new()
        }
    }

    /// None means filter OFF; Some is the unchanged filter_decide result.
    /// Return an immediate inject and append displaced/rejected lines to held.
    fn offer(
        &mut self,
        line: &str,
        filter_inject: Option<bool>,
        now: u64,
        held: &mut Vec<String>,
    ) -> Option<String> {
        if self.window_secs > 0 {
            if let Some((sender, _)) = line_sender_and_body(line) {
                if filter_inject != Some(true) {
                    let pending = self.pending.entry(sender).or_insert_with(|| PendingEdit {
                        line: String::new(), first: now, last: now, edits: 0,
                        last_ping: now, pinged_edits: 0,
                    });
                    if pending.edits > 0 {
                        held.push(std::mem::replace(&mut pending.line, line.to_string()));
                    } else {
                        pending.line = line.to_string();
                    }
                    pending.last = now;
                    pending.edits += 1;
                    return None;
                }
                if let Some(pending) = self.pending.remove(&sender) {
                    held.push(pending.line);
                }
            }
        }
        if filter_inject.unwrap_or(true) {
            Some(line.to_string())
        } else {
            held.push(line.to_string());
            None
        }
    }

    fn due(&mut self, now: u64) -> Vec<String> {
        let mut lines = Vec::new();
        self.pending.retain(|_, pending| {
            if now.saturating_sub(pending.last) >= self.window_secs {
                lines.push(pending.line.clone());
                false
            } else {
                true
            }
        });
        lines
    }

    fn progress(&mut self, name: &str, now: u64) -> Vec<String> {
        let mut lines = Vec::new();
        for (sender, pending) in &mut self.pending {
            if pending.edits > 1
                && pending.edits > pending.pinged_edits
                && now.saturating_sub(pending.first) >= PROGRESS_PING_SECS
                && now.saturating_sub(pending.last_ping) >= PROGRESS_PING_SECS
                && now.saturating_sub(pending.last) < self.window_secs
            {
                lines.push(format!(
                    "[{name} debounce] {sender} still streaming ({} edits, {} s)",
                    pending.edits, now.saturating_sub(pending.first)
                ));
                pending.last_ping = now;
                pending.pinged_edits = pending.edits;
            }
        }
        lines
    }
}

struct Cfg {
    session: String,
    tttt_bin: String,
}

/// Make untrusted text (chat lines) inert for tttt's send_keys while it
/// still SHOWS verbatim in the session. send_keys turns `[TAG]`, `^X`,
/// `\n`/`\r`/`\t` and `\xHH` into keystrokes (tttt
/// crates/tttt-pty/src/keys.rs `process_special_keys`), so a chat line
/// saying "^C" or "\r" would interrupt or submit. Its `\xHH` form is
/// the one literal escape it has: `\`, `^` and `[` are sent as `\x5c`,
/// `\x5e`, `\x5b` and arrive as themselves. Real control characters
/// and invisible format characters are shown as visible `\xHH` /
/// `\u{...}` text: sending them would be a keystroke, and an invisible
/// character makes the Claude Code TUI hold the input unsubmitted ("Removed
/// 1 invisible character", the 2026-10-01 overnight stall). The old
/// zero-width-space escapes did exactly that, so none are used here.
fn neutralize_key_tokens(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    let chars: Vec<char> = text.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        match c {
            '\\' => out.push_str("\\x5c"),
            '^' => out.push_str("\\x5e"),
            '[' => out.push_str("\\x5b"),
            // Token-leading '@' (start of text or after whitespace/bracketing
            // punctuation) triggers the Claude Code TUI's @-mention file
            // autocomplete when the text is typed: the token can be
            // fuzzy-matched to a LOCAL file and auto-read into the session's
            // context. A FULLWIDTH '＠' (U+FF20) reads the same, triggers no
            // autocomplete and submits normally (tested 2026-10-01). Mid-word
            // '@' (email addresses) is left alone.
            '@' if (i == 0
                || chars[i - 1].is_whitespace()
                || matches!(chars[i - 1], '(' | '[' | '{' | '"' | '\'' | ':' | ';' | ',' | '<' | '|'))
                && i + 1 < chars.len()
                && !chars[i + 1].is_whitespace()
                && chars[i + 1] != '@' =>
            {
                out.push('\u{ff20}')
            }
            c if (c as u32) < 0x100 && c.is_control() => {
                // C0, DEL and C1: visible text, never the key itself.
                out.push_str(&format!("\\x5cx{:02x}", c as u32));
            }
            c if is_invisible_format(c) => {
                out.push_str(&format!("\\x5cu{{{:x}}}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

/// Zero-width and bidi-control characters (Unicode Cf that renders as
/// nothing); the TUI strips them and then holds the input for review.
fn is_invisible_format(c: char) -> bool {
    matches!(c as u32,
        0x00ad | 0x061c | 0x180e | 0x200b..=0x200f | 0x202a..=0x202e
        | 0x2060..=0x2064 | 0x2066..=0x206f | 0xfeff | 0xfff9..=0xfffb)
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
    let filter = Arc::clone(&h.filter);
    let backlog = Arc::clone(&h.backlog);
    let follow = Arc::clone(&h.follow);
    let follow_window = Arc::clone(&h.follow_window);
    let debounce = Arc::clone(&h.debounce);
    let tool = h.spec.tool.clone();
    let args = h.spec.args.clone();
    let interval = Duration::from_secs_f64(h.spec.interval_secs);
    // A hung subordinate holds each poll for the whole call timeout, so the
    // timeout bounds transport-failure detection latency. Loops polling cheap
    // tools should pass poll_timeout_secs (~wait_secs + margin); the default
    // stays generous for long-poll or slow tools.
    let call_timeout = h
        .spec
        .poll_timeout_secs
        .map(Duration::from_secs_f64)
        .unwrap_or(interval + Duration::from_secs(630));
    let ignore = h.spec.ignore.clone();
    let prefix = h.spec.prefix.clone();
    std::thread::spawn(move || {
        let mut err_streak: u64 = 0;
        let mut alerted_at: Option<u64> = None;
        let mut last_ok: u64 = now_secs();
        while !stop.load(Ordering::Relaxed) {
            polls.fetch_add(1, Ordering::Relaxed);
            let resp = sub.call(
                "tools/call",
                json!({"name": tool, "arguments": args}),
                call_timeout,
            );
            let mut poll_err: Option<String> = None;
            match resp {
                Ok(r) => {
                    let (text, is_err) = result_text(&r);
                    let t = text.trim();
                    if is_err {
                        poll_err = Some(t.chars().take(160).collect());
                    } else if !t.is_empty() && t != ignore {
                        let pats = filter.lock().unwrap().clone();
                        let mut d = debounce.lock().unwrap();
                        let to_inject: Option<String> = match pats.as_ref() {
                            None if d.window_secs == 0 => Some(t.to_string()),
                            _ => {
                                // Per line: matching lines interrupt, the
                                // rest wait in debounce or the backlog.
                                let mut pass: Vec<String> = Vec::new();
                                let mut held: Vec<String> = Vec::new();
                                let now = now_secs();
                                let win = follow_window.load(Ordering::Relaxed);
                                let mut fmap = follow.lock().unwrap();
                                for line in t.lines() {
                                    let l = line.trim();
                                    if l.is_empty() {
                                        continue;
                                    }
                                    let decision = pats.as_ref()
                                        .map(|p| filter_decide(l, p, &mut fmap, win, now));
                                    if let Some(line) = d.offer(l, decision, now, &mut held) {
                                        pass.push(line);
                                    }
                                }
                                drop(fmap);
                                if !held.is_empty() {
                                    let mut b = backlog.lock().unwrap();
                                    let ts = now_secs();
                                    for l in held {
                                        if b.entries.len() >= BACKLOG_CAP {
                                            b.entries.pop_front();
                                            b.dropped += 1;
                                        }
                                        b.entries.push_back((ts, l));
                                    }
                                }
                                if pass.is_empty() {
                                    None
                                } else {
                                    Some(pass.join("\n"))
                                }
                            }
                        };
                        drop(d);
                        if let Some(body) = to_inject {
                            // One poll's output = one batched inject, with provenance.
                            let msg = format!("{prefix}{body}");
                            if inject_into_tttt(&cfg, &msg).is_ok() {
                                injects.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Err(e) => {
                    poll_err = Some(e.chars().take(160).collect());
                }
            }
            // Check even when the poll has no fresh output or returns an error.
            let mut d = debounce.lock().unwrap();
            let now = now_secs();
            let due = d.due(now);
            let progress = d.progress(&name, now);
            drop(d);
            let mut messages = Vec::new();
            if !due.is_empty() {
                messages.push(format!("{prefix}{}", due.join("\n")));
            }
            if !progress.is_empty() {
                messages.push(progress.join("\n"));
            }
            for msg in messages {
                if inject_into_tttt(&cfg, &msg).is_ok() {
                    injects.fetch_add(1, Ordering::Relaxed);
                }
            }
            // A persistently failing poll must not be indistinguishable from
            // "no news": alarm after ~3 min without a good poll, re-alarm
            // every 10 min, notice on recovery. Wall-clock,
            // not poll-count: a HUNG subordinate burns interval+630s per
            // attempt, so counting errors would stretch "3 min" into hours.
            // Alarms bypass the inject filter on purpose.
            const ERR_ALERT_SECS: u64 = 180;
            const ERR_REALERT_SECS: u64 = 600;
            match poll_err {
                Some(e) => {
                    err_streak += 1;
                    let failing = now_secs().saturating_sub(last_ok);
                    let due = alerted_at
                        .map_or(true, |t| now_secs().saturating_sub(t) >= ERR_REALERT_SECS);
                    if failing >= ERR_ALERT_SECS && due {
                        let msg = format!(
                            "{prefix}LOOP-ALARM: '{tool}' failing for {:.0} min ({err_streak} consecutive errors; last: {e}). Incoming messages may be silently missed.",
                            failing as f64 / 60.0
                        );
                        if inject_into_tttt(&cfg, &msg).is_ok() {
                            alerted_at = Some(now_secs());
                        }
                    }
                }
                None => {
                    if alerted_at.is_some() {
                        let msg = format!(
                            "{prefix}LOOP-RECOVERED: '{tool}' polling ok again after {err_streak} consecutive errors."
                        );
                        let _ = inject_into_tttt(&cfg, &msg);
                    }
                    err_streak = 0;
                    alerted_at = None;
                    last_ok = now_secs();
                }
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
                    "prefix": {"type": "string", "description": "Provenance prefix for injected text (default: \"[<name>] \")"},
                    "poll_timeout_secs": {"type": "number", "description": "Per-poll call timeout (clamped 5-3600; default interval+630). Set to the polled tool's wait_secs + ~30s so a dead/hung subordinate is detected in minutes, not the long-poll default."}
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
            "description": "List active inject loops with their poll/inject counters, filter state, edit_debounce_secs and backlog depth.",
            "inputSchema": {"type": "object", "properties": {}}
        }),
        json!({
            "name": "set_inject_filter",
            "description": "Set case-insensitive 'patterns' for immediate injects (including continuation/follow matches; omit or empty for filter OFF), with other lines held in fetch_inject_backlog unless edit_debounce_secs delays each sender's latest unmatched line (every sender line with filter OFF) until quiet, with streaming progress pings at most every 30s; omit edit_debounce_secs to preserve it or set 0 to disable it and flush pending lines to the backlog.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Loop name"},
                    "patterns": {"type": "array", "items": {"type": "string"}, "description": "Substrings that make a line inject; omit or empty to turn the filter off"},
                    "follow_window_secs": {"type": "integer", "description": "After an injected line from a sender, further lines from the same sender within this many seconds also inject (default 60, max 3600, 0 = off). \"…\" continuation fragments always inherit the previous line's decision."},
                    "edit_debounce_secs": {"type": "integer", "minimum": 0, "maximum": 600, "description": "Per-sender quiet window (default 0 = off, max 600); omit to keep the current value, or set 0 to flush pending lines to the backlog"}
                },
                "required": ["name"]
            }
        }),
        json!({
            "name": "fetch_inject_backlog",
            "description": "Read lines held back by an inject loop's filter, oldest first, each with its age. Drains what it returns unless peek=true. Returns '(backlog empty)' when there is nothing.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Loop name"},
                    "max_lines": {"type": "integer", "description": "At most this many lines (default: all)"},
                    "peek": {"type": "boolean", "description": "Read without draining (default false)"}
                },
                "required": ["name"]
            }
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
    let poll_timeout_secs = a["poll_timeout_secs"].as_f64().map(|v| v.clamp(5.0, 3600.0));

    let mut map = loops.lock().unwrap();
    if map.contains_key(&name) {
        return Err(format!("loop '{name}' already exists"));
    }
    let handle = LoopHandle {
        spec: LoopSpec { tool: tool.clone(), args, interval_secs, ignore, prefix, poll_timeout_secs },
        stop: Arc::new(AtomicBool::new(false)),
        polls: Arc::new(AtomicU64::new(0)),
        injects: Arc::new(AtomicU64::new(0)),
        filter: Arc::new(Mutex::new(None)),
        follow: Arc::new(Mutex::new(HashMap::new())),
        follow_window: Arc::new(AtomicU64::new(60)),
        debounce: Arc::new(Mutex::new(Debounce::default())),
        backlog: Arc::new(Mutex::new(Backlog { entries: VecDeque::new(), dropped: 0 })),
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
            let filt = match h.filter.lock().unwrap().as_ref() {
                Some(p) => format!(
                    "filter ON [{}] follow {}s",
                    p.join(", "),
                    h.follow_window.load(Ordering::Relaxed)
                ),
                None => "filter off".to_string(),
            };
            let edit_debounce_secs = h.debounce.lock().unwrap().window_secs;
            let b = h.backlog.lock().unwrap();
            format!(
                "{name}: poll {} {} every {}s — {} poll(s), {} inject(s), {}, edit_debounce_secs {edit_debounce_secs}, backlog {}{}",
                h.spec.tool,
                h.spec.args,
                h.spec.interval_secs,
                h.polls.load(Ordering::Relaxed),
                h.injects.load(Ordering::Relaxed),
                filt,
                b.entries.len(),
                if b.dropped > 0 { format!(" (+{} dropped)", b.dropped) } else { String::new() },
            )
        })
        .collect();
    lines.sort();
    Ok(lines.join("\n"))
}

fn tool_set_filter(loops: &Loops, a: &Value) -> Result<String, String> {
    let name = a["name"].as_str().ok_or("missing 'name'")?;
    let map = loops.lock().unwrap();
    let h = map.get(name).ok_or_else(|| format!("no loop named '{name}'"))?;
    let pats: Vec<String> = a["patterns"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if let Some(w) = a["follow_window_secs"].as_u64() {
        h.follow_window.store(w.min(3600), Ordering::Relaxed);
    }
    h.follow.lock().unwrap().clear();
    let win = h.follow_window.load(Ordering::Relaxed);
    let mut d = h.debounce.lock().unwrap();
    if let Some(secs) = a["edit_debounce_secs"].as_u64() {
        let held = d.set_window(secs);
        let mut b = h.backlog.lock().unwrap();
        let ts = now_secs();
        for line in held {
            if b.entries.len() >= BACKLOG_CAP {
                b.entries.pop_front();
                b.dropped += 1;
            }
            b.entries.push_back((ts, line));
        }
    }
    let debounce_status = if d.window_secs == 0 {
        "edit debounce OFF".to_string()
    } else {
        format!("edit debounce {}s — latest pending line per sender injects after quiet; streaming pings every {PROGRESS_PING_SECS}s", d.window_secs)
    };
    let filter_off_action = if d.window_secs == 0 {
        "every fresh line injects again"
    } else {
        "sender lines wait for debounce; senderless lines inject immediately"
    };
    drop(d);
    if pats.is_empty() {
        *h.filter.lock().unwrap() = None;
        let held = h.backlog.lock().unwrap().entries.len();
        Ok(format!(
            "loop '{name}': filter OFF — {filter_off_action}; {debounce_status}{}",
            if held > 0 {
                format!(" ({held} line(s) still in backlog; fetch_inject_backlog to read them)")
            } else {
                String::new()
            }
        ))
    } else {
        *h.filter.lock().unwrap() = Some(pats.clone());
        Ok(format!(
            "loop '{name}': filter ON — lines containing [{}] inject; \"…\" continuation fragments inherit the previous line's decision; other lines from a sender within {win}s of an injected one follow it in; the rest wait in debounce or the backlog; {debounce_status}",
            pats.join(", ")
        ))
    }
}

fn tool_fetch_backlog(loops: &Loops, a: &Value) -> Result<String, String> {
    let name = a["name"].as_str().ok_or("missing 'name'")?;
    let max = a["max_lines"].as_u64().map(|n| n as usize);
    let peek = a["peek"].as_bool().unwrap_or(false);
    let map = loops.lock().unwrap();
    let h = map.get(name).ok_or_else(|| format!("no loop named '{name}'"))?;
    let mut b = h.backlog.lock().unwrap();
    if b.entries.is_empty() && b.dropped == 0 {
        return Ok("(backlog empty)".into());
    }
    let take = max.unwrap_or(b.entries.len()).min(b.entries.len());
    let mut out: Vec<String> = Vec::with_capacity(take + 1);
    if b.dropped > 0 {
        out.push(format!(
            "(!) {} oldest line(s) were dropped past the {}-line cap",
            b.dropped, BACKLOG_CAP
        ));
    }
    if peek {
        for (ts, l) in b.entries.iter().take(take) {
            out.push(format!("({}) {}", age_str(*ts), l));
        }
    } else {
        for _ in 0..take {
            if let Some((ts, l)) = b.entries.pop_front() {
                out.push(format!("({}) {}", age_str(ts), l));
            }
        }
        b.dropped = 0;
    }
    let left = b.entries.len();
    if peek {
        out.push(format!("(peek — {left} line(s) remain buffered)"));
    } else if left > 0 {
        out.push(format!("({left} more line(s) still buffered)"));
    }
    Ok(out.join("\n"))
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
                    "set_inject_filter" => Some(tool_set_filter(&loops, &args)),
                    "fetch_inject_backlog" => Some(tool_fetch_backlog(&loops, &args)),
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

#[cfg(test)]
mod filter_tests {
    use super::{filter_decide, line_sender_and_body};
    use std::collections::HashMap;

    fn pats() -> Vec<String> {
        vec!["claude-mini".into(), "@all".into(), "<ayourtch>".into()]
    }

    #[test]
    fn sender_extraction() {
        assert_eq!(
            line_sender_and_body("[#claude] <claude-helper> hello there"),
            Some(("claude-helper".into(), "hello there"))
        );
        assert_eq!(line_sender_and_body("[webex] plain text no nick"), None);
        assert_eq!(
            line_sender_and_body("[msgd #rusty] Rusty: progress <tag> ▍"),
            Some(("rusty".into(), "progress <tag> ▍"))
        );
        assert_eq!(line_sender_and_body("[msgd #rusty] no sender: body"), None);
        assert_eq!(line_sender_and_body("[msgd #rusty] : body"), None);
    }

    #[test]
    fn continuation_inherits_inject() {
        let mut f = HashMap::new();
        assert!(filter_decide("[#claude] <helper> claude-mini: part one", &pats(), &mut f, 0, 100));
        assert!(filter_decide("[#claude] <helper> … the tail", &pats(), &mut f, 0, 102));
    }

    #[test]
    fn continuation_inherits_hold() {
        let mut f = HashMap::new();
        assert!(!filter_decide("[#claude] <helper> otherbot: not for us", &pats(), &mut f, 0, 100));
        assert!(!filter_decide("[#claude] <helper> … its tail stays held", &pats(), &mut f, 0, 102));
    }

    #[test]
    fn follow_window_pulls_in_burst_and_expires() {
        let mut f = HashMap::new();
        assert!(filter_decide("[#claude] <helper> claude-mini: tagged", &pats(), &mut f, 60, 100));
        assert!(filter_decide("[#claude] <helper> follow-up thought", &pats(), &mut f, 60, 130));
        assert!(filter_decide("[#claude] <helper> chained follow-up", &pats(), &mut f, 60, 185));
        assert!(!filter_decide("[#claude] <helper> an hour later", &pats(), &mut f, 60, 4000));
    }

    #[test]
    fn window_zero_disables_follow_not_marker() {
        let mut f = HashMap::new();
        assert!(filter_decide("[#claude] <helper> claude-mini: tagged", &pats(), &mut f, 0, 100));
        assert!(!filter_decide("[#claude] <helper> unmarked follow-up", &pats(), &mut f, 0, 101));
        // held line resets inheritance for the next marker fragment
        assert!(!filter_decide("[#claude] <helper> … tail of the HELD line stays held", &pats(), &mut f, 0, 102));
    }

    #[test]
    fn senders_do_not_cross_contaminate() {
        let mut f = HashMap::new();
        assert!(filter_decide("[#claude] <helper> claude-mini: tagged", &pats(), &mut f, 60, 100));
        assert!(!filter_decide("[#claude] <otherbot> unrelated line", &pats(), &mut f, 60, 101));
        assert!(!filter_decide("[#claude] <otherbot> … and its tail", &pats(), &mut f, 60, 102));
    }
}

#[cfg(test)]
mod debounce_tests {
    use super::*;

    #[test]
    fn latest_edit_injects_only_after_quiet_with_filter_on_or_off() {
        for decision in [Some(false), None] {
            let mut d = Debounce::default();
            d.set_window(5);
            let mut backlog = Vec::new();
            for now in [0, 2, 4] {
                let line = format!("[msgd #rusty] rusty: version {now}");
                assert_eq!(d.offer(&line, decision, now, &mut backlog), None);
                assert!(d.due(now).is_empty());
            }
            assert_eq!(backlog, vec![
                "[msgd #rusty] rusty: version 0", "[msgd #rusty] rusty: version 2",
            ]);
            for now in 5..9 {
                assert!(d.due(now).is_empty());
            }
            assert_eq!(d.due(9), vec!["[msgd #rusty] rusty: version 4"]);
            assert!(d.due(10).is_empty());
        }
    }

    #[test]
    fn pattern_match_clears_pending_to_backlog() {
        let mut d = Debounce::default();
        d.set_window(5);
        let mut backlog = Vec::new();
        let mut follow = HashMap::new();
        let patterns = vec!["@all".to_string()];
        let pending = "[msgd #rusty] rusty: working ▍";
        let decision = filter_decide(pending, &patterns, &mut follow, 60, 0);
        assert!(!decision);
        assert_eq!(d.offer(pending, Some(decision), 0, &mut backlog), None);
        let matching = "[msgd #rusty] rusty: @all ready";
        let decision = filter_decide(matching, &patterns, &mut follow, 60, 3);
        assert!(decision);
        assert_eq!(d.offer(matching, Some(decision), 3, &mut backlog), Some(matching.into()));
        assert_eq!(backlog, vec![pending]);
        assert!(d.due(10).is_empty());
        assert!(d.progress("rusty", 30).is_empty());
        for (now, line) in [
            (4, "[msgd #rusty] rusty: follow-up"),
            (5, "[msgd #rusty] rusty: … continuation"),
        ] {
            let decision = filter_decide(line, &patterns, &mut follow, 60, now);
            assert!(decision);
            assert_eq!(d.offer(line, Some(decision), now, &mut backlog), Some(line.into()));
        }
        assert!(d.due(20).is_empty());
    }

    #[test]
    fn continuous_edits_ping_at_30_and_60_but_not_45() {
        let mut d = Debounce::default();
        d.set_window(5);
        let mut backlog = Vec::new();
        for now in 0..=60 {
            let line = format!("[msgd #rusty] rusty: edit {now} ▍");
            assert_eq!(d.offer(&line, Some(false), now, &mut backlog), None);
            assert!(d.due(now).is_empty());
            let pings = d.progress("peer", now);
            if now == 30 || now == 60 {
                assert_eq!(pings, vec![format!(
                    "[peer debounce] rusty still streaming ({} edits, {now} s)", now + 1,
                )]);
                assert!(d.progress("peer", now).is_empty());
            } else {
                assert!(pings.is_empty(), "unexpected ping at {now}");
            }
        }
        assert_eq!(d.due(65), vec!["[msgd #rusty] rusty: edit 60 ▍"]);
        assert!(d.progress("peer", 90).is_empty());
    }

    #[test]
    fn zero_window_preserves_filter_decisions_and_filter_off_passthrough() {
        let mut d = Debounce::default();
        let mut backlog = Vec::new();
        let line = "[#chat] <rusty> hello";
        assert_eq!(d.offer(line, None, 0, &mut backlog), Some(line.into()));
        assert_eq!(d.offer(line, Some(true), 1, &mut backlog), Some(line.into()));
        assert!(backlog.is_empty());
        assert_eq!(d.offer(line, Some(false), 2, &mut backlog), None);
        assert_eq!(backlog, vec![line]);
        assert!(d.due(100).is_empty());
        assert!(d.progress("peer", 100).is_empty());
    }

    #[test]
    fn senders_have_independent_windows_and_senderless_lines_keep_filtering() {
        let mut d = Debounce::default();
        d.set_window(5);
        let mut backlog = Vec::new();
        let first = "[#chat] <rusty> first";
        let second = "[#chat] <other> second";
        assert_eq!(d.offer(first, None, 0, &mut backlog), None);
        assert_eq!(d.offer(second, None, 2, &mut backlog), None);
        assert_eq!(d.due(5), vec![first]);
        assert_eq!(d.due(7), vec![second]);
        let plain = "[webex] plain text without sender";
        assert_eq!(d.offer(plain, None, 8, &mut backlog), Some(plain.into()));
        assert_eq!(d.offer(plain, Some(true), 8, &mut backlog), Some(plain.into()));
        assert_eq!(d.offer(plain, Some(false), 8, &mut backlog), None);
        assert_eq!(backlog, vec![plain]);
        assert!(d.due(20).is_empty());
    }

    #[test]
    fn progress_requires_replacement_and_new_edits_since_last_ping() {
        let mut d = Debounce::default();
        d.set_window(600);
        let mut backlog = Vec::new();
        d.offer("[#chat] <rusty> initial", None, 0, &mut backlog);
        assert!(d.progress("peer", 30).is_empty());
        d.offer("[#chat] <rusty> edited", None, 31, &mut backlog);
        assert_eq!(d.progress("peer", 31).len(), 1);
        assert!(d.progress("peer", 61).is_empty());
    }

    #[test]
    fn set_filter_preserves_caps_and_disables_debounce_into_backlog() {
        let loops = Mutex::new(HashMap::from([("peer".to_string(), LoopHandle {
            spec: LoopSpec {
                tool: "recv".into(), args: json!({}), interval_secs: 1.0,
                ignore: String::new(), prefix: "[peer] ".into(), poll_timeout_secs: None,
            },
            stop: Arc::new(AtomicBool::new(false)),
            polls: Arc::new(AtomicU64::new(0)),
            injects: Arc::new(AtomicU64::new(0)),
            filter: Arc::new(Mutex::new(None)),
            backlog: Arc::new(Mutex::new(Backlog { entries: VecDeque::new(), dropped: 0 })),
            follow: Arc::new(Mutex::new(HashMap::new())),
            follow_window: Arc::new(AtomicU64::new(60)),
            debounce: Arc::new(Mutex::new(Debounce::default())),
        })]));
        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 0"));
        tool_set_filter(&loops, &json!({"name": "peer", "edit_debounce_secs": 700})).unwrap();
        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 600"));
        tool_set_filter(&loops, &json!({"name": "peer", "edit_debounce_secs": 5})).unwrap();
        let pending = "[msgd #rusty] rusty: pending";
        {
            let map = loops.lock().unwrap();
            let mut held = Vec::new();
            map["peer"].debounce.lock().unwrap().offer(pending, None, 0, &mut held);
            assert!(held.is_empty());
        }
        tool_set_filter(&loops, &json!({"name": "peer", "patterns": ["@all"]})).unwrap();
        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 5"));
        assert_eq!(tool_fetch_backlog(&loops, &json!({"name": "peer"})).unwrap(), "(backlog empty)");
        tool_set_filter(&loops, &json!({"name": "peer", "edit_debounce_secs": 0})).unwrap();
        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 0"));
        assert!(tool_fetch_backlog(&loops, &json!({"name": "peer"})).unwrap().contains(pending));
        let map = loops.lock().unwrap();
        let mut d = map["peer"].debounce.lock().unwrap();
        assert!(d.due(100).is_empty());
        assert!(d.progress("peer", 100).is_empty());
    }
}

#[cfg(test)]
mod neutralize_tests {
    use super::neutralize_key_tokens as n;

    /// A SUPERSET of tttt's send_keys decoder (tttt
    /// crates/tttt-pty/src/keys.rs `process_special_keys`): every
    /// `[UPPER+DIGITS]` run counts as a key (tttt only knows a list), every
    /// `^letter`, and `\n` `\r` `\t` `\xHH` as there. Text that survives
    /// this decoder unchanged survives tttt's. If tttt grows NEW syntax
    /// (not a new [TAG] name), update this mirror and the escaper.
    fn decode(input: &str) -> Vec<u8> {
        let b = input.as_bytes();
        let (mut out, mut i) = (Vec::new(), 0);
        while i < b.len() {
            if b[i] == b'^' && i + 1 < b.len() && b[i + 1].is_ascii_alphabetic() {
                out.push(0x01); // a control key fired
                i += 2;
                continue;
            }
            if b[i] == b'\\' && i + 1 < b.len() {
                match b[i + 1] {
                    b'n' | b'r' | b't' => {
                        out.push(0x01);
                        i += 2;
                        continue;
                    }
                    b'x' if i + 3 < b.len()
                        && b[i + 2].is_ascii_hexdigit()
                        && b[i + 3].is_ascii_hexdigit() =>
                    {
                        out.push(u8::from_str_radix(&input[i + 2..i + 4], 16).unwrap());
                        i += 4;
                        continue;
                    }
                    _ => {}
                }
            }
            if b[i] == b'[' {
                if let Some(end) = input[i..].find(']') {
                    let tag = &input[i + 1..i + end];
                    if !tag.is_empty() && tag.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'+') {
                        out.push(0x01);
                        i += end + 1;
                        continue;
                    }
                }
            }
            out.push(b[i]);
            i += 1;
        }
        out
    }
    fn shown(text: &str) -> String {
        String::from_utf8(decode(&n(text))).unwrap()
    }

    #[test]
    fn key_syntax_arrives_verbatim() {
        for t in ["^C ^c ^M ^D", r"\r \n \t \x1b \x03", "[ENTER] [CTRL+C] [F1] [ESC]",
                  r"C:\new\tmp ^Z", r"\x5c already escaped", "a[b]c ^ \\ [", "x^2 + y^2"] {
            assert_eq!(shown(t), t, "{t:?}");
        }
    }

    #[test]
    fn all_printable_ascii_arrives_verbatim() {
        // every printable byte, alone and in pairs ('@' excluded: it is rewritten on purpose)
        let p: Vec<char> = (0x20u8..0x7f).map(|b| b as char).filter(|c| *c != '@').collect();
        for &a in &p {
            for &b in &p {
                let t = format!("x{a}{b}AB]C");
                assert_eq!(shown(&t), t);
            }
        }
    }

    #[test]
    fn control_and_invisible_chars_become_visible_text() {
        assert_eq!(shown("a\u{1b}b\u{3}c\u{7f}d\u{85}"), r"a\x1bb\x03c\x7fd\x85");
        assert_eq!(shown("a\u{200b}b\u{feff}c\u{202e}"), r"a\u{200b}b\u{feff}c\u{202e}");
    }

    #[test]
    fn nothing_decodes_to_a_key_or_invisible_char() {
        let all: String = (0u32..0x3000).filter_map(char::from_u32).collect();
        let out = shown(&all);
        assert!(out.bytes().all(|b| b >= 0x20 && b != 0x7f), "a key byte survived");
        assert!(!out.chars().any(|c| ('\u{80}'..='\u{9f}').contains(&c) || super::is_invisible_format(c)));
    }

    #[test]
    fn token_leading_at_becomes_fullwidth() {
        assert_eq!(n("@claudine is op"), "\u{ff20}claudine is op");
        assert_eq!(n("hi @all folks"), "hi \u{ff20}all folks");
        assert_eq!(n("(@claudine)"), "(\u{ff20}claudine)");
        assert_eq!(n("users: @claudine"), "users: \u{ff20}claudine");
        assert_eq!(shown("[@x]"), "[\u{ff20}x]");
    }

    #[test]
    fn email_and_bare_at_untouched() {
        assert_eq!(n("Eimolough@yahoo.com"), "Eimolough@yahoo.com");
        assert_eq!(n("mail from a@b.c today"), "mail from a@b.c today");
        assert_eq!(n("an @ alone"), "an @ alone");
        assert_eq!(n("ends with @"), "ends with @");
        assert_eq!(n("@@weird"), "@@weird");
    }
}
