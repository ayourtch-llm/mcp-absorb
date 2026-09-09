Implemented the debounce brief in `src/main.rs`; changes are left uncommitted for review.

The per-loop debounce defaults to 0, is capped at 600 seconds, preserves its value when omitted from `set_inject_filter`, and flushes pending lines to the capped backlog when explicitly disabled. Each sender's latest held line injects after the quiet window; replacements and pending lines cleared by immediate filter matches go to the backlog. Filter OFF also debounces sender lines when enabled. Senderless lines retain their filtering behavior.

The existing poll loop checks pending lines on every completed poll, including empty, ignored, and failed results. Delayed lines use the configured provenance prefix, while streaming pings use `[<loop> debounce] <sender> still streaming (<n> edits, <secs> s)` with `PROGRESS_PING_SECS = 30`. No timer thread was added, and `filter_decide` itself is unchanged. The existing sender splitter now also recognizes the brief's `[msgd #rusty] rusty: ...` format, which it previously did not recognize. Both tool descriptions were updated to one sentence, and loop listings include `edit_debounce_secs`.

Files touched (line counts relative to the starting working tree):

| File | Added | Removed |
| --- | ---: | ---: |
| `src/main.rs` | +343 | -18 |
| `DONE-DEBOUNCE.md` (new) | +509 | -0 |

The new report is untracked, so plain `git diff` below covers `src/main.rs`; its own added-line count is included above. The pre-existing untracked `BRIEF-DEBOUNCE.md` and `lane-debounce.log` were not edited.

Validation results:

- `cargo build --release`: PASS, exit 0; optimized release build completed successfully.
- `cargo test`: PASS, exit 0; 17 passed, 0 failed, 0 ignored, 0 measured, 0 filtered out.
- The seven new debounce tests cover the required t=0/2/4 -> t=9 collapse, a pattern match at t=3 clearing pending to backlog, pings at t=30/60 but not t=45, and window-zero passthrough, plus filter OFF, follow/continuation behavior, independent senders, senderless lines, idle ping suppression, option preservation, the 600-second cap, listing state, and disable-to-backlog behavior.
- A temporary end-to-end harness ran the release binary with a mock MCP subordinate and mock `tttt`: PASS. It checked the argument schema, collapse with filters ON and OFF, release after empty or failed polls, immediate pattern matches with pending cancellation, custom provenance prefixes, existing key-token neutralization, and displaced-line backlogs. Exactly four expected injections occurred, with no duplicate pending injection. The temporary harness was removed automatically.
- `git diff --check`: PASS, exit 0.

Full `git diff` output:

```diff
diff --git a/src/main.rs b/src/main.rs
index eef72f0..132cf48 100644
--- a/src/main.rs
+++ b/src/main.rs
@@ -167,12 +167,20 @@ struct LoopHandle {
     /// (ayourtch 2026-08-14: tagged-once means interested in the burst).
     /// 0 disables the window; the "…" continuation inherit (b) stays on.
     follow_window: Arc<AtomicU64>,
+    debounce: Arc<Mutex<Debounce>>,
 }
 
-/// Extract "<nick>" sender and the message body from a provenance-prefixed
-/// line like "[#claude] <claude-helper> text". None for lines without one
-/// (webex/email formats) — those get plain pattern filtering only.
+/// Extract the sender and body from "[#claude] <nick> text" or
+/// "[msgd #rusty] rusty: text". Other senderless lines get plain filtering.
 fn line_sender_and_body(line: &str) -> Option<(String, &str)> {
+    if line.starts_with("[msgd ") {
+        let (_, message) = line.split_once(']')?;
+        let (sender, body) = message.trim_start().split_once(':')?;
+        if sender.is_empty() || sender.chars().any(char::is_whitespace) {
+            return None;
+        }
+        return Some((sender.to_lowercase(), body.trim_start()));
+    }
     let lt = line.find('<')?;
     let gt = line[lt..].find('>')? + lt;
     let sender = line[lt + 1..gt].trim();
@@ -223,6 +231,106 @@ fn filter_decide(
     inject
 }
 
+const PROGRESS_PING_SECS: u64 = 30;
+
+struct PendingEdit {
+    line: String,
+    first: u64,
+    last: u64,
+    edits: u64,
+    last_ping: u64,
+    pinged_edits: u64,
+}
+
+#[derive(Default)]
+struct Debounce {
+    window_secs: u64,
+    pending: HashMap<String, PendingEdit>,
+}
+
+impl Debounce {
+    /// Return lines released to the backlog when debounce is disabled.
+    fn set_window(&mut self, secs: u64) -> Vec<String> {
+        self.window_secs = secs.min(600);
+        if self.window_secs == 0 {
+            self.pending.drain().map(|(_, p)| p.line).collect()
+        } else {
+            Vec::new()
+        }
+    }
+
+    /// None means filter OFF; Some is the unchanged filter_decide result.
+    /// Return an immediate inject and append displaced/rejected lines to held.
+    fn offer(
+        &mut self,
+        line: &str,
+        filter_inject: Option<bool>,
+        now: u64,
+        held: &mut Vec<String>,
+    ) -> Option<String> {
+        if self.window_secs > 0 {
+            if let Some((sender, _)) = line_sender_and_body(line) {
+                if filter_inject != Some(true) {
+                    let pending = self.pending.entry(sender).or_insert_with(|| PendingEdit {
+                        line: String::new(), first: now, last: now, edits: 0,
+                        last_ping: now, pinged_edits: 0,
+                    });
+                    if pending.edits > 0 {
+                        held.push(std::mem::replace(&mut pending.line, line.to_string()));
+                    } else {
+                        pending.line = line.to_string();
+                    }
+                    pending.last = now;
+                    pending.edits += 1;
+                    return None;
+                }
+                if let Some(pending) = self.pending.remove(&sender) {
+                    held.push(pending.line);
+                }
+            }
+        }
+        if filter_inject.unwrap_or(true) {
+            Some(line.to_string())
+        } else {
+            held.push(line.to_string());
+            None
+        }
+    }
+
+    fn due(&mut self, now: u64) -> Vec<String> {
+        let mut lines = Vec::new();
+        self.pending.retain(|_, pending| {
+            if now.saturating_sub(pending.last) >= self.window_secs {
+                lines.push(pending.line.clone());
+                false
+            } else {
+                true
+            }
+        });
+        lines
+    }
+
+    fn progress(&mut self, name: &str, now: u64) -> Vec<String> {
+        let mut lines = Vec::new();
+        for (sender, pending) in &mut self.pending {
+            if pending.edits > 1
+                && pending.edits > pending.pinged_edits
+                && now.saturating_sub(pending.first) >= PROGRESS_PING_SECS
+                && now.saturating_sub(pending.last_ping) >= PROGRESS_PING_SECS
+                && now.saturating_sub(pending.last) < self.window_secs
+            {
+                lines.push(format!(
+                    "[{name} debounce] {sender} still streaming ({} edits, {} s)",
+                    pending.edits, now.saturating_sub(pending.first)
+                ));
+                pending.last_ping = now;
+                pending.pinged_edits = pending.edits;
+            }
+        }
+        lines
+    }
+}
+
 struct Cfg {
     session: String,
     tttt_bin: String,
@@ -309,6 +417,7 @@ fn run_loop(name: String, h: &LoopHandle, sub: Arc<Sub>, cfg: Arc<Cfg>) {
     let backlog = Arc::clone(&h.backlog);
     let follow = Arc::clone(&h.follow);
     let follow_window = Arc::clone(&h.follow_window);
+    let debounce = Arc::clone(&h.debounce);
     let tool = h.spec.tool.clone();
     let args = h.spec.args.clone();
     let interval = Duration::from_secs_f64(h.spec.interval_secs);
@@ -343,12 +452,13 @@ fn run_loop(name: String, h: &LoopHandle, sub: Arc<Sub>, cfg: Arc<Cfg>) {
                         poll_err = Some(t.chars().take(160).collect());
                     } else if !t.is_empty() && t != ignore {
                         let pats = filter.lock().unwrap().clone();
-                        let to_inject: Option<String> = match pats {
-                            None => Some(t.to_string()),
-                            Some(pats) => {
+                        let mut d = debounce.lock().unwrap();
+                        let to_inject: Option<String> = match pats.as_ref() {
+                            None if d.window_secs == 0 => Some(t.to_string()),
+                            _ => {
                                 // Per line: matching lines interrupt, the
-                                // rest wait in the backlog.
-                                let mut pass: Vec<&str> = Vec::new();
+                                // rest wait in debounce or the backlog.
+                                let mut pass: Vec<String> = Vec::new();
                                 let mut held: Vec<String> = Vec::new();
                                 let now = now_secs();
                                 let win = follow_window.load(Ordering::Relaxed);
@@ -358,10 +468,10 @@ fn run_loop(name: String, h: &LoopHandle, sub: Arc<Sub>, cfg: Arc<Cfg>) {
                                     if l.is_empty() {
                                         continue;
                                     }
-                                    if filter_decide(l, &pats, &mut fmap, win, now) {
-                                        pass.push(l);
-                                    } else {
-                                        held.push(l.to_string());
+                                    let decision = pats.as_ref()
+                                        .map(|p| filter_decide(l, p, &mut fmap, win, now));
+                                    if let Some(line) = d.offer(l, decision, now, &mut held) {
+                                        pass.push(line);
                                     }
                                 }
                                 drop(fmap);
@@ -383,6 +493,7 @@ fn run_loop(name: String, h: &LoopHandle, sub: Arc<Sub>, cfg: Arc<Cfg>) {
                                 }
                             }
                         };
+                        drop(d);
                         if let Some(body) = to_inject {
                             // One poll's output = one batched inject, with provenance.
                             let msg = format!("{prefix}{body}");
@@ -396,6 +507,24 @@ fn run_loop(name: String, h: &LoopHandle, sub: Arc<Sub>, cfg: Arc<Cfg>) {
                     poll_err = Some(e.chars().take(160).collect());
                 }
             }
+            // Check even when the poll has no fresh output or returns an error.
+            let mut d = debounce.lock().unwrap();
+            let now = now_secs();
+            let due = d.due(now);
+            let progress = d.progress(&name, now);
+            drop(d);
+            let mut messages = Vec::new();
+            if !due.is_empty() {
+                messages.push(format!("{prefix}{}", due.join("\n")));
+            }
+            if !progress.is_empty() {
+                messages.push(progress.join("\n"));
+            }
+            for msg in messages {
+                if inject_into_tttt(&cfg, &msg).is_ok() {
+                    injects.fetch_add(1, Ordering::Relaxed);
+                }
+            }
             // A persistently failing poll must not be indistinguishable from
             // "no news": alarm after ~3 min without a good poll, re-alarm
             // every 10 min, notice on recovery. Wall-clock,
@@ -478,18 +607,19 @@ fn own_tools_json(cfg: &Cfg) -> Vec<Value> {
         }),
         json!({
             "name": "list_tttt_inject_loops",
-            "description": "List active inject loops with their poll/inject counters, filter state and backlog depth.",
+            "description": "List active inject loops with their poll/inject counters, filter state, edit_debounce_secs and backlog depth.",
             "inputSchema": {"type": "object", "properties": {}}
         }),
         json!({
             "name": "set_inject_filter",
-            "description": "Less-interrupts mode for one inject loop. With 'patterns' (case-insensitive substrings): only polled lines containing at least one pattern are injected (interrupt the session); all other lines are held in a backlog readable via fetch_inject_backlog. Without 'patterns' (or with an empty list): filter OFF, every fresh line injects again (the default). Typical IRC patterns: [\"mynick:\", \"@mynick\", \"@all\", \"<operator-nick>\"].",
+            "description": "Set case-insensitive 'patterns' for immediate injects (including continuation/follow matches; omit or empty for filter OFF), with other lines held in fetch_inject_backlog unless edit_debounce_secs delays each sender's latest unmatched line (every sender line with filter OFF) until quiet, with streaming progress pings at most every 30s; omit edit_debounce_secs to preserve it or set 0 to disable it and flush pending lines to the backlog.",
             "inputSchema": {
                 "type": "object",
                 "properties": {
                     "name": {"type": "string", "description": "Loop name"},
                     "patterns": {"type": "array", "items": {"type": "string"}, "description": "Substrings that make a line inject; omit or empty to turn the filter off"},
-                    "follow_window_secs": {"type": "integer", "description": "After an injected line from a sender, further lines from the same sender within this many seconds also inject (default 60, max 3600, 0 = off). \"…\" continuation fragments always inherit the previous line's decision."}
+                    "follow_window_secs": {"type": "integer", "description": "After an injected line from a sender, further lines from the same sender within this many seconds also inject (default 60, max 3600, 0 = off). \"…\" continuation fragments always inherit the previous line's decision."},
+                    "edit_debounce_secs": {"type": "integer", "minimum": 0, "maximum": 600, "description": "Per-sender quiet window (default 0 = off, max 600); omit to keep the current value, or set 0 to flush pending lines to the backlog"}
                 },
                 "required": ["name"]
             }
@@ -533,6 +663,7 @@ fn tool_create_loop(loops: &Loops, sub: &Arc<Sub>, cfg: &Arc<Cfg>, a: &Value) ->
         filter: Arc::new(Mutex::new(None)),
         follow: Arc::new(Mutex::new(HashMap::new())),
         follow_window: Arc::new(AtomicU64::new(60)),
+        debounce: Arc::new(Mutex::new(Debounce::default())),
         backlog: Arc::new(Mutex::new(Backlog { entries: VecDeque::new(), dropped: 0 })),
     };
     run_loop(name.clone(), &handle, Arc::clone(sub), Arc::clone(cfg));
@@ -570,9 +701,10 @@ fn tool_list_loops(loops: &Loops) -> Result<String, String> {
                 ),
                 None => "filter off".to_string(),
             };
+            let edit_debounce_secs = h.debounce.lock().unwrap().window_secs;
             let b = h.backlog.lock().unwrap();
             format!(
-                "{name}: poll {} {} every {}s — {} poll(s), {} inject(s), {}, backlog {}{}",
+                "{name}: poll {} {} every {}s — {} poll(s), {} inject(s), {}, edit_debounce_secs {edit_debounce_secs}, backlog {}{}",
                 h.spec.tool,
                 h.spec.args,
                 h.spec.interval_secs,
@@ -607,11 +739,35 @@ fn tool_set_filter(loops: &Loops, a: &Value) -> Result<String, String> {
     }
     h.follow.lock().unwrap().clear();
     let win = h.follow_window.load(Ordering::Relaxed);
+    let mut d = h.debounce.lock().unwrap();
+    if let Some(secs) = a["edit_debounce_secs"].as_u64() {
+        let held = d.set_window(secs);
+        let mut b = h.backlog.lock().unwrap();
+        let ts = now_secs();
+        for line in held {
+            if b.entries.len() >= BACKLOG_CAP {
+                b.entries.pop_front();
+                b.dropped += 1;
+            }
+            b.entries.push_back((ts, line));
+        }
+    }
+    let debounce_status = if d.window_secs == 0 {
+        "edit debounce OFF".to_string()
+    } else {
+        format!("edit debounce {}s — latest pending line per sender injects after quiet; streaming pings every {PROGRESS_PING_SECS}s", d.window_secs)
+    };
+    let filter_off_action = if d.window_secs == 0 {
+        "every fresh line injects again"
+    } else {
+        "sender lines wait for debounce; senderless lines inject immediately"
+    };
+    drop(d);
     if pats.is_empty() {
         *h.filter.lock().unwrap() = None;
         let held = h.backlog.lock().unwrap().entries.len();
         Ok(format!(
-            "loop '{name}': filter OFF — every fresh line injects again{}",
+            "loop '{name}': filter OFF — {filter_off_action}; {debounce_status}{}",
             if held > 0 {
                 format!(" ({held} line(s) still in backlog; fetch_inject_backlog to read them)")
             } else {
@@ -621,7 +777,7 @@ fn tool_set_filter(loops: &Loops, a: &Value) -> Result<String, String> {
     } else {
         *h.filter.lock().unwrap() = Some(pats.clone());
         Ok(format!(
-            "loop '{name}': filter ON — lines containing [{}] inject; \"…\" continuation fragments inherit the previous line's decision; other lines from a sender within {win}s of an injected one follow it in; the rest go to the backlog",
+            "loop '{name}': filter ON — lines containing [{}] inject; \"…\" continuation fragments inherit the previous line's decision; other lines from a sender within {win}s of an injected one follow it in; the rest wait in debounce or the backlog; {debounce_status}",
             pats.join(", ")
         ))
     }
@@ -820,6 +976,12 @@ mod filter_tests {
             Some(("claude-helper".into(), "hello there"))
         );
         assert_eq!(line_sender_and_body("[webex] plain text no nick"), None);
+        assert_eq!(
+            line_sender_and_body("[msgd #rusty] Rusty: progress <tag> ▍"),
+            Some(("rusty".into(), "progress <tag> ▍"))
+        );
+        assert_eq!(line_sender_and_body("[msgd #rusty] no sender: body"), None);
+        assert_eq!(line_sender_and_body("[msgd #rusty] : body"), None);
     }
 
     #[test]
@@ -863,6 +1025,169 @@ mod filter_tests {
     }
 }
 
+#[cfg(test)]
+mod debounce_tests {
+    use super::*;
+
+    #[test]
+    fn latest_edit_injects_only_after_quiet_with_filter_on_or_off() {
+        for decision in [Some(false), None] {
+            let mut d = Debounce::default();
+            d.set_window(5);
+            let mut backlog = Vec::new();
+            for now in [0, 2, 4] {
+                let line = format!("[msgd #rusty] rusty: version {now}");
+                assert_eq!(d.offer(&line, decision, now, &mut backlog), None);
+                assert!(d.due(now).is_empty());
+            }
+            assert_eq!(backlog, vec![
+                "[msgd #rusty] rusty: version 0", "[msgd #rusty] rusty: version 2",
+            ]);
+            for now in 5..9 {
+                assert!(d.due(now).is_empty());
+            }
+            assert_eq!(d.due(9), vec!["[msgd #rusty] rusty: version 4"]);
+            assert!(d.due(10).is_empty());
+        }
+    }
+
+    #[test]
+    fn pattern_match_clears_pending_to_backlog() {
+        let mut d = Debounce::default();
+        d.set_window(5);
+        let mut backlog = Vec::new();
+        let mut follow = HashMap::new();
+        let patterns = vec!["@all".to_string()];
+        let pending = "[msgd #rusty] rusty: working ▍";
+        let decision = filter_decide(pending, &patterns, &mut follow, 60, 0);
+        assert!(!decision);
+        assert_eq!(d.offer(pending, Some(decision), 0, &mut backlog), None);
+        let matching = "[msgd #rusty] rusty: @all ready";
+        let decision = filter_decide(matching, &patterns, &mut follow, 60, 3);
+        assert!(decision);
+        assert_eq!(d.offer(matching, Some(decision), 3, &mut backlog), Some(matching.into()));
+        assert_eq!(backlog, vec![pending]);
+        assert!(d.due(10).is_empty());
+        assert!(d.progress("rusty", 30).is_empty());
+        for (now, line) in [
+            (4, "[msgd #rusty] rusty: follow-up"),
+            (5, "[msgd #rusty] rusty: … continuation"),
+        ] {
+            let decision = filter_decide(line, &patterns, &mut follow, 60, now);
+            assert!(decision);
+            assert_eq!(d.offer(line, Some(decision), now, &mut backlog), Some(line.into()));
+        }
+        assert!(d.due(20).is_empty());
+    }
+
+    #[test]
+    fn continuous_edits_ping_at_30_and_60_but_not_45() {
+        let mut d = Debounce::default();
+        d.set_window(5);
+        let mut backlog = Vec::new();
+        for now in 0..=60 {
+            let line = format!("[msgd #rusty] rusty: edit {now} ▍");
+            assert_eq!(d.offer(&line, Some(false), now, &mut backlog), None);
+            assert!(d.due(now).is_empty());
+            let pings = d.progress("peer", now);
+            if now == 30 || now == 60 {
+                assert_eq!(pings, vec![format!(
+                    "[peer debounce] rusty still streaming ({} edits, {now} s)", now + 1,
+                )]);
+                assert!(d.progress("peer", now).is_empty());
+            } else {
+                assert!(pings.is_empty(), "unexpected ping at {now}");
+            }
+        }
+        assert_eq!(d.due(65), vec!["[msgd #rusty] rusty: edit 60 ▍"]);
+        assert!(d.progress("peer", 90).is_empty());
+    }
+
+    #[test]
+    fn zero_window_preserves_filter_decisions_and_filter_off_passthrough() {
+        let mut d = Debounce::default();
+        let mut backlog = Vec::new();
+        let line = "[#chat] <rusty> hello";
+        assert_eq!(d.offer(line, None, 0, &mut backlog), Some(line.into()));
+        assert_eq!(d.offer(line, Some(true), 1, &mut backlog), Some(line.into()));
+        assert!(backlog.is_empty());
+        assert_eq!(d.offer(line, Some(false), 2, &mut backlog), None);
+        assert_eq!(backlog, vec![line]);
+        assert!(d.due(100).is_empty());
+        assert!(d.progress("peer", 100).is_empty());
+    }
+
+    #[test]
+    fn senders_have_independent_windows_and_senderless_lines_keep_filtering() {
+        let mut d = Debounce::default();
+        d.set_window(5);
+        let mut backlog = Vec::new();
+        let first = "[#chat] <rusty> first";
+        let second = "[#chat] <other> second";
+        assert_eq!(d.offer(first, None, 0, &mut backlog), None);
+        assert_eq!(d.offer(second, None, 2, &mut backlog), None);
+        assert_eq!(d.due(5), vec![first]);
+        assert_eq!(d.due(7), vec![second]);
+        let plain = "[webex] plain text without sender";
+        assert_eq!(d.offer(plain, None, 8, &mut backlog), Some(plain.into()));
+        assert_eq!(d.offer(plain, Some(true), 8, &mut backlog), Some(plain.into()));
+        assert_eq!(d.offer(plain, Some(false), 8, &mut backlog), None);
+        assert_eq!(backlog, vec![plain]);
+        assert!(d.due(20).is_empty());
+    }
+
+    #[test]
+    fn progress_requires_replacement_and_new_edits_since_last_ping() {
+        let mut d = Debounce::default();
+        d.set_window(600);
+        let mut backlog = Vec::new();
+        d.offer("[#chat] <rusty> initial", None, 0, &mut backlog);
+        assert!(d.progress("peer", 30).is_empty());
+        d.offer("[#chat] <rusty> edited", None, 31, &mut backlog);
+        assert_eq!(d.progress("peer", 31).len(), 1);
+        assert!(d.progress("peer", 61).is_empty());
+    }
+
+    #[test]
+    fn set_filter_preserves_caps_and_disables_debounce_into_backlog() {
+        let loops = Mutex::new(HashMap::from([("peer".to_string(), LoopHandle {
+            spec: LoopSpec {
+                tool: "recv".into(), args: json!({}), interval_secs: 1.0,
+                ignore: String::new(), prefix: "[peer] ".into(), poll_timeout_secs: None,
+            },
+            stop: Arc::new(AtomicBool::new(false)),
+            polls: Arc::new(AtomicU64::new(0)),
+            injects: Arc::new(AtomicU64::new(0)),
+            filter: Arc::new(Mutex::new(None)),
+            backlog: Arc::new(Mutex::new(Backlog { entries: VecDeque::new(), dropped: 0 })),
+            follow: Arc::new(Mutex::new(HashMap::new())),
+            follow_window: Arc::new(AtomicU64::new(60)),
+            debounce: Arc::new(Mutex::new(Debounce::default())),
+        })]));
+        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 0"));
+        tool_set_filter(&loops, &json!({"name": "peer", "edit_debounce_secs": 700})).unwrap();
+        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 600"));
+        tool_set_filter(&loops, &json!({"name": "peer", "edit_debounce_secs": 5})).unwrap();
+        let pending = "[msgd #rusty] rusty: pending";
+        {
+            let map = loops.lock().unwrap();
+            let mut held = Vec::new();
+            map["peer"].debounce.lock().unwrap().offer(pending, None, 0, &mut held);
+            assert!(held.is_empty());
+        }
+        tool_set_filter(&loops, &json!({"name": "peer", "patterns": ["@all"]})).unwrap();
+        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 5"));
+        assert_eq!(tool_fetch_backlog(&loops, &json!({"name": "peer"})).unwrap(), "(backlog empty)");
+        tool_set_filter(&loops, &json!({"name": "peer", "edit_debounce_secs": 0})).unwrap();
+        assert!(tool_list_loops(&loops).unwrap().contains("edit_debounce_secs 0"));
+        assert!(tool_fetch_backlog(&loops, &json!({"name": "peer"})).unwrap().contains(pending));
+        let map = loops.lock().unwrap();
+        let mut d = map["peer"].debounce.lock().unwrap();
+        assert!(d.due(100).is_empty());
+        assert!(d.progress("peer", 100).is_empty());
+    }
+}
+
 #[cfg(test)]
 mod neutralize_tests {
     use super::neutralize_key_tokens;
```
