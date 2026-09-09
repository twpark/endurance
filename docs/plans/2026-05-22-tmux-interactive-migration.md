# Claude `-p` to Interactive tmux Migration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace `claude -p --output-format stream-json` subprocess spawning with persistent interactive Claude CLI sessions managed via tmux, eliminating per-token `-p` billing while preserving all streaming, cancellation, and session features.

**Architecture:** Add a new `tmux` module (`src/services/tmux.rs`) that manages interactive CLI sessions. `claude.rs` gains a parallel code path: when tmux mode is enabled, `execute_command_streaming` delegates to `tmux::send_and_stream()` instead of spawning a `-p` subprocess. The `StreamMessage` enum and `Sender<StreamMessage>` channel are unchanged — tmux output is parsed into the same variants, so telegram.rs and all other consumers require zero changes.

**Tech Stack:** Rust, std::process::Command (for tmux CLI), regex (ANSI stripping), existing StreamMessage/CancelToken types.

---

## Architecture Overview

```
BEFORE:
  telegram.rs → claude::execute_command_streaming()
                  → Command::new("claude") -p --output-format stream-json
                  → stdin.write_all(prompt) + drop(stdin)
                  → BufReader(stdout) line-by-line JSON parse
                  → sender.send(StreamMessage::*)

AFTER:
  telegram.rs → claude::execute_command_streaming()  [unchanged call site]
                  → if tmux_mode:
                      tmux::ensure_session(bot_key, working_dir, session_id, system_prompt)
                      tmux::send_and_stream(bot_key, prompt, sender, cancel_token)
                        → tmux send-keys -t <session> "prompt" Enter
                        → tail -f /tmp/cokacdir-<bot>.log
                        → parse raw text → StreamMessage::Text/Done
                        → sender.send(StreamMessage::*)
                  → else:
                      [existing -p code path, unchanged]
```

**Key design decisions:**
1. **Dual mode** — tmux vs -p selected per-bot via `bot_settings.json` field `"backend": "tmux"` (default remains `"pipe"` for backwards compat)
2. **StreamMessage unchanged** — all downstream consumers (telegram.rs, bridge.rs, ai_screen.rs) work without modification
3. **Session lifecycle** — tmux session created on first message, killed after idle timeout, resumed via `--resume` or `--continue`
4. **`/btw` for interrupts** — messages arriving during active response sent as `/btw <msg>` via send-keys
5. **verify_completion replaced** — same-session `/btw` verification instead of separate `-p --fork-session` call

---

## Task 1: Create `src/services/tmux.rs` — Session Management

**Files:**
- Create: `src/services/tmux.rs`
- Modify: `src/services/mod.rs` (add `pub mod tmux;`)

**Step 1: Create tmux.rs with session lifecycle functions**

```rust
use std::process::Command;
use crate::services::claude::debug_log;

/// Check if a tmux session exists
pub fn session_exists(session_name: &str) -> bool {
    Command::new("tmux")
        .args(["has-session", "-t", session_name])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Create a new tmux session running interactive claude CLI
/// Returns Ok(()) if session created or already exists
pub fn create_session(
    session_name: &str,
    working_dir: &str,
    claude_bin: &str,
    session_id: Option<&str>,
    system_prompt_file: Option<&str>,
) -> Result<(), String> {
    if session_exists(session_name) {
        debug_log(&format!("[tmux] session '{}' already exists", session_name));
        return Ok(());
    }

    let mut cmd_str = format!(
        "cd {:?} && {} --dangerously-skip-permissions",
        working_dir, claude_bin
    );

    if let Some(sid) = session_id {
        cmd_str.push_str(&format!(" --resume {}", sid));
    }
    if let Some(spf) = system_prompt_file {
        cmd_str.push_str(&format!(" --append-system-prompt-file {:?}", spf));
    }

    debug_log(&format!("[tmux] creating session '{}': {}", session_name, cmd_str));

    let output = Command::new("tmux")
        .args(["new-session", "-d", "-s", session_name, "-x", "200", "-y", "50"])
        .arg(cmd_str)
        .output()
        .map_err(|e| format!("Failed to create tmux session: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("tmux new-session failed: {}", stderr));
    }

    // Set up pipe-pane for output capture
    let log_path = log_path_for(session_name);
    // Clear previous log
    let _ = std::fs::write(&log_path, "");

    Command::new("tmux")
        .args(["pipe-pane", "-t", session_name, "-o", &format!("cat >> {}", log_path)])
        .output()
        .map_err(|e| format!("Failed to set pipe-pane: {}", e))?;

    debug_log(&format!("[tmux] session '{}' created, logging to {}", session_name, log_path));
    Ok(())
}

/// Kill a tmux session
pub fn kill_session(session_name: &str) {
    debug_log(&format!("[tmux] killing session '{}'", session_name));
    let _ = Command::new("tmux")
        .args(["kill-session", "-t", session_name])
        .output();
}

/// Send text to a tmux session via send-keys
pub fn send_keys(session_name: &str, text: &str) -> Result<(), String> {
    debug_log(&format!("[tmux] send-keys to '{}': {} chars", session_name, text.len()));
    let output = Command::new("tmux")
        .args(["send-keys", "-t", session_name, text, "Enter"])
        .output()
        .map_err(|e| format!("tmux send-keys failed: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("tmux send-keys error: {}", stderr));
    }
    Ok(())
}

/// Send /btw message (side message without interrupting current task)
pub fn send_btw(session_name: &str, message: &str) -> Result<(), String> {
    let btw_cmd = format!("/btw {}", message);
    send_keys(session_name, &btw_cmd)
}

/// Send Ctrl+C to cancel current operation
pub fn send_cancel(session_name: &str) -> Result<(), String> {
    debug_log(&format!("[tmux] sending C-c to '{}'", session_name));
    Command::new("tmux")
        .args(["send-keys", "-t", session_name, "C-c"])
        .output()
        .map_err(|e| format!("tmux send C-c failed: {}", e))?;
    Ok(())
}

/// Get the log file path for a session
pub fn log_path_for(session_name: &str) -> String {
    format!("/tmp/cokacdir-{}.log", session_name)
}
```

**Step 2: Register module in mod.rs**

In `src/services/mod.rs`, add:
```rust
pub mod tmux;
```

**Step 3: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`
Expected: No errors (module compiles but isn't called yet)

**Step 4: Commit**

```bash
git add src/services/tmux.rs src/services/mod.rs
git commit -m "feat: add tmux session management module"
```

---

## Task 2: ANSI Stripping and Output Parser

**Files:**
- Modify: `src/services/tmux.rs` (add output parsing functions)

**Step 1: Add ANSI stripping and response parser**

Append to `tmux.rs`:

```rust
use std::sync::OnceLock;
use regex::Regex;

/// Compiled regex for ANSI escape codes
fn ansi_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?\x07|\x1b\[.*?[mGKHJ]").unwrap()
    })
}

/// Strip ANSI escape codes from text
pub fn strip_ansi(text: &str) -> String {
    ansi_regex().replace_all(text, "").to_string()
}

/// Detect if a line is a Claude CLI prompt marker (response complete)
/// Claude CLI shows "❯" or ">" when ready for input
pub fn is_prompt_marker(line: &str) -> bool {
    let trimmed = line.trim();
    // Claude CLI prompt patterns
    trimmed == "❯" || trimmed == ">" ||
    trimmed.ends_with("❯") || trimmed.ends_with("> ") ||
    // After tool use, prompt reappears
    trimmed.starts_with("❯ ")
}

/// Detect if a line indicates Claude is thinking
pub fn is_thinking_indicator(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("Thinking") || trimmed.contains("⏳")
}

/// Detect tool use output patterns
pub fn is_tool_marker(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("⚙") || trimmed.starts_with("Tool:") ||
    trimmed.starts_with("Running:") || trimmed.starts_with("Reading:")
}
```

**Step 2: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`
Expected: No errors

**Step 3: Commit**

```bash
git add src/services/tmux.rs
git commit -m "feat: add ANSI stripping and output parsing to tmux module"
```

---

## Task 3: `send_and_stream()` — Core Streaming via tmux

**Files:**
- Modify: `src/services/tmux.rs` (add main streaming function)

**Step 1: Implement the streaming function**

This is the core function that replaces `execute_command_streaming`'s subprocess logic. Append to `tmux.rs`:

```rust
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::io::{BufRead, BufReader};
use crate::services::claude::{StreamMessage, CancelToken};

/// Send a prompt to tmux session and stream the response back via StreamMessage channel.
/// This replaces the `-p --output-format stream-json` subprocess approach.
///
/// Flow:
/// 1. Record current log file size (mark)
/// 2. Send prompt via send-keys
/// 3. Tail log from mark, parse output, send StreamMessage variants
/// 4. Detect prompt marker → send StreamMessage::Done
pub fn send_and_stream(
    session_name: &str,
    prompt: &str,
    sender: Sender<StreamMessage>,
    cancel_token: Option<Arc<CancelToken>>,
    session_id: Option<&str>,
) -> Result<(), String> {
    debug_log(&format!("[tmux] send_and_stream: session={}, prompt_len={}", session_name, prompt.len()));

    let log_path = log_path_for(session_name);

    // Record starting position in log file
    let start_pos = std::fs::metadata(&log_path)
        .map(|m| m.len())
        .unwrap_or(0);

    // Send Init message
    let sid = session_id.unwrap_or("tmux-session").to_string();
    let _ = sender.send(StreamMessage::Init { session_id: sid.clone() });

    // Send prompt to tmux
    send_keys(session_name, prompt)?;

    // Open log file and seek to start position
    let file = std::fs::File::open(&log_path)
        .map_err(|e| format!("Failed to open log: {}", e))?;
    let mut reader = BufReader::new(file);
    // Seek past existing content
    std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(start_pos))
        .map_err(|e| format!("Failed to seek: {}", e))?;

    let mut full_response = String::new();
    let mut consecutive_empty = 0u32;
    let mut prompt_seen = false;
    let mut last_data_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(300); // 5 min max wait

    loop {
        // Check cancellation
        if let Some(ref token) = cancel_token {
            if token.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = send_cancel(session_name);
                debug_log("[tmux] cancelled by user");
                break;
            }
        }

        // Check timeout
        if last_data_time.elapsed() > timeout {
            debug_log("[tmux] timeout waiting for response");
            let _ = sender.send(StreamMessage::Error {
                message: "Timeout waiting for response".to_string(),
                stdout: full_response.clone(),
                stderr: String::new(),
                exit_code: None,
            });
            return Err("Timeout".to_string());
        }

        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                // No new data yet — poll
                consecutive_empty += 1;
                if consecutive_empty > 10 {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                continue;
            }
            Ok(_) => {
                consecutive_empty = 0;
                last_data_time = std::time::Instant::now();

                let clean = strip_ansi(&line);
                let trimmed = clean.trim();

                if trimmed.is_empty() {
                    continue;
                }

                // Skip the echoed prompt line
                if trimmed == prompt.trim() || trimmed.ends_with(prompt.trim()) {
                    continue;
                }

                // Check for prompt marker (response complete)
                if is_prompt_marker(trimmed) {
                    if !full_response.is_empty() {
                        prompt_seen = true;
                        break;
                    }
                    // Prompt before any content = still waiting
                    continue;
                }

                // Accumulate response text
                full_response.push_str(trimmed);
                full_response.push('\n');

                let _ = sender.send(StreamMessage::Text {
                    content: format!("{}\n", trimmed),
                });
            }
            Err(e) => {
                debug_log(&format!("[tmux] read error: {}", e));
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }

    // Send Done
    let _ = sender.send(StreamMessage::Done {
        result: full_response.trim().to_string(),
        session_id: Some(sid),
    });

    if prompt_seen {
        debug_log("[tmux] response complete (prompt marker seen)");
    }

    Ok(())
}
```

**Step 2: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`
Expected: No errors

**Step 3: Commit**

```bash
git add src/services/tmux.rs
git commit -m "feat: implement tmux send_and_stream with log tailing"
```

---

## Task 4: Add `backend` Field to Bot Settings

**Files:**
- Modify: `src/services/telegram.rs` (bot settings deserialization)

**Step 1: Find bot settings struct and add backend field**

Search for the struct that deserializes `bot_settings.json` entries. It should have fields like `display_name`, `token`, `silent`, etc. Add:

```rust
// In the bot settings struct:
pub backend: Option<String>,  // "tmux" or "pipe" (default)
```

**Step 2: Add helper to check if tmux mode**

```rust
fn is_tmux_backend(settings: &BotSettings) -> bool {
    settings.backend.as_deref() == Some("tmux")
}
```

**Step 3: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`

**Step 4: Commit**

```bash
git add src/services/telegram.rs
git commit -m "feat: add 'backend' field to bot settings for tmux mode"
```

---

## Task 5: Wire tmux Mode into `execute_command_streaming`

**Files:**
- Modify: `src/services/claude.rs` (add tmux branch in execute_command_streaming)

**Step 1: Add tmux mode parameter and branch**

Add a new parameter `use_tmux: bool` to `execute_command_streaming` (or read from a new env var / config). At the top of the function, before the existing `-p` spawn logic:

```rust
// At top of execute_command_streaming, after debug logging:
if use_tmux {
    let bot_session = format!("cokacdir-{}", simple_uuid());
    // Reuse session_name from a stable identifier if available
    let claude_bin = get_claude_path()
        .ok_or_else(|| "Claude CLI not found".to_string())?;

    // Write system prompt file (reuse existing logic)
    let sp_path = /* existing system prompt file logic */;

    crate::services::tmux::create_session(
        &bot_session,
        working_dir,
        &claude_bin,
        session_id,
        sp_path.as_deref(),
    )?;

    return crate::services::tmux::send_and_stream(
        &bot_session,
        prompt,
        sender,
        cancel_token,
        session_id,
    );
}

// ... existing -p code follows unchanged ...
```

**Step 2: Update all call sites to pass `use_tmux`**

The function has 5 call sites:
1. `telegram.rs:6997` — pass based on bot settings backend field
2. `telegram.rs:9382` — pass based on bot settings
3. `telegram.rs:10199` — pass based on bot settings
4. `ai_screen.rs:903` — always `false` (TUI uses local pipe)
5. `main.rs:1010` — always `false` (smoke test)

**Step 3: Compile and test**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`

**Step 4: Commit**

```bash
git add src/services/claude.rs src/services/telegram.rs src/ui/ai_screen.rs src/main.rs
git commit -m "feat: wire tmux mode into execute_command_streaming"
```

---

## Task 6: Replace `verify_completion` with `/btw`

**Files:**
- Modify: `src/services/telegram.rs` (verify_completion call site at line ~7721)

**Step 1: Replace verify_completion call with /btw**

At the call site (~line 7721), instead of spawning a separate `-p` process:

```rust
// BEFORE:
// let verify_result = claude::verify_completion(&session_id, working_dir)?;

// AFTER (tmux mode):
if is_tmux_backend {
    let session_name = /* get tmux session name for this bot */;
    let verify_prompt = "Review what you just did. If complete, say mission_complete. Otherwise say mission_pending followed by what's left.";
    tmux::send_btw(&session_name, verify_prompt)?;
    // Parse next output for mission_complete/mission_pending
} else {
    // Keep existing verify_completion for -p mode
    let verify_result = claude::verify_completion(&session_id, working_dir)?;
}
```

**Step 2: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`

**Step 3: Commit**

```bash
git add src/services/telegram.rs
git commit -m "feat: replace verify_completion with /btw in tmux mode"
```

---

## Task 7: Handle `/btw` for Concurrent Messages

**Files:**
- Modify: `src/services/telegram.rs` (message queueing logic)

**Step 1: Find the message queueing logic**

When a message arrives while the bot is already processing, instead of queueing it for later:

```rust
// If bot is currently processing AND tmux mode:
if is_processing && is_tmux_backend {
    let session_name = /* get tmux session name */;
    tmux::send_btw(&session_name, &incoming_message)?;
    // Acknowledge to user that message was sent as side-note
    return; // Don't queue
}
```

**Step 2: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`

**Step 3: Commit**

```bash
git add src/services/telegram.rs
git commit -m "feat: route concurrent messages via /btw in tmux mode"
```

---

## Task 8: Session Lifecycle — Idle Timeout and Resume

**Files:**
- Modify: `src/services/tmux.rs` (add idle management)
- Modify: `src/services/telegram.rs` (track last activity per session)

**Step 1: Add idle timeout tracking to tmux.rs**

```rust
use std::collections::HashMap;
use std::sync::Mutex;

/// Track last activity time per session
static LAST_ACTIVITY: OnceLock<Mutex<HashMap<String, std::time::Instant>>> = OnceLock::new();

fn activity_map() -> &'static Mutex<HashMap<String, std::time::Instant>> {
    LAST_ACTIVITY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record activity for a session
pub fn touch_session(session_name: &str) {
    if let Ok(mut map) = activity_map().lock() {
        map.insert(session_name.to_string(), std::time::Instant::now());
    }
}

/// Check and kill idle sessions (call periodically)
pub fn cleanup_idle_sessions(idle_timeout: std::time::Duration) {
    let mut to_kill = Vec::new();
    if let Ok(mut map) = activity_map().lock() {
        let now = std::time::Instant::now();
        map.retain(|name, last| {
            if now.duration_since(*last) > idle_timeout {
                to_kill.push(name.clone());
                false
            } else {
                true
            }
        });
    }
    for name in to_kill {
        debug_log(&format!("[tmux] killing idle session: {}", name));
        kill_session(&name);
    }
}
```

**Step 2: Add periodic cleanup call in telegram.rs**

In the main polling loop or a background task, call `tmux::cleanup_idle_sessions()` every 60 seconds with a 5-minute timeout.

**Step 3: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`

**Step 4: Commit**

```bash
git add src/services/tmux.rs src/services/telegram.rs
git commit -m "feat: add tmux session idle timeout and auto-cleanup"
```

---

## Task 9: Replace `extract_result_summary` and `extract_context_summary`

**Files:**
- Modify: `src/services/telegram.rs` (line ~9837 for result_summary)
- Modify: `src/main.rs` (line ~418 for context_summary)

**Step 1: Replace with /btw in tmux mode**

Both functions currently fork the session via `-p`. In tmux mode, use `/btw` to ask the same questions within the live session:

For `extract_result_summary`:
```rust
if is_tmux_backend {
    tmux::send_btw(&session_name, "Summarize the results of the task just performed. Provide key information concisely.")?;
    // Collect response from stream
} else {
    claude::extract_result_summary(session_id, working_dir, model)?;
}
```

For `extract_context_summary`:
```rust
if is_tmux_backend {
    tmux::send_btw(&session_name, &format!(
        "Summarize the current session context needed for this scheduled task: {:?}", schedule_prompt
    ))?;
} else {
    claude::extract_context_summary(session_id, schedule_prompt, working_dir)?;
}
```

**Step 2: Compile and verify**

Run: `cd ~/work/cokacdir && cargo check 2>&1 | head -20`

**Step 3: Commit**

```bash
git add src/services/telegram.rs src/main.rs
git commit -m "feat: replace extract_*_summary with /btw in tmux mode"
```

---

## Task 10: CancelToken Integration

**Files:**
- Modify: `src/services/tmux.rs` (already handled in send_and_stream, verify it works with existing CancelToken flow)

**Step 1: Verify cancel flow**

The existing CancelToken flow in telegram.rs stores tokens per chat:
```rust
cancel_tokens: HashMap<ChatId, Arc<CancelToken>>
```

When `/stop` is received, it sets `cancelled = true`. In `send_and_stream`, we already check this and call `send_cancel()` (Ctrl+C). Verify this integrates with the existing `/stop` handler in telegram.rs without changes.

**Step 2: Test manually**

1. Set a bot to `"backend": "tmux"` in bot_settings.json
2. Send a long-running prompt
3. Send `/stop`
4. Verify Ctrl+C is sent and the bot stops gracefully

**Step 3: Commit (if any changes needed)**

---

## Task 11: Build and Integration Test

**Files:**
- All modified files

**Step 1: Full build**

Run: `cd ~/work/cokacdir && cargo build --release 2>&1 | tail -20`
Expected: Successful build

**Step 2: Test with one bot**

In `~/.cokacdir/bot_settings.json`, add `"backend": "tmux"` to one bot (e.g., KIPP for testing):

```json
{
  "0e63c2a291e5201d": {
    "backend": "tmux",
    ...
  }
}
```

**Step 3: Manual test sequence**

1. Send a simple message → verify response appears in Telegram
2. Send a message requiring tool use → verify tool output appears
3. Send a message while bot is responding → verify /btw delivery
4. Send `/stop` → verify cancellation
5. Wait 5+ minutes → verify idle session cleanup
6. Send another message → verify session resume

**Step 4: Commit final changes**

```bash
git add -A
git commit -m "feat: complete tmux interactive mode migration"
```

---

## Migration Rollout

1. **Phase 1:** KIPP only (test bot) — `"backend": "tmux"` in bot_settings.json
2. **Phase 2:** If stable for 3+ days, migrate CASE
3. **Phase 3:** If stable for 1 week, migrate TARS
4. **Rollback:** Remove `"backend": "tmux"` → instant revert to `-p` mode

## Open Questions (to resolve during implementation)

1. **Prompt marker detection** — Claude CLI's exact prompt character may vary. Need to test `❯`, `>`, and session-specific patterns.
2. **Multiline prompts** — Large prompts with newlines need escaping for `tmux send-keys`. May need to write to a temp file and use `send-keys -l` (literal) or paste via `tmux load-buffer`.
3. **Log rotation** — `/tmp/cokacdir-<bot>.log` will grow. Need periodic truncation or rotation.
4. **tmux not installed** — Graceful fallback to `-p` mode if tmux is not available.
5. **Session naming** — Use bot key hash vs display name vs chat_id for tmux session names. Must be unique per bot per chat.
