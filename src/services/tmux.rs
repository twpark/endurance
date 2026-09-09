use std::process::Command;
use std::sync::OnceLock;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::Mutex;
use crate::services::claude::{debug_log, StreamMessage, CancelToken};

/// Build a stable tmux session name from bot key + chat ID.
/// Format: "ck-{first8 of bot_key}-{chat_id_abs}"
/// Example: "ck-0e63c2a2-5251432108"
pub fn session_name(bot_key: &str, chat_id: i64) -> String {
    let key_prefix: String = bot_key.chars().take(8).collect();
    format!("ck-{}-{}", key_prefix, chat_id.unsigned_abs())
}

/// Check if a tmux session exists
pub fn session_exists(name: &str) -> bool {
    Command::new("tmux")
        .args(["has-session", "-t", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Create a new tmux session running interactive Claude CLI.
/// If session already exists, returns Ok immediately.
pub fn create_session(
    name: &str,
    working_dir: &str,
    claude_bin: &str,
    session_id: Option<&str>,
    system_prompt_file: Option<&str>,
) -> Result<(), String> {
    if session_exists(name) {
        debug_log(&format!("[tmux] session '{}' already exists", name));
        return Ok(());
    }

    let mut claude_args = vec![
        claude_bin.to_string(),
        "--dangerously-skip-permissions".to_string(),
    ];

    if let Some(sid) = session_id {
        claude_args.push("--resume".to_string());
        claude_args.push(sid.to_string());
    }
    if let Some(spf) = system_prompt_file {
        claude_args.push("--append-system-prompt-file".to_string());
        claude_args.push(spf.to_string());
    }

    let shell_cmd = claude_args.join(" ");
    debug_log(&format!("[tmux] creating session '{}' in {}: {}", name, working_dir, shell_cmd));

    let output = Command::new("tmux")
        .args([
            "new-session", "-d",
            "-s", name,
            "-x", "200", "-y", "50",
            "-c", working_dir,
        ])
        .arg(&shell_cmd)
        .output()
        .map_err(|e| format!("tmux new-session failed: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("tmux new-session error: {}", stderr));
    }

    // Set scrollback buffer large enough for long responses
    let _ = Command::new("tmux")
        .args(["set-option", "-t", name, "history-limit", "50000"])
        .output();

    debug_log(&format!("[tmux] session '{}' created", name));
    Ok(())
}

/// Kill a tmux session
pub fn kill_session(name: &str) {
    debug_log(&format!("[tmux] killing session '{}'", name));
    let _ = Command::new("tmux")
        .args(["kill-session", "-t", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

/// Send text to a tmux session via send-keys.
/// Text and Enter are sent as two separate calls with a small delay between,
/// so the input has time to land in Claude CLI's input box before submission.
/// For multiline or special-char prompts, uses `load-buffer` + `paste-buffer`.
pub fn send_keys(name: &str, text: &str) -> Result<(), String> {
    debug_log(&format!("[tmux] send-keys to '{}': {} chars", name, text.len()));

    // For short single-line text, use send-keys directly
    if text.len() < 500 && !text.contains('\n') {
        let output = Command::new("tmux")
            .args(["send-keys", "-t", name, "--", text])
            .output()
            .map_err(|e| format!("tmux send-keys failed: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("tmux send-keys error: {}", stderr));
        }
        // Let the input settle before submitting
        std::thread::sleep(std::time::Duration::from_millis(80));
        let _ = Command::new("tmux")
            .args(["send-keys", "-t", name, "Enter"])
            .output()
            .map_err(|e| format!("tmux send-keys (Enter) failed: {}", e))?;
        return Ok(());
    }

    // For long/multiline prompts: write to temp file, load-buffer, paste, Enter
    let tmp = format!("/tmp/ck-input-{}", std::process::id());
    std::fs::write(&tmp, text).map_err(|e| format!("write temp failed: {}", e))?;

    let _ = Command::new("tmux")
        .args(["load-buffer", "-b", "ck-paste", &tmp])
        .output();
    let _ = Command::new("tmux")
        .args(["paste-buffer", "-b", "ck-paste", "-t", name])
        .output();
    std::thread::sleep(std::time::Duration::from_millis(80));
    let _ = Command::new("tmux")
        .args(["send-keys", "-t", name, "Enter"])
        .output();
    let _ = std::fs::remove_file(&tmp);

    Ok(())
}

/// Clear the current input line in Claude CLI (Ctrl+U = kill-line).
/// Useful before sending a new prompt if there is residual text in the input box.
pub fn clear_input(name: &str) {
    let _ = Command::new("tmux")
        .args(["send-keys", "-t", name, "C-u"])
        .output();
}

/// Poll the pane until Claude CLI's status bar ("⏵⏵ bypass permissions on …")
/// is stably visible, meaning the UI is ready to accept input. Returns Ok
/// when ready, or after `timeout` (caller proceeds anyway — best effort).
pub fn wait_for_ready(name: &str, timeout: std::time::Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    let mut stable = 0u32;
    while start.elapsed() < timeout {
        let lines = match capture_pane(name) {
            Ok(l) => l,
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(200));
                continue;
            }
        };
        // The "⏵⏵ bypass permissions on" status bar always sits below the
        // input box once the CLI has finished drawing its welcome screen.
        let has_marker = lines.iter().rev().take(6)
            .any(|l| l.contains("bypass permissions"));
        if has_marker {
            stable += 1;
            if stable >= 3 {
                debug_log(&format!("[tmux] session '{}' ready", name));
                return Ok(());
            }
        } else {
            stable = 0;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    debug_log(&format!("[tmux] session '{}' ready-wait timeout, proceeding", name));
    Ok(())
}

/// Send /btw message (side channel — doesn't interrupt current task)
pub fn send_btw(name: &str, message: &str) -> Result<(), String> {
    let btw = format!("/btw {}", message);
    send_keys(name, &btw)
}

/// Send Ctrl+C to cancel current operation
pub fn send_cancel(name: &str) -> Result<(), String> {
    debug_log(&format!("[tmux] sending C-c to '{}'", name));
    let _ = Command::new("tmux")
        .args(["send-keys", "-t", name, "C-c"])
        .output()
        .map_err(|e| format!("tmux C-c failed: {}", e))?;
    Ok(())
}

/// Check if tmux is available on this system
pub fn is_available() -> bool {
    Command::new("tmux")
        .arg("-V")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// capture-pane based output reading
// ---------------------------------------------------------------------------

/// Public: capture pane as a single string (for verify_completion etc.)
pub fn capture_pane_text(name: &str) -> Result<String, String> {
    let lines = capture_pane(name)?;
    Ok(lines.join("\n"))
}

/// Capture the full scrollback + visible content of a tmux pane as clean text.
fn capture_pane(name: &str) -> Result<Vec<String>, String> {
    let output = Command::new("tmux")
        .args(["capture-pane", "-p", "-t", name, "-S", "-"])
        .output()
        .map_err(|e| format!("capture-pane failed: {}", e))?;
    if !output.status.success() {
        return Err(format!("capture-pane error: {}", String::from_utf8_lossy(&output.stderr)));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(text.lines().map(|l| l.to_string()).collect())
}

/// Check if the last non-empty line of the pane is a prompt marker (❯),
/// meaning Claude CLI finished and is waiting for input.
fn pane_has_prompt(lines: &[String]) -> bool {
    for line in lines.iter().rev() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        // Claude CLI prompt: "❯" optionally preceded by path/status info
        // Also check for bypass permissions indicator which sits below prompt
        if t.contains("bypass permissions") || t.contains("shift+tab") {
            continue;
        }
        // Horizontal rule (─────) appears around prompt area
        if t.chars().all(|c| c == '─' || c.is_whitespace()) && t.contains('─') {
            continue;
        }
        // The actual prompt marker
        return t == "❯" || t.ends_with(" ❯") || t.ends_with("❯ ");
    }
    false
}

/// Extract the Claude response from captured pane lines.
/// Looks for content between the prompt line (❯ <user_prompt>) and the next
/// prompt marker (❯), filtering out tool-use chrome.
fn extract_response(lines: &[String], baseline_len: usize) -> Vec<String> {
    let new_lines = if baseline_len < lines.len() {
        &lines[baseline_len..]
    } else {
        &[]
    };

    let mut result = Vec::new();
    let mut in_response = false;

    for line in new_lines {
        let t = line.trim();

        // Skip empty lines at the start
        if !in_response && t.is_empty() {
            continue;
        }

        // Skip the echoed prompt line (❯ <text>)
        if !in_response && (t.starts_with("❯ ") || t == "❯") {
            continue;
        }

        // Skip horizontal rules around prompt
        if t.chars().all(|c| c == '─' || c.is_whitespace()) && t.contains('─') {
            continue;
        }

        // Skip bypass permissions / status bar line(s)
        if t.contains("bypass permissions") || t.starts_with("⏵⏵") {
            continue;
        }

        // Skip effort indicator line
        if t.contains("· /effort") {
            continue;
        }

        // Skip Claude Code welcome banner (only appears on first session boot).
        // The whole box uses ╭ ─ ╮ │ ╰ ╯ glyphs.
        if t.starts_with("╭") || t.starts_with("╰") || t.starts_with("│") {
            continue;
        }
        if t.starts_with("Tip:") {
            continue;
        }

        // Skip tmux's own advisory messages
        if t.starts_with("tmux ") || t.starts_with("set -g ") {
            continue;
        }

        // Skip SessionStart hook noise
        if t.contains("SessionStart:") && t.contains("hook") {
            continue;
        }
        if t.contains("Failed with non-blocking") {
            continue;
        }

        // Final prompt marker — stop
        if (t == "❯" || t.ends_with(" ❯")) && in_response {
            break;
        }

        in_response = true;
        result.push(line.clone());
    }

    // Trim trailing empty lines and spinner residue (✻ / ✶ status indicators).
    while result.last().map_or(false, |l| {
        let t = l.trim();
        t.is_empty() || t.starts_with("✻ ") || t.starts_with("✶ ")
    }) {
        result.pop();
    }

    result
}

// ---------------------------------------------------------------------------
// Core streaming: send prompt → poll capture-pane → emit StreamMessage
// ---------------------------------------------------------------------------

pub fn send_and_stream(
    name: &str,
    prompt: &str,
    sender: Sender<StreamMessage>,
    cancel_token: Option<Arc<CancelToken>>,
    session_id: Option<&str>,
) -> Result<(), String> {
    debug_log(&format!(
        "[tmux] send_and_stream: session={}, prompt_len={}", name, prompt.len()
    ));

    // Wait for Claude CLI to finish drawing its welcome screen and any
    // `--resume` history replay BEFORE we capture the baseline. Otherwise the
    // baseline misses lines that get drawn afterwards, and they get streamed
    // back to the user as if they were the new response.
    let _ = wait_for_ready(name, std::time::Duration::from_secs(15));

    // Clear any text that may have leaked into the input box from previous
    // send-keys attempts (e.g. a botched first message before the CLI was up).
    clear_input(name);
    std::thread::sleep(std::time::Duration::from_millis(120));

    // Capture baseline pane content AFTER the UI is ready
    let baseline = capture_pane(name).unwrap_or_default();
    let baseline_len = baseline.len();
    debug_log(&format!("[tmux] baseline: {} lines", baseline_len));

    // Emit Init
    let sid = session_id.unwrap_or("tmux-session").to_string();
    let _ = sender.send(StreamMessage::Init { session_id: sid.clone() });

    // Send prompt (text + Enter are now split with a small delay inside send_keys)
    send_keys(name, prompt)?;

    // Wait briefly for CLI to start processing
    std::thread::sleep(std::time::Duration::from_millis(500));

    let mut full_response = String::new();
    let mut last_emitted_lines: usize = 0;
    let mut last_change = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(600); // 10 min

    loop {
        // Cancellation check
        if let Some(ref tok) = cancel_token {
            if tok.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = send_cancel(name);
                debug_log("[tmux] cancelled");
                break;
            }
        }

        if last_change.elapsed() > timeout {
            debug_log("[tmux] timeout");
            let _ = sender.send(StreamMessage::Error {
                message: "tmux response timeout".into(),
                stdout: full_response.clone(),
                stderr: String::new(),
                exit_code: None,
            });
            return Err("timeout".into());
        }

        // Capture current pane content
        let current = match capture_pane(name) {
            Ok(lines) => lines,
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(200));
                continue;
            }
        };

        // Extract response lines from new content
        let response_lines = extract_response(&current, baseline_len);

        // Emit any new lines we haven't sent yet
        if response_lines.len() > last_emitted_lines {
            last_change = std::time::Instant::now();
            for line in &response_lines[last_emitted_lines..] {
                let _ = sender.send(StreamMessage::Text {
                    content: format!("{}\n", line),
                });
            }
            last_emitted_lines = response_lines.len();

            // Rebuild full response
            full_response = response_lines.join("\n");
        }

        // Check if Claude CLI is done (prompt marker appeared)
        if pane_has_prompt(&current) && last_emitted_lines > 0 {
            debug_log("[tmux] response complete (prompt marker)");
            break;
        }

        // Poll interval — balance between responsiveness and CPU
        std::thread::sleep(std::time::Duration::from_millis(300));
    }

    let _ = sender.send(StreamMessage::Done {
        result: full_response.trim().to_string(),
        session_id: Some(sid),
    });

    touch_session(name);
    Ok(())
}

// ---------------------------------------------------------------------------
// Session idle tracking & cleanup
// ---------------------------------------------------------------------------

static LAST_ACTIVITY: OnceLock<Mutex<HashMap<String, std::time::Instant>>> = OnceLock::new();

fn activity_map() -> &'static Mutex<HashMap<String, std::time::Instant>> {
    LAST_ACTIVITY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn touch_session(name: &str) {
    if let Ok(mut m) = activity_map().lock() {
        m.insert(name.to_string(), std::time::Instant::now());
    }
}

/// Kill sessions that have been idle longer than `timeout`.
/// Call this periodically (e.g. every 60 s) from the main polling loop.
pub fn cleanup_idle(timeout: std::time::Duration) {
    let mut to_kill = Vec::new();
    if let Ok(mut m) = activity_map().lock() {
        let now = std::time::Instant::now();
        m.retain(|name, last| {
            if now.duration_since(*last) > timeout {
                to_kill.push(name.clone());
                false
            } else {
                true
            }
        });
    }
    for name in to_kill {
        debug_log(&format!("[tmux] idle cleanup: {}", name));
        kill_session(&name);
    }
}
