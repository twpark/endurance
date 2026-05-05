use rusqlite::{Connection, params};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

static GLOBAL_DB: OnceLock<EnduranceDb> = OnceLock::new();

pub fn init_global_db(data_dir: &PathBuf) -> Result<(), rusqlite::Error> {
    let db = EnduranceDb::open(data_dir)?;
    let _ = GLOBAL_DB.set(db);
    Ok(())
}

pub fn get_db() -> Option<&'static EnduranceDb> {
    GLOBAL_DB.get()
}

pub struct EnduranceDb {
    conn: Arc<Mutex<Connection>>,
}

impl EnduranceDb {
    pub fn open(data_dir: &PathBuf) -> Result<Self, rusqlite::Error> {
        let db_path = data_dir.join("endurance.db");
        let conn = Connection::open(&db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        let db = Self { conn: Arc::new(Mutex::new(conn)) };
        db.init_tables()?;
        Ok(db)
    }

    fn init_tables(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                bot_id TEXT,
                role TEXT NOT NULL,
                text TEXT,
                telegram_message_id INTEGER,
                session_id TEXT,
                timestamp TEXT DEFAULT (datetime('now')),
                UNIQUE(chat_id, telegram_message_id)
            );

            CREATE TABLE IF NOT EXISTS reasoning (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL REFERENCES messages(id),
                thinking_text TEXT NOT NULL,
                seq INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS tool_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL REFERENCES messages(id),
                tool_name TEXT NOT NULL,
                tool_input TEXT,
                tool_result TEXT,
                duration_ms INTEGER,
                seq INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS cron_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cron_id TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                finished_at TEXT,
                status TEXT DEFAULT 'running',
                summary TEXT,
                full_log TEXT,
                token_cost REAL,
                session_id TEXT
            );

            CREATE TABLE IF NOT EXISTS cron_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cron_run_id INTEGER REFERENCES cron_runs(id),
                severity TEXT DEFAULT 'info',
                description TEXT NOT NULL,
                resolved INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_reasoning_msg ON reasoning(message_id);
            CREATE INDEX IF NOT EXISTS idx_tool_calls_msg ON tool_calls(message_id);
            CREATE INDEX IF NOT EXISTS idx_cron_runs_cron ON cron_runs(cron_id, started_at);
        ")?;
        Ok(())
    }

    // --- Message storage ---

    pub fn store_message(
        &self, chat_id: &str, bot_id: Option<&str>, role: &str,
        text: &str, telegram_msg_id: Option<i64>, session_id: Option<&str>,
    ) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO messages (chat_id, bot_id, role, text, telegram_message_id, session_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![chat_id, bot_id, role, text, telegram_msg_id, session_id],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn update_message_text(&self, id: i64, text: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("UPDATE messages SET text = ?2 WHERE id = ?1", params![id, text])?;
        Ok(())
    }

    pub fn store_reasoning(&self, message_id: i64, thinking: &str, seq: i32) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO reasoning (message_id, thinking_text, seq) VALUES (?1, ?2, ?3)",
            params![message_id, thinking, seq],
        )?;
        Ok(())
    }

    pub fn store_tool_call(
        &self, message_id: i64, name: &str, input: &str, seq: i32,
    ) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO tool_calls (message_id, tool_name, tool_input, seq) VALUES (?1, ?2, ?3, ?4)",
            params![message_id, name, input, seq],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn update_tool_result(
        &self, tool_call_id: i64, result: &str, duration_ms: Option<i64>,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE tool_calls SET tool_result = ?2, duration_ms = ?3 WHERE id = ?1",
            params![tool_call_id, result, duration_ms],
        )?;
        Ok(())
    }

    // --- Query methods (for HTTP API) ---

    pub fn get_messages(&self, chat_id: &str, since: Option<&str>, limit: i64) -> Result<Vec<Value>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut results = Vec::new();

        if let Some(since_ts) = since {
            let mut stmt = conn.prepare(
                "SELECT id, chat_id, bot_id, role, text, telegram_message_id, session_id, timestamp
                 FROM messages WHERE chat_id = ?1 AND timestamp > ?2
                 ORDER BY timestamp ASC LIMIT ?3"
            )?;
            let rows = stmt.query_map(params![chat_id, since_ts, limit], |row| {
                Ok(json!({
                    "id": row.get::<_, i64>(0)?,
                    "chat_id": row.get::<_, String>(1)?,
                    "bot_id": row.get::<_, Option<String>>(2)?,
                    "role": row.get::<_, String>(3)?,
                    "text": row.get::<_, Option<String>>(4)?,
                    "telegram_message_id": row.get::<_, Option<i64>>(5)?,
                    "session_id": row.get::<_, Option<String>>(6)?,
                    "timestamp": row.get::<_, String>(7)?,
                    "has_reasoning": false,
                    "has_tools": false,
                }))
            })?;
            for row in rows {
                results.push(row?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT id, chat_id, bot_id, role, text, telegram_message_id, session_id, timestamp
                 FROM messages WHERE chat_id = ?1
                 ORDER BY timestamp DESC LIMIT ?2"
            )?;
            let rows = stmt.query_map(params![chat_id, limit], |row| {
                Ok(json!({
                    "id": row.get::<_, i64>(0)?,
                    "chat_id": row.get::<_, String>(1)?,
                    "bot_id": row.get::<_, Option<String>>(2)?,
                    "role": row.get::<_, String>(3)?,
                    "text": row.get::<_, Option<String>>(4)?,
                    "telegram_message_id": row.get::<_, Option<i64>>(5)?,
                    "session_id": row.get::<_, Option<String>>(6)?,
                    "timestamp": row.get::<_, String>(7)?,
                    "has_reasoning": false,
                    "has_tools": false,
                }))
            })?;
            for row in rows {
                results.push(row?);
            }
            results.reverse();
        }

        // Annotate with has_reasoning / has_tools flags
        for msg in results.iter_mut() {
            if let Some(id) = msg.get("id").and_then(|v| v.as_i64()) {
                let has_r: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM reasoning WHERE message_id = ?1)", params![id], |r| r.get(0)
                ).unwrap_or(false);
                let has_t: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM tool_calls WHERE message_id = ?1)", params![id], |r| r.get(0)
                ).unwrap_or(false);
                msg["has_reasoning"] = json!(has_r);
                msg["has_tools"] = json!(has_t);
            }
        }

        Ok(results)
    }

    pub fn get_message_detail(&self, id: i64) -> Result<Value, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();

        // Get message
        let msg = conn.query_row(
            "SELECT id, chat_id, bot_id, role, text, telegram_message_id, session_id, timestamp
             FROM messages WHERE id = ?1",
            params![id],
            |row| Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "chat_id": row.get::<_, String>(1)?,
                "bot_id": row.get::<_, Option<String>>(2)?,
                "role": row.get::<_, String>(3)?,
                "text": row.get::<_, Option<String>>(4)?,
                "telegram_message_id": row.get::<_, Option<i64>>(5)?,
                "session_id": row.get::<_, Option<String>>(6)?,
                "timestamp": row.get::<_, String>(7)?,
            }))
        )?;

        // Get reasoning
        let mut stmt = conn.prepare(
            "SELECT thinking_text, seq FROM reasoning WHERE message_id = ?1 ORDER BY seq"
        )?;
        let reasoning: Vec<Value> = stmt.query_map(params![id], |row| {
            Ok(json!({ "text": row.get::<_, String>(0)?, "seq": row.get::<_, i32>(1)? }))
        })?.filter_map(|r| r.ok()).collect();

        // Get tool calls
        let mut stmt = conn.prepare(
            "SELECT id, tool_name, tool_input, tool_result, duration_ms, seq
             FROM tool_calls WHERE message_id = ?1 ORDER BY seq"
        )?;
        let tools: Vec<Value> = stmt.query_map(params![id], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "name": row.get::<_, String>(1)?,
                "input": row.get::<_, Option<String>>(2)?,
                "result": row.get::<_, Option<String>>(3)?,
                "duration_ms": row.get::<_, Option<i64>>(4)?,
                "seq": row.get::<_, i32>(5)?,
            }))
        })?.filter_map(|r| r.ok()).collect();

        Ok(json!({
            "message": msg,
            "reasoning": reasoning,
            "tool_calls": tools,
        }))
    }

    // --- Cron storage ---

    pub fn start_cron_run(&self, cron_id: &str, bot_id: &str, session_id: Option<&str>) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO cron_runs (cron_id, bot_id, session_id) VALUES (?1, ?2, ?3)",
            params![cron_id, bot_id, session_id],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_cron_run(&self, run_id: i64, status: &str, summary: &str, full_log: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE cron_runs SET finished_at = datetime('now'), status = ?2, summary = ?3, full_log = ?4 WHERE id = ?1",
            params![run_id, status, summary, full_log],
        )?;
        Ok(())
    }

    pub fn add_cron_issue(&self, run_id: i64, severity: &str, description: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO cron_issues (cron_run_id, severity, description) VALUES (?1, ?2, ?3)",
            params![run_id, severity, description],
        )?;
        Ok(())
    }

    pub fn get_cron_history(&self, cron_id: &str, limit: i64) -> Result<Vec<Value>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, cron_id, bot_id, started_at, finished_at, status, summary, token_cost
             FROM cron_runs WHERE cron_id = ?1 ORDER BY started_at DESC LIMIT ?2"
        )?;
        let rows = stmt.query_map(params![cron_id, limit], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "cron_id": row.get::<_, String>(1)?,
                "bot_id": row.get::<_, String>(2)?,
                "started_at": row.get::<_, String>(3)?,
                "finished_at": row.get::<_, Option<String>>(4)?,
                "status": row.get::<_, String>(5)?,
                "summary": row.get::<_, Option<String>>(6)?,
                "token_cost": row.get::<_, Option<f64>>(7)?,
            }))
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub fn get_unresolved_issues(&self) -> Result<Vec<Value>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT ci.id, ci.cron_run_id, ci.severity, ci.description, ci.created_at, cr.cron_id
             FROM cron_issues ci LEFT JOIN cron_runs cr ON ci.cron_run_id = cr.id
             WHERE ci.resolved = 0 ORDER BY ci.created_at DESC"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "cron_run_id": row.get::<_, Option<i64>>(1)?,
                "severity": row.get::<_, String>(2)?,
                "description": row.get::<_, String>(3)?,
                "created_at": row.get::<_, String>(4)?,
                "cron_id": row.get::<_, Option<String>>(5)?,
            }))
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub fn resolve_issue(&self, id: i64) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("UPDATE cron_issues SET resolved = 1 WHERE id = ?1", params![id])?;
        Ok(())
    }
}
