use axum::{
    Router, Json,
    extract::{Query, Path, State},
    response::sse::{Event as SseEvent, Sse, KeepAlive},
    routing::{get, post},
};
use tower_http::cors::{CorsLayer, Any};
use std::sync::Arc;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use super::storage::{self, EnduranceDb};

pub struct ApiState {
    pub db: &'static EnduranceDb,
}

pub fn create_router(db: &'static EnduranceDb) -> Router {
    let state = Arc::new(ApiState { db });
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/api/health", get(health))
        .route("/api/messages", get(get_messages))
        .route("/api/message/:id", get(get_message_detail))
        .route("/api/crons", get(get_crons))
        .route("/api/crons/:id/history", get(get_cron_history))
        .route("/api/crons/issues", get(get_cron_issues))
        .route("/api/crons/issues/:id/resolve", post(resolve_issue))
        .route("/api/send", post(send_message))
        .route("/api/usage", get(get_usage))
        .route("/api/stream", get(stream_events))
        .layer(cors)
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({
        "ok": true,
        "version": "endurance-0.5.0",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

#[derive(Deserialize)]
struct MessagesQuery {
    chat_id: String,
    since: Option<String>,
    limit: Option<i64>,
}

async fn get_messages(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<MessagesQuery>,
) -> Json<Value> {
    let limit = q.limit.unwrap_or(50).min(200);
    match state.db.get_messages(&q.chat_id, q.since.as_deref(), limit) {
        Ok(msgs) => Json(json!({ "ok": true, "messages": msgs })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn get_message_detail(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<i64>,
) -> Json<Value> {
    match state.db.get_message_detail(id) {
        Ok(detail) => Json(json!({ "ok": true, "detail": detail })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn get_crons() -> Json<Value> {
    // Read from ~/.endurance/schedule/*.json
    let schedule_dir = dirs::home_dir()
        .map(|h| h.join(".cokacdir").join("schedule"))
        .unwrap_or_default();

    let mut crons = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&schedule_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().map_or(false, |e| e == "json") {
                if let Ok(content) = std::fs::read_to_string(entry.path()) {
                    if let Ok(val) = serde_json::from_str::<Value>(&content) {
                        crons.push(val);
                    }
                }
            }
        }
    }
    Json(json!({ "ok": true, "crons": crons }))
}

async fn get_cron_history(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Json<Value> {
    match state.db.get_cron_history(&id, 20) {
        Ok(history) => Json(json!({ "ok": true, "history": history })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn get_cron_issues(State(state): State<Arc<ApiState>>) -> Json<Value> {
    match state.db.get_unresolved_issues() {
        Ok(issues) => Json(json!({ "ok": true, "issues": issues })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn resolve_issue(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<i64>,
) -> Json<Value> {
    match state.db.resolve_issue(id) {
        Ok(_) => Json(json!({ "ok": true })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

#[derive(Deserialize)]
struct SendRequest {
    chat_id: String,
    text: String,
}

async fn send_message(Json(req): Json<SendRequest>) -> Json<Value> {
    // TODO: Wire to grammers MTProto (Task 5)
    Json(json!({ "ok": false, "error": "User API not yet implemented — use tg-proxy" }))
}

async fn get_usage() -> Json<Value> {
    // Proxy to Anthropic OAuth usage API
    // Read token from macOS keychain via security command
    let output = tokio::process::Command::new("security")
        .args(["find-generic-password", "-s", "Claude Code-credentials", "-w"])
        .output()
        .await;

    let token = match output {
        Ok(out) => {
            let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
            serde_json::from_str::<Value>(&raw)
                .ok()
                .and_then(|v| v.get("claudeAiOauth")?.get("accessToken")?.as_str().map(String::from))
        }
        Err(_) => None,
    };

    let Some(token) = token else {
        return Json(json!({ "ok": false, "error": "OAuth token not found" }));
    };

    let client = reqwest::Client::new();
    match client.get("https://api.anthropic.com/api/oauth/usage")
        .header("Authorization", format!("Bearer {}", token))
        .header("anthropic-beta", "oauth-2025-04-20")
        .send()
        .await
    {
        Ok(resp) => {
            if let Ok(body) = resp.json::<Value>().await {
                Json(json!({ "ok": true, "usage": body }))
            } else {
                Json(json!({ "ok": false, "error": "Failed to parse usage response" }))
            }
        }
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

#[derive(Deserialize)]
struct StreamQuery {
    chat_id: Option<String>,
}

async fn stream_events(
    Query(q): Query<StreamQuery>,
) -> Sse<impl tokio_stream::Stream<Item = Result<SseEvent, std::convert::Infallible>>> {
    let rx = storage::get_broadcast()
        .expect("broadcast not initialized")
        .subscribe();

    let chat_filter = q.chat_id.unwrap_or_default();

    let stream = BroadcastStream::new(rx)
        .filter_map(move |msg| {
            match msg {
                Ok(evt) if chat_filter.is_empty() || evt.chat_id == chat_filter => {
                    let event_type = evt.event_type.clone();
                    let data = serde_json::to_string(&evt).unwrap_or_default();
                    Some(Ok(SseEvent::default().event(event_type).data(data)))
                }
                _ => None,
            }
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

pub async fn start_api(port: u16) {
    let db = storage::get_db().expect("Endurance DB not initialized before start_api");
    let router = create_router(db);
    let addr = format!("0.0.0.0:{}", port);
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            eprintln!("[endurance] API server on http://{}", addr);
            if let Err(e) = axum::serve(listener, router).await {
                eprintln!("[endurance] API server error: {}", e);
            }
        }
        Err(e) => {
            eprintln!("[endurance] Failed to bind API port {}: {}", port, e);
        }
    }
}
