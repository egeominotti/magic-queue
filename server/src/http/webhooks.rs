//! Webhook HTTP handlers.

use axum::{
    extract::{Path, State},
    response::Json,
};
use serde_json::Value;

use crate::protocol::{Job, WebhookConfig};

use super::types::{ApiResponse, AppState, CreateWebhookRequest};

/// List all webhooks.
pub async fn list_webhooks(State(qm): State<AppState>) -> Json<ApiResponse<Vec<WebhookConfig>>> {
    let webhooks = qm.list_webhooks().await;
    ApiResponse::success(webhooks)
}

/// Create a webhook.
pub async fn create_webhook(
    State(qm): State<AppState>,
    Json(req): Json<CreateWebhookRequest>,
) -> Json<ApiResponse<String>> {
    match qm
        .add_webhook(req.url, req.events, req.queue, req.secret)
        .await
    {
        Ok(id) => ApiResponse::success(id),
        Err(e) => ApiResponse::error(e),
    }
}

/// Delete a webhook.
pub async fn delete_webhook(
    State(qm): State<AppState>,
    Path(id): Path<String>,
) -> Json<ApiResponse<bool>> {
    let deleted = qm.delete_webhook(&id).await;
    ApiResponse::success(deleted)
}

/// Incoming webhook - push job to queue.
pub async fn incoming_webhook(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Json(data): Json<Value>,
) -> Json<ApiResponse<Job>> {
    match qm
        .push(
            queue, data, 0, None, None, None, None, None, None, None, None, false, false, false,
            None, None, None, None, None, None,
        )
        .await
    {
        Ok(job) => ApiResponse::success(job),
        Err(e) => ApiResponse::error(e),
    }
}
