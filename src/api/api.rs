///HTTP listener
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};

use tokio::signal;
use tokio::sync::Mutex;
use tracing::info;

use crate::api::schemas::EchoRequest;
use crate::core::structs::queue::Queue;

pub fn build_app(queue: Arc<Mutex<Queue>>) -> Router {
    let app = Router::new()
        .route("/echo", post(echo))
        .route("/get_messages_sum", get(messages_number).with_state(queue));
    app
}

async fn echo(Json(payload): Json<EchoRequest>) -> Json<EchoRequest> {
    Json(payload)
}
async fn messages_number(State(queue): State<Arc<Mutex<Queue>>>) -> Json<usize> {
    let len = queue.lock().await.size();
    Json(len)
}

pub async fn shutdown_signal() {
    info!("Waiting for Ctrl+C or termination signal");
    signal::ctrl_c()
        .await
        .expect("Failed to listen for shutdown signal");
    info!("Shutdown signal received");
}
