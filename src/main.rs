use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

pub mod api;
pub mod core;

use crate::api::api::build_app;
use crate::core::structs::client::Clients;
use crate::core::structs::dispatcher::Dispatcher;
use crate::core::structs::in_progress_buffer::InProgressBuffer;
use crate::core::structs::queue::Queue;
use crate::core::structs::sender::process_messages;

const BUFFER_SIZE: usize = 10_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let queue = Arc::new(Mutex::new(Queue::new(BUFFER_SIZE)));
    let buffer = Arc::new(InProgressBuffer::new(BUFFER_SIZE));

    //starting the http listener
    let app = build_app(Arc::clone(&queue));
    let http_listener = TcpListener::bind("127.0.0.1:3000").await?;
    let clients: Clients = Arc::new(DashMap::new());

    tokio::spawn(async move {
        axum::serve(http_listener, app).await.ok();
    });

    //starting dipsatcher
    let dispatcher = Dispatcher::new("127.0.0.1:5672").await?;
    let queue_clone = Arc::clone(&queue);
    let buffer_clone = Arc::clone(&buffer);
    let clients_clone = Arc::clone(&clients);
    tokio::spawn(async move {
        dispatcher
            .init(queue_clone, buffer_clone, clients_clone)
            .await
    });

    //starting buffer
    let queue_clone = Arc::clone(&queue);
    let buffer_clone = Arc::clone(&buffer);
    let clients_clone = Arc::clone(&clients);
    tokio::spawn(async move { process_messages(queue_clone, buffer_clone, clients_clone).await });

    //checking the queue size, and shrink it, if need
    let queue_clone = Arc::clone(&queue);
    let buffer_clone = Arc::clone(&buffer);
    tokio::spawn(async move {
        let period = Duration::from_secs(120);
        loop {
            tokio::time::sleep(period).await;

            let t0 = std::time::Instant::now();

            // shrink the queue
            let (q_len_before, q_cap_before, q_len_after, q_cap_after) = {
                let mut q = queue_clone.lock().await;
                let len_b = q.size();
                let cap_b = q.capacity();
                q.shrink_if_sparse(BUFFER_SIZE);
                let len_a = q.size();
                let cap_a = q.capacity();
                (len_b, cap_b, len_a, cap_a)
            };

            // shrink in-progress buffer
            let ip_len = buffer_clone.len();
            let ip_cap = buffer_clone.capacity();

            let dt = t0.elapsed();
            info!(
                queue_len_before=%q_len_before, queue_cap_before=%q_cap_before,
                queue_len_after=%q_len_after,  queue_cap_after=%q_cap_after,
                inprog_len=%ip_len, inprog_cap=%ip_cap,
                elapsed=?dt,
                "maintenance_tick",
            );
        }
    });
    match signal::ctrl_c().await {
        Ok(()) => Ok(()),
        Err(err) => Ok({
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .compact()
        .init();
}
