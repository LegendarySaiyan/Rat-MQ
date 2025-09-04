///Message processor
use std::{net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;

use tracing::info;

use crate::core::structs::{
    client::{Client, Clients},
    in_progress_buffer::InProgressBuffer,
    queue::Queue,
};

pub async fn process_messages(
    queue: Arc<Mutex<Queue>>,
    inprog: Arc<InProgressBuffer>,
    clients: Clients,
) {
    const MAX_BURST_PER_CLIENT: usize = 64;

    loop {
        let snapshot: Vec<(SocketAddr, Arc<Client>)> = clients
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect();

        if snapshot.is_empty() {
            tokio::task::yield_now().await;
            continue;
        }

        let mut to_remove: Vec<SocketAddr> = Vec::new();

        for (addr, client) in snapshot {
            for _ in 0..MAX_BURST_PER_CLIENT {
                if !client.try_acquire() {
                    break;
                }

                let maybe_msg = {
                    let mut q = queue.lock().await;
                    q.pop_front()
                };
                let Some(msg) = maybe_msg else {
                    client.release();
                    break;
                };

                let refer = msg.id().clone();

                inprog.insert(msg.clone());

                let payload = Client::build_payload(&msg.text());
                match client.try_send(payload) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(payload)) => {
                        inprog.remove(&refer);
                        client.release();

                        let mut q = queue.lock().await;
                        q.add(msg);
                        drop(q);

                        tokio::task::yield_now().await;
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_payload)) => {
                        inprog.remove(&refer);
                        client.release();

                        let mut q = queue.lock().await;
                        q.add(msg);
                        drop(q);

                        to_remove.push(addr);
                        break;
                    }
                }
            }
        }

        if !to_remove.is_empty() {
            for a in to_remove {
                clients.remove(&a);
                info!(%a, "client_removed_closed_outbox");
            }
        }

        tokio::task::yield_now().await;
    }
}
