///Consumer Client
use core::net::SocketAddr;

use dashmap::DashMap;

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio::time::{Duration, timeout};
use tokio::{io::AsyncWriteExt, sync::mpsc};

use tracing::{info, warn};

pub type Clients = Arc<DashMap<SocketAddr, Arc<Client>>>;

pub struct Client {
    prefetch: u32,
    in_flight: AtomicU32,
    tx: mpsc::Sender<Vec<u8>>,
}

impl Client {
    ///Create the new client, start the sender task and return the pointer
    pub fn new(prefetch: u32, writer: tokio::net::tcp::OwnedWriteHalf) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
        tokio::spawn(async move {
            info!("writer_started: {:?}", writer);

            let mut w = writer;
            while let Some(body) = rx.recv().await {
                let len = (body.len() as u32).to_be_bytes();
                if timeout(Duration::from_secs(5), w.write_all(&len))
                    .await
                    .is_err()
                {
                    warn!("writer_timeout_len: {:?}", w);
                    break;
                }
                if timeout(Duration::from_secs(5), w.write_all(&body))
                    .await
                    .is_err()
                {
                    warn!("writer_timeout_body: {:?}", w);
                    break;
                }
            }
            warn!("writer_stopped: {:?}", w);
        });

        Arc::new(Self {
            prefetch,
            in_flight: AtomicU32::new(0),
            tx,
        })
    }

    ///If prefetch number allows, client is allowed to acquire new message
    #[inline]
    pub fn try_acquire(&self) -> bool {
        let mut cur = self.in_flight.load(Ordering::Relaxed);
        loop {
            if cur >= self.prefetch {
                return false;
            }
            match self.in_flight.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    ///If the message is acquired, we release the prefetch
    #[inline]
    pub fn release(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }

    ///Build the message from the message string
    #[inline]
    pub fn build_payload(xml: &str) -> Vec<u8> {
        xml.as_bytes().to_vec()
    }

    //Trying to send the message
    #[inline]
    pub fn try_send(&self, payload: Vec<u8>) -> Result<(), mpsc::error::TrySendError<Vec<u8>>> {
        self.tx.try_send(payload)
    }
}
