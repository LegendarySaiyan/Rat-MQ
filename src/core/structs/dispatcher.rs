/// Message Dispatcher
///
/// Takes the 4 messages types:
///
/// 1) subscribe
/// 2) send
/// 3) ack
/// 4) pong
///
///
/// and pprocess them
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

use quick_xml::events::Event;
use quick_xml::reader::Reader;

use tracing::{debug, info, instrument, trace, warn};

use crate::core::structs::client::{Client, Clients};
use crate::core::structs::in_progress_buffer::InProgressBuffer;
use crate::core::structs::message::Message;
use crate::core::structs::queue::Queue;

///Helper for tacking the first tag of the message
fn first_tag(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                return Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                warn!(error=%e, "xml_first_tag_error");
                break;
            }
            _ => {}
        }
        buf.clear();
    }
    None
}

///Helper for extraction the attribute
fn extract_attr(xml: &str, name: &str) -> Option<String> {
    let mut r = Reader::from_str(xml);
    r.config_mut().trim_text(true);
    let mut buf = Vec::new();

    loop {
        match r.read_event_into(&mut buf).ok()? {
            Event::Start(e) | Event::Empty(e) => {
                for a in e.attributes().flatten() {
                    if a.key.as_ref() == name.as_bytes() {
                        return String::from_utf8(a.value.into_owned()).ok();
                    }
                }
                return None;
            }
            Event::Eof => return None,
            _ => {}
        }
        buf.clear();
    }
}

///Dispatcher for the incoming messages
pub struct Dispatcher {
    listener: TcpListener,
}

impl Dispatcher {
    ///Returns the new TCP Listener with the address as argument
    pub async fn new(addr: &str) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "dispatcher_listening");
        Ok(Self { listener })
    }

    ///Initing the dispatcher listener
    pub async fn init(
        &self,
        queue: Arc<tokio::sync::Mutex<Queue>>,
        in_progress: Arc<InProgressBuffer>,
        clients: Clients,
    ) -> Result<(), std::io::Error> {
        loop {
            let (socket, addr) = self.listener.accept().await?;
            info!(%addr, "accepted_connection");

            if let Err(e) = socket.set_nodelay(true) {
                warn!(%addr, error=%e, "set_nodelay_failed");
            }

            let clients_cl = clients.clone();
            let q_cl = Arc::clone(&queue);
            let inprog_cl = Arc::clone(&in_progress);

            tokio::spawn(async move {
                if let Err(e) =
                    Dispatcher::handle_connection(socket, addr, q_cl, inprog_cl, clients_cl).await
                {
                    info!(%addr, error=%e, "connection_closed");
                }
            });
        }
    }

    #[instrument(name="conn", skip_all, fields(addr=%addr))]
    async fn handle_connection(
        socket: TcpStream,
        addr: SocketAddr,
        queue: Arc<tokio::sync::Mutex<Queue>>,
        in_progress: Arc<InProgressBuffer>,
        clients: Clients,
    ) -> Result<(), std::io::Error> {
        let (mut reader, writer_half) = socket.into_split();
        let mut writer_opt = Some(writer_half);
        let mut registered = false;

        trace!("reader_writer_split");

        loop {
            //len of the message from the buffer
            let mut len_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut len_buf).await {
                if registered {
                    let removed = clients.remove(&addr).is_some();
                    info!(%addr, removed, error=%e, "read_len_eof_or_error");
                } else {
                    info!(%addr, error=%e, "read_len_before_register");
                }
                return Err(e);
            }
            let msg_len = u32::from_be_bytes(len_buf) as usize;
            trace!(msg_len, "frame_len_ok");

            let mut buf = vec![0u8; msg_len];
            if let Err(e) = reader.read_exact(&mut buf).await {
                if registered {
                    let removed = clients.remove(&addr).is_some();
                    info!(%addr, removed, error=%e, "read_payload_eof_or_error");
                } else {
                    info!(%addr, error=%e, "read_payload_before_register");
                }
                return Err(e);
            }

            let Ok(xml_msg) = String::from_utf8(buf) else {
                warn!("payload_not_utf8_skip");
                continue;
            };

            if let Some(tag) = first_tag(&xml_msg) {
                match tag.as_str() {
                    "subscribe" => {
                        if registered {
                            warn!("duplicate_subscribe_ignored");
                            continue;
                        }
                        //register the new client with it prefetch size
                        if let Some(prefetch) = extract_attr(&xml_msg, "prefetch") {
                            match prefetch.parse::<u32>() {
                                Ok(pref) => {
                                    if let Some(w) = writer_opt.take() {
                                        let client = Client::new(pref, w);
                                        clients.insert(addr, client.clone());
                                        registered = true;
                                        info!(prefetch = pref, "client_registered");
                                    } else {
                                        warn!("subscribe_without_writer (already taken)");
                                    }
                                }
                                Err(e) => {
                                    warn!(raw=%prefetch, error=%e, "prefetch_parse_error");
                                }
                            }
                        } else {
                            warn!("subscribe_without_prefetch");
                        }
                    }

                    //receive the message and add it into the queue
                    "send" => {
                        if let Some(refer) = extract_attr(&xml_msg, "refer") {
                            trace!(%refer, "send_frame_received");
                            queue.lock().await.add(Message::new(&xml_msg, &refer));
                        } else {
                            warn!("send_without_refer");
                        }
                    }

                    //receive the message refer and remove it from the in progress buffer
                    "ack" => {
                        if let Some(refer) = extract_attr(&xml_msg, "refer") {
                            let removed = in_progress.remove(&refer).is_some();
                            debug!(%refer, removed, "ack_received");
                            if removed {
                                if let Some(c) = clients.get(&addr).map(|v| v.clone()) {
                                    c.release();
                                } else {
                                    warn!(%addr, "ack_from_unknown_client");
                                }
                            } else {
                                warn!(%refer, "ack_unknown_refer");
                            }
                        } else {
                            warn!("ack_without_refer");
                        }
                    }

                    "pong" => {
                        trace!("pong_received");
                    }

                    other => {
                        warn!(%other, "unknown_tag");
                    }
                }
            } else {
                warn!("xml_no_root_tag");
            }
        }
    }
}
