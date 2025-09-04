/// In progress buffer that receives the messages from the queue, until they processed
use dashmap::DashMap;

use crate::core::structs::message::Message;

///In progress buffer
pub struct InProgressBuffer {
    buffer: DashMap<String, Message>,
}
impl InProgressBuffer {
    ///Create and return the new in progress buffer
    pub fn new(size: usize) -> Self {
        Self {
            buffer: DashMap::with_capacity(size),
        }
    }

    ///insert into the buffer
    #[inline]
    pub fn insert(&self, message: Message) {
        self.buffer.insert(message.id(), message);
    }

    ///remove from the buffer
    #[inline]
    pub fn remove(&self, id: &str) -> Option<Message> {
        self.buffer.remove(id).map(|(_, v)| v)
    }

    ///len of the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    ///capacity of the buffer
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    ///blocking shrink the buffer
    pub fn shrink_if_sparse(&self, min_cap: usize) {
        let mut items = Vec::with_capacity(self.len());
        for kv in self.buffer.iter() {
            items.push((kv.key().clone(), kv.value().clone()));
        }

        let len = items.len();
        let cap = self.capacity();
        if cap <= 2 * len {
            return;
        }
        let target = (cap / 2).max(min_cap).max(len);

        let hasher = self.buffer.hasher().clone();
        let new_map = DashMap::with_capacity_and_hasher(target, hasher);
        for (k, v) in items {
            new_map.insert(k, v);
        }
    }
}
