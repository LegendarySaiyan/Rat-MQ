///Message queue, that collects all the messages
use std::cell::RefCell;
use std::collections::VecDeque;

use crate::core::structs::message::Message;

pub struct Queue {
    buffer: VecDeque<Message>,
    size: RefCell<usize>,
}
impl Queue {
    ///Create the new queue
    pub fn new(size: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(size),
            size: RefCell::new(0),
        }
    }
    ///Get the first message in queue
    pub fn pop_front(&mut self) -> Option<Message> {
        let pop = self.buffer.pop_front();

        if pop.is_some() {
            self.decrement();
            return pop;
        }
        None
    }
    ///Push back the message
    pub fn add(&mut self, message: Message) {
        self.buffer.push_back(message);
        self.increment();
    }
    ///Size of the queue
    pub fn size(&self) -> usize {
        *self.size.borrow()
    }
    ///Capacity of the queue
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
    ///Shrink the queue size
    pub fn shrink_if_sparse(&mut self, min_cap: usize) {
        let size = self.size();
        let cap = self.capacity();
        if cap <= 2 * size {
            return;
        }
        let mut target = cap / 2;
        if target < min_cap {
            target = min_cap;
        }
        if target < size {
            target = size;
        }

        let mut new_q = VecDeque::with_capacity(target);
        new_q.append(&mut self.buffer);
        self.buffer = new_q;
    }
    fn decrement(&self) {
        let mut size = self.size.borrow_mut();
        if *size != 0 {
            *size -= 1;
        }
    }
    fn increment(&self) {
        *(self.size.borrow_mut()) += 1;
    }
}
