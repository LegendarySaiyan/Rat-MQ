///The primitive message
use std::sync::Arc;

#[derive(Clone)]
pub struct Message {
    id: Arc<str>,
    text: Arc<[u8]>,
}
impl Message {
    ///Creating the new message
    pub fn new(text: Vec<u8>, id: String) -> Self {
        Self {
            id: Arc::from(id),
            text: Arc::<[u8]>::from(text),
        }
    }
    ///Getting the ID
    pub fn id(&self) -> Arc<str> {
        Arc::clone(&self.id)
    }
    ///Text of the message
    pub fn text(&self) -> Arc<[u8]> {
        Arc::clone(&self.text)
    }
}
