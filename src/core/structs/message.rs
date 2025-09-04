///The primitive message

#[derive(Clone)]
pub struct Message {
    id: String,
    text: String,
}
impl Message {
    ///Creating the new message
    pub fn new(text: &str, id: &str) -> Self {
        Self {
            id: id.to_string(),
            text: text.to_string(),
        }
    }
    ///Getting the ID
    pub fn id(&self) -> String {
        self.id.clone()
    }
    ///Text of the message
    pub fn text(&self) -> String {
        self.text.clone()
    }
}
