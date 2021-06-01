use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Hello,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Hello,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
}

impl From<Message> for Request {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Request(rq) => rq,
            Message::Response(_) => panic!("Message is not Request"),
        }
    }
}

impl From<Message> for Response {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Request(_) => panic!("Message is not Response"),
            Message::Response(rs) => rs,
        }
    }
}
