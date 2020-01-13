use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::BufStream;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Delimits frames using a length header.
#[derive(Debug)]
pub struct Connection {
    stream: Framed<BufStream<TcpStream>, LengthDelimitedCodec>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        // TODO here `BufStream` will allocate two buffers, one for reading and another one for
        // writing; this may be unnecessarily inneficient for users that will only read or write; on
        // the other end, the allocation only occurs once, so it's probably fine to do this
        let stream = BufStream::new(stream);
        let stream = Framed::new(stream, LengthDelimitedCodec::new());
        Self { stream }
    }

    // TODO here we only need a reference to the value
    pub async fn send<V>(&mut self, value: V)
    where
        V: Serialize,
    {
        // serialize and send
        let bytes = Self::serialize(&value);
        self.send_serialized(bytes).await;
    }

    pub async fn send_serialized(&mut self, bytes: Bytes) {
        if let Err(e) = self.stream.send(bytes).await {
            println!("[connection] error while writing to socket: {:?}", e);
        }
    }

    pub async fn recv<V>(&mut self) -> Option<V>
    where
        V: DeserializeOwned,
    {
        match self.stream.next().await {
            Some(Ok(bytes)) => {
                // if it is, and not an error, deserialize it
                let value = Self::deserialize(bytes);
                Some(value)
            }
            Some(Err(e)) => {
                println!("[connection] error while reading from socket: {:?}", e);
                None
            }
            None => None,
        }
    }

    pub fn serialize<V>(value: &V) -> Bytes
    where
        V: Serialize,
    {
        // TODO can we avoid `Bytes`?
        let bytes = bincode::serialize(value).expect("[connection] serialize should work");
        Bytes::from(bytes)
    }

    fn deserialize<V>(bytes: BytesMut) -> V
    where
        V: DeserializeOwned,
    {
        bincode::deserialize(&bytes).expect("[connection] deserialize should work")
    }
}
