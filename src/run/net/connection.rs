use bytes::Bytes;
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Delimits frames using a length header.
#[derive(Debug)]
pub struct Connection {
    stream: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let stream = Framed::new(stream, LengthDelimitedCodec::new());
        Self { stream }
    }

    pub async fn send<V>(&mut self, value: &V)
    where
        V: Serialize,
    {
        // serialize
        let bytes = bincode::serialize(value).expect("serialize should work");
        // TODO do we need `Bytes`?
        let bytes = Bytes::from(bytes);
        if let Err(e) = self.stream.send(bytes).await {
            println!("error while writing to socket: {:?}", e);
        }
    }

    pub async fn recv<V>(&mut self) -> Option<V>
    where
        V: DeserializeOwned,
    {
        self.stream
            .next()
            .await
            // at this point we have `Option<Result<_, _>>`
            .map(|result| match result {
                Ok(bytes) => {
                    let value = bincode::deserialize(&bytes).expect("deserialize should work");
                    Some(value)
                }
                Err(e) => {
                    println!("error while reading from socket: {:?}", e);
                    None
                }
            })
            // at this point we have `Option<Option<V>>`
            .flatten()
    }
}
