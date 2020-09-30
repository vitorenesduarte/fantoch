// This module contains the definition of `Connection`.
mod connection;

// Re-exports.
pub use connection::Connection;

use crate::warn;
use bytes::{Bytes, BytesMut};
use futures::sink::{Sink, SinkExt};
use futures::stream::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite, BufStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Delimits frames using a length header.
/// TODO take a look at async_bincode: https://docs.rs/async-bincode/0.5.1/async_bincode/index.html
#[derive(Debug)]
pub struct Rw<S> {
    rw: Framed<BufStream<S>, LengthDelimitedCodec>,
}

impl<S> Rw<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    pub fn from(reader_capacity: usize, writer_capacity: usize, rw: S) -> Self {
        // buffer rw
        let rw = BufStream::with_capacity(reader_capacity, writer_capacity, rw);
        // frame rw
        let rw = Framed::new(rw, LengthDelimitedCodec::new());
        Self { rw }
    }

    pub async fn recv<V>(&mut self) -> Option<V>
    where
        V: DeserializeOwned,
    {
        match self.rw.next().await {
            Some(Ok(bytes)) => {
                // if it is, and not an error, deserialize it
                let value = deserialize(bytes);
                Some(value)
            }
            Some(Err(e)) => {
                warn!("[rw] error while reading from stream: {:?}", e);
                None
            }
            None => None,
        }
    }

    pub async fn send<V>(&mut self, value: &V)
    where
        V: Serialize,
    {
        let bytes = serialize(value);
        if let Err(e) = self.rw.send(bytes).await {
            warn!("[rw] error while writing to sink: {:?}", e);
        }
    }

    pub async fn write<V>(&mut self, value: &V)
    where
        V: Serialize,
    {
        let bytes = serialize(value);
        if let Err(e) =
            futures::future::poll_fn(|cx| Pin::new(&mut self.rw).poll_ready(cx))
                .await
        {
            warn!("[rw] error while polling sink ready: {:?}", e);
        }

        if let Err(e) = Pin::new(&mut self.rw).start_send(bytes) {
            warn!("[rw] error while starting send to sink: {:?}", e);
        }
    }

    pub async fn flush(&mut self) {
        if let Err(e) =
            futures::future::poll_fn(|cx| Pin::new(&mut self.rw).poll_flush(cx))
                .await
        {
            panic!("[rw] error while flushing sink: {:?}", e);
        }
    }
}

fn deserialize<V>(bytes: BytesMut) -> V
where
    V: DeserializeOwned,
{
    bincode::deserialize(&bytes).expect("[rw] deserialize should work")
}

fn serialize<V>(value: &V) -> Bytes
where
    V: Serialize,
{
    // TODO can we avoid `Bytes`?
    let bytes = bincode::serialize(value).expect("[rw] serialize should work");
    Bytes::from(bytes)
}
