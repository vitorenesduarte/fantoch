use bytes::Bytes;
use futures::prelude::*;
use pin_project::pin_project;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{BufReader, BufWriter};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::stream::Stream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// tokio default (in 0.2.9) that may change
const DEFAULT_BUF_SIZE: usize = 8 * 1024;

/// Delimits frames using a length header.
#[pin_project]
#[derive(Debug)]
pub struct Connection<V> {
    #[pin]
    stream: Framed<TcpStream, LengthDelimitedCodec>,
    phantom: PhantomData<V>,
}

impl<V> Connection<V> {
    pub fn new(stream: TcpStream) -> Self {
        let stream = Framed::new(stream, LengthDelimitedCodec::new());
        Self {
            stream,
            phantom: PhantomData,
        }
    }
}

impl<V> Connection<V>
where
    V: Serialize + DeserializeOwned,
{
    // TODO here we only need a reference to the message; can we optimize this and not have each
    // connection own a copy of the message when sending it?
    pub async fn send(&mut self, value: &V) {
        // serialize
        let bytes = bincode::serialize(value).expect("serialize should work");
        // TODO do we need `Bytes`?
        let bytes = Bytes::from(bytes);
        if let Err(e) = self.stream.send(bytes).await {
            println!("error while writing to socket: {:?}", e);
        }
    }

    pub async fn recv(&mut self) -> Option<V> {
        match self.stream.next().await {
            Some(result) => match result {
                Ok(bytes) => {
                    // if it is, and not an error, deserialize it
                    let value =
                        bincode::deserialize(&bytes).expect("[connection] deserialize should work");
                    Some(value)
                }
                Err(e) => {
                    println!("[connection] error while reading from socket: {:?}", e);
                    None
                }
            },
            None => None,
        }
    }
}

impl<V> Stream for Connection<V>
where
    V: DeserializeOwned,
{
    type Item = V;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().stream.poll_next(cx) {
            Poll::Ready(result) => match result {
                // if the result is ready, check if it's something
                Some(result) => match result {
                    Ok(bytes) => {
                        // if it is, and not an error, deserialize it
                        let value = bincode::deserialize(&bytes)
                            .expect("[connection] deserialize should work");
                        Poll::Ready(Some(value))
                    }
                    Err(e) => {
                        println!("[connection] error while reading from socket: {:?}", e);
                        Poll::Pending
                    }
                },
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
