use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{self, BufStream};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Delimits frames using a length header.
#[derive(Debug)]
pub struct Connection {
    stream: Framed<BufStream<TcpStream>, LengthDelimitedCodec>,
}

impl Connection {
    // TODO here `BufStream` will allocate two buffers, one for reading and another one for
    // writing; this may be unnecessarily inneficient for users that will only read or write; on
    // the other end, the allocation only occurs once, so it's probably fine to do this
    pub fn new(stream: TcpStream, tcp_nodelay: bool, socket_buffer_size: usize) -> Self {
        // set TCP_NODELAY
        stream
            .set_nodelay(tcp_nodelay)
            .expect("setting TCP_NODELAY should work");
        // buffer stream
        let stream = BufStream::with_capacity(socket_buffer_size, socket_buffer_size, stream);
        // frame stream
        let stream = Framed::new(stream, LengthDelimitedCodec::new());
        Connection { stream }
    }

    pub async fn recv<V>(&mut self) -> Option<V>
    where
        V: DeserializeOwned,
    {
        next(&mut self.stream).await
    }

    // TODO here we only need a reference to the value
    pub async fn send<V>(&mut self, value: V)
    where
        V: Serialize,
    {
        send(&mut self.stream, value).await;
    }
}

fn deserialize<V>(bytes: BytesMut) -> V
where
    V: DeserializeOwned,
{
    bincode::deserialize(&bytes).expect("[connection] deserialize should work")
}

pub fn serialize<V>(value: &V) -> Bytes
where
    V: Serialize,
{
    // TODO can we avoid `Bytes`?
    let bytes = bincode::serialize(value).expect("[connection] serialize should work");
    Bytes::from(bytes)
}

/// By implementing this method based on `Stream`s, it will make it trivial in the future to
/// support it for e.g. `FramedRead<BufReader<ReadHalf<TcpStream>>, LengthDelimitedCodec>`. At this
/// point this makes no sense as `ReadHalf` needs to lock `TcpStream` in order to perform a `recv`.
async fn next<S, V>(stream: &mut S) -> Option<V>
where
    S: Stream<Item = Result<BytesMut, io::Error>> + Unpin,
    V: DeserializeOwned,
{
    match stream.next().await {
        Some(Ok(bytes)) => {
            // if it is, and not an error, deserialize it
            let value = deserialize(bytes);
            Some(value)
        }
        Some(Err(e)) => {
            println!("[connection] error while reading from socket: {:?}", e);
            None
        }
        None => None,
    }
}

/// By implementing this method based on `Sink`s, it will make it trivial in the future to
/// support it for e.g. `FramedWrite<BufWriter<WriteHalf<TcpStream>>, LengthDelimitedCodec>`. At
/// this point this makes no sense as `WriteHalf` needs to lock `TcpStream` in order to perform a
/// `send`.
async fn send<S, V>(sink: &mut S, value: V)
where
    S: Sink<Bytes, Error = io::Error> + Unpin,
    V: Serialize,
{
    // TODO here we only need a reference to the value
    let bytes = serialize(&value);
    if let Err(e) = sink.send(bytes).await {
        println!("[connection] error while writing to socket: {:?}", e);
    }
}
