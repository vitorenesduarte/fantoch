use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{self, BufReader, BufStream, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, FramedRead, FramedWrite, LengthDelimitedCodec};

/// Delimits frames using a length header.
#[derive(Debug)]
pub struct Connection {
    stream: Framed<BufStream<TcpStream>, LengthDelimitedCodec>,
}

#[derive(Debug)]
pub struct ConnectionReadHalf {
    read: FramedRead<BufReader<ReadHalf<TcpStream>>, LengthDelimitedCodec>,
}

#[derive(Debug)]
pub struct ConnectionWriteHalf {
    write: FramedWrite<BufWriter<WriteHalf<TcpStream>>, LengthDelimitedCodec>,
}

pub fn new_connection(stream: TcpStream) -> Connection {
    // TODO here `BufStream` will allocate two buffers, one for reading and another one for
    // writing; this may be unnecessarily inneficient for users that will only read or write; on
    // the other end, the allocation only occurs once, so it's probably fine to do this
    let stream = BufStream::new(stream);
    let stream = Framed::new(stream, LengthDelimitedCodec::new());
    Connection { stream }
}

pub fn new_splitted_connection(stream: TcpStream) -> (ConnectionReadHalf, ConnectionWriteHalf) {
    let (read, write) = tokio::io::split(stream);
    // buffer halves
    let read = BufReader::new(read);
    let write = BufWriter::new(write);
    // frame halves
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    (ConnectionReadHalf { read }, ConnectionWriteHalf { write })
}

impl Connection {
    pub async fn recv<V>(&mut self) -> Option<V>
    where
        V: DeserializeOwned,
    {
        recv(&mut self.stream).await
    }

    // TODO here we only need a reference to the value
    pub async fn send<V>(&mut self, value: V)
    where
        V: Serialize,
    {
        send(&mut self.stream, value).await;
    }

    pub async fn send_serialized(&mut self, bytes: Bytes) {
        send_serialized(&mut self.stream, bytes).await;
    }
}

impl ConnectionReadHalf {
    pub async fn recv<V>(&mut self) -> Option<V>
    where
        V: DeserializeOwned,
    {
        recv(&mut self.read).await
    }
}

impl ConnectionWriteHalf {
    pub async fn send<V>(&mut self, value: V)
    where
        V: Serialize,
    {
        send(&mut self.write, value).await;
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

pub async fn recv<S, V>(stream: &mut S) -> Option<V>
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

// TODO here we only need a reference to the value
async fn send<S, V>(sink: &mut S, value: V)
where
    S: Sink<Bytes, Error = io::Error> + Unpin,
    V: Serialize,
{
    let bytes = serialize(&value);
    send_serialized(sink, bytes).await;
}

async fn send_serialized<S>(sink: &mut S, bytes: Bytes)
where
    S: Sink<Bytes, Error = tokio::io::Error> + Unpin,
{
    if let Err(e) = sink.send(bytes).await {
        println!("[connection] error while writing to socket: {:?}", e);
    }
}
