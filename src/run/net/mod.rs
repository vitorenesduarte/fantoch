use bytes::Bytes;
use futures::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::Duration;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const LOCALHOST: &str = "127.0.0.1";
const CONNECT_RETRIES: usize = 100;

/// Connect to all processes. It receives:
/// - local port to bind to
/// - list of addresses to connect to
pub async fn connect_to_all<A>(
    port: u16,
    addresses: Vec<A>,
) -> Result<(Vec<Connection>, Vec<Connection>), Box<dyn Error>>
where
    A: ToSocketAddrs + Debug + Clone,
{
    // number of processes
    let n = addresses.len();

    // try to bind localy
    let listener = TcpListener::bind((LOCALHOST, port)).await?;

    // create channel to communicate with listener
    let (tx, mut rx) = mpsc::unbounded_channel();

    // spawn listener
    tokio::spawn(listen(listener, tx));

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio parallel-tcp-socket-read-write
    //   limitation, we going to use in streams for reading and out streams for writing, which can
    //   be done in parallel
    let mut outgoing = Vec::with_capacity(n);
    let mut incoming = Vec::with_capacity(n);

    // connect to all addresses (and get the writers)
    for address in addresses {
        let mut tries = 0;
        loop {
            match TcpStream::connect(address.clone()).await {
                Ok(stream) => {
                    // save stream if connected successfully
                    let connection = Connection::new(stream);
                    outgoing.push(connection);
                    break;
                }
                Err(e) => {
                    // if not, try again if we shouldn't give up (due to too many attempts)
                    tries += 1;
                    if tries < CONNECT_RETRIES {
                        println!("failed to connect to {:?}: {}", address, e);
                        println!("will try again in 1 second");
                        tokio::time::delay_for(Duration::from_secs(1)).await;
                    } else {
                        return Err(Box::new(e));
                    }
                }
            }
        }
    }

    // receive from listener all connected (the readers)
    for _ in 0..n {
        let stream = rx
            .recv()
            .await
            .expect("should receive stream from listener");
        let connection = Connection::new(stream);
        incoming.push(connection);
    }

    Ok((incoming, outgoing))
}

/// Listen on new connections and send them to parent process.
async fn listen(mut listener: TcpListener, parent: UnboundedSender<TcpStream>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("new connection: {:?}", addr);

                if let Err(e) = parent.send(stream) {
                    println!("error sending stream to parent process: {:?}", e);
                }
            }
            Err(e) => println!("couldn't accept new connection: {:?}", e),
        }
    }
}

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
