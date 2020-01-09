use futures::join;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::Duration;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

const LOCALHOST: &str = "127.0.0.1";

/// Connect to all processes. It receives:
/// - local port to bind to
/// - list of addresses to connect to
pub async fn connect_to_all<A>(
    port: u16,
    addresses: Vec<A>,
) -> Result<(Vec<TcpStream>, Vec<TcpStream>), Box<dyn Error>>
where
    A: ToSocketAddrs + Debug + Copy,
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
        loop {
            match TcpStream::connect(address).await {
                Ok(stream) => {
                    outgoing.push(stream);
                    break;
                }
                Err(e) => {
                    println!("failed to connect to {:?}: {}", address, e);
                    println!("will try again in 1 second");
                    tokio::time::delay_for(Duration::from_secs(1)).await;
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
        incoming.push(stream);
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

async fn reader(mut reader: FramedRead<TcpStream, LengthDelimitedCodec>) {
    println!("reader spawned!");
}

async fn writer(mut writer: FramedWrite<TcpStream, LengthDelimitedCodec>) {
    println!("writer spawned!");
}

// let (reader, writer) = stream.split();
// let reader = FramedRead::new(reader, LengthDelimitedCodec::new());
// let writer = FramedWrite::new(writer, LengthDelimitedCodec::new());
