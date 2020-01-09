use futures::join;
use std::error::Error;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Connect to all processes. It receives:
/// - local port to bind to
/// - list of addresses to connect to
pub async fn connect_to_all<A: ToSocketAddrs>(
    port: u16,
    addresses: Vec<A>,
) -> Result<(Vec<TcpStream>, Vec<TcpStream>), Box<dyn Error>> {
    // number of processes
    let n = addresses.len();

    // try to bind localy
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;

    // create channel to communicate with listener
    let (tx, mut rx) = mpsc::unbounded_channel();

    // spawn listener
    tokio::spawn(listen(listener, tx));

    // create list of readers and writers
    let mut writers = Vec::with_capacity(n);
    let mut readers = Vec::with_capacity(n);

    // connect to all addresses (and get the writers)
    for address in addresses {
        let writer = TcpStream::connect(address).await?;
        writers.push(writer);
    }

    // receive from listener all connected (the readers)
    for _ in 0..n {
        let reader = rx
            .recv()
            .await
            .expect("should receive stream from listener");
        readers.push(reader);
    }

    Ok((readers, writers))
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
