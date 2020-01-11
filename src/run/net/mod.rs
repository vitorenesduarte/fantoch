// This module contains the definition of `Connection`.
mod connection;

use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId};
use crate::protocol::Process;
use connection::Connection;
use futures::future::{self, Either, Future, FutureExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

const LOCALHOST: &str = "127.0.0.1";
const CONNECT_RETRIES: usize = 100;

// type MessageSender<P> = UnboundedSender<<P as Process>::Message>;
// type MessageReceiver<P> = UnboundedReceiver<<P as Process>::Message>;

// pub type ClientReceiver = UnboundedReceiver<FromClient>;
// pub type ClientSender = UnboundedSender<FromClient>;
// pub type ClientResultReceiver = UnboundedReceiver<CommandResult>;
// pub type ClientResultSender = UnboundedSender<CommandResult>;

// #[derive(Debug)]
// pub enum FromClient {
//     // clients can register
//     Register(ClientId, ClientResultSender),
//     // or submit new commands
//     Submit(Command),
// }

// #[derive(Debug, Serialize, Deserialize)]
// struct ProcessHi(ProcessId);

// #[derive(Debug, Serialize, Deserialize)]
// struct ClientHi(ClientId);

// /// Connect to all processes. It receives:
// /// - local port to bind to
// /// - list of addresses to connect to
// pub async fn init<P, A>(
//     port: u16,
//     addresses: Vec<A>,
//     client_port: u16,
//     process_id: ProcessId,
// ) -> Result<
//     (
//         MessageReceiver<P>,
//         HashMap<ProcessId, MessageSender<P>>,
//         ClientReceiver,
//     ),
//     Box<dyn Error>,
// >
// where
//     P: Process + 'static, // TODO what does this 'static do?
//     A: ToSocketAddrs + Debug + Clone,
// {
//     // connect to all
//     let (connections_0, connections_1) = connect_to_all(port, addresses).await?;

//     // say hi
//     let (connections_0, connections_1) =
//         say_hi::<P>(process_id, connections_0, connections_1).await?;

//     // start readers and writers
//     let from_readers = start_readers::<P>(connections_0).await?;
//     let to_writers = start_writers::<P>(connections_1).await?;

//     // start client listener
//     let from_clients = client_listener(client_port).await?;
//     Ok((from_readers, to_writers, from_clients))
// }

// async fn connect_to_all<A>(
//     port: u16,
//     addresses: Vec<A>,
// ) -> Result<(Vec<Connection<ProcessHi>>, Vec<Connection<ProcessHi>>), Box<dyn Error>>
// where
//     A: ToSocketAddrs + Debug + Clone,
// {
//     // number of processes
//     let n = addresses.len();

//     // try to bind localy
//     let listener = TcpListener::bind((LOCALHOST, port)).await?;

//     // create channel to communicate with listener
//     let (tx, mut rx) = mpsc::unbounded_channel();

//     // spawn listener
//     tokio::spawn(listener_task(listener, tx));

//     // create list of in and out connections:
//     // - even though TCP is full-duplex, due to the current tokio parallel-tcp-socket-read-write
//     //   limitation, we going to use in streams for reading and out streams for writing, which
// can     //   be done in parallel
//     let mut outgoing = Vec::with_capacity(n);
//     let mut incoming = Vec::with_capacity(n);

//     // connect to all addresses (and get the writers)
//     for address in addresses {
//         let mut tries = 0;
//         loop {
//             match connect(&address).await {
//                 Ok(connection) => {
//                     // save connection if connected successfully
//                     outgoing.push(connection);
//                     break;
//                 }
//                 Err(e) => {
//                     // if not, try again if we shouldn't give up (due to too many attempts)
//                     tries += 1;
//                     if tries < CONNECT_RETRIES {
//                         println!("failed to connect to {:?}: {}", address, e);
//                         println!("will try again in 1 second");
//                         tokio::time::delay_for(Duration::from_secs(1)).await;
//                     } else {
//                         return Err(e);
//                     }
//                 }
//             }
//         }
//     }

//     // receive from listener all connected (the readers)
//     for _ in 0..n {
//         let connection = rx
//             .recv()
//             .await
//             .expect("should receive connection from listener");
//         incoming.push(connection);
//     }

//     Ok((incoming, outgoing))
// }

// async fn connect<A, V>(address: &A) -> Result<Connection<V>, Box<dyn Error>>
// where
//     A: ToSocketAddrs + Debug + Clone,
// {
//     let stream = TcpStream::connect(address.clone()).await?;
//     let connection = Connection::new(stream);
//     Ok(connection)
// }

// async fn say_hi<P>(
//     process_id: ProcessId,
//     connections: Vec<Connection<ProcessHi>>,
//     mut connections_say_hi: Vec<Connection<ProcessHi>>,
// ) -> Result<
//     (
//         Vec<Connection<P::Message>>,
//         HashMap<ProcessId, Connection<P::Message>>,
//     ),
//     Box<dyn Error>,
// >
// where
//     P: Process + 'static, // TODO what does this 'static do?
// {
//     // say hi to all processes
//     let hi = ProcessHi(process_id);
//     for connection in connections_say_hi.iter_mut() {
//         connection.send(hi).await;
//     }
//     println!("said hi to all processes");

//     // create mapping from process id to connection
//     let mut id_to_connection = HashMap::new();
//     for mut connection in connections {
//         if let Some(ProcessHi(from)) = connection.recv().await {
//             // save entry and check it has not been inserted before
//             let res = id_to_connection.insert(from, connection.into());
//             assert!(res.is_none());
//         } else {
//             panic!("error receiving hi");
//         }
//     }
//     Ok((
//         connections_say_hi
//             .into_iter()
//             .map(|connection| connection.into())
//             .collect(),
//         id_to_connection,
//     ))
// }

// /// Starts a reader task per connection received and returns an unbounded channel to which
// readers /// will write to.
// async fn start_readers<P>(
//     connections: Vec<Connection<P::Message>>,
// ) -> Result<UnboundedReceiver<P::Message>, Box<dyn Error>>
// where
//     P: Process + 'static, // TODO what does this 'static do?
// {
//     // create channel where readers should write to
//     let (tx, rx) = mpsc::unbounded_channel();

//     // start one reader task per connection
//     for connection in connections {
//         tokio::spawn(reader_task::<P>(connection, tx.clone()));
//     }

//     Ok(rx)
// }

// async fn start_writers<P>(
//     connections: HashMap<ProcessId, Connection<P::Message>>,
// ) -> Result<HashMap<ProcessId, UnboundedSender<P::Message>>, Box<dyn Error>>
// where
//     P: Process + 'static, // TODO what does this 'static do?
// {
//     // mapping from process id to channel parent should write to
//     let mut writers = HashMap::new();

//     // start on writer task per connection
//     for (process_id, connection) in connections {
//         // create channel where parent should write to
//         let (tx, rx) = mpsc::unbounded_channel();
//         tokio::spawn(writer_task::<P>(connection, rx));
//         writers.insert(process_id, tx);
//     }

//     Ok(writers)
// }

// async fn client_listener(client_port: u16) -> Result<ClientReceiver, Box<dyn Error>> {
//     // try to bind localy
//     let listener = TcpListener::bind((LOCALHOST, client_port)).await?;

//     // create channel to communicate with listener
//     let (tx, rx) = mpsc::unbounded_channel();

//     // spawn listener
//     tokio::spawn(client_listener_task(listener, tx));

//     Ok(rx)
// }

// /// Listen on new client connections.
// async fn client_listener_task(listener: TcpListener, parent: ClientSender) {
//     // create channel to communicate with listener
//     let (tx, mut rx) = mpsc::unbounded_channel();

//     // start listener task
//     tokio::spawn(listener_task(listener, tx));

//     loop {
//         match rx.recv().await {
//             Some(connection) => {
//                 println!("[client_listener] new connection");
//                 // start client task
//                 tokio::spawn(client_task(connection, parent.clone()));
//             }
//             None => {
//                 println!("[client_listener] error receiving message from connection");
//             }
//         }
//     }
// }

// /// Listen on new connections and send them to parent process.
// async fn listener_task<V>(mut listener: TcpListener, parent: UnboundedSender<Connection<V>>)
// where
//     V: Debug,
// {
//     loop {
//         match listener.accept().await {
//             Ok((stream, addr)) => {
//                 println!("[listener] new connection: {:?}", addr);

//                 // create connection
//                 let connection = Connection::new(stream);

//                 if let Err(e) = parent.send(connection) {
//                     println!("[listener] error sending stream to parent process: {:?}", e);
//                 }
//             }
//             Err(e) => println!("[listener] couldn't accept new connection: {:?}", e),
//         }
//     }
// }

// /// Reader task.
// async fn reader_task<P>(mut connection: Connection<P::Message>, parent:
// UnboundedSender<P::Message>) where
//     P: Process + 'static, // TODO what does this 'static do?
// {
//     loop {
//         match connection.recv().await {
//             Some(msg) => {
//                 if let Err(e) = parent.send(msg) {
//                     println!("[reader] error notifying parent task with new msg: {:?}", e);
//                 }
//             }
//             None => {
//                 println!("[reader] error receiving message from connection");
//             }
//         }
//     }
// }

// /// Writer task.
// async fn writer_task<P>(
//     mut connection: Connection<P::Message>,
//     mut parent: UnboundedReceiver<P::Message>,
// ) where
//     P: Process + 'static, // TODO what does this 'static do?
// {
//     loop {
//         if let Some(msg) = parent.recv().await {
//             connection.send(msg).await;
//         } else {
//             println!("[writer] error receiving message from parent");
//         }
//     }
// }

// /// Client taskl. Checks messages both from the client connection (new commands) and parent (new
// command results). async fn client_task<V>(mut connection: Connection<V>, parent_tx: ClientSender)
// {     let mut parent_rx = client_say_hi(&mut connection, &parent_tx).await;

//     loop {
//         let a = connection_fut
//             .take()
//             .unwrap_or_else(|| Box::pin(connection.recv::<Command>()));
//         let b = parent_rx_fut
//             .take()
//             .unwrap_or_else(|| Box::pin(parent_rx.recv()));
//         match future::select(a, b).await {
//             Either::Left((new_msg, b)) => {
//                 // save remaining future
//                 parent_rx_fut = Some(b);
//                 println!("new msg: {:?}", new_msg);
//             }
//             Either::Right((new_submit, a)) => {
//                 // save remaining future
//                 connection_fut = Some(a);
//                 println!("new submit: {:?}", new_submit);
//             }
//         }
//     }
// }

// async fn client_say_hi<V>(
//     connection: &mut Connection<V>,
//     parent_tx: &ClientSender,
// ) -> ClientResultReceiver {
//     let (tx, rx) = mpsc::unbounded_channel();
//     // TODO receive hi from client and register in parent, sending it tx
//     let client_id = 0;
//     if let Err(e) = parent_tx.send(FromClient::Register(client_id, tx)) {
//         println!("error while registering client in parent: {:?}", e);
//     }
//     rx
// }
