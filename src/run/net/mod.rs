// This module contains the definition of `Connection`.
mod connection;

// This module contains the definition of...
mod util;

use crate::id::{ClientId, ProcessId};
use crate::protocol::{Process, ToSend};
use crate::run::FromClient;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Debug;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

const LOCALHOST: &str = "127.0.0.1";
const CONNECT_RETRIES: usize = 100;

#[derive(Debug, Serialize, Deserialize)]
struct ClientHi(ClientId);

/// Connect to all processes. It receives:
/// - local port to bind to
/// - list of addresses to connect to
pub async fn init<P, A>(
    process_id: ProcessId,
    port: u16,
    addresses: Vec<A>,
    client_port: u16,
) -> Result<
    (
        UnboundedReceiver<P::Message>,
        UnboundedSender<ToSend<P::Message>>,
        UnboundedReceiver<FromClient>,
    ),
    Box<dyn Error>,
>
where
    P: Process + 'static, // TODO what does this 'static do?
    A: ToSocketAddrs + Debug + Clone,
{
    // connect to all processes
    let listener = TcpListener::bind((LOCALHOST, port)).await?;
    let (from_readers, to_writer) =
        util::process::connect_to_all(process_id, listener, addresses, CONNECT_RETRIES).await?;

    // start client listener
    let listener = TcpListener::bind((LOCALHOST, client_port)).await?;
    let from_clients = util::client::start_listener(listener);

    Ok((from_readers, to_writer, from_clients))
}
