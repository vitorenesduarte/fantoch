// This module contains the definition of...
pub mod task;

// This module contains the definition of...
pub mod net;

use crate::command::{Command, CommandResult};
use crate::id::ClientId;
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub enum FromClient {
    // clients can register
    Register(ClientId, UnboundedSender<CommandResult>),
    // or submit new commands
    Submit(Command),
}

// pub async fn run<P, A>(
//     port: u16,
//     addresses: Vec<A>,
//     client_port: u16,
//     process_id: ProcessId,
// ) -> Result<(), Box<dyn Error>>
// where
//     P: Process + 'static, // TODO what does this 'static do?
//     A: ToSocketAddrs + Debug + Clone,
// {
//     // check ports are different
//     assert!(port != client_port);

//     let (from_readers, to_writers, from_clients) =
//         net::init::<P, A>(port, addresses, client_port, process_id).await?;
//     println!("connected to processes: {:?}", to_writers.keys());

//     loop {
//         // match future::select(from_readers.recv(), from_clients.recv()) {
//         //     Either::Left(new_msg) => {
//         //         println!("new msg: {:?}", new_msg);
//         //     }
//         //     Either::Right(new_submit) => {
//         //         println!("new submit: {:?}", new_submit);
//         //     }
//         // }
//     }
// }
