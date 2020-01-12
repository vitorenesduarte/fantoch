// This module contains the definition of `Connection`.
pub mod connection;

// This module contains the definition of ...
pub mod process;

// This module contains the definition of ...
pub mod client;

use crate::run::net::connection::Connection;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;

/// Listen on new connections and send them to parent process.
pub async fn listener_task(mut listener: TcpListener, parent: UnboundedSender<Connection>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("[listener] new connection: {:?}", addr);

                // create connection
                let connection = Connection::new(stream);

                if let Err(e) = parent.send(connection) {
                    println!("[listener] error sending stream to parent process: {:?}", e);
                }
            }
            Err(e) => println!("[listener] couldn't accept new connection: {:?}", e),
        }
    }
}
