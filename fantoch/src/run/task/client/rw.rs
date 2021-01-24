use crate::command::CommandResult;
use crate::hash_map::HashMap;
use crate::id::{ClientId, ProcessId};
use crate::run::chan::{self, ChannelReceiver, ChannelSender};
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::run::task;
use crate::{trace, warn};

pub fn start_client_rw_tasks(
    client_ids: &Vec<ClientId>,
    channel_buffer_size: usize,
    connections: Vec<(ProcessId, Connection)>,
) -> (
    ChannelReceiver<CommandResult>,
    HashMap<ProcessId, ChannelSender<ClientToServer>>,
) {
    // create server-to-client channels: although we keep one connection per
    // shard, we'll have all rw tasks will write to the same channel; this means
    // the client will read from a single channel (and potentially receive
    // messages from any of the shards)
    let (mut s2c_tx, s2c_rx) = chan::channel(channel_buffer_size);
    s2c_tx.set_name(format!(
        "server_to_client_{}",
        task::util::ids_repr(&client_ids)
    ));

    let mut process_to_tx = HashMap::with_capacity(connections.len());
    for (process_id, connection) in connections {
        // create client-to-server channels: since clients may send operations
        // to different shards, we create one client-to-rw channel per rw task
        let (mut c2s_tx, c2s_rx) = chan::channel(channel_buffer_size);
        c2s_tx.set_name(format!(
            "client_to_server_{}_{}",
            process_id,
            task::util::ids_repr(&client_ids)
        ));

        // spawn rw task
        task::spawn(client_rw_task(connection, s2c_tx.clone(), c2s_rx));
        process_to_tx.insert(process_id, c2s_tx);
    }
    (s2c_rx, process_to_tx)
}

async fn client_rw_task(
    mut connection: Connection,
    mut to_parent: ServerToClientSender,
    mut from_parent: ClientToServerReceiver,
) {
    loop {
        tokio::select! {
            to_client = connection.recv() => {
                trace!("[client_rw] to client: {:?}", to_client);
                if let Some(to_client) = to_client {
                    if let Err(e) = to_parent.send(to_client).await {
                        warn!("[client_rw] error while sending message from server to parent: {:?}", e);
                    }
                } else {
                    warn!("[client_rw] error while receiving message from server to parent");
                    break;
                }
            }
            to_server = from_parent.recv() => {
                trace!("[client_rw] from client: {:?}", to_server);
                if let Some(to_server) = to_server {
                    if let Err(e) = connection.send(&to_server).await {
                        warn!("[client_rw] error while sending message to server: {:?}", e);
                    }
                } else {
                    warn!("[client_rw] error while receiving message from parent to server");
                    // in this case it means that the parent (the client) is done, and so we can exit the loop
                    break;
                }
            }
        }
    }
}
