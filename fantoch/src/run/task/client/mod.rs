use crate::id::{ClientId, ProcessId, ShardId};
use crate::run::chan;
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::run::task;
use crate::HashMap;
use crate::{trace, warn};

pub async fn client_say_hi(
    client_ids: Vec<ClientId>,
    connection: &mut Connection,
) -> Option<(ProcessId, ShardId)> {
    trace!("[client] will say hi with ids {:?}", client_ids);
    // say hi
    let hi = ClientHi(client_ids.clone());
    if let Err(e) = connection.send(&hi).await {
        warn!("[client] error while sending hi: {:?}", e);
    }

    // receive hi back
    if let Some(ProcessHi {
        process_id,
        shard_id,
    }) = connection.recv().await
    {
        trace!(
            "[client] clients {:?} received hi from process {} with shard id {}",
            client_ids,
            process_id,
            shard_id
        );
        Some((process_id, shard_id))
    } else {
        warn!("[client] clients {:?} couldn't receive process id from connected process", client_ids);
        None
    }
}

pub fn start_client_rw_tasks(
    client_ids: &Vec<ClientId>,
    channel_buffer_size: usize,
    connections: Vec<(ProcessId, Connection)>,
) -> (
    ServerToClientReceiver,
    HashMap<ProcessId, ClientToServerSender>,
) {
    // create server-to-client channels: although we keep one connection per
    // shard, we'll have all rw tasks will write to the same channel; this means
    // the client will read from a single channel (and potentially receive
    // messages from any of the shards)
    let (mut s2c_tx, s2c_rx) = chan::channel(channel_buffer_size);
    s2c_tx.set_name(format!(
        "server_to_client_{}",
        super::util::ids_repr(&client_ids)
    ));

    let mut process_to_tx = HashMap::with_capacity(connections.len());
    for (process_id, connection) in connections {
        // create client-to-server channels: since clients may send operations
        // to different shards, we create one client-to-rw channel per rw task
        let (mut c2s_tx, c2s_rx) = chan::channel(channel_buffer_size);
        c2s_tx.set_name(format!(
            "client_to_server_{}_{}",
            process_id,
            super::util::ids_repr(&client_ids)
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
