use super::batch::Batch;
use super::pending::ShardsPending;
use crate::command::CommandResult;
use crate::id::{Rifl, ShardId};
use crate::run::chan::{ChannelReceiver, ChannelSender};
use crate::run::prelude::ClientToServer;
use crate::warn;
use crate::HashMap;
use color_eyre::eyre::{eyre, Report};

pub async fn unbatcher(
    mut from: ChannelReceiver<Batch>,
    mut to: ChannelSender<Vec<Rifl>>,
    mut read: ChannelReceiver<CommandResult>,
    mut shard_to_writer: HashMap<ShardId, ChannelSender<ClientToServer>>,
) {
    // create pending
    let mut pending = ShardsPending::new();

    loop {
        tokio::select! {
            from_batcher = from.recv() => {
                let handle_from_batcher = handle_from_batcher(from_batcher, &mut shard_to_writer, &mut pending).await;
                if let Err(e) = handle_from_batcher {
                    warn!("[batcher] {:?}", e);
                }
            }
            from_server = read.recv() => {
                let handle_from_server = handle_from_server(from_server, &mut to, &mut pending).await;
                if let Err(e) = handle_from_server {
                    warn!("[batcher] {:?}", e);
                }
            }
        }
    }
}

async fn handle_from_batcher(
    batch: Option<Batch>,
    shard_to_writer: &mut HashMap<ShardId, ChannelSender<ClientToServer>>,
    pending: &mut ShardsPending,
) -> Result<(), Report> {
    if let Some(batch) = batch {
        handle_batch(batch, shard_to_writer, pending).await;
        Ok(())
    } else {
        Err(eyre!("error receiving message from parent"))
    }
}

async fn handle_batch(
    batch: Batch,
    shard_to_writer: &mut HashMap<ShardId, ChannelSender<ClientToServer>>,
    pending: &mut ShardsPending,
) {
    // extract info from batch
    let (target_shard, cmd, rifls) = batch.unpack();

    // register command in pending (which will aggregate several
    // `CommandResult`s if the command acesses more than one shard)
    pending.register(&cmd, rifls);

    // 1. register the command in all shards but the target shard
    for shard in cmd.shards().filter(|shard| **shard != target_shard) {
        let msg = ClientToServer::Register(cmd.clone());
        send_to_shard(shard_to_writer, shard, msg).await
    }

    // 2. submit the command to the target shard
    let msg = ClientToServer::Submit(cmd);
    send_to_shard(shard_to_writer, &target_shard, msg).await
}

async fn send_to_shard(
    shard_to_writer: &mut HashMap<ShardId, ChannelSender<ClientToServer>>,
    shard_id: &ShardId,
    msg: ClientToServer,
) {
    // find process writer
    let writer = shard_to_writer
        .get_mut(shard_id)
        .expect("[unbatcher] dind't find writer for target shard");
    if let Err(e) = writer.send(msg).await {
        warn!(
            "[unbatcher] error while sending message to client rw task: {:?}",
            e
        );
    }
}

async fn handle_from_server(
    cmd_result: Option<CommandResult>,
    to: &mut ChannelSender<Vec<Rifl>>,
    pending: &mut ShardsPending,
) -> Result<(), Report> {
    if let Some(cmd_result) = cmd_result {
        handle_cmd_result(cmd_result, to, pending).await;
        Ok(())
    } else {
        Err(eyre!("error receiving message from parent"))
    }
}

async fn handle_cmd_result(
    cmd_result: CommandResult,
    to: &mut ChannelSender<Vec<Rifl>>,
    pending: &mut ShardsPending,
) {
    if let Some(rifls) = pending.add(cmd_result) {
        if let Err(e) = to.send(rifls).await {
            warn!("[unbatcher] error while sending message to client: {:?}", e);
        }
    }
}
