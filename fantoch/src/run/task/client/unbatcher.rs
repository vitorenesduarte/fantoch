use super::batch::Batch;
use super::pending::ShardsPending;
use crate::command::CommandResult;
use crate::id::{Rifl, ShardId};
use crate::run::chan::{ChannelReceiver, ChannelSender};
use crate::run::prelude::ClientToServer;
use crate::warn;
use crate::HashMap;

pub async fn unbatcher(
    mut from: ChannelReceiver<Batch>,
    mut to: ChannelSender<Rifl>,
    mut read: ChannelReceiver<CommandResult>,
    mut shard_to_writer: HashMap<ShardId, ChannelSender<ClientToServer>>,
) {

    // TODO
    // if let Some(rifl) = pending.add(cmd_result) {
    // } else {
    //     // no new command completed, so return non
    //     None
    // }
}

async fn send_batch(
    batch: Batch,
    shard_to_writer: &mut HashMap<ShardId, ChannelSender<ClientToServer>>,
    pending: &mut ShardsPending,
) {
    // extract info from batch
    let (target_shard, cmd, rifls) = batch.unpack();

    // register command in pending (which will aggregate several
    // `CommandResult`s if the command acesses more than one shard)
    pending.register(&cmd);

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
            "[unbacher] error while sending message to client rw task: {:?}",
            e
        );
    }
}
