use super::batch::Batch;
use crate::command::Command;
use crate::id::ShardId;
use crate::run::chan::{ChannelReceiver, ChannelSender};
use crate::run::task;
use crate::warn;
use color_eyre::eyre::{eyre, Report};
use tokio::time::{self, Duration};

struct BatchingConfig {
    batch_max_size: usize,
    batch_delay: Duration,
}

pub async fn batcher(
    mut from: ChannelReceiver<(ShardId, Command)>,
    mut to: ChannelSender<Batch>,
    batch_max_size: usize,
    batch_delay: Duration,
) {
    // create batching config
    let config = BatchingConfig {
        batch_max_size,
        batch_delay,
    };
    // variable to hold the next batch to be sent
    let mut next_batch = None;

    loop {
        match next_batch.as_ref() {
            None => {
                let received = from.recv().await;
                let add_to_batch =
                    add_to_batch(received, &mut next_batch, &mut to, &config)
                        .await;
                if let Err(e) = add_to_batch {
                    warn!("[batcher] {:?}", e);
                    break;
                }
            }
            Some(batch) => {
                tokio::select! {
                    _ = time::sleep_until(batch.deadline()) => {
                        if let Err(e) = send_batch(&mut next_batch, &mut to).await {
                            warn!("[batcher] error forwarding batch: {:?}", e);
                            break;
                        }
                    }
                    received = from.recv() => {
                        let add_to_batch =
                            add_to_batch(received, &mut next_batch, &mut to, &config)
                                .await;
                        if let Err(e) = add_to_batch {
                            warn!("[batcher] {:?}", e);
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn add_to_batch(
    received: Option<(ShardId, Command)>,
    next_batch: &mut Option<Batch>,
    to: &mut ChannelSender<Batch>,
    config: &BatchingConfig,
) -> Result<(), Report> {
    if let Some((target_shard, cmd)) = received {
        match next_batch.as_mut() {
            Some(batch) => {
                batch.merge(target_shard, cmd);
            }
            None => {
                // create a new batch only with this command
                let deadline = task::util::deadline(config.batch_delay);
                let batch = Batch::new(target_shard, cmd, deadline);
                *next_batch = Some(batch);
            }
        }

        let batch = next_batch.as_ref().expect("there should be a batch");
        // if the batch was reached the batch max size, then send it right away
        // (i.e., do not wait for its deadline)
        if batch.size() == config.batch_max_size {
            send_batch(next_batch, to).await?;
        }
        Ok(())
    } else {
        Err(eyre!("error receiving message from parent"))
    }
}

async fn send_batch(
    next_batch: &mut Option<Batch>,
    to: &mut ChannelSender<Batch>,
) -> Result<(), Report> {
    let batch = next_batch.take().expect("a batch should exist");
    to.send(batch).await
}
