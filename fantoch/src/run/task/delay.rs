use super::chan::{ChannelReceiver, ChannelSender};
use tokio::stream::StreamExt;

async fn delay_task<M>(
    mut from: ChannelReceiver<M>,
    mut to: ChannelSender<M>,
    delay: u64,
) where
    M: std::fmt::Debug + 'static,
{
    let mut queue = tokio::time::DelayQueue::new();
    let delay = tokio::time::Duration::from_millis(delay);
    loop {
        tokio::select! {
            msg = from.recv() => {
                queue.insert(msg, delay);
            }
            msg = queue.next() => {
                if let Some(msg) = msg {
                    match msg {
                        Ok(msg) => {
                            let msg = msg.into_inner().expect("[delay_task] there should be a msg");
                            if let Err(e) = to.send(msg).await {
                                println!("[delay_task] error forwarding message: {:?}", e);
                            }
                        }
                        Err(e) => {
                            println!("[delay_task] error taking message from queue: {:?}", e);
                        }
                    }
                } else {
                    println!("[delay_task] nothing to take from the queue");
                }
            }
        }
    }
}
