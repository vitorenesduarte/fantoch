use super::chan::{ChannelReceiver, ChannelSender};
use tokio::time::{self, Duration, Instant};

async fn delay_task<M>(
    mut from: ChannelReceiver<M>,
    mut to: ChannelSender<M>,
    delay: u64,
) where
    M: std::fmt::Debug + 'static,
{
    let mut queue = Vec::new();
    let delay = Duration::from_millis(delay);
    loop {
        match queue.first() {
            None => {
                let msg = from.recv().await;
                enqueue(msg, delay, &mut queue);
            }
            Some((next_instant, _)) => {
                tokio::select! {
                    _ = time::delay_until(*next_instant) => {
                        let (_, msg) = queue.pop().unwrap();
                        if let Err(e) = to.send(msg).await {
                            println!("[delay_task] error forwarding message: {:?}", e);
                        }
                    }
                    msg = from.recv() => {
                        enqueue(msg, delay, &mut queue);
                    }
                }
            }
        }
    }
}

fn enqueue<M>(msg: Option<M>, delay: Duration, queue: &mut Vec<(Instant, M)>) {
    if let Some(msg) = msg {
        queue.push((deadline(delay), msg));
    } else {
        println!("[delay_task] error receiving message from parent");
    }
}

fn deadline(delay: Duration) -> Instant {
    Instant::now()
        .checked_add(delay)
        .expect("deadline should exist")
}
