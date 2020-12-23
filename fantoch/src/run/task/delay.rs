use super::chan::{ChannelReceiver, ChannelSender};
use crate::warn;
use std::collections::VecDeque;
use tokio::time::{self, Duration, Instant};

pub async fn delay_task<M>(
    mut from: ChannelReceiver<M>,
    mut to: ChannelSender<M>,
    delay: Duration,
) where
    M: std::fmt::Debug + 'static,
{
    // important to use VecDeque here since we want to always pop the first
    // element
    let mut queue = VecDeque::new();
    let mut error_shown = false;
    loop {
        match queue.front() {
            None => {
                let msg = from.recv().await;
                enqueue(msg, delay, &mut queue, &mut error_shown);
            }
            Some((next_instant, _)) => {
                tokio::select! {
                    _ = time::sleep_until(*next_instant) => {
                        let msg = dequeue(&mut queue);
                        if let Err(e) = to.send(msg).await {
                            warn!("[delay_task] error forwarding message: {:?}", e);
                            break;
                        }
                    }
                    msg = from.recv() => {
                        enqueue(msg, delay, &mut queue, &mut error_shown);
                    }
                }
            }
        }
    }
}

fn enqueue<M>(
    msg: Option<M>,
    delay: Duration,
    queue: &mut VecDeque<(Instant, M)>,
    error_shown: &mut bool,
) {
    if let Some(msg) = msg {
        queue.push_back((deadline(delay), msg));
    } else {
        if !*error_shown {
            *error_shown = true;
            warn!("[delay_task] error receiving message from parent");
        }
    }
}

fn dequeue<M>(queue: &mut VecDeque<(Instant, M)>) -> M {
    let (_, msg) = queue.pop_front().expect("a first element should exist");
    msg
}

fn deadline(delay: Duration) -> Instant {
    Instant::now()
        .checked_add(delay)
        .expect("deadline should exist")
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use tokio::time::{Duration, Instant};

    const OPERATIONS: usize = 1000;
    const MAX_SLEEP: u64 = 20;
    const DELAY: Duration = Duration::from_millis(42);

    #[tokio::test]
    async fn delay_test() {
        // make sure there's enough space in the buffer channel
        let (tx, mut rx) = crate::run::task::channel::<Instant>(OPERATIONS * 2);
        let (mut delay_tx, delay_rx) =
            crate::run::task::channel::<Instant>(OPERATIONS * 2);

        // spawn delay task
        tokio::spawn(super::delay_task(delay_rx, tx, DELAY));

        // spawn reader
        let reader = tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(OPERATIONS);
            for _ in 0..OPERATIONS {
                let start = rx.recv().await.expect("operation received");

                // compute
                let delay = start.elapsed().as_millis() as u64;
                latencies.push(delay);
            }

            // compute average
            let sum = latencies.into_iter().sum::<u64>();
            sum / (OPERATIONS as u64)
        });

        // spawn writer
        let writer = tokio::spawn(async move {
            for _ in 0..OPERATIONS {
                let sleep_time =
                    rand::thread_rng().gen_range(1..(MAX_SLEEP + 1));
                tokio::time::sleep(Duration::from_millis(sleep_time)).await;
                let start = Instant::now();
                // send to delay task
                delay_tx.send(start).await.expect("operation sent");
            }
        });

        writer.await.expect("writer finished");
        let latency = reader.await.expect("reader finished");
        // allow messages to be suffer an extra delay of 1/2ms
        let delay = DELAY.as_millis() as u64;
        assert!(
            latency == delay || latency == delay + 1 || latency == delay + 2
        );
    }
}
