use super::chan::{ChannelReceiver, ChannelSender};
use std::collections::VecDeque;
use tokio::time::{self, Duration, Instant};

#[derive(PartialEq)]
enum ReadStatus {
    Fail,
    Read,
    FailAfterRead,
}

pub async fn delay_task<M>(
    mut from: ChannelReceiver<M>,
    mut to: ChannelSender<M>,
    delay: u64,
) where
    M: std::fmt::Debug + 'static,
{
    // important to use VecDeque here since we want to always pop the first
    // element
    let mut queue = VecDeque::new();
    let delay = Duration::from_millis(delay);
    let mut read_status = ReadStatus::Fail;
    loop {
        match queue.front() {
            None => {
                let msg = from.recv().await;
                if read_status != ReadStatus::FailAfterRead {
                    enqueue(msg, delay, &mut queue, &mut read_status);
                }
            }
            Some((next_instant, _)) => {
                tokio::select! {
                    _ = time::delay_until(*next_instant) => {
                        let msg = dequeue(&mut queue);
                        if let Err(e) = to.send(msg).await {
                            println!("[delay_task] error forwarding message: {:?}", e);
                            break;
                        }
                    }
                    msg = from.recv() => {
                        if read_status != ReadStatus::FailAfterRead {
                            enqueue(msg, delay, &mut queue, &mut read_status);
                        }
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
    read_status: &mut ReadStatus,
) {
    if let Some(msg) = msg {
        queue.push_back((deadline(delay), msg));
        if let ReadStatus::Fail = read_status {
            // means we were in Fail, thus => Read
            *read_status = ReadStatus::Read;
        }
    } else {
        println!("[delay_task] error receiving message from parent");
        if let ReadStatus::Read = read_status {
            // means we were in Fail, then went to Read, thus =>
            // FailAfterRead
            *read_status = ReadStatus::FailAfterRead;
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
    const DELAY: u64 = 42;

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
                let sleep_time = rand::thread_rng().gen_range(1, MAX_SLEEP + 1);
                tokio::time::delay_for(Duration::from_millis(sleep_time)).await;
                let start = Instant::now();
                // send to delay task
                delay_tx.send(start).await.expect("operation sent");
            }
        });

        writer.await.expect("writer finished");
        let latency = reader.await.expect("reader finished");
        // allow messages to be suffer an extra delay of 1/2ms
        assert!(
            latency == DELAY || latency == DELAY + 1 || latency == DELAY + 2
        );
    }
}
