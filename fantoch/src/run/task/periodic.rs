use crate::log;
use crate::protocol::Protocol;
use crate::run::prelude::*;
use tokio::time::{self, Duration, Instant};

pub async fn periodic_task<P, R>(
    events: Vec<(P::PeriodicEvent, usize)>,
    mut periodic_to_workers: PeriodicToWorkers<P, R>,
    to_periodic_inspect: Option<InspectReceiver<P, R>>,
) where
    P: Protocol + 'static,
    R: Clone + 'static,
{
    // TODO we only support one periodic event for now
    assert_eq!(events.len(), 1);
    let (event, millis) = events[0].clone();
    let millis = Duration::from_millis(millis as u64);

    log!("[periodic_task] event: {:?} | interval {:?}", event, millis);

    // compute first tick
    let first_tick = Instant::now()
        .checked_add(millis)
        .expect("first tick in periodic task should exist");

    let mut interval = time::interval_at(first_tick, millis);

    // create event msg
    let event_msg = FromPeriodicMessage::Event(event);

    // different loop depending on whether there's an inspect channel or not
    match to_periodic_inspect {
        None => loop {
            let _ = interval.tick().await;
            // create event msg
            periodic_task_send_msg(&mut periodic_to_workers, event_msg.clone())
                .await;
        },
        Some(mut to_periodic_inspect) => loop {
            tokio::select! {
                _ = interval.tick() => {
                    periodic_task_send_msg(&mut periodic_to_workers, event_msg.clone()).await;
                }
                inspect = to_periodic_inspect.recv() => {
                    if let Some((inspect_fun, reply_chan)) = inspect {
                        let inspect_msg = FromPeriodicMessage::Inspect(inspect_fun, reply_chan);
                        periodic_task_send_msg(&mut periodic_to_workers, inspect_msg).await;
                    } else {
                        println!(
                            "[periodic] error while receiving new inspect message"
                        );
                    }
                }
            }
        },
    }
}

async fn periodic_task_send_msg<P, R>(
    periodic_to_workers: &mut PeriodicToWorkers<P, R>,
    msg: FromPeriodicMessage<P, R>,
) where
    P: Protocol + 'static,
    R: Clone + 'static,
{
    if let Err(e) = periodic_to_workers.forward(msg).await {
        println!("[periodic] error sending message to workers: {:?}", e);
    }
}
