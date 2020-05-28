use crate::log;
use crate::protocol::Protocol;
use crate::run::prelude::*;
use tokio::time::{self, Duration, Instant, Interval};

// TODO: check async-timer for <1ms intervals
// https://github.com/DoumanAsh/async-timer/

pub async fn periodic_task<P, R>(
    events: Vec<(P::PeriodicEvent, u64)>,
    periodic_to_workers: PeriodicToWorkers<P, R>,
    to_periodic_inspect: Option<InspectReceiver<P, R>>,
) where
    P: Protocol + 'static,
    R: Clone + 'static,
{
    // create intervals
    let intervals = make_intervals(events);

    // different loop depending on whether there's an inspect channel or not
    match to_periodic_inspect {
        None => {
            periodic_loop_without_inspect(intervals, periodic_to_workers).await
        }
        Some(to_periodic_inspect) => {
            periodic_loop_with_inspect(
                intervals,
                periodic_to_workers,
                to_periodic_inspect,
            )
            .await
        }
    }
}

fn make_intervals<P, R>(
    events: Vec<(P::PeriodicEvent, u64)>,
) -> Vec<(FromPeriodicMessage<P, R>, Interval)>
where
    P: Protocol + 'static,
{
    events
        .into_iter()
        .map(|(event, millis)| {
            log!("[periodic] event: {:?} | interval {:?}", event, millis);

            // create event msg
            let event_msg = FromPeriodicMessage::Event(event);

            // compute first tick
            let duration = Duration::from_millis(millis as u64);
            let first_tick = Instant::now()
                .checked_add(duration)
                .expect("first tick in periodic task should exist");

            // create interval
            let interval = time::interval_at(first_tick, duration);

            (event_msg, interval)
        })
        .collect()
}

async fn periodic_loop_without_inspect<P, R>(
    mut intervals: Vec<(FromPeriodicMessage<P, R>, Interval)>,
    mut periodic_to_workers: PeriodicToWorkers<P, R>,
) where
    P: Protocol + 'static,
    R: Clone + 'static,
{
    match intervals.len() {
        1 => {
            let (event_msg0, mut interval0) = intervals.remove(0);
            loop {
                let _ = interval0.tick().await;
                // create event msg
                periodic_task_send_msg(
                    &mut periodic_to_workers,
                    event_msg0.clone(),
                )
                .await;
            }
        }
        2 => {
            let (event_msg0, mut interval0) = intervals.remove(0);
            let (event_msg1, mut interval1) = intervals.remove(0);
            loop {
                tokio::select! {
                    _ = interval0.tick() => {
                        periodic_task_send_msg(&mut periodic_to_workers, event_msg0.clone()).await;
                    }
                    _ = interval1.tick() => {
                        periodic_task_send_msg(&mut periodic_to_workers, event_msg1.clone()).await;
                    }
                }
            }
        }
        n => {
            panic!("number of periodic events {:?} not supported", n);
        }
    }
}

async fn periodic_loop_with_inspect<P, R>(
    mut intervals: Vec<(FromPeriodicMessage<P, R>, Interval)>,
    mut periodic_to_workers: PeriodicToWorkers<P, R>,
    mut to_periodic_inspect: InspectReceiver<P, R>,
) where
    P: Protocol + 'static,
    R: Clone + 'static,
{
    match intervals.len() {
        1 => {
            let (event_msg0, mut interval0) = intervals.remove(0);
            loop {
                tokio::select! {
                    _ = interval0.tick() => {
                        periodic_task_send_msg(&mut periodic_to_workers, event_msg0.clone()).await;
                    }
                    inspect = to_periodic_inspect.recv() => {
                        periodic_task_inspect(&mut periodic_to_workers, inspect).await
                    }
                }
            }
        }
        2 => {
            let (event_msg0, mut interval0) = intervals.remove(0);
            let (event_msg1, mut interval1) = intervals.remove(0);
            loop {
                tokio::select! {
                    _ = interval0.tick() => {
                        periodic_task_send_msg(&mut periodic_to_workers, event_msg0.clone()).await;
                    }
                    _ = interval1.tick() => {
                        periodic_task_send_msg(&mut periodic_to_workers, event_msg1.clone()).await;
                    }
                    inspect = to_periodic_inspect.recv() => {
                        periodic_task_inspect(&mut periodic_to_workers, inspect).await
                    }
                }
            }
        }
        n => {
            panic!("number of periodic events {:?} not supported", n);
        }
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

async fn periodic_task_inspect<P, R>(
    periodic_to_workers: &mut PeriodicToWorkers<P, R>,
    inspect: Option<InspectFun<P, R>>,
) where
    P: Protocol + 'static,
    R: Clone + 'static,
{
    if let Some((inspect_fun, reply_chan)) = inspect {
        let inspect_msg = FromPeriodicMessage::Inspect(inspect_fun, reply_chan);
        periodic_task_send_msg(periodic_to_workers, inspect_msg).await;
    } else {
        println!("[periodic] error while receiving new inspect message");
    }
}
