use crate::log;
use fantoch_timing::TimingSubscriber;
use tokio::time::{self, Duration};

pub async fn tracer_task(tracer_show_interval: Option<usize>) {
    // if no interval, do not trace
    if tracer_show_interval.is_none() {
        return;
    }
    let tracer_show_interval = tracer_show_interval.unwrap();

    // set tracing subscriber
    let subscriber = TimingSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting tracing default should work");

    // create tokio interval
    let millis = Duration::from_millis(tracer_show_interval as u64);
    log!("[tracker_task] interval {:?}", millis);
    let mut interval = time::interval(millis);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // show metrics
                println!("{:?}", subscriber);
            }
        }
    }
}
