#[cfg(not(feature = "prof"))]
pub async fn tracer_task(tracer_show_interval: Option<usize>) {
    match tracer_show_interval {
        Some(_) => {
            panic!("[tracer_task] tracer show interval was set but the 'prof' feature is disabled");
        }
        None => {
            println!("[tracer_task] disabled since the 'prof' feature is not enabled");
        }
    }
}

#[cfg(feature = "prof")]
pub async fn tracer_task(tracer_show_interval: Option<usize>) {
    use crate::log;
    use fantoch_prof::ProfSubscriber;
    use tokio::time::{self, Duration};

    // if no interval, do not trace
    if tracer_show_interval.is_none() {
        println!("[tracer_task] tracer show interval was not set even though the 'prof' feature is enabled");
        return;
    }
    let tracer_show_interval = tracer_show_interval.unwrap();

    // set tracing subscriber
    let subscriber = ProfSubscriber::new();
    tracing::subscriber::set_global_default(subscriber.clone()).unwrap_or_else(
        |e| println!("tracing global default subscriber already set: {:?}", e),
    );

    // create tokio interval
    let millis = Duration::from_millis(tracer_show_interval as u64);
    log!("[tracer_task] interval {:?}", millis);
    let mut interval = time::interval(millis);

    loop {
        // wait tick
        let _ = interval.tick().await;
        // show metrics
        println!("{:?}", subscriber);
    }
}
