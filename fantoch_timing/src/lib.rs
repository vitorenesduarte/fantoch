use dashmap::DashMap;
use hdrhistogram::Histogram;
use quanta::Clock;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::event::Event;
use tracing::span::{Attributes, Id, Record};
use tracing::{Metadata, Subscriber};

thread_local! {
    static CLOCK: Clock = Clock::new();
    static START_TIMES: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
}

// assume 1 second as the highest execution time
const MAX_FUNCTION_EXECUTION_TIME: u64 = 1_000_0000;

/// Compute current time.
fn now() -> u64 {
    CLOCK.with(|clock| clock.now().as_u64())
}

/// Record function start time.
fn start(id: u64, time: u64) {
    START_TIMES.with(|start_times| {
        let res = start_times.borrow_mut().insert(id, time);
        assert!(res.is_none());
    })
}

/// Retrieve function start time.
fn end(id: u64) -> u64 {
    START_TIMES.with(|start_times| {
        start_times
            .borrow_mut()
            .remove(&id)
            .expect("function should have been started")
    })
}

#[derive(Clone)]
pub struct TimingSubscriber {
    next_id: Arc<AtomicU64>,
    // mapping from function name to id used for that function
    functions: DashMap<&'static str, u64>,
    // mapping from function name to its histogram
    histograms: DashMap<u64, Histogram<u64>>,
}

impl TimingSubscriber {
    pub fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(1)), // span ids must be > 0
            functions: DashMap::new(),
            histograms: DashMap::new(),
        }
    }
}

impl Subscriber for TimingSubscriber {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes) -> Id {
        // compute function name
        let function_name = span.metadata().name();
        println!("function name: {}", function_name);

        // get function id
        let id = match self.functions.get(function_name) {
            Some(id) => {
                // if the `function_name` already has an id associated, use it
                *id.value()
            }
            None => {
                // if here, it means that when `self.functions.get` was
                // executed, `function_name` had no id associated; let's try to
                // create one (making sure that no two threads create different
                // ids for the same function name)
                *self.functions.entry(function_name).or_insert_with(|| {
                    self.next_id.fetch_add(1, Ordering::SeqCst)
                })
            }
        };
        println!("function id: {}", id);
        Id::from_u64(id)
    }

    fn record(&self, _span: &Id, _values: &Record) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event) {}

    fn enter(&self, span: &Id) {
        let id = span.into_u64();
        println!("span entered: {}", id);
        let start_time = now();
        start(id, start_time);
    }

    fn exit(&self, span: &Id) {
        let id = span.into_u64();
        println!("span exited: {}", id);
        println!("span exited: {}", id);
        let end_time = now();
        let start_time = end(id);
        // function execution time in nanos
        if end_time > start_time {
            let time = end_time - start_time;
            if time > MAX_FUNCTION_EXECUTION_TIME {
                println!("some function took {}ns", time);
                return;
            }

            // retrieve histogram
            let mut histogram =
                self.histograms.entry(id).or_insert_with(|| {
                    Histogram::<u64>::new_with_bounds(
                        1,
                        MAX_FUNCTION_EXECUTION_TIME,
                        3,
                    )
                    .expect("creating histogram should work")
                });
            histogram
                .record(time)
                .expect("adding to histogram should work");
        }
    }
}

impl fmt::Debug for TimingSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name_to_id: Vec<(&'static str, u64)> = self
            .functions
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        write!(
            f,
            "functions found: {} or {}",
            self.functions.len(),
            name_to_id.len()
        )?;

        for (function_name, id) in name_to_id {
            // find function's histogram
            match self.histograms.get(&id) {
                Some(histogram) => {
                    let histogram = histogram.value();
                    write!(
                        f,
                        "{} | count={}   min={}   max={}   avg={}   std={}   p99={}   p99.99={}",
                        function_name,
                        histogram.len(),
                        histogram.min(),
                        histogram.max(),
                        histogram.mean(),
                        histogram.stdev(),
                        histogram.value_at_percentile(0.99),
                        histogram.value_at_percentile(0.9999)
                    )?;
                }
                None => {}
            }
        }
        Ok(())
    }
}
