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

// assume 10ms as the highest execution time
const MAX_FUNCTION_EXECUTION_TIME: u64 = 10_000_000;

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
    functions: Arc<DashMap<&'static str, u64>>,
    // mapping from function name to its histogram
    histograms: Arc<DashMap<u64, Histogram<u64>>>,
}

impl TimingSubscriber {
    pub fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(1)), // span ids must be > 0
            functions: Arc::new(DashMap::new()),
            histograms: Arc::new(DashMap::new()),
        }
    }
}

impl Subscriber for TimingSubscriber {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes) -> Id {
        // getfunction name
        let function_name = span.metadata().name();
        self.new_span_from_function_name(function_name)
    }

    fn record(&self, _span: &Id, _values: &Record) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event) {}

    fn enter(&self, span: &Id) {
        let id = span.into_u64();
        let start_time = now();
        start(id, start_time);
    }

    fn exit(&self, span: &Id) {
        let id = span.into_u64();
        let end_time = now();
        let start_time = end(id);
        // function execution time in nanos
        if end_time > start_time {
            let time = end_time - start_time;
            if time > MAX_FUNCTION_EXECUTION_TIME {
                println!("some function took {}ms", time / 1_000_000);
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

impl TimingSubscriber {
    fn new_span_from_function_name(&self, function_name: &'static str) -> Id {
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
        Id::from_u64(id)
    }
}

impl fmt::Debug for TimingSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name_to_id: Vec<(&'static str, u64)> = self
            .functions
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();

        for (function_name, id) in name_to_id {
            // find function's histogram
            match self.histograms.get(&id) {
                Some(histogram) => {
                    let histogram = histogram.value();
                    writeln!(
                        f,
                        "{:>35} | count={:<8} min={:<8} max={:<8} avg={:<8} std={:<8} p99={:<8} p99.99={:<8}",
                        function_name,
                        histogram.len(),
                        histogram.min(),
                        histogram.max(),
                        histogram.mean().round(),
                        histogram.stdev().round(),
                        histogram.value_at_percentile(0.99),
                        histogram.value_at_percentile(0.9999),
                    )?;
                }
                None => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn clone_test() {
        let s_1 = TimingSubscriber::new();
        let s_2 = s_1.clone();

        // no functions or histograms in the beginning
        assert_eq!(s_2.functions.len(), 0);
        assert_eq!(s_2.histograms.len(), 0);

        // create span
        let span = s_1.new_span_from_function_name("my_fun");

        // now there's one function
        assert_eq!(s_2.functions.len(), 1);
        assert_eq!(s_2.histograms.len(), 0);

        s_1.enter(&span);
        thread::sleep(Duration::from_millis(1));
        s_1.exit(&span);

        // now there's one function and one histogram
        assert_eq!(s_2.functions.len(), 1);
        assert_eq!(s_2.histograms.len(), 1);
    }
}
