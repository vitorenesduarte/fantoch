#![deny(rust_2018_idioms)]

pub mod metrics;

use dashmap::DashMap;
use metrics::Histogram;
use quanta::Clock;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::event::Event;
use tracing::span::{Attributes, Id, Record};
use tracing::{Metadata, Subscriber};

#[macro_export]
macro_rules! elapsed {
    ( $x:expr ) => {{
        use std::time::Instant;
        let start = Instant::now();
        let result = $x;
        let time = start.elapsed();
        (time, result)
    }};
}

struct FunctionStartInfo {
    time: u64,
}

thread_local! {
    static CLOCK: RefCell<Clock> = RefCell::new(Clock::new());
    static INFOS: RefCell<HashMap<u64, FunctionStartInfo>> = RefCell::new(HashMap::new());
}

// assume 100ms as the highest execution time
const MAX_FUNCTION_EXECUTION_TIME: u64 = 100_000_000;

/// Compute current time.
fn current_time() -> u64 {
    CLOCK.with(|clock| clock.borrow_mut().now().as_u64())
}

/// Record function start time.
fn start(id: u64) {
    // create start function info
    let info = FunctionStartInfo {
        time: current_time(),
    };

    INFOS.with(|infos| {
        let res = infos.borrow_mut().insert(id, info);
        assert!(res.is_none());
    })
}

/// Retrieve function start time.
fn end(id: u64) -> Option<u64> {
    let function_start_info = INFOS.with(|infos| {
        infos
            .borrow_mut()
            .remove(&id)
            .expect("function should have been started")
    });

    // compute function execution time
    let end_time = current_time();
    let start_time = function_start_info.time;
    if end_time > start_time {
        Some(end_time - start_time)
    } else {
        None
    }
}

#[derive(Clone)]
pub struct ProfSubscriber {
    next_id: Arc<AtomicU64>,
    // mapping from function name to id used for that function
    functions: Arc<DashMap<&'static str, u64>>,
    // mapping from function name to its histogram
    histograms: Arc<DashMap<u64, Histogram>>,
}

impl ProfSubscriber {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(1)), // span ids must be > 0
            functions: Arc::new(DashMap::new()),
            histograms: Arc::new(DashMap::new()),
        }
    }
}

impl Subscriber for ProfSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        // get function name
        let function_name = span.metadata().name();
        self.new_span_from_function_name(function_name)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        let id = span.into_u64();
        start(id);
    }

    fn exit(&self, span: &Id) {
        let id = span.into_u64();
        let time = end(id);

        // retrieve histograms
        let mut histogram =
            self.histograms.entry(id).or_insert_with(Histogram::new);

        // maybe record execution time
        if let Some(time) = time {
            if time > MAX_FUNCTION_EXECUTION_TIME {
                println!("[prof] some function took {}ms", time / 1_000_000);
                return;
            }
            histogram.increment(time);
        }
    }
}

impl ProfSubscriber {
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

impl fmt::Debug for ProfSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name_to_id: Vec<(&'static str, u64)> = self
            .functions
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();

        for (function_name, id) in name_to_id {
            // find function's histogram
            if let Some(time_histogram) = self.histograms.get(&id) {
                writeln!(
                    f,
                    "{:<35} | count={:<10} max={:<10} avg={:<10} p90={:<10} p99={:<10} p99.99={:<10}",
                    function_name,
                    time_histogram.count(),
                    time_histogram.max().round(),
                    time_histogram.mean().round(),
                    time_histogram.percentile(0.90).round(),
                    time_histogram.percentile(0.99).round(),
                    time_histogram.percentile(0.9999).round(),
                )?;
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
        let s_1 = ProfSubscriber::new();
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
