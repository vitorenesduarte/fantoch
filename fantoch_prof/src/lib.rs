#![deny(rust_2018_idioms)]

use dashmap::DashMap;
use hdrhistogram::Histogram;
use quanta::Clock;
use std::alloc::{GlobalAlloc, Layout};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::event::Event;
use tracing::span::{Attributes, Id, Record};
use tracing::{Metadata, Subscriber};

struct FunctionStartInfo {
    time: u64,
    memory_size: i64,
}

#[derive(Default)]
struct AllocCounters {
    memory_size: i64,
}

thread_local! {
    static CLOCK: RefCell<Clock> = RefCell::new(Clock::new());
    static ALLOCS: RefCell<AllocCounters> = RefCell::new(Default::default());
    static INFOS: RefCell<HashMap<u64, FunctionStartInfo>> = RefCell::new(HashMap::new());
}

// assume 100ms as the highest execution time
const MAX_FUNCTION_EXECUTION_TIME: u64 = 100_000_000;
// assume 100MB as the highest memory allocated
const MAX_FUNCTION_MEMORY_ALLOC: u64 = 100_000_000;

/// Compute current time.
fn current_time() -> u64 {
    CLOCK.with(|clock| clock.borrow_mut().now().as_u64())
}

/// Compute current memory.
fn current_memory() -> i64 {
    ALLOCS.with(|allocs| allocs.borrow().memory_size)
}

/// Record function start time.
fn start(id: u64) {
    // create start function info
    let info = FunctionStartInfo {
        time: current_time(),
        memory_size: current_memory(),
    };

    INFOS.with(|infos| {
        let res = infos.borrow_mut().insert(id, info);
        assert!(res.is_none());
    })
}

/// Retrieve function start time.
fn end(id: u64) -> (Option<u64>, Option<u64>) {
    let function_start_info = INFOS.with(|infos| {
        infos
            .borrow_mut()
            .remove(&id)
            .expect("function should have been started")
    });

    // compute function execution time
    let end_time = current_time();
    let start_time = function_start_info.time;
    let time = if end_time > start_time {
        Some(end_time - start_time)
    } else {
        None
    };

    // compute function allocated memory
    let end_memory = current_memory();
    let start_memory = function_start_info.memory_size;
    let memory = if end_memory > start_memory {
        Some((end_memory - start_memory) as u64)
    } else {
        None
    };

    (time, memory)
}

fn track_memory(size: i64) {
    ALLOCS.with(|allocs| {
        allocs.borrow_mut().memory_size += size;
    })
}

pub struct AllocProf<T: GlobalAlloc> {
    allocator: T,
}

impl AllocProf<jemallocator::Jemalloc> {
    pub const fn new() -> Self {
        Self {
            allocator: jemallocator::Jemalloc,
        }
    }
}

unsafe impl<T> GlobalAlloc for AllocProf<T>
where
    T: GlobalAlloc,
{
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        track_memory(layout.size() as i64);
        self.allocator.alloc(layout)
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        track_memory(layout.size() as i64);
        self.allocator.alloc_zeroed(layout)
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        track_memory(layout.size() as i64 * -1);
        self.allocator.dealloc(ptr, layout)
    }

    #[inline]
    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        //  ----------------------------------------
        // | layout_size | new_size | memory change |
        //  ----------------------------------------
        // |      10     |    10    |       0       |  (no changes)
        // |      10     |     5    |      -5       |  (dealloc)
        // |       5     |    10    |      +5       |  (alloc)
        //  ----------------------------------------
        let change = new_size as i64 - layout.size() as i64;
        track_memory(change);

        self.allocator.realloc(ptr, layout, new_size)
    }
}

#[derive(Clone)]
pub struct ProfSubscriber {
    next_id: Arc<AtomicU64>,
    // mapping from function name to id used for that function
    functions: Arc<DashMap<&'static str, u64>>,
    // mapping from function name to its histogram
    histograms: Arc<DashMap<u64, (Histogram<u64>, Histogram<u64>)>>,
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
        // getfunction name
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
        let (time, memory) = end(id);

        // retrieve histograms
        let mut histograms = self.histograms.entry(id).or_insert_with(|| {
            let time_histogram = Histogram::<u64>::new_with_bounds(
                1,
                MAX_FUNCTION_EXECUTION_TIME,
                3,
            )
            .expect("creating time histogram should work");
            let memory_histogram = Histogram::<u64>::new_with_bounds(
                1,
                MAX_FUNCTION_MEMORY_ALLOC,
                3,
            )
            .expect("creating memory histogram should work");
            (time_histogram, memory_histogram)
        });

        // maybe record execution time
        if let Some(time) = time {
            if time > MAX_FUNCTION_EXECUTION_TIME {
                println!("[prof] some function took {}ms", time / 1_000_000);
                return;
            }
            histograms
                .0
                .record(time)
                .expect("adding to time histogram should work");
        }

        // maybe record memory allocated
        if let Some(memory) = memory {
            if memory > MAX_FUNCTION_MEMORY_ALLOC {
                println!(
                    "[prof] some function allocated {}MB",
                    memory / 1_000_000
                );
                return;
            }
            histograms
                .1
                .record(memory)
                .expect("adding to memory histogram should work");
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
            if let Some(histograms) = self.histograms.get(&id) {
                let (time_histogram, memory_histogram) = histograms.value();
                writeln!(
                        f,
                        "{:<35} | count={:<10} | time: max={:<10} avg={:<10} std={:<10} | mem: max={:<10} avg={:<10} std={:<10}",
                        function_name,
                        (time_histogram.len() + memory_histogram.len()) / 2,
                        time_histogram.max(),
                        time_histogram.mean().round(),
                        time_histogram.stdev().round(),
                        memory_histogram.max(),
                        memory_histogram.mean().round(),
                        memory_histogram.stdev().round(),
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
