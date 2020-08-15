use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::util;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;
use threshold::AEClock;

#[derive(Debug, Clone)]
pub struct ExecutedClock {
    process_id: ProcessId,
    // clock: Arc<RwLock<AEClock<ProcessId>>>,
    clock: AEClock<ProcessId>,
}

impl ExecutedClock {
    pub fn new(process_id: ProcessId, config: &Config) -> Self {
        let ids: Vec<_> = util::all_process_ids(config.shards(), config.n())
            .map(|(process_id, _)| process_id)
            .collect();
        // let clock = Arc::new(RwLock::new(AEClock::with(ids)));
        let clock = AEClock::with(ids);
        Self { process_id, clock }
    }

    #[cfg(test)]
    pub fn from(process_id: ProcessId, clock: AEClock<ProcessId>) -> Self {
        Self {
            process_id,
            // clock: Arc::new(RwLock::new(clock)),
            clock,
        }
    }

    pub fn read<R>(
        &self,
        tag: &'static str,
        f: impl FnOnce(&AEClock<ProcessId>) -> R,
    ) -> R {
        // let guard = self.clock.read();
        // let result = f(&*guard);
        // RwLockReadGuard::unlock_fair(guard);
        // result
        f(&self.clock)
        // self.clock.try_read().unwrap_or_else(|| {
        //     panic!(
        //         "p{}: ExecutedClock::read failed at {}",
        //         self.process_id, tag
        //     )
        // })
    }

    pub fn write<R>(
        &mut self,
        tag: &'static str,
        f: impl FnOnce(&mut AEClock<ProcessId>) -> R,
    ) -> R {
        // let mut guard = self.clock.write();
        // let result = f(&mut *guard);
        // RwLockWriteGuard::unlock_fair(guard);
        // result
        f(&mut self.clock)
        // self.clock.try_write().unwrap_or_else(|| {
        //     panic!(
        //         "p{}: ExecutedClock::write failed at {}",
        //         self.process_id, tag
        //     )
        // })
    }
}
