use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::util;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;
use threshold::AEClock;

#[derive(Debug, Clone)]
pub struct ExecutedClock {
    process_id: ProcessId,
    clock: Arc<RwLock<AEClock<ProcessId>>>,
}

impl ExecutedClock {
    pub fn new(process_id: ProcessId, config: &Config) -> Self {
        let ids: Vec<_> = util::all_process_ids(config.shards(), config.n())
            .map(|(process_id, _)| process_id)
            .collect();
        let clock = Arc::new(RwLock::new(AEClock::with(ids)));
        Self { process_id, clock }
    }

    #[cfg(test)]
    pub fn from(process_id: ProcessId, clock: AEClock<ProcessId>) -> Self {
        Self {
            process_id,
            clock: Arc::new(RwLock::new(clock)),
        }
    }

    pub fn read(
        &self,
        tag: &'static str,
    ) -> RwLockReadGuard<'_, AEClock<ProcessId>> {
        self.clock.read()
        // self.clock.try_read().unwrap_or_else(|| {
        //     panic!(
        //         "p{}: ExecutedClock::read failed at {}",
        //         self.process_id, tag
        //     )
        // })
    }

    pub fn write(
        &self,
        tag: &'static str,
    ) -> RwLockWriteGuard<'_, AEClock<ProcessId>> {
        self.clock.write()
        // self.clock.try_write().unwrap_or_else(|| {
        //     panic!(
        //         "p{}: ExecutedClock::write failed at {}",
        //         self.process_id, tag
        //     )
        // })
    }

    pub fn fair_unlock_read(guard: RwLockReadGuard<'_, AEClock<ProcessId>>) {
        RwLockReadGuard::unlock_fair(guard)
    }

    // pub fn fair_unlock_write(guard: RwLockWriteGuard<'_, AEClock<ProcessId>>) {
    //     RwLockWriteGuard::unlock_fair(guard)
    // }
}
