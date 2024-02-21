#[cfg(test)]
pub use tests::{gen_cmd, vclock};


#[cfg(test)]
mod tests {
    use fantoch::kvs::Value;
    use fantoch::command::Command;
    use fantoch::id::ProcessId;
    use fantoch::id::Rifl;
    use fantoch::kvs::KVOp;
    use rand::Rng;
    use threshold::{Clock, EventSet, MaxSet, VClock};

    #[cfg(test)]
    /// Returns a new `VClock` setting its frontier with the sequences in the
    /// iterator.
    pub fn vclock<I: IntoIterator<Item = u64>>(iter: I) -> VClock<ProcessId> {
        Clock::from(
            iter.into_iter()
                .enumerate()
                .map(|(actor, seq)| ((actor + 1) as ProcessId, seq)) // make ids 1..=n
                .map(|(actor, seq)| (actor, MaxSet::from_event(seq))),
        )
    }

    //TODO: Update to add ADD and SUBTRACT operations
    #[cfg(test)]
    // Generates a random `Command` with at most `max_keys_per_command` where
    // the number of keys is `keys_number`.
    pub fn gen_cmd(
        max_keys_per_command: usize,
        keys_number: usize,
        noop_probability: usize,
    ) -> Option<Command> {
        assert!(noop_probability <= 100);
        // get random
        let mut rng = rand::thread_rng();
        // select keys per command
        let key_number = rng.gen_range(1..(max_keys_per_command + 1));
        // generate command data
        let cmd_data: Vec<_> = (0..key_number)
            .map(|_| {
                // select random key
                let key = format!("{}", rng.gen_range(0..keys_number));
                let value = rng.gen_range(Value::MIN..Value::MAX);
                (key, KVOp::Put(value))
            })
            .collect();
        // create fake rifl
        let rifl = Rifl::new(0, 0);
        // create multi put command
        Some(Command::from(rifl, cmd_data))
    }
}
