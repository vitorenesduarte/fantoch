// This module contains the implementation of data structured used to hold info
// about commands.
pub mod info;

// This module contains the definition of `SharedClocks`.
mod shared_clocks;

// This module contains definitions common to dependency-graph-based protocols.
pub mod graph;

// This module contains definitions common to votes-table-based protocols.
pub mod table;

// This module contains the implementation of Paxos Synod Protocol.
pub mod synod;

#[cfg(test)]
mod tests {
    use crate::command::Command;
    use crate::id::Rifl;
    use rand::Rng;

    // Generates a random `Command` with at most `max_keys_per_command` where the number of keys is `keys_number`.
    pub fn gen_cmd(max_keys_per_command: usize, keys_number: usize) -> Command {
        // get random
        let mut rng = rand::thread_rng();
        // select keys per command
        let key_number = rng.gen_range(1, max_keys_per_command + 1);
        // generate command data
        let cmd_data: Vec<_> = (0..key_number)
            .map(|_| {
                // select random key
                let key = format!("{}", rng.gen_range(0, keys_number));
                let value = String::from("");
                (key, value)
            })
            .collect();
        // create fake rifl
        let rifl = Rifl::new(0, 0);
        // create multi put command
        Command::multi_put(rifl, cmd_data)
    }
}
