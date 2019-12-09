use crate::client::client::RiflGen;
use crate::command::Command;
use crate::kvs::Key;
use rand::Rng;

#[derive(Clone, Copy)]
pub struct Workload {
    /// conflict rate  of this workload
    conflict_rate: usize,
    /// number of commands to be submitted in this workload
    total_commands: usize,
    /// number of commands already issued in this workload
    command_count: usize,
}

impl Workload {
    pub fn new(conflict_rate: usize, total_commands: usize) -> Self {
        Self {
            conflict_rate,
            total_commands,
            command_count: 0,
        }
    }

    /// Generate the next command.
    pub fn next_cmd(&mut self, rifl_gen: &mut RiflGen) -> Option<Command> {
        // check if we should generate more commands
        if self.command_count < self.total_commands {
            // increment command count
            self.command_count += 1;
            // generate new command
            Some(self.gen_cmd(rifl_gen))
        } else {
            None
        }
    }

    /// Generate a command.
    fn gen_cmd(&mut self, rifl_gen: &mut RiflGen) -> Command {
        // generate rifl, key and value
        let rifl = rifl_gen.next_id();
        let key = self.gen_cmd_key(&rifl_gen);
        // TODO: generate something with a given payload size if outside of simulation
        let value = String::from("");

        // generate put command
        // TODO: make it configurable so that we can generate other commands besides put commands.
        Command::put(rifl, key, value)
    }

    /// Generate a command given
    fn gen_cmd_key(&mut self, rifl_gen: &RiflGen) -> Key {
        // check if we should generate a conflict
        let should_conflict = match rand::thread_rng().gen_range(0, 100) {
            0 => false,
            n => n < self.conflict_rate,
        };
        if should_conflict {
            // black color to generate a conflict
            String::from("black")
        } else {
            // avoid conflict with unique client key
            rifl_gen.source().to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_cmd_key() {
        // create rilf gen
        let client_id = 1;
        let rifl_gen = RiflGen::new(client_id);

        // total commands
        let total_commands = 100;

        // create conflicting workload
        let conflict_rate = 100;
        let mut workload = Workload::new(conflict_rate, total_commands);
        assert_eq!(workload.gen_cmd_key(&rifl_gen), String::from("black"));

        // create non-conflicting workload
        let conflict_rate = 0;
        let mut workload = Workload::new(conflict_rate, total_commands);
        assert_eq!(workload.gen_cmd_key(&rifl_gen), String::from("1"));
    }

    #[test]
    fn next_cmd() {
        // create rilf gen
        let client_id = 1;
        let mut rifl_gen = RiflGen::new(client_id);

        // total commands
        let total_commands = 10;

        // create workload
        let conflict_rate = 100;
        let mut workload = Workload::new(conflict_rate, total_commands);

        // the first 10 commands are `Some`
        for _ in 1..=10 {
            assert!(workload.next_cmd(&mut rifl_gen).is_some());
        }

        // at this point, the number of commands generated equals `total_commands`
        assert_eq!(workload.command_count, total_commands);

        // after this, no more commands are generated
        for _ in 1..=10 {
            assert!(workload.next_cmd(&mut rifl_gen).is_none());
        }

        // at the number of commands generated is still `total_commands`
        assert_eq!(workload.command_count, total_commands);
    }
}
