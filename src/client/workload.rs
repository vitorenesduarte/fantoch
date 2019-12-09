use crate::client::{client::RiflGen, ClientId};
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
        // increment command count and generate command
        self.command_count += 1;

        // check if we should generate more commands
        let should_gen = self.command_count <= self.total_commands;

        // if we should, generate a new command
        if should_gen {
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
        if rand::thread_rng().gen_range(0, 100) < self.conflict_rate {
            // black color to generate a conflict
            String::from("black")
        } else {
            // avoid conflict with unique client key
            rifl_gen.source().to_string()
        }
    }
}
