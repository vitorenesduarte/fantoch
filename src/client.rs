use crate::base::ProcId;
use crate::command::{MultiCommand, MultiCommandResult};
use crate::store::Key;
use rand::Rng;

// for info on RIFL see: http://sigops.org/sosp/sosp15/current/2015-Monterey/printable/126-lee.pdf
pub type ClientId = u64;
pub type Rifl = (ClientId, u64);

pub struct Client {
    /// id of this client
    client_id: ClientId,
    /// number of RIFL identifiers already generated
    /// TODO is this equal to `command_count`?
    rifl_count: u64,
    /// id of the process this client is connected to
    proc_id: ProcId,
    /// conflict rate  of this workload
    conflict_rate: usize,
    /// number of commands to be submitted in this workload
    commands: usize,
    /// number of commands already issued in this workload
    command_count: usize,
}

impl Client {
    /// Creates a new client.
    pub fn new(client_id: ClientId, proc_id: ProcId) -> Self {
        // workload specific configuration
        let conflict_rate = 100;
        let commands = 10;

        // create client
        Client {
            client_id,
            rifl_count: 0,
            proc_id,
            conflict_rate,
            commands,
            command_count: 0,
        }
    }

    /// TODO pass current time to start and handle function
    /// and record command initial time to measure its overall latency

    /// Generate client's first command.
    pub fn start(&mut self) -> (ProcId, MultiCommand) {
        assert_eq!(self.command_count, 0);
        assert!(self.commands > 0);
        self.gen_cmd()
    }

    /// Handle executed commands.
    pub fn handle(
        &mut self,
        commands: Vec<(Rifl, MultiCommandResult)>,
    ) -> Option<(ProcId, MultiCommand)> {
        // check if we should generate a new command or not
        if self.command_count < self.commands {
            Some(self.gen_cmd())
        } else {
            None
        }
    }

    /// Generate the next command.
    fn gen_cmd(&mut self) -> (ProcId, MultiCommand) {
        // increment command count and generate command
        self.command_count += 1;
        let rifl = self.next_rifl();
        let key = self.gen_cmd_key();
        (self.proc_id, MultiCommand::get(rifl, key))
    }

    /// Generate the next RIFL identifier.
    /// If the client identifier is 10:
    /// - the first RIFL will be (10, 1)
    /// - the second RIFL will be (10, 2)
    /// - and so on...
    fn next_rifl(&mut self) -> Rifl {
        self.rifl_count += 1;
        (self.client_id, self.rifl_count)
    }

    /// Generate a command given
    fn gen_cmd_key(&mut self) -> Key {
        if rand::thread_rng().gen_range(0, 100) < self.conflict_rate {
            // black color to generate a conflict
            String::from("black")
        } else {
            // avoid conflict with unique client key
            self.client_id.to_string()
        }
    }
}
