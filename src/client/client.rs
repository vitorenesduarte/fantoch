use crate::base::ProcId;
use crate::client::Workload;
use crate::command::{Command, CommandResult};
use crate::id::{Id, IdGen};
use crate::planet::{Planet, Region};
use crate::time::SysTime;
use crate::util;

pub type ClientId = u64;

// for info on RIFL see: http://sigops.org/sosp/sosp15/current/2015-Monterey/printable/126-lee.pdf
pub type Rifl = Id<ClientId>;
pub type RiflGen = IdGen<ClientId>;

pub struct Client {
    /// id of this client
    client_id: ClientId,
    /// region where this client is
    region: Region,
    planet: Planet,
    /// id of the process this client is connected to
    proc_id: Option<ProcId>,
    /// rifl id generator
    rifl_gen: RiflGen,
    /// workload configuration
    workload: Workload,
}

impl Client {
    /// Creates a new client.
    pub fn new(client_id: ClientId, region: Region, planet: Planet, workload: Workload) -> Self {
        // create client
        Self {
            client_id,
            region,
            planet,
            proc_id: None,
            rifl_gen: RiflGen::new(client_id),
            workload,
        }
    }

    /// Returns the client identifier.
    pub fn id(&self) -> ClientId {
        self.client_id
    }

    /// Generate client's first command.
    pub fn discover(&mut self, mut procs: Vec<(ProcId, Region)>) -> Option<(ProcId, Command)> {
        // set the closest process
        util::sort_procs_by_distance(&self.region, &self.planet, &mut procs);
        self.proc_id = procs.into_iter().map(|(proc_id, _)| proc_id).next();

        // generate command
        let cmd = self.next_cmd();
        util::option_zip(self.proc_id, cmd)
    }

    /// Handle executed command.
    /// TODO: pass current time to start and handle function
    /// and record command initial time to measure its overall latency
    pub fn handle(
        &mut self,
        cmd_result: CommandResult,
        time: &dyn SysTime,
    ) -> Option<(ProcId, Command)> {
        // TODO do something with `cmd_result`
        // generate command
        let cmd = self.next_cmd();
        util::option_zip(self.proc_id, cmd)
    }

    fn next_cmd(&mut self) -> Option<Command> {
        self.workload.next_cmd(&mut self.rifl_gen)
    }
}
