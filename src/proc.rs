use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{Dot, DotGen, ProcId};
use crate::planet::{Planet, Region};
use crate::util;

#[derive(Clone, PartialEq, Debug)]
pub enum ToSend<M> {
    // new command to be sent to a coordinator
    ToCoordinator(ProcId, Command),
    // a protocol message to be sent to a list of processes
    ToProcs(Vec<ProcId>, M),
    // a list of command results to be sent to the issuing clients
    ToClients(Vec<CommandResult>),
    // nothing to send
    Nothing,
}

impl<M> ToSend<M> {
    /// Check if it's something to be sent to a coordinator.
    pub fn to_coordinator(&self) -> bool {
        match *self {
            ToSend::ToCoordinator(_, _) => true,
            _ => false,
        }
    }

    /// Check if it' ssomething to be sent to processes.
    pub fn to_procs(&self) -> bool {
        match *self {
            ToSend::ToProcs(_, _) => true,
            _ => false,
        }
    }

    /// Check if it's something to be sent to clients.
    pub fn to_clients(&self) -> bool {
        match *self {
            ToSend::ToClients(_) => true,
            _ => false,
        }
    }

    /// Check if there's nothing to be sent.
    pub fn is_nothing(&self) -> bool {
        match *self {
            ToSend::Nothing => true,
            _ => false,
        }
    }
}

pub trait Proc {
    type Message: Clone;

    fn new(id: ProcId, region: Region, planet: Planet, config: Config) -> Self;
    fn id(&self) -> ProcId;
    fn discover(&mut self, procs: Vec<(ProcId, Region)>) -> bool;
    fn submit(&mut self, cmd: Command) -> ToSend<Self::Message>;
    fn handle(&mut self, msg: Self::Message) -> ToSend<Self::Message>;
}

// a `BaseProc` has all functionalities shared by Atlas, Newt, ...
pub struct BaseProc {
    pub id: ProcId,
    pub region: Region,
    pub planet: Planet,
    pub config: Config,
    // fast quorum size
    pub q: usize,
    pub all_procs: Option<Vec<ProcId>>,
    pub fast_quorum: Option<Vec<ProcId>>,
    dot_gen: DotGen,
}

impl BaseProc {
    /// Creates a new `BaseProc`.
    pub fn new(id: ProcId, region: Region, planet: Planet, config: Config, q: usize) -> Self {
        // since processes lead with ballot `id` when taking the slow path and
        // we may rely on the fact that a zero accepted ballot means the process
        // has never been through Paxos phase-2, all ids must non-zero
        assert!(id != 0);
        Self {
            id,
            region,
            planet,
            config,
            q,
            all_procs: None,
            fast_quorum: None,
            dot_gen: DotGen::new(id),
        }
    }

    /// Updates the processes known by this process.
    pub fn discover(&mut self, mut procs: Vec<(ProcId, Region)>) -> bool {
        // create all procs
        util::sort_procs_by_distance(&self.region, &self.planet, &mut procs);
        let all_procs: Vec<_> = procs.into_iter().map(|(id, _)| id).collect();

        // create fast quorum by taking the first `q` elements
        let fast_quorum: Vec<_> = all_procs.clone().into_iter().take(self.q).collect();

        // check if we have enough fast quorum processes
        let connected = fast_quorum.len() == self.q;

        // set fast quorum and all procs
        self.all_procs = Some(all_procs);
        self.fast_quorum = Some(fast_quorum);

        connected
    }

    // Returns the next dot.
    pub fn next_dot(&mut self) -> Dot {
        self.dot_gen.next_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover() {
        // procs
        let procs = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("asia-northeast1")),
            (2, Region::new("asia-south1")),
            (3, Region::new("asia-southeast1")),
            (4, Region::new("australia-southeast1")),
            (5, Region::new("europe-north1")),
            (6, Region::new("europe-west1")),
            (7, Region::new("europe-west2")),
            (8, Region::new("europe-west3")),
            (9, Region::new("europe-west4")),
            (10, Region::new("northamerica-northeast1")),
            (11, Region::new("southamerica-east1")),
            (12, Region::new("us-central1")),
            (13, Region::new("us-east1")),
            (14, Region::new("us-east4")),
            (15, Region::new("us-west1")),
            (16, Region::new("us-west2")),
        ];

        // config
        let n = 17;
        let f = 3;
        let config = Config::new(n, f);

        // bp
        let id = 8;
        let region = Region::new("europe-west3");
        let planet = Planet::new("latency/");
        let q = 6;
        let mut bp = BaseProc::new(id, region, planet, config, q);

        // no quorum is set yet
        assert_eq!(bp.fast_quorum, None);
        assert_eq!(bp.all_procs, None);

        // discover procs
        bp.discover(procs);

        assert_eq!(
            bp.all_procs,
            Some(vec![
                8, 9, 6, 7, 5, 14, 10, 13, 12, 15, 16, 11, 1, 0, 4, 3, 2
            ])
        );
        assert_eq!(bp.fast_quorum, Some(vec![8, 9, 6, 7, 5, 14]));
    }

    #[test]
    fn discover_same_region() {
        // procs
        let procs = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("asia-east1")),
            (2, Region::new("europe-north1")),
            (3, Region::new("europe-north1")),
            (4, Region::new("europe-west1")),
        ];

        // config
        let n = 5;
        let f = 2;
        let config = Config::new(n, f);

        // bp
        let id = 2;
        let region = Region::new("europe-north1");
        let planet = Planet::new("latency/");
        let q = 3;
        let mut bp = BaseProc::new(id, region, planet, config, q);

        // discover procs
        bp.discover(procs);

        assert_eq!(bp.all_procs, Some(vec![2, 3, 4, 0, 1]));
        assert_eq!(bp.fast_quorum, Some(vec![2, 3, 4]));
    }
}
