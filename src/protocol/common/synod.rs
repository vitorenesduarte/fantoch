use crate::id::ProcessId;
use std::collections::{HashMap, HashSet};

type Ballot = u64;

/// Implementation of Flexible single-decree Paxos in which:
/// - phase-1 waits for n - f promises
/// - phase-2 waits for f + 1 accepts
pub enum SynodMessage<V> {
    // messages to acceptor
    MPrepare(Ballot),
    MAccept(Ballot, V),
    // messages to proposer
    MPromise(Ballot, Accepted<V>),
    MAccepted(Ballot),
    // message to be handled by user
    MChosen(V),
}

pub struct Synod<V> {
    // paxos agents
    proposer: Proposer<V>,
    acceptor: Acceptor<V>,
}

#[allow(dead_code)]
impl<V> Synod<V>
where
    V: Default + Clone,
{
    pub fn new<F>(process_id: ProcessId, n: usize, f: usize, proposal_gen: Box<F>) -> Self
    where
        // proposal generator that given all values reported computes a new consensus proposal
        F: 'static + FnOnce(HashMap<ProcessId, V>) -> V,
    {
        Self {
            proposer: Proposer::new(process_id, n, f, proposal_gen),
            acceptor: Acceptor::new(),
        }
    }

    // Set the consensus value if no value has been accepted yet.
    #[must_use]
    pub fn maybe_set_value<F>(&mut self, value_gen: F) -> bool
    where
        F: FnOnce() -> V,
    {
        self.acceptor.maybe_set_value(value_gen)
    }

    // Creates a ballot onwed by this process. This ballot is greater than any seen ballot.
    pub fn new_prepare(&mut self) -> SynodMessage<V> {
        self.proposer.new_prepare(&self.acceptor)
    }

    pub fn handle<F>(&mut self, from: ProcessId, msg: SynodMessage<V>) -> Option<SynodMessage<V>> {
        match msg {
            // handle messages to acceptor
            SynodMessage::MPrepare(b) => self.acceptor.handle_prepare(b),
            SynodMessage::MAccept(b, value) => self.acceptor.handle_accept(b, value),
            // handle messages to proposer
            SynodMessage::MPromise(b, previous) => self.proposer.handle_promise(from, b, previous),
            SynodMessage::MAccepted(b) => self.proposer.handle_accepted(from, b),
            SynodMessage::MChosen(_) => panic!(
                "MChosen messages are supposed to be handled outside of this Synod abstraction"
            ),
        }
    }
}

struct Proposer<V> {
    // process identifier
    process_id: ProcessId,
    // number of processes
    n: usize,
    // maximum number of allowed failures
    f: usize,
    // ballot used in prepare
    ballot: Ballot,
    // set of accepted values received in promises
    promises: HashMap<ProcessId, Accepted<V>>,
    // set of processes that have accepted a proposal
    accepted: HashSet<ProcessId>,
    // proposal generator that will be used once enough promises have been collected
    proposal_gen: Box<dyn FnOnce(HashMap<ProcessId, V>) -> V>,
    // value proposed
    proposal: Option<V>,
}

impl<V> Proposer<V>
where
    V: Clone + Default,
{
    fn new<F>(process_id: ProcessId, n: usize, f: usize, proposal_gen: Box<F>) -> Self
    where
        F: 'static + FnOnce(HashMap<ProcessId, V>) -> V,
    {
        Self {
            process_id,
            n,
            f,
            ballot: 0,
            promises: HashMap::new(),
            accepted: HashSet::new(),
            proposal_gen: Box::new(proposal_gen),
            proposal: None,
        }
    }

    // Creates a ballot onwed by this process.
    // The ballot is greater than any ballot seen by the acceptor.
    // By feeding the proposer with the highest ballot seen by the acceptor, this increases the
    // likelyhood of having this new prepare accepted. And since the created Prepare will be
    // delivered imediately at the local acceptor, this ensures that the ballots created are unique.
    fn new_prepare(&mut self, acceptor: &Acceptor<V>) -> SynodMessage<V> {
        // get number of processes
        let n = self.n as u64;
        // compute "round" of current ballot
        let round = acceptor.current_ballot() / n;
        // compute the next "round"
        let next_round = round + 1;
        // compute ballot owned by this process in the next round
        self.ballot = self.process_id + n * next_round;

        // empty `promises`, `accepted` and `proposal`
        self.promises = HashMap::new();
        self.accepted = HashSet::new();
        self.proposal = None;

        // create prepare message
        let prepare = SynodMessage::MPrepare(self.ballot);
        prepare
    }

    fn handle_promise(
        &mut self,
        from: ProcessId,
        b: Ballot,
        accepted: Accepted<V>,
    ) -> Option<SynodMessage<V>> {
        // check if it's a promise about the current ballot
        if self.ballot == b {
            // if yes, update set of promises
            self.promises.insert(from, accepted);
            // generate an `MAccept` if we have enough promises
            if self.promises.len() == self.n - self.f {
                // TODO
                // check if any value has been accepted before
                // - if yes, select the value accepted at the highest ballot
                // - if not, generate proposal using the generator
                // let accept = SynodMessage()
                // finally, save the proposal
            }
        }
        None
    }

    fn handle_accepted(&mut self, from: ProcessId, b: Ballot) -> Option<SynodMessage<V>> {
        // check if it's an accept about the current ballot
        if self.ballot == b {
            // if yes, update set of processes that have accepted
            self.accepted.insert(from);
            // generate `MChosen` if we have enough accepts
            if self.accepted.len() == self.f + 1 {
                //
            }
        }
        None
    }
}

// the first component is the ballot in which the value (the second component) was accepted.
type Accepted<Value> = (Ballot, Value);

struct Acceptor<Value> {
    ballot: Ballot,
    accepted: Accepted<Value>,
}

impl<V> Acceptor<V>
where
    V: Default + Clone,
{
    fn new() -> Self {
        Self {
            ballot: 0,
            accepted: (0, V::default()),
        }
    }

    // Set the consensus value if no value has been accepted yet.
    fn maybe_set_value<F>(&mut self, value_gen: F) -> bool
    where
        F: FnOnce() -> V,
    {
        if self.ballot == 0 {
            self.accepted = (0, value_gen());
            true
        } else {
            false
        }
    }

    // Returns the ballot that the acceptor is currently in.
    fn current_ballot(&self) -> Ballot {
        self.ballot
    }

    // The reply to this prepare request contains:
    // - a promise to never accept a proposal numbered less than `b`
    // - the proposal accepted with the highest number less than `b`, if any
    fn handle_prepare(&mut self, b: Ballot) -> Option<SynodMessage<V>> {
        if b > self.ballot {
            // update current ballot
            self.ballot = b;
            // create promise message
            let promise = SynodMessage::MPromise(b, self.accepted.clone());
            Some(promise)
        } else {
            None
        }
    }

    fn handle_accept(&mut self, b: Ballot, value: V) -> Option<SynodMessage<V>> {
        if b >= self.ballot {
            // update current ballot
            self.ballot = b;
            // update the accepted value
            self.accepted = (b, value);
            // create accepted message
            let accepted = SynodMessage::MAccepted(b);
            Some(accepted)
        } else {
            None
        }
    }

    // pub fn accept(&mut self, b: u64, previous: (u64, V)) -> Option<u64> {
    //     if remote_ballot >= self.ballot {
    //         // update current ballot
    //         self.ballot = remote_ballot;
    //         // update accepted ballot and value
    //         self.accepted = Some((remote_ballot, value));
    //         // return accepted ballot
    //         Some(remote_ballot)
    //     } else {
    //         None
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synod_flow() {
        let mut synod = Synod::new();

        synod.prepare(10);
        synod.accept(10, String::from("A"));
    }
}
