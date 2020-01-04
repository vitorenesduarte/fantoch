use crate::id::ProcessId;
use std::collections::{HashMap, HashSet};
use std::mem;

type Ballot = u64;

/// Implementation of Flexible single-decree Paxos in which:
/// - phase-1 waits for n - f promises
/// - phase-2 waits for f + 1 accepts
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SynodMessage<V> {
    // messages to acceptor
    MPrepare(Ballot),
    MAccept(Ballot, V),
    // messages to proposer
    MPromise(Ballot, Accepted<V>),
    MAccepted(Ballot),
    // message to be handled by user of this module
    MChosen(V),
}

#[derive(Clone)]
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
    /// Creates a new Synod instance.
    /// After executing phase-1, if the proposer sees that no proposal has been accepted before, it
    /// resorts to the proposal generator to compute a new consensus proposal given all values
    /// reported by the phase-1 quorum. We know that none of the values reported were accepted
    /// because they're tagged with ballot 0.
    pub fn new(
        process_id: ProcessId,
        n: usize,
        f: usize,
        proposal_gen: fn(HashMap<ProcessId, V>) -> V,
    ) -> Self {
        Self {
            proposer: Proposer::new(process_id, n, f, proposal_gen),
            acceptor: Acceptor::new(),
        }
    }

    /// Set the consensus value if no value has been accepted yet (i.e. ballot is still 0). If the
    /// value was successfully changed, `true` is returned and `false` otherwise.
    #[must_use]
    pub fn maybe_set_value<F>(&mut self, value_gen: F) -> bool
    where
        F: FnOnce() -> V,
    {
        self.acceptor.maybe_set_value(value_gen)
    }

    /// Returns the current consensus value (not necessarily accepted).
    pub fn value(&self) -> &V {
        self.acceptor.value()
    }

    /// Creates a new prepare message with a ballot onwed by this process. This ballot is greater
    /// than any ballot seen the by local acceptor agent.
    /// Feeding the proposer with the highest ballot seen by the acceptor increases the likelyhood
    /// of having this new prepare accepted. And since the created Prepare will be delivered
    /// imediately at the local acceptor, this ensures that the ballots created are unique.
    ///
    /// TODO how do we ensure that the prepare is delivered immediately?
    pub fn new_prepare(&mut self) -> SynodMessage<V> {
        self.proposer.new_prepare(&self.acceptor)
    }

    /// Handles `SynodMessage`s generated by this `Synod` module by forwarding them to the proper
    /// Paxos agent (i.e. either the proposer or the acceptor). All messages should be handled
    /// here with the exeception of the `SynodMessage::MChosen` that should be handled outside.
    pub fn handle(&mut self, from: ProcessId, msg: SynodMessage<V>) -> Option<SynodMessage<V>> {
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

type Promises<V> = HashMap<ProcessId, Accepted<V>>;
type Accepts = HashSet<ProcessId>;
type Proposal<V> = Option<V>;

#[derive(Clone)]
struct Proposer<V> {
    // process identifier
    process_id: ProcessId,
    // number of processes
    n: usize,
    // maximum number of allowed failures
    f: usize,
    // ballot used in prepare
    ballot: Ballot,
    // proposal generator that will be used once enough promises have been collected
    proposal_gen: fn(HashMap<ProcessId, V>) -> V,
    // what follows is paper-slip state:
    // - promises: mapping from phase-1 quorum processes to the values in their promises
    // - accepts: set of processes that have accepted a proposal
    // - proposal: proposal generated by the proposal generator
    promises: Promises<V>,
    accepts: Accepts,
    proposal: Proposal<V>,
}

impl<V> Proposer<V>
where
    V: Clone + Default,
{
    /// Creates a new proposer.
    fn new(
        process_id: ProcessId,
        n: usize,
        f: usize,
        proposal_gen: fn(HashMap<ProcessId, V>) -> V,
    ) -> Self {
        Self {
            process_id,
            n,
            f,
            ballot: 0,
            proposal_gen,
            promises: HashMap::new(),
            accepts: HashSet::new(),
            proposal: None,
        }
    }

    /// Generates a new prepare. See top-level docs (in `Synod`) for more info.
    fn new_prepare(&mut self, acceptor: &Acceptor<V>) -> SynodMessage<V> {
        // the acceptor's ballot should be at least as high as the proposer's ballot (if this is not
        // the case, it's because prepare messages are not delivered locally immediately)
        assert!(acceptor.ballot() >= self.ballot);

        // generate the next ballot
        self.next_ballot(acceptor.ballot());

        // the new ballot should be higher than the acceptor's ballot
        assert!(acceptor.ballot() < self.ballot);

        // reset paper-slip state
        self.reset_state();

        // create prepare message
        let prepare = SynodMessage::MPrepare(self.ballot);
        prepare
    }

    /// Changes the ballot to a ballot owned by this proposer. This new ballot is higher than the
    /// ballot the acceptor is currently in, which should increase the likelyhood of this ballot
    /// being accepted by other acceptors.
    fn next_ballot(&mut self, acceptor_current_ballot: Ballot) {
        // get number of processes
        let n = self.n as u64;
        // compute "round" of current ballot
        let round = acceptor_current_ballot / n;
        // compute the next "round"
        let next_round = round + 1;
        // compute ballot owned by this process in the next round
        self.ballot = self.process_id + n * next_round;
    }

    /// Resets the local (paper-slip) state (promises received, accepts received, and proposal
    /// sent), returning the previous value of promises and proposal.
    fn reset_state(&mut self) -> (Promises<V>, Proposal<V>) {
        // reset promises
        let mut promises = HashMap::new();
        mem::swap(&mut promises, &mut self.promises);

        // reset accepts
        self.accepts = HashSet::new();

        // reset proposal
        let mut proposal = None;
        mem::swap(&mut proposal, &mut self.proposal);

        // return previous promises and proposal
        (promises, proposal)
    }

    fn handle_promise(
        &mut self,
        from: ProcessId,
        b: Ballot,
        accepted: Accepted<V>,
    ) -> Option<SynodMessage<V>> {
        // check if it's a promise about the current ballot (so that we only process promises about
        // the current ballot)
        if self.ballot == b {
            // if yes, update set of promises
            self.promises.insert(from, accepted);

            // check if we have enough (i.e. n - f) promises
            if self.promises.len() == self.n - self.f {
                // if we do, check if any value has been accepted before:
                // - if yes, select the value accepted at the highest ballot
                // - if not, generate proposal using the generator

                // reset state and get promises
                let (mut promises, _) = self.reset_state();

                // compute the proposal accepted at the highest ballot
                let (highest_ballot, from) = promises
                    .iter()
                    // get highest proposal
                    .max_by_key(|(_process, (ballot, _value))| ballot)
                    // extract ballot and process
                    .map(|(process, (ballot, _))| (*ballot, *process))
                    .expect("there should n - f promises, and thus, there's a highest value");

                // compute our proposal depending on whether there was a previously accepted
                // proposal
                let proposal = if highest_ballot == 0 {
                    // if the highest ballot is 0, use the proposal generator to generate
                    // anything we want
                    // TODO do we need to collect here? also, maybe we could simply upstream the
                    // ballots, even though they're all 0
                    let values = promises
                        .into_iter()
                        .map(|(process, (_ballot, value))| (process, value))
                        .collect();
                    (self.proposal_gen)(values)
                } else {
                    // otherwise, we must propose the value accepted at the highest ballot
                    // TODO this scheme of removing the value from `promises` prevents cloning
                    // the value when we only have a reference to it; is there a better way?
                    promises.remove(&from).map(|(_ballot, value)| value).expect(
                        "a promise from this process must exists as it was the highest promise",
                    )
                };

                // save the proposal
                self.proposal = Some(proposal.clone());

                // create accept message
                let accept = SynodMessage::MAccept(b, proposal);
                return Some(accept);
            }
        }
        None
    }

    fn handle_accepted(&mut self, from: ProcessId, b: Ballot) -> Option<SynodMessage<V>> {
        // check if it's an accept about the current ballot (so that we only process accepts about
        // the current ballot)
        if self.ballot == b {
            // if yes, update set of accepts
            self.accepts.insert(from);

            // check if we have enough (i.e. f + 1) accepts
            if self.accepts.len() == self.f + 1 {
                // if we do, our proposal can be chosen

                // reset state and get proposal
                let (_, proposal) = self.reset_state();

                // extract proposal
                // TODO could the proposal be the value that is currently stored in the local
                // acceptor, or could that value have been overwritten in the meantime?
                let proposal =
                    proposal.expect("there should have been proposal before a value can be chosen");

                // create chosen message
                let chosen = SynodMessage::MChosen(proposal);
                return Some(chosen);
            }
        }
        None
    }
}

// The first component is the ballot in which the value (the second component) was accepted.
// If the ballot is 0, the value has not been accepted yet.
type Accepted<Value> = (Ballot, Value);

#[derive(Clone)]
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

    // Retrieves consensus value (not necessarily accepted).
    fn value(&self) -> &V {
        let (_, v) = &self.accepted;
        v
    }

    // Returns the ballot that the acceptor is currently in.
    fn ballot(&self) -> Ballot {
        self.ballot
    }

    // The reply to this prepare request contains:
    // - a promise to never accept a proposal numbered less than `b`
    // - the proposal accepted with the highest number less than `b`, if any
    fn handle_prepare(&mut self, b: Ballot) -> Option<SynodMessage<V>> {
        // since we need to promise that we won't accept any proposal numbered less then `b`,
        // there's no point in letting such proposal be prepared, and so, we ignore such prepares
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
}

#[cfg(test)]
mod tests {
    use super::*;

    // generate proposals by multiplying all the values reported by phase-1 quorum processes
    fn proposal_gen(values: HashMap<ProcessId, u64>) -> u64 {
        values.into_iter().map(|(_, v)| v).fold(1, |acc, v| acc * v)
    }

    #[test]
    fn synod_flow() {
        // n and f
        let n = 5;
        let f = 1;

        // create all synods
        let mut synod_1 = Synod::new(1, n, f, proposal_gen);
        let mut synod_2 = Synod::new(2, n, f, proposal_gen);
        let mut synod_3 = Synod::new(3, n, f, proposal_gen);
        let mut synod_4 = Synod::new(4, n, f, proposal_gen);
        let mut synod_5 = Synod::new(5, n, f, proposal_gen);

        // check it's possible to set values (as ballots are still 0), and check value
        assert!(synod_1.maybe_set_value(|| 2));
        assert_eq!(synod_1.value(), &2);
        assert!(synod_2.maybe_set_value(|| 3));
        assert_eq!(synod_2.value(), &3);
        assert!(synod_3.maybe_set_value(|| 5));
        assert_eq!(synod_3.value(), &5);
        assert!(synod_4.maybe_set_value(|| 7));
        assert_eq!(synod_4.value(), &7);
        assert!(synod_5.maybe_set_value(|| 11));
        assert_eq!(synod_5.value(), &11);

        // again
        assert!(synod_1.maybe_set_value(|| 13));
        assert_eq!(synod_1.value(), &13);

        // synod 1: generate prepare
        let prepare = synod_1.new_prepare();

        // it's still possible to set the value as the prepare has not been handled
        assert!(synod_1.maybe_set_value(|| 2));
        assert_eq!(synod_1.value(), &2);

        // handle the prepare at n - f processes, including synod 1
        let promise_1 = synod_1
            .handle(1, prepare.clone())
            .expect("there should a promise from 1");
        let promise_2 = synod_2
            .handle(1, prepare.clone())
            .expect("there should a promise from 2");
        let promise_3 = synod_3
            .handle(1, prepare.clone())
            .expect("there should a promise from 3");
        let promise_4 = synod_4
            .handle(1, prepare.clone())
            .expect("there should a promise from 4");

        // check it's no longer possible to set the value
        assert!(!synod_1.maybe_set_value(|| 13));
        assert_eq!(synod_1.value(), &2);

        // synod 1: handle promises
        let result = synod_1.handle(1, promise_1);
        assert!(result.is_none());
        let result = synod_1.handle(2, promise_2);
        assert!(result.is_none());
        let result = synod_1.handle(3, promise_3);
        assert!(result.is_none());
        // only in the last one there should be an accept message
        let accept = synod_1
            .handle(4, promise_4)
            .expect("there should an accept message");

        // handle the accept at f + 1 processes, including synod 1
        let accepted_1 = synod_1
            .handle(1, accept.clone())
            .expect("there should an accept from 1");
        let accepted_5 = synod_5
            .handle(1, accept.clone())
            .expect("there should an accept from 5");

        // synod 1: handle accepts
        let result = synod_1.handle(1, accepted_1);
        assert!(result.is_none());
        let chosen = synod_1
            .handle(5, accepted_5)
            .expect("there should be a chosen message");

        // check that 210 (2 * 3 * 5 * 7 * 11, i.e. the ballot-0 values from phase-1
        // processes) was chosen
        assert_eq!(chosen, SynodMessage::MChosen(210));
    }

    #[test]
    fn synod_prepare_with_lower_ballot_fails() {
        // n and f
        let n = 3;
        let f = 1;

        // create all synods
        let mut synod_1 = Synod::new(1, n, f, proposal_gen);
        let mut synod_2 = Synod::new(2, n, f, proposal_gen);
        let mut synod_3 = Synod::new(3, n, f, proposal_gen);

        // synod 1 and 3: generate prepare
        let prepare_a = synod_1.new_prepare();
        let prepare_c = synod_3.new_prepare();

        // handle the prepare_a at synod 1
        synod_1
            .handle(1, prepare_a.clone())
            .expect("there should a promise from 1");

        // handle the prepare_c at synod 3
        synod_3
            .handle(3, prepare_c.clone())
            .expect("there should a promise from 3");

        // handle the prepare_c at synod 2
        synod_2
            .handle(3, prepare_c.clone())
            .expect("there should a promise from 2");

        // handle the prepare_a at synod 2
        let result = synod_2.handle(1, prepare_a.clone());
        // there should be no promise from synod 2
        assert!(result.is_none());
    }

    #[test]
    fn synod_recovery() {
        // n and f
        let n = 3;
        let f = 1;

        // create all synods
        let mut synod_1 = Synod::new(1, n, f, proposal_gen);
        let mut synod_2 = Synod::new(2, n, f, proposal_gen);
        let mut synod_3 = Synod::new(3, n, f, proposal_gen);

        // set values at all synods
        assert!(synod_1.maybe_set_value(|| 2));
        assert_eq!(synod_1.value(), &2);
        assert!(synod_2.maybe_set_value(|| 3));
        assert_eq!(synod_2.value(), &3);
        assert!(synod_3.maybe_set_value(|| 5));
        assert_eq!(synod_3.value(), &5);

        // synod 1: generate prepare
        let prepare = synod_1.new_prepare();

        // handle the prepare at synod 1
        let promise_1 = synod_1
            .handle(1, prepare.clone())
            .expect("there should a promise from 1");

        // handle the prepare at synod 2
        let promise_2 = synod_2
            .handle(1, prepare.clone())
            .expect("there should a promise from 2");

        // synod 1: handle promises
        let result = synod_1.handle(1, promise_1);
        assert!(result.is_none());
        // only in the last one there should be an accept message
        let accept = synod_1
            .handle(2, promise_2)
            .expect("there should an accept message");

        // check the value in the accept
        if let SynodMessage::MAccept(ballot, value) = accept {
            assert_eq!(ballot, 4); // 8 is the ballot from round-1 (n=3 * round=1 + id=1) that belongs to process 1
            assert_eq!(value, 6); // 2 * 3, the values stored by processes 1 and 2
        } else {
            panic!("process 1 should have generated an accept")
        }

        // handle the accept only at synod 1
        synod_1
            .handle(1, accept)
            .expect("there should an accept from 1");

        // at this point, if another process tries to recover, there are two possible situations:
        // - if process 1 is part of that phase-1 quorum, this new process needs to propose the same
        //   value that was proposed by 1 (i.e. 6)
        // - if process 2 is *not* part of that phase-1 quorum, this new process can propose
        //   anything it wants; this value will be 15, i.e. the multiplication of the values stored
        //   by processes 2 and 3

        // start recovery by synod 2
        let prepare = synod_2.new_prepare();

        // handle prepare at synod 2
        let promise_2 = synod_2
            .handle(2, prepare.clone())
            .expect("there should be a promise from 2");

        // synod 2: handle promise by 2
        let result = synod_2.handle(2, promise_2);
        assert!(result.is_none());

        // check case 1
        case_1(prepare.clone(), synod_1.clone(), synod_2.clone());

        // check case 2
        case_2(prepare.clone(), synod_2.clone(), synod_3.clone());

        // in this case, the second prepare is handled by synod 1
        fn case_1(prepare: SynodMessage<u64>, mut synod_1: Synod<u64>, mut synod_2: Synod<u64>) {
            // handle prepare at synod 1
            let promise_1 = synod_1
                .handle(2, prepare.clone())
                .expect("there should be a promise from 1");

            // synod 2: handle promise from 1
            let accept = synod_2
                .handle(1, promise_1)
                .expect("there should an accept message");

            // check the value in the accept
            if let SynodMessage::MAccept(ballot, value) = accept {
                assert_eq!(ballot, 8); // 8 is the ballot from round-2 (n=3 * round=2 + id=2) that belongs to process 2
                assert_eq!(value, 6); // the value proposed by process 1
            } else {
                panic!("process 2 should have generated an accept")
            }
        }

        // in this case, the second prepare is handled by synod 3
        fn case_2(prepare: SynodMessage<u64>, mut synod_2: Synod<u64>, mut synod_3: Synod<u64>) {
            // handle prepare at synod 3
            let promise_3 = synod_3
                .handle(2, prepare.clone())
                .expect("there should be a promise from 3");

            // synod 2: handle promise from 3
            let accept = synod_2
                .handle(3, promise_3)
                .expect("there should an accept message");

            // check the value in the accept
            if let SynodMessage::MAccept(ballot, value) = accept {
                assert_eq!(ballot, 8); // 8 is the ballot from round-2 (n=3 * round=2 + id=2) that belongs to process 2
                assert_eq!(value, 15); // 3 * 5, the values stored by processes 2 and 3
            } else {
                panic!("process 2 should have generated an accept")
            }
        }
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use std::cell::RefCell;
    use std::convert::TryInto;

    // number of processes and tolerated faults
    const N: usize = 5;
    const F: usize = 2;
    // quorum size:
    // - since we consider that f = 2, the quorum size is 3
    const Q: usize = 3;

    // a list of pairs where the second component indicates whether the reply from the first
    // component is lost (if true) or not (if false)
    type Quorum = Vec<(ProcessId, bool)>;

    #[derive(Clone, Debug)]
    struct Action {
        source: ProcessId, // either 1 or 2
        q1: Quorum,
        q2: Quorum,
    }

    fn bound_id(id: ProcessId, bound: usize) -> ProcessId {
        // make sure ids are between 1 and `bound`
        id % (bound as u64) + 1
    }

    impl Arbitrary for Action {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            // generate source: either 1 or 2
            let source: ProcessId = Arbitrary::arbitrary(g);
            let source = bound_id(source, 2);

            // generate q1 and q2
            let q1 = arbitrary_quorum(source, g);
            let q2 = arbitrary_quorum(source, g);

            // return action
            Self { source, q1, q2 }
        }
        // actions can't be shriked
        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new(std::iter::empty::<Self>())
        }
    }

    // generate a quorum of size `Q` (`Q - 1` actually as `from` is always part of the quorum)
    fn arbitrary_quorum<G: Gen>(source: ProcessId, g: &mut G) -> Quorum {
        // compute expected size
        let expected_size: usize = (Q - 1)
            .try_into()
            .expect("it should be possible to subtract 1 as the quorum size is non-zero");

        // ids of processes in the quorum
        let mut ids = HashSet::new();

        // loop while we don't generate a quorum with the expected size
        while ids.len() < expected_size {
            // generate random id
            let process: ProcessId = Arbitrary::arbitrary(g);
            let process = bound_id(process, N);

            // add process if not source
            if process != source {
                ids.insert(process);
            }
        }
        // for each quorum process, generate whether its reply will be lost or not
        ids.into_iter()
            .map(|id| {
                let reply_lost = Arbitrary::arbitrary(g);
                (id, reply_lost)
            })
            .collect()
    }

    type ConsensusValue = u64;
    type Synods = HashMap<ProcessId, RefCell<Synod<ConsensusValue>>>;

    // generate proposals by multiplying all the values reported by phase-1 quorum processes
    fn proposal_gen(values: HashMap<ProcessId, ConsensusValue>) -> ConsensusValue {
        values.into_iter().map(|(_, v)| v).fold(1, |acc, v| acc * v)
    }

    fn create_synods() -> Synods {
        // create ids and their initial values
        let data = vec![(1, 2), (2, 3), (3, 5), (4, 7), (5, 11)];

        // create synods
        data.into_iter()
            .map(|(id, value)| {
                // get id
                let id = id as u64;
                // create synod
                let mut synod = Synod::new(id, N, F, proposal_gen);
                // set its value
                assert!(synod.maybe_set_value(|| value));
                (id, RefCell::new(synod))
            })
            .collect()
    }

    #[quickcheck]
    fn a_single_value_is_chosen(actions: Vec<Action>) -> bool {
        fn do_action(action: Action, synods: &Synods, chosen_values: &mut HashSet<ConsensusValue>) {
            // get source
            let source = action.source;

            // get synod
            let mut synod = synods
                .get(&source)
                .expect("synod with such id should exist")
                .borrow_mut();

            // create prepare
            let prepare = synod.new_prepare();

            // handle it locally
            let local_promise = synod
                .handle(source, prepare.clone())
                .expect("local promises should always be generated");
            synod.handle(source, local_promise);

            // handle it in all `q1`
            let outcome = handle_in_quorum(source, &mut synod, synods, prepare, &action.q1);

            // check if phase-1 ended
            if outcome.len() == 1 {
                // if yes, start phase-2
                let accept = &outcome[0];

                // handle it locally
                let local_accept = synod
                    .handle(source, accept.clone())
                    .expect("local accepts should always be generated");
                synod.handle(source, local_accept);

                // handle msg in all `q2`
                let outcome =
                    handle_in_quorum(source, &mut synod, synods, accept.clone(), &action.q2);

                // check if phase-2 ended
                if outcome.len() == 1 {
                    // if yes, save chosen value
                    if let SynodMessage::MChosen(value) = outcome[0] {
                        chosen_values.insert(value);
                    }
                }
            }
        }

        fn handle_in_quorum(
            source: ProcessId,
            synod: &mut Synod<ConsensusValue>,
            synods: &Synods,
            msg: SynodMessage<ConsensusValue>,
            quorum: &Quorum,
        ) -> Vec<SynodMessage<ConsensusValue>> {
            quorum
                .iter()
                .filter_map(|(dest, reply_lost)| {
                    // get dest synod
                    let mut dest_synod = synods
                        .get(&dest)
                        .expect("synod with such id should exist")
                        .borrow_mut();
                    // handle msg in destination
                    let reply = dest_synod.handle(source, msg.clone());

                    // check if there's a reply
                    if let Some(reply) = reply {
                        // if yes and reply shouldn't be lost, handle it
                        if !reply_lost {
                            return synod.handle(*dest, reply);
                        }
                    }
                    None
                })
                .collect()
        }

        // create synods
        let synods = create_synods();

        // set with all chosen values:
        // - if in the end this set has more than one value, there's a bug
        let mut chosen_values = HashSet::new();

        actions.into_iter().for_each(|action| {
            do_action(action, &synods, &mut chosen_values);
        });

        // we're good if there was at most one chosen value
        chosen_values.len() <= 1
    }
}
