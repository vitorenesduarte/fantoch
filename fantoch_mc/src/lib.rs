use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::ProcessId;
use fantoch::protocol::{Action, Protocol};
use fantoch::time::RunTime;
use fantoch::util;
use fantoch::{HashMap, HashSet};
use stateright::actor::{Actor, Id};
use std::marker::PhantomData;

const SHARD_ID: u64 = 0;

pub struct ProtocolActor<P: Protocol> {
    config: Config,
    topology: HashMap<ProcessId, Vec<ProcessId>>,
    _phantom: PhantomData<P>,
}

impl<P> ProtocolActor<P>
where
    P: Protocol,
{
    pub fn new(
        config: Config,
        topology: HashMap<ProcessId, Vec<ProcessId>>,
    ) -> Self {
        Self::check_topology(config.n(), topology.clone());
        Self {
            config,
            topology,
            _phantom: PhantomData,
        }
    }

    fn check_topology(n: usize, topology: HashMap<ProcessId, Vec<ProcessId>>) {
        let ids = Self::usort(util::process_ids(SHARD_ID, n));
        let keys = Self::usort(topology.keys().cloned());
        assert_eq!(ids, keys);
        for peers in topology.values() {
            let peers = Self::usort(peers.iter().cloned());
            assert_eq!(ids, peers);
        }
    }

    fn usort<I>(ids: I) -> Vec<ProcessId>
    where
        I: Iterator<Item = ProcessId>,
    {
        let mut ids: Vec<_> = ids.collect();
        ids.sort();
        ids.dedup();
        ids
    }
}

#[derive(Clone)]
pub struct ProtocolActorState<P: Protocol> {
    protocol: P,
    executor: <P as Protocol>::Executor,
}

#[derive(Clone, Debug)]
pub enum KV<M> {
    Access(Command),
    Internal(M),
}

fn to_process_id(id: Id) -> ProcessId {
    usize::from(id) as ProcessId
}

fn from_process_id(id: ProcessId) -> Id {
    Id::from(id as usize)
}

impl<P> Actor for ProtocolActor<P>
where
    P: Protocol,
{
    type Msg = KV<<P as Protocol>::Message>;
    type State = ProtocolActorState<P>;
}

//     fn init(i: InitIn<Self>, o: &mut Out<Self>) {
//         // fetch id and config
//         let process_id: ProcessId = usize::from(i.id) as ProcessId;
//         let config = i.context.config;

//         // our ids range from 1..n
//         assert!(process_id > 0);

//         // create protocol
//         let (mut protocol, periodic_events) = P::new(process_id, config);

//         if !periodic_events.is_empty() {
//             todo!("schedule periodic events: {:?}", periodic_events);
//         }

//         // discover peers
//         let peers = i
//             .context
//             .topology
//             .get(&process_id)
//             .cloned()
//             .expect("each process should have a set of peers");
//         protocol.discover(peers);

//         // create executor
//         let executor = <<P as Protocol>::Executor>::new(process_id, config);

//         // set actor state
//         let state = ProtocolActorState { protocol, executor };
//         o.set_state(state);
//     }

//     fn next(i: NextIn<Self>, o: &mut Out<Self>) {
//         // get current protocol state
//         let mut state = i.state.clone();

//         // get msg received
//         let Event::Receive(from, msg) = i.event;
//         let from = to_process_id(from);

//         // handle msg
//         let to_sends = match msg {
//             KV::Access(cmd) => Self::handle_submit(cmd, &mut state),
//             KV::Internal(msg) => Self::handle_msg(from, msg, &mut state),
//         };

//         // send new messages
//         for (recipients, msg) in to_sends {
//             let recipients: Vec<_> =
//                 recipients.into_iter().map(from_process_id).collect();
//             let msg = KV::Internal(msg);
//             o.broadcast(&recipients, &msg);
//         }

//         // set new protocol state
//         o.set_state(state);
//     }
// }

impl<P> ProtocolActor<P>
where
    P: Protocol,
{
    #[must_use]
    fn handle_submit(
        cmd: Command,
        state: &mut ProtocolActorState<P>,
    ) -> Vec<(HashSet<ProcessId>, P::Message)> {
        state.protocol.submit(None, cmd, &RunTime);
        Self::handle_to_executors(state);
        Self::handle_to_processes(state)
    }

    #[must_use]
    fn handle_msg(
        from: ProcessId,
        msg: P::Message,
        state: &mut ProtocolActorState<P>,
    ) -> Vec<(HashSet<ProcessId>, P::Message)> {
        // handle message
        state.protocol.handle(from, SHARD_ID, msg, &RunTime);
        Self::handle_to_executors(state);
        Self::handle_to_processes(state)
    }

    fn handle_to_executors(state: &mut ProtocolActorState<P>) {
        // handle new execution info
        for execution_info in state.protocol.to_executors_iter() {
            state.executor.handle(execution_info, &RunTime);
            // assert that there's nothing to other executors (since we're
            // assuming full replication (and only Janus needs that in partial
            // replication))
            assert!(state.executor.to_executors().is_none());
            for executor_result in state.executor.to_clients_iter() {
                todo!("send result to client: {:?}", executor_result);
            }
        }
    }

    #[must_use]
    fn handle_to_processes(
        state: &mut ProtocolActorState<P>,
    ) -> Vec<(HashSet<ProcessId>, P::Message)> {
        // get the id of this process
        let process_id = state.protocol.id();
        let actions: Vec<_> = state.protocol.to_processes_iter().collect();

        // handle all new actions
        actions
            .into_iter()
            .flat_map(|action| {
                match action {
                    Action::ToSend { msg, mut target } => {
                        if target.remove(&process_id) {
                            // handle message locally, if message also to self,
                            // and remove self from target
                            let mut to_sends = Self::handle_msg(
                                process_id,
                                msg.clone(),
                                state,
                            );
                            to_sends.push((target, msg));
                            to_sends
                        } else {
                            vec![(target, msg)]
                        }
                    }
                    Action::ToForward { msg } => {
                        // there's a single worker, so just handle it locally
                        Self::handle_msg(process_id, msg, state)
                    }
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::protocol::Basic;

    #[test]
    fn it_works() {
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);
        let mut topology = HashMap::new();
        topology.insert(1, vec![1, 2, 3]);
        topology.insert(2, vec![2, 3, 1]);
        topology.insert(3, vec![3, 1, 2]);
        let _ = ProtocolActor::<Basic>::new(config, topology);
    }
}
