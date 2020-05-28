use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::ProcessId;
use fantoch::protocol::{Action, Protocol};
use fantoch::time::RunTime;
use stateright::actor::{Actor, Event, Id, InitIn, NextIn, Out};
use std::marker::PhantomData;

struct ProtocolActor<P: Protocol> {
    config: Config,
    _phantom: PhantomData<P>,
}

impl<P> ProtocolActor<P>
where
    P: Protocol,
{
    pub fn new(config: Config) -> Self {
        Self {
            config,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone)]
struct ProtocolActorState<P: Protocol> {
    protocol: P,
    executor: <P as Protocol>::Executor,
}

#[derive(Debug)]
enum KV<M> {
    Access(Command),
    Internal(M),
}

fn process_id(id: Id) -> ProcessId {
    usize::from(id) as ProcessId
}

impl<P> Actor for ProtocolActor<P>
where
    P: Protocol,
{
    type Msg = KV<<P as Protocol>::Message>;
    type State = ProtocolActorState<P>;

    fn init(i: InitIn<Self>, o: &mut Out<Self>) {
        // fetch id and config
        let process_id: ProcessId = usize::from(i.id) as ProcessId;
        let config = i.context.config;

        // our ids range from 1..n
        assert!(process_id > 0);

        // create protocol
        let (protocol, _periodic_events) = P::new(process_id, config);

        // TODO:
        // - discover
        // - periodic events

        // create executor
        let executor = <<P as Protocol>::Executor>::new(process_id, config);

        // set actor state
        let state = ProtocolActorState { protocol, executor };
        o.set_state(state);
    }

    fn next(i: NextIn<Self>, o: &mut Out<Self>) {
        // get current protocol state
        let mut state = i.state.clone();

        // get msg received
        let Event::Receive(from, msg) = i.event;
        let from = process_id(from);

        // handle msg
        match msg {
            KV::Access(cmd) => Self::handle_submit(cmd, &mut state),
            KV::Internal(msg) => Self::handle_msg(from, msg, &mut state),
        }

        // set new protocol state
        o.set_state(state);
    }
}

impl<P> ProtocolActor<P>
where
    P: Protocol,
{
    fn handle_submit(cmd: Command, state: &mut ProtocolActorState<P>) {
        let actions = state.protocol.submit(None, cmd, &RunTime);
        Self::handle_actions(actions, state);
    }

    fn handle_msg(
        from: ProcessId,
        msg: P::Message,
        state: &mut ProtocolActorState<P>,
    ) {
        let actions = state.protocol.handle(from, msg, &RunTime);
        Self::handle_actions(actions, state);
    }

    fn handle_actions(
        actions: Vec<Action<P>>,
        state: &mut ProtocolActorState<P>,
    ) {
        // get the id of this process
        let process_id = state.protocol.id();

        for action in actions {
            match action {
                Action::ToSend { msg, target } => {
                    todo!("send to peers");
                }
                Action::ToForward { msg } => {
                    // there's a single worker, so just handle it locally
                    Self::handle_msg(process_id, msg, state);
                }
            }
        }
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
        let _ = ProtocolActor::<Basic>::new(config);
    }
}
