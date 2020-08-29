use crate::log;
use fantoch::command::Command;
use fantoch::id::{Dot, ProcessId};
use fantoch::protocol::{Action, BaseProcess, Protocol};
use fantoch::singleton;
use fantoch::HashSet;
use std::fmt::Debug;

pub fn submit_actions<P>(
    bp: &BaseProcess,
    dot: Dot,
    cmd: &Command,
    target_shard: bool,
    create_mforward_submit: impl Fn(Dot, Command) -> <P as Protocol>::Message,
    to_processes: &mut Vec<Action<P>>,
) where
    P: Protocol,
{
    if target_shard {
        // create forward submit messages if:
        // - we're the target shard (i.e. the shard to which the client sent the
        //   command)
        // - command touches more than one shard
        let my_shard_id = bp.shard_id;
        for shard_id in
            cmd.shards().filter(|shard_id| **shard_id != my_shard_id)
        {
            let mforward_submit = create_mforward_submit(dot, cmd.clone());
            let target = singleton![bp.closest_process(shard_id)];
            to_processes.push(Action::ToSend {
                target,
                msg: mforward_submit,
            })
        }
    }
}

pub fn mcommit_actions<P, I, D1, D2>(
    bp: &BaseProcess,
    shards_commits: &mut Option<ShardsCommits<I>>,
    shard_count: usize,
    dot: Dot,
    data1: D1,
    data2: D2,
    create_mcommit: impl FnOnce(Dot, D1, D2) -> <P as Protocol>::Message,
    create_mshard_commit: impl FnOnce(Dot, D1) -> <P as Protocol>::Message,
    update_shards_commits_info: impl FnOnce(&mut I, D2),
    to_processes: &mut Vec<Action<P>>,
) where
    P: Protocol,
    I: Default + Debug,
{
    match shard_count {
        1 => {
            // create `MCommit`
            final_mcommit_action(
                bp,
                dot,
                data1,
                data2,
                create_mcommit,
                to_processes,
            )
        }
        _ => {
            // if the command accesses more than one shard, send an
            // MCommitShard to the process in the shard targetted by the
            // client; this process will then aggregate all the MCommitShard
            // and send an MCommitShardAggregated back once it receives an
            // MCommitShard from each shard; assuming that all
            // shards take the fast path, this approach should work well; if
            // there are slow paths, we probably want to disseminate each
            // shard commit clock to all participants so that detached votes
            // are generated ASAP; with n = 3 or f = 1, this is not a
            // problem since we'll always take the fast path
            // - TODO: revisit this approach once we implement recovery for
            //   partial replication

            // initialize shards commit info if not yet initialized:
            // - it may already be initialized if we receive the `MCommitShard`
            //   from another shard before we were able to commit the command in
            //   our own shard
            let shards_commits =
                init_shards_commits(shards_commits, bp.process_id, shard_count);

            // update shards commit info
            shards_commits.update(|shards_commit_info| {
                update_shards_commits_info(shards_commit_info, data2)
            });

            // create `MShardCommit`
            let mshard_commit = create_mshard_commit(dot, data1);
            // the aggregation with occurs at the process in targetted shard
            // (which is the owner of the commmand's `dot`)
            let target = singleton!(dot.source());
            to_processes.push(Action::ToSend {
                target,
                msg: mshard_commit,
            });
        }
    }
}

pub fn handle_mshard_commit<P, I, D1>(
    bp: &BaseProcess,
    shards_commits: &mut Option<ShardsCommits<I>>,
    shard_count: usize,
    from: ProcessId,
    dot: Dot,
    data: D1,
    add_shards_commits_info: impl FnOnce(&mut I, D1),
    create_mshard_aggregated_commit: impl FnOnce(
        Dot,
        &I,
    ) -> <P as Protocol>::Message,
    to_processes: &mut Vec<Action<P>>,
) where
    P: Protocol,
    I: Default + Debug,
{
    // make sure shards commit info is initialized:
    // - it may not be if we receive the `MCommitShard` from another shard
    //   before we were able to commit the command in our own shard
    let shards_commits =
        init_shards_commits(shards_commits, bp.process_id, shard_count);

    // add new clock, checking if we have received all clocks
    let done = shards_commits.add(from, |shards_commits_info| {
        add_shards_commits_info(shards_commits_info, data)
    });
    if done {
        // create `MShardAggregatedCommit`
        let mshard_aggregated_commit =
            create_mshard_aggregated_commit(dot, &shards_commits.info);
        let target = shards_commits.participants.clone();

        // save new action
        to_processes.push(Action::ToSend {
            target,
            msg: mshard_aggregated_commit,
        });
    }
}

pub fn handle_mshard_aggregated_commit<P, I, D1, D2>(
    bp: &BaseProcess,
    shards_commits: &mut Option<ShardsCommits<I>>,
    dot: Dot,
    data1: D1,
    extract_mcommit_extra_data: impl FnOnce(I) -> D2,
    create_mcommit: impl FnOnce(Dot, D1, D2) -> <P as Protocol>::Message,
    to_processes: &mut Vec<Action<P>>,
) where
    P: Protocol,
{
    // take shards commit info
    let shards_commits = if let Some(shards_commits) = shards_commits.take() {
        shards_commits
    } else {
        panic!("no shards commit info when handling MShardAggregatedCommit about dot {:?}", dot)
    };

    // get extra commit data
    let data2 = extract_mcommit_extra_data(shards_commits.info);

    // create `MCommit`
    final_mcommit_action(bp, dot, data1, data2, create_mcommit, to_processes)
}

fn final_mcommit_action<P, D1, D2>(
    bp: &BaseProcess,
    dot: Dot,
    data1: D1,
    data2: D2,
    create_mcommit: impl FnOnce(Dot, D1, D2) -> <P as Protocol>::Message,
    to_processes: &mut Vec<Action<P>>,
) where
    P: Protocol,
{
    let mcommit = create_mcommit(dot, data1, data2);
    let target = bp.all();
    to_processes.push(Action::ToSend {
        target,
        msg: mcommit,
    });
}

fn init_shards_commits<'a, I>(
    shards_commits: &'a mut Option<ShardsCommits<I>>,
    process_id: ProcessId,
    shard_count: usize,
) -> &'a mut ShardsCommits<I>
where
    I: Default + Debug,
{
    match shards_commits {
        Some(shards_commits) => shards_commits,
        None => {
            *shards_commits =
                Some(ShardsCommits::new(process_id, shard_count, I::default()));
            shards_commits.as_mut().unwrap()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardsCommits<I> {
    process_id: ProcessId,
    shard_count: usize,
    participants: HashSet<ProcessId>,
    info: I,
}

impl<I> ShardsCommits<I>
where
    I: Debug,
{
    fn new(process_id: ProcessId, shard_count: usize, info: I) -> Self {
        let participants = HashSet::with_capacity(shard_count);
        Self {
            process_id,
            shard_count,
            participants,
            info,
        }
    }

    fn add(&mut self, from: ProcessId, add: impl FnOnce(&mut I)) -> bool {
        assert!(self.participants.insert(from));
        add(&mut self.info);
        log!(
            "p{}: ShardsCommits::add {} | current info = {:?} | participants = {:?} | shard count = {}",
            self.process_id,
            from,
            self.info,
            self.participants,
            self.shard_count
        );

        // we're done once we have received a message from each shard
        self.participants.len() == self.shard_count
    }

    pub fn update(&mut self, update: impl FnOnce(&mut I)) {
        update(&mut self.info);
    }
}
