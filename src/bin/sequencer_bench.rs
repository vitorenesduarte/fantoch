#![feature(no_more_cas)]

use clap::{App, Arg};
use futures::future::join_all;
use lazy_static::lazy_static;
use planet_sim::metrics::Histogram;
use planet_sim::run::task;
use planet_sim::run::task::chan::{ChannelReceiver, ChannelSender};
use planet_sim::time::{RunTime, SysTime};
use rand::Rng;
use std::cmp::max;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::iter::FromIterator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::oneshot;

const KEYS: usize = 100;

const DEFAULT_KEYS_PER_COMMAND: usize = 1;
const DEFAULT_CLIENTS: usize = 10;
const DEFAULT_COMMANDS_PER_CLIENT: usize = 10000;

const CHANNEL_BUFFER_SIZE: usize = 10000;

type Key = usize;
type Command = BTreeSet<Key>;
type VoteRange = (Key, u64, u64);

lazy_static! {
    static ref SEQUENCER: AtomicSequencer = AtomicSequencer::new(KEYS);
}

fn main() -> Result<(), Box<dyn Error>> {
    let (client_number, commands_per_client, keys_per_command) = parse_args();

    // get number of cpus
    let cpus = num_cpus::get();
    println!("cpus: {}", cpus);

    // maybe warn about number of keys
    if KEYS < cpus {
        println!(
            "warning: number of keys {} is lower than the number of cpus {}",
            KEYS, cpus
        );
    }

    // create tokio runtime
    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(cpus)
        .thread_name("sequencer-bench")
        .build()
        .expect("tokio runtime build should work");

    runtime.block_on(bench(
        cpus,
        client_number,
        commands_per_client,
        keys_per_command,
    ))
}

async fn bench(
    cpus: usize,
    client_number: usize,
    commands_per_client: usize,
    keys_per_command: usize,
) -> Result<(), Box<dyn Error>> {
    // create as many workers as cpus
    let to_workers: Vec<_> = (0..cpus)
        .map(|_| task::spawn_consumer(CHANNEL_BUFFER_SIZE, |rx| worker(rx)))
        .collect();

    // spawn clients
    let handles = (0..client_number).map(|_| {
        tokio::spawn(client(
            KEYS,
            commands_per_client,
            keys_per_command,
            to_workers.clone(),
        ))
    });

    // wait for all clients to complete and aggregate values
    let mut latency = Histogram::new();
    let mut all_votes = HashMap::new();

    for join_result in join_all(handles).await {
        let (client_histogram, votes) = join_result?;
        latency.merge(&client_histogram);
        for (key, vote_start, vote_end) in votes {
            // println!("checking vote {}-{} on key {}", vote_start, vote_end, key);
            let current_key_votes = all_votes.entry(key).or_insert_with(BTreeSet::new);
            for vote in vote_start..=vote_end {
                // insert vote and check it hasn't been added before
                assert!(current_key_votes.insert(vote));
            }
        }
    }

    // check that we have all votes (no gaps that would prevent timestamp-stability)
    for (_key, key_votes) in all_votes {
        // get number of votes
        let key_votes_count = key_votes.len();
        // we should have all votes from 1 to `key_votes_count`
        assert_eq!(
            key_votes,
            BTreeSet::from_iter((1..=key_votes_count).map(|vote| vote as u64))
        );
    }

    println!("latency: {:?}", latency);
    Ok(())
}

// async fn worker(sequencer: )
async fn worker(mut requests: ChannelReceiver<(u64, Command, oneshot::Sender<Vec<VoteRange>>)>) {
    while let Some((proposal, cmd, client)) = requests.recv().await {
        let result = SEQUENCER.next(proposal, cmd);
        if let Err(e) = client.send(result) {
            println!("error while sending next result to client: {:?}", e);
        }
    }
}

async fn client(
    keys_number: usize,
    commands_per_client: usize,
    keys_per_command: usize,
    mut to_workers: Vec<ChannelSender<(u64, Command, oneshot::Sender<Vec<VoteRange>>)>>,
) -> (Histogram, Vec<VoteRange>) {
    // create histogram and list with all votes received
    let mut histogram = Histogram::new();
    let mut all_votes = Vec::new();

    // create time and highest proposal seen
    let time = RunTime;
    let mut proposal: u64 = 0;

    for _ in 0..commands_per_client {
        // generate random command
        let mut command = BTreeSet::new();
        while command.len() < keys_per_command {
            // generate random key
            let key = rand::thread_rng().gen_range(0, keys_number);
            command.insert(key);
        }

        // increase highest proposal by 1
        proposal += 1;

        // create oneshot channel and send command
        let (tx, rx) = oneshot::channel();

        // get one key touched by the command
        let key = command
            .iter()
            .next()
            .expect("minimum keys per command should be 1");

        // select worker responsible for that key
        let worker_index = key % to_workers.len();

        // create request
        let request = (proposal, command, tx);

        // register start time
        let start_start = time.now();

        // send request to worker
        if let Err(e) = to_workers[worker_index].send(request).await {
            println!("error sending request to worker {}: {:?}", worker_index, e);
        }

        // wait for reply
        match rx.await {
            Ok(votes) => {
                // println!("received votes {:?} from {}", votes, worker_index);
                // register end time
                let end_time = time.now();

                // update highest proposal seen:w
                let highest_reply = votes
                    .iter()
                    .map(|(_, _, vote_end)| vote_end)
                    .max()
                    .expect("there should be at least one vote");
                proposal = max(*highest_reply, proposal);

                // update histogram
                let latency = end_time - start_start;
                histogram.increment(latency as u64);

                // update list with all votes
                all_votes.extend(votes);
            }
            Err(e) => {
                println!(
                    "error receiving reply from worker {}: {:?}",
                    worker_index, e
                );
            }
        }
    }

    (histogram, all_votes)
}

trait Sequencer {
    fn new(keys_number: usize) -> Self;
    fn next(&self, proposal: u64, cmd: Command) -> Vec<VoteRange>;
}

struct LockSequencer {
    keys: Vec<Mutex<u64>>,
}

impl Sequencer for LockSequencer {
    fn new(keys_number: usize) -> Self {
        let mut keys = Vec::with_capacity(keys_number);
        keys.resize_with(keys_number, Default::default);
        Self { keys }
    }

    fn next(&self, proposal: u64, cmd: Command) -> Vec<VoteRange> {
        let vote_count = cmd.len();
        let mut votes = Vec::with_capacity(vote_count);

        let mut max_sequence = 0;

        cmd.into_iter()
            .map(|key| {
                let value = self.keys[key].lock().expect("should be able to lock");
                max_sequence = max(proposal, *value + 1);
                (key, value)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|(key, mut previous_value)| {
                // compute vote start and vote end
                let vote_start = *previous_value + 1;

                // save vote range
                votes.push((key, vote_start, max_sequence));

                // set new value
                *previous_value = max_sequence;
            });

        assert_eq!(votes.capacity(), vote_count);
        votes
    }
}

struct AtomicSequencer {
    keys: Vec<AtomicU64>,
}

impl Sequencer for AtomicSequencer {
    fn new(keys_number: usize) -> Self {
        let mut keys = Vec::with_capacity(keys_number);
        keys.resize_with(keys_number, Default::default);
        Self { keys }
    }

    fn next(&self, proposal: u64, cmd: Command) -> Vec<VoteRange> {
        let max_vote_count = cmd.len() * 2 - 1;
        let mut votes = Vec::with_capacity(max_vote_count);

        let max_sequence = cmd
            .into_iter()
            .map(|key| {
                let previous_value = self.keys[key]
                    .fetch_update(
                        |value| Some(max(proposal, value + 1)),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .expect("updates always succeed");

                // compute vote start and vote end
                let vote_start = previous_value + 1;
                let vote_end = max(proposal, previous_value + 1);

                // save vote range
                votes.push((key, vote_start, vote_end));

                // return vote end
                vote_end
            })
            .max()
            .expect("there should be a maximum sequence");

        let new_votes: Vec<_> = votes
            .iter()
            .filter_map(|(key, _vote_start, vote_end)| {
                // check if we should vote more
                if *vote_end < max_sequence {
                    let result = self.keys[*key].fetch_update(
                        |value| {
                            if value < max_sequence {
                                Some(max_sequence)
                            } else {
                                None
                            }
                        },
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                    // check if we generated more votes (maybe votes by other threads have been
                    // generated and it's no longer possible to generate votes)
                    if let Ok(previous_value) = result {
                        let vote_start = previous_value + 1;
                        let vote_end = max_sequence;
                        return Some((*key, vote_start, vote_end));
                    }
                }
                None
            })
            .collect();

        votes.extend(new_votes);
        assert_eq!(votes.capacity(), max_vote_count);
        votes
    }
}

fn parse_args() -> (usize, usize, usize) {
    let matches = App::new("sequencer_bench")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Benchmark timestamp-assignment in newt")
        .arg(
            Arg::with_name("clients")
                .long("clients")
                .value_name("CLIENTS")
                .help("total number of clients; default: 10")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("commands_per_client")
                .long("commands_per_client")
                .value_name("COMMANDS_PER_CLIENT")
                .help("number of commands to be issued by each client; default: 10000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("keys_per_command")
                .long("keys_per_command")
                .value_name("KEYS_PER_COMMAND")
                .help("number of keys accessed by each command; default: 1")
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let clients = parse_clients(matches.value_of("clients"));
    let commands_per_client = parse_commands_per_client(matches.value_of("commands_per_client"));
    let keys_per_command = parse_keys_per_command(matches.value_of("keys_per_command"));

    println!("clients: {:?}", clients);
    println!("commands per client: {:?}", commands_per_client);
    println!("keys per command: {:?}", keys_per_command);

    (clients, commands_per_client, keys_per_command)
}

fn parse_keys_per_command(keys_per_command: Option<&str>) -> usize {
    parse_number(keys_per_command).unwrap_or(DEFAULT_KEYS_PER_COMMAND)
}

fn parse_clients(clients: Option<&str>) -> usize {
    parse_number(clients).unwrap_or(DEFAULT_CLIENTS)
}

fn parse_commands_per_client(commands_per_client: Option<&str>) -> usize {
    parse_number(commands_per_client).unwrap_or(DEFAULT_COMMANDS_PER_CLIENT)
}

fn parse_number(number: Option<&str>) -> Option<usize> {
    number.map(|number| number.parse::<usize>().expect("should be a number"))
}
