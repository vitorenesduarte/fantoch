#![feature(no_more_cas)]

use clap::{App, Arg};
use futures::future::join_all;
use planet_sim::metrics::Histogram;
use planet_sim::run::task;
use planet_sim::run::task::chan::{ChannelReceiver, ChannelSender};
use planet_sim::time::{RunTime, SysTime};
use rand::Rng;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;

const DEFAULT_KEYS: usize = 100;
const DEFAULT_KEYS_PER_COMMAND: usize = 1;
const DEFAULT_CLIENTS: usize = 10;
const DEFAULT_COMMANDS_PER_CLIENT: usize = 10000;

const CHANNEL_BUFFER_SIZE: usize = 10000;

type Key = usize;
type Command = HashSet<Key>;
type VoteRange = (Key, u64, u64);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (keys_number, client_number, commands_per_client, keys_per_command) = parse_args();

    // get number of cpus
    let cpus = num_cpus::get_physical();

    // maybe warn about number of keys
    if keys_number < cpus {
        println!(
            "warning: number of keys {} is lower than the number of cpus {}",
            keys_number, cpus
        );
    }

    // initialize sequencer
    let sequencer = Arc::new(Sequencer::new(keys_number));

    // create as many workers as cpus
    let to_workers: Vec<_> = (0..cpus)
        .map(|_| task::spawn_consumer(CHANNEL_BUFFER_SIZE, |rx| worker(rx, sequencer.clone())))
        .collect();

    // spawn clients
    let handles = (0..client_number).map(|_| {
        tokio::spawn(client(
            keys_number,
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
            let current_key_votes = all_votes.entry(key).or_insert_with(HashSet::new);
            for vote in vote_start..=vote_end {
                // insert vote and check it hasn't been added before
                assert!(current_key_votes.insert(vote));
            }
        }
    }

    // TODO check that we have all votes (no gaps that would prevent timestamp-stability)

    println!("latency: {:?}", latency);
    Ok(())
}

// async fn worker(sequencer: )
async fn worker(
    mut requests: ChannelReceiver<(u64, Command, oneshot::Sender<Vec<VoteRange>>)>,
    sequencer: Arc<Sequencer>,
) {
    while let Some((proposal, cmd, client)) = requests.recv().await {
        let result = sequencer.next(proposal, cmd);
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
        let mut command = HashSet::new();
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

struct Sequencer {
    keys: Vec<AtomicU64>,
}

impl Sequencer {
    fn new(keys_number: usize) -> Self {
        let mut keys = Vec::with_capacity(keys_number);
        keys.resize_with(keys_number, Default::default);
        Self { keys }
    }

    fn next(&self, proposal: u64, cmd: Command) -> Vec<VoteRange> {
        let mut votes = Vec::with_capacity(cmd.len() * 2);

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
                    if let Ok(new_vote_start) = result {
                        return Some((*key, new_vote_start, max_sequence));
                    }
                }
                None
            })
            .collect();

        votes.extend(new_votes);
        votes
    }
}

// unsafe impl Send for Sequencer {}

fn parse_args() -> (usize, usize, usize, usize) {
    let matches = App::new("sequencer_bench")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Benchmark timestamp-assignment in newt")
        .arg(
            Arg::with_name("keys")
                .long("keys")
                .value_name("KEYS")
                .help("total number of existing keys; default: 100")
                .takes_value(true),
        )
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
    let keys = parse_keys(matches.value_of("keys"));
    let clients = parse_clients(matches.value_of("clients"));
    let commands_per_client = parse_commands_per_client(matches.value_of("commands_per_client"));
    let keys_per_command = parse_keys_per_command(matches.value_of("keys_per_command"));

    println!("keys: {:?}", keys);
    println!("clients: {:?}", clients);
    println!("commands per client: {:?}", commands_per_client);
    println!("keys per command: {:?}", keys_per_command);

    (keys, clients, commands_per_client, keys_per_command)
}

fn parse_keys(keys: Option<&str>) -> usize {
    parse_number(keys).unwrap_or(DEFAULT_KEYS)
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
