mod common;

use clap::{App, Arg};
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::run::rw::Rw;
use fantoch::time::RunTime;
use fantoch_ps::executor::GraphExecutor;
use tokio::fs::File;

const BUFFER_SIZE: usize = 8 * 1024; // 8KB

#[tokio::main]
async fn main() {
    let process_id = 1;
    let shard_id = 0;
    let (config, execution_log) = parse_args();
    // create graph executor
    let mut executor = GraphExecutor::new(process_id, shard_id, config);
    let time = RunTime;

    // open execution log file
    let file = File::open(execution_log)
        .await
        .expect("execution log should exist");

    // create log parse
    let mut rw = Rw::from(BUFFER_SIZE, BUFFER_SIZE, file);

    while let Some(execution_info) = rw.recv().await {
        println!("adding {:?}", execution_info);
        // result should be empty as we're not wait for any rifl
        executor.handle(execution_info, &time);
        let res: Vec<_> = executor.to_clients_iter().collect();
        assert!(res.is_empty());
        executor.show_internal_status();
    }
}

fn parse_args() -> (Config, String) {
    let matches = App::new("executor_replay")
        .version("0.1")
        .author("Vitor Enes <vitorenesduarte@gmail.com>")
        .about("Replays an execution log.")
        .arg(
            Arg::with_name("n")
                .long("processes")
                .value_name("PROCESS_NUMBER")
                .help("total number of processes")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("f")
                .long("faults")
                .value_name("FAULT_NUMBER")
                .help("total number of allowed faults")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("execution_log")
                .long("execution_log")
                .value_name("EXECUTION_LOG")
                .help("log file with execution infos")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    // parse arguments
    let n = common::protocol::parse_n(matches.value_of("n"));
    let f = common::protocol::parse_f(matches.value_of("f"));
    let config = Config::new(n, f);
    let execution_log = common::protocol::parse_execution_log(
        matches.value_of("execution_log"),
    )
    .expect("execution log should be set");

    println!("config: {:?}", config);
    println!("execution log: {:?}", execution_log);

    (config, execution_log)
}
