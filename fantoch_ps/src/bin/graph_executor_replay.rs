mod common;

use clap::{App, Arg};
use fantoch::config::Config;
use fantoch_ps::executor::{GraphExecutor};
use fantoch::executor::{Executor};
use fantoch::run::rw::Rw;
use tokio::fs::File;

const BUFFER_SIZE: usize = 8 * 1024; // 8KB

#[tokio::main]
async fn main() {
    let (config, execution_log) = parse_args();
    // create graph executor
    let mut executor = GraphExecutor::new(config);

    // open execution log file
    let file = File::open(execution_log)
        .await
        .expect("execution log should exist");

    // create log parse
    let mut rw = Rw::from(BUFFER_SIZE, BUFFER_SIZE, file);

    while let Some(execution_info) = rw.recv().await {
        println!("adding {:?}", execution_info);
        // result should be empty as we're not wait for any rifl
        let res = executor.handle(execution_info);
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
            Arg::with_name("transitive_conflicts")
                .long("transitive_conflicts")
                .value_name("TRANSITIVE_CONFLICTS")
                .help("bool indicating whether we can assume that the conflict relation is transitive; default: false")
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
    let execute_at_commit = None;
    let config = common::parse_config(
        matches.value_of("n"),
        matches.value_of("f"),
        matches.value_of("transitive_conflicts"),
        execute_at_commit,
    );
    let execution_log =
        common::parse_execution_log(matches.value_of("execution_log"))
            .expect("execution log should be set");

    println!("config: {:?}", config);
    println!("execution log: {:?}", execution_log);

    (config, execution_log)
}
