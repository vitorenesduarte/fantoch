use fantoch::client::{KeyGen, Workload};
use fantoch::id::{ClientId, RiflGen};
use fantoch_prof::metrics::Histogram;

fn main() {
    let n = 3;
    let keys_per_command = 2;
    let commands_per_client = 500;
    let payload_size = 0;
    let clients_per_region = 1024;
    let shard_counts = vec![2, 3, 4, 5, 6];
    let coefficients = vec![
        0.001, 0.25, 0.5, 0.75, 1.0, 1.25, 1.50, 2.0, 2.5, 3.0, 3.5, 4.0,
    ];

    // start csvs
    let header = format!(
        ",{}",
        shard_counts
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut s_csv = header.clone();
    let mut k_csv = header.clone();

    for coefficient in coefficients {
        println!("zipf = {}", coefficient);
        let key_gen = KeyGen::Zipf {
            coefficient,
            keys_per_shard: 1_000_000,
        };

        // add row start to csvs
        s_csv = format!("{}\n{}", s_csv, coefficient);
        k_csv = format!("{}\n{}", k_csv, coefficient);

        for shard_count in shard_counts.clone() {
            let total_clients = clients_per_region * n;

            // create target shard histogram
            let mut shards_histogram = Histogram::new();
            let mut keys_histogram = Histogram::new();

            for client_id in 1..=total_clients {
                let client_id = client_id as ClientId;
                let mut workload = Workload::new(
                    shard_count,
                    key_gen,
                    keys_per_command,
                    commands_per_client,
                    payload_size,
                );
                let mut rifl_gen = RiflGen::new(client_id);
                let mut key_gen_state = workload
                    .key_gen()
                    .initial_state(workload.shard_count(), client_id);
                while let Some((_target_shard, cmd)) =
                    workload.next_cmd(&mut rifl_gen, &mut key_gen_state)
                {
                    // update histograms
                    shards_histogram.increment(cmd.shard_count() as u64);
                    keys_histogram.increment(cmd.total_key_count() as u64);
                }
            }

            let total_commands = total_clients * commands_per_client;
            let percentage = |hist: &Histogram, key| {
                hist.inner().get(&key).unwrap_or(&0) * 100 / total_commands
            };
            let s2_percentage = percentage(&shards_histogram, 2);
            let k2_percentage = percentage(&keys_histogram, 2);
            match keys_per_command {
                2 => {
                    println!(
                        "  shards = {} | #s=2 -> %{:<3} | #k=2 -> %{:<3}",
                        shard_count, s2_percentage, k2_percentage
                    );
                    s_csv = format!("{},{}", s_csv, s2_percentage);
                    k_csv = format!("{},{}", k_csv, k2_percentage);
                }
                3 => {
                    let s3_percentage = percentage(&shards_histogram, 3);
                    let k3_percentage = percentage(&keys_histogram, 3);
                    println!(
                        "  shards = {} | #s=2 -> %{:<3} | #s=3 -> %{:<3} | #k=2 -> %{:<3} | #k=3 -> %{:<3}",
                        shard_count, s2_percentage, s3_percentage, k2_percentage, k3_percentage
                    );
                    s_csv = format!(
                        "{},{} | {}",
                        s_csv, s2_percentage, s3_percentage
                    );
                    k_csv = format!(
                        "{},{} | {}",
                        k_csv, k2_percentage, k3_percentage
                    );
                }
                _ => {
                    panic!(
                        "unsupported keys_per_command = {}",
                        keys_per_command
                    );
                }
            }
        }
        println!("{}", s_csv);
        println!("{}", k_csv);
    }
}
