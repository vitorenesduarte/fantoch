#!/usr/bin/env bash

PID_FILE=server.pid

# Replica 1
echo "Réplica 1"
cargo run --bin epaxos -- --id  1 --addresses  127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 --processes 2 --faults 1 --shard_count 3 --shard_id 1 --port 3000 --log_file .log_server_1 --metrics_file .metrics_server_1 &
echo $! >> ${PID_FILE}


# Replica 2
echo "Réplica 2"
cargo run --bin epaxos -- --id  2 --addresses  127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 --processes 2 --faults 1 --shard_count 3 --shard_id 1 --port 3001 --log_file .log_server_2 --metrics_file .metrics_server_2 &
echo $! >> ${PID_FILE}

# Replica 3
echo "Réplica 3"
cargo run --bin epaxos -- --id  3 --addresses  127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 --processes 2 --faults 1 --shard_count 3 --shard_id 1 --port 3002 --log_file .log_server_3 --metrics_file .metrics_server_3 &
echo $! >> ${PID_FILE}
