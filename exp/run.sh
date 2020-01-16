#!/usr/bin/env bash
# shellcheck disable=SC2086

DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=util.sh
source "${DIR}/util.sh"

# seconds
KILL_WAIT=3

# mode can be: release, flamegraph, leaks
# PROCESS_RUN_MODE="flamegraph"
PROCESS_RUN_MODE="flamegraph"
CLIENT_RUN_MODE="release"

# processes config
PORT=3000
CLIENT_PORT=4000
PROTOCOL="newt"
PROCESSES=3
FAULTS=1

# clients config
CLIENT_MACHINES_NUMBER=3
CLIENTS_PER_MACHINE=1000
CONFLICT_RATE=0
COMMANDS_PER_CLIENT=200

# overall config
TCP_NODELAY=true
# by default, each socket stream is buffered (with a buffer of size 8KBs),
# which should greatly reduce the number of syscalls for small-sized messages
SOCKET_BUFFER_SIZE=$((8 * 1024))
# if this value is 100, the run doesn't finish, which probably means there's a deadlock somewhere
# with 1000 we can see that channels fill up sometimes
# with 10000 that doesn't seem to happen
CHANNEL_BUFFER_SIZE=10000

bin_script() {
    if [ $# -ne 2 ]; then
        echo "usage: bin_script mode binary"
        exit 1
    fi
    local mode=$1
    local binary=$2
    local prefix="source \${HOME}/.cargo/env && cd planet_sim"

    case "${mode}" in
    "release")
        # for release runs
        echo "${prefix} && sed -i 's/debug = true/debug = false/g' Cargo.toml && cargo build --release --bins && ./target/release/${binary}"
        ;;
    "flamegraph")
        # for flamegraph runs
        echo "${prefix} && sed -i 's/debug = false/debug = true/g' Cargo.toml && cargo flamegraph --bin=${binary} --"
        ;;
    "leaks")
        # for memory-leak runs
        echo "${prefix} && env RUSTFLAGS=\"-Z sanitizer=leak\" cargo +nightly run --release --bin ${binary} --"
        ;;
    *)
        echo "invalid run mode: ${mode}"
        exit 1
        ;;
    esac
}

process_file() {
    if [ $# -ne 1 ]; then
        echo "usage: process_file id"
        exit 1
    fi

    # variables
    local id=$1
    echo "\${HOME}/.log_process_${id}"
}

client_file() {
    if [ $# -ne 1 ]; then
        echo "usage: client_file index"
        exit 1
    fi

    # variables
    local index=$1
    echo "\${HOME}/.log_client_${index}"
}

wait_process_started() {
    if [ $# -ne 2 ]; then
        echo "usage: wait_process_started id machine"
        exit 1
    fi
    local id=$1
    local machine=$2
    local cmd
    cmd="grep -c \"process ${id} started\" $(process_file ${id})"

    local started=0
    while [[ ${started} != 1 ]]; do
        # shellcheck disable=SC2029
        started=$(ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null | xargs)
        sleep 1
    done
}

wait_client_ended() {
    if [ $# -ne 2 ]; then
        echo "usage: wait_client_ended index machine"
        exit 1
    fi
    local index=$1
    local machine=$2
    local cmd
    cmd="grep -c \"all clients ended\" $(client_file ${id})"

    local ended=0
    while [[ ${ended} != 1 ]]; do
        # shellcheck disable=SC2029
        ended=$(ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null | xargs)
        sleep 1
    done
}

stop_process() {
    if [ $# -ne 1 ]; then
        echo "usage: stop_process machine"
        exit 1
    fi

    # variables
    local machine=$1

    # kill process running on ${PORT}
    ssh "${SSH_ARGS}" ${machine} fuser ${PORT}/tcp --kill </dev/null
}

stop_processes() {
    # stop previous processes
    for id in $(seq 1 ${PROCESSES}); do
        machine=$(topology "process" ${id} ${PROCESSES} ${CLIENT_MACHINES_NUMBER} | awk '{ print $1 }')

        # stop previous process
        stop_process ${machine} &
    done
    wait_jobs
}

# start process
start_process() {
    if [ $# -ne 6 ]; then
        echo "usage: start_process machine protocol id sorted ip addresses"
        exit 1
    fi

    # variables
    local machine=$1
    local protocol=$2
    local id=$3
    local sorted=$4
    local ip=$5
    local addresses=$6
    local command_args

    # create commands args
    command_args="\
        --id ${id} \
        --sorted ${sorted} \
        --ip ${ip} \
        --port ${PORT} \
        --addresses ${addresses} \
        --client_port ${CLIENT_PORT} \
        --processes ${PROCESSES} \
        --faults ${FAULTS} \
        --tcp_nodelay ${TCP_NODELAY} \
        --socket_buffer_size ${SOCKET_BUFFER_SIZE} \
        --channel_buffer_size ${CHANNEL_BUFFER_SIZE}"

    # compute script (based on run mode)
    script=$(bin_script "${PROCESS_RUN_MODE}" "${protocol}")

    info "starting ${protocol} with: $(echo ${script} ${command_args} | tr -s ' ')"

    # shellcheck disable=SC2029
    ssh "${SSH_ARGS}" ${machine} "${script} ${command_args}" \>"$(process_file ${id})" 2\>\&1 </dev/null
}

start_processes() {
    # variables
    local result
    local machine
    local sorted
    local ip
    local ips
    local addresses
    # mapping from id to machine
    declare -A machines

    # start new processes
    for id in $(seq 1 ${PROCESSES}); do
        # compute topology
        result=$(topology "process" ${id} ${PROCESSES} ${CLIENT_MACHINES_NUMBER})
        machine=$(echo "${result}" | awk '{ print $1 }')
        sorted=$(echo "${result}" | awk '{ print $2 }')
        ip=$(echo "${result}" | awk '{ print $3 }')
        ips=$(echo "${result}" | awk '{ print $4 }')

        # save machine
        machines[${id}]=${machine}

        # append port to each ip
        addresses=$(echo ${ips} | tr ',' '\n' | awk -v port=${PORT} '{ print $1":"port }' | tr '\n' ',' | sed 's/,$//')

        # start a new process
        info "process ${id} spawned"
        start_process ${machine} ${PROTOCOL} ${id} ${sorted} ${ip} ${addresses} &
    done

    # wait for processes started
    for id in $(seq 1 ${PROCESSES}); do
        wait_process_started ${id} ${machines[${id}]}
    done
}

# start client
start_client() {
    if [ $# -ne 3 ]; then
        echo "usage: start_client machine index address"
        exit 1
    fi

    # variables
    local machine=$1
    local index=$2
    local address=$3
    local id_start
    local id_end
    local command_args
    local script

    # check that index is at least 1
    if [[ ${index} -lt 1 ]]; then
        echo "client index should be at least 1"
        exit 1
    fi

    # if ${CLIENTS_PER_MACHINE} is 4 and:
    # - index is 1, then start should be 1 and end 4
    # - index is 2, then start should be 5 and end 8
    # - and so on

    # compute id start and id end
    # - first compute the id end
    id_end="$((index * CLIENTS_PER_MACHINE))"
    # - to compute id start simply just subtract ${CLIENTS_PER_MACHINE} and add 1
    id_start="$((id_end - CLIENTS_PER_MACHINE + 1))"

    # create commands args
    command_args="\
        --ids ${id_start}-${id_end} \
        --address ${address} \
        --conflict_rate ${CONFLICT_RATE} \
        --commands_per_client ${COMMANDS_PER_CLIENT} \
        --tcp_nodelay ${TCP_NODELAY} \
        --socket_buffer_size ${SOCKET_BUFFER_SIZE} \
        --channel_buffer_size ${CHANNEL_BUFFER_SIZE}"
    # TODO for open-loop clients:
    # --interval 1

    # compute script (based on run mode)
    script=$(bin_script "${CLIENT_RUN_MODE}" "client")

    info "starting client with: $(echo ${script} ${command_args} | tr -s ' ')"

    # shellcheck disable=SC2029
    ssh "${SSH_ARGS}" ${machine} "${script} ${command_args}" \>"$(client_file ${id})" 2\>\&1 </dev/null
}

run_clients() {
    # variables
    local result
    local machine
    local ip
    local address
    # mapping from index to machine
    declare -A machines

    # start clients
    for index in $(seq 1 ${CLIENT_MACHINES_NUMBER}); do
        # compute topology
        result=$(topology "client" ${index} ${PROCESSES} ${CLIENT_MACHINES_NUMBER})
        machine=$(echo "${result}" | awk '{ print $1 }')
        ip=$(echo "${result}" | awk '{ print $2 }')

        # save machine
        machines[${index}]=${machine}

        # append port to ip
        address="${ip}:${CLIENT_PORT}"

        # start a new client
        info "client ${index} spawned"
        start_client ${machine} ${index} ${address} &
    done

    # wait for clients ended
    for index in $(seq 1 ${CLIENT_MACHINES_NUMBER}); do
        wait_client_ended ${index} ${machines[${index}]}
    done
}

if [[ $1 == "stop" ]]; then
    stop_processes
else
    stop_processes
    sleep ${KILL_WAIT}
    info "hopefully all processes are stopped now"

    # TODO launch dstat in all all machines with something like:
    # dstat -tsmdn -c -C 0,1,2,3,4,5,6,7,8,9,10,11,total --noheaders --output a.csv

    start_processes
    info "all processes have been started"

    run_clients
    info "all clients have ended"
fi
