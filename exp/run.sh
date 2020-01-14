#!/usr/bin/env bash
# shellcheck disable=SC2086

DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=util.sh
source "${DIR}/util.sh"

KILL_WAIT=3 # seconds
BUILD_FOLDER="./planet_sim/target/release"
PORT=3000
CLIENT_PORT=4000

PROTOCOL="atlas"
PROCESSES=3
FAULTS=1

CLIENT_MACHINES_NUMBER=3
CLIENTS_PER_MACHINE=10
COMMANDS_PER_CLIENT=10000

process_file() {
    if [ $# -ne 1 ]; then
        echo "usage: process_file id"
        exit 1
    fi

    # variables
    local id=$1
    echo ".log_process_${id}"
}

client_file() {
    if [ $# -ne 1 ]; then
        echo "usage: client_file index"
        exit 1
    fi

    # variables
    local index=$1
    echo ".log_client_${index}"
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
        --faults ${FAULTS}"

    # shellcheck disable=SC2029
    ssh "${SSH_ARGS}" ${machine} "./${BUILD_FOLDER}/${protocol} ${command_args}" </dev/null
}

wait_process_started() {
    if [ $# -ne 1 ]; then
        echo "usage: wait_process_started id"
        exit 1
    fi
    local id=$1

    started=0
    while [[ ${started} != 1 ]]; do
        started=$(grep -c "process ${id} started" "$(process_file ${id})" | xargs)
        sleep 1
    done
}

start_processes() {
    # start new processes
    for id in $(seq 1 ${PROCESSES}); do
        # compute topology
        result=$(topology "process" ${id} ${PROCESSES} ${CLIENT_MACHINES_NUMBER})
        machine=$(echo "${result}" | awk '{ print $1 }')
        sorted=$(echo "${result}" | awk '{ print $2 }')
        ip=$(echo "${result}" | awk '{ print $3 }')
        ips=$(echo "${result}" | awk '{ print $4 }')

        # append port to each ip
        addresses=$(echo ${ips} | tr ',' '\n' | awk -v port=${PORT} '{ print $1":"port }' | tr '\n' ',' | sed 's/,$//')

        # start a new process
        info "process ${id} spawned"
        start_process ${machine} ${PROTOCOL} ${id} ${sorted} ${ip} ${addresses} >"$(process_file ${id})" 2>&1 &
    done

    # wait for processes started
    for id in $(seq 1 ${PROCESSES}); do
        wait_process_started ${id}
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
        --commands_per_client ${COMMANDS_PER_CLIENT}"
    # TODO for open-loop clients:
    # --interval 1

    # shellcheck disable=SC2029
    ssh "${SSH_ARGS}" ${machine} "./${BUILD_FOLDER}/client ${command_args}" </dev/null
}

run_clients() {
    # start clients
    for index in $(seq 1 ${CLIENT_MACHINES_NUMBER}); do
        # compute topology
        result=$(topology "client" ${index} ${PROCESSES} ${CLIENT_MACHINES_NUMBER})
        machine=$(echo "${result}" | awk '{ print $1 }')
        ip=$(echo "${result}" | awk '{ print $2 }')

        # append port to ip
        address="${ip}:${CLIENT_PORT}"

        # start a new client
        info "client ${index} spawned"
        start_client ${machine} ${index} ${address} >"$(client_file ${index})" 2>&1 &
    done

    # TODO fix this wait (it's hanging)
    # wait for clients to end
    wait_jobs
}

stop_processes
sleep ${KILL_WAIT}
info "hopefully all processes are stopped now"

start_processes
info "all processes have been started"

run_clients
info "all clients have run"
