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

process_file() {
    if [ $# -ne 1 ]; then
        echo "usage: process_file id"
        exit 1
    fi

    # variables
    local id=$1
    echo ".log_process_${id}"
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

# start processes
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

# stop previous processes
for id in $(seq 1 ${PROCESSES}); do
    machine=$(topology ${id} ${PROCESSES} | awk '{ print $1 }')

    # stop previous process
    stop_process ${machine} &
done
wait_jobs

sleep ${KILL_WAIT}
info "hopefully all processes are stopped now"

# start new processes
for id in $(seq 1 ${PROCESSES}); do
    # compute topology
    result=$(topology ${id} ${PROCESSES})
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
    # check if id started
    started=0
    while [[ ${started} != 1 ]]; do
        started=$(grep -c "process ${id} started" "$(process_file ${id})" | xargs)
        sleep 1
    done
done
info "all processes have been started"
