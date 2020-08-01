#!/usr/bin/env bash
# shellcheck disable=SC2086

DIR=$(dirname "${BASH_SOURCE[0]}")

MACHINES_FILE="${DIR}/machines"
SSH_ARGS="-oStrictHostKeyChecking=no"

wait_jobs() {
    for job in $(jobs -p); do
        wait ${job}
    done
}

stop_fantoch() {
    if [ $# -ne 2 ]; then
        echo "usage: stop_fantoch binary machine"
        exit 1
    fi

    # variables
    local binary=$1
    local machine=$2
    local cmd

    # stop process
    cmd="pkill ${binary}"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null

    # wait for process to end
    cmd="ps -aux | grep ${binary} | grep -v grep | wc -l"
    local running=-1
    while [[ ${running} != 0 ]]; do
        # shellcheck disable=SC2029
        running=$(ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null | xargs)
        sleep 1
    done

    # stop clients
    cmd="ps -aux | grep fantoch/target/release/client | grep -v grep | awk '{ print \"kill -SIGKILL \"\$2 }' | bash"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null

    # stop dstat
    cmd="ps -aux | grep dstat | grep -v grep | awk '{ print \"kill -SIGKILL \"\$2 }' | bash"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null

    # remove files
    cmd="rm -f *.metrics *.log *.dstat.csv heaptrack.*.gz"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null
}

stop_all() {
    if [ $# -ne 1 ]; then
        echo "usage: stop_all binary"
        exit 1
    fi

    # variables
    local binary=$1
    local machine

    while IFS= read -r machine; do
        stop_fantoch ${binary} ${machine} &
    done <"${MACHINES_FILE}"
    wait_jobs
}

stop_all $1

