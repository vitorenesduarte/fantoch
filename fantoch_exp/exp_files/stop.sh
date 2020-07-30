#!/usr/bin/env bash
# shellcheck disable=SC2086

DIR=$(dirname "${BASH_SOURCE[0]}")

MACHINES_FILE="${DIR}/machines"
SSH_ARGS="-oStrictHostKeyChecking=no"
PORT=3000
CLIENT_PORT=4000
MAX_PROCS=24

wait_jobs() {
    for job in $(jobs -p); do
        wait ${job}
    done
}

stop_fantoch() {
    if [ $# -ne 1 ]; then
        echo "usage: stop_fantoch machine"
        exit 1
    fi

    # variables
    local machine=$1
    local cmd

    # stop processes
    cmd="lsof -i :${PORT} -i :${CLIENT_PORT} | grep -v PID | awk '{ print \"kill -SIGKILL \"\$2 }' | sort -u | bash"
    # shellcheck disable=SC2029
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null

    # stop clients
    cmd="ps -aux | grep fantoch/target/release/client | grep -v grep | awk '{ print \"kill -SIGKILL \"\$2 }' | bash"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null

    # stop dstat
    cmd="ps -aux | grep dstat | grep -v grep | awk '{ print \"kill -SIGKILL \"\$2 }' | bash"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null

    for process_id in $(seq 1 ${MAX_PROCS}); do
        # compute ports
        local port=$(( PORT + process_id ))
        local client_port=$(( CLIENT_PORT + process_id ))

        # wait for processes to end
        cmd="lsof -i :${port} -i :${client_port} | wc -l"
        local running=-1
        while [[ ${running} != 0 ]]; do
            # shellcheck disable=SC2029
            running=$(ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null | xargs)
        done
    done

    # remove files
    cmd="rm -f .metrics .log .metrics dstat.csv heaptrack.*.gz"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null
}

stop_all() {
    # variables
    local machine

    while IFS= read -r machine; do
        stop_fantoch ${machine} &
    done <"${MACHINES_FILE}"
    wait_jobs
}

stop_all

