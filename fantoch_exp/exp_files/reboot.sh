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

reboot_machine() {
    if [ $# -ne 1 ]; then
        echo "usage: reboot_machine machine"
        exit 1
    fi

    # variables
    local machine=$1
    local cmd

    # reboot machine
    cmd="sudo reboot"
    ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null

    # wait for machine to boot
    cmd="ls"
    local running=""
    while [[ "${running}" == "" ]]; do
        # shellcheck disable=SC2029
        running=$(ssh "${SSH_ARGS}" ${machine} "${cmd}" </dev/null | xargs)
        sleep 1
    done
}

reboot_all() {
    if [ $# -ne 0 ]; then
        echo "usage: reboot_all"
        exit 1
    fi

    # variables
    local machine

    while IFS= read -r machine; do
        reboot_machine ${machine} &
    done <"${MACHINES_FILE}"
    wait_jobs
}

reboot_all

