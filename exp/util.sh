#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

# prelude
export CONFIG_FILE="${DIR}/config"
export MACHINES_FILE="${DIR}/files/machines"
export MACHINE_IPS_FILE="${DIR}/files/machine_ips"
export BUILD_FILE="${DIR}/files/build.sh"
export TOPOLOGY_FILE="${DIR}/files/topology.py"
export SSH_ARGS="-oStrictHostKeyChecking=no"

config() {
    if [ $# -ne 1 ]; then
        echo "usage: config key"
        exit 1
    fi
    local key=$1
    grep -E "^${key}=" "${CONFIG_FILE}" | cut -d= -f2
}

log() {
    if [ $# -ne 1 ]; then
        echo "usage: log message"
        exit 1
    fi
    local message=$1
    echo "[$(date +%H:%M:%S)] ${message}"
}

# only displayed if verbose in config file
info() {
    if [ $# -ne 1 ]; then
        echo "usage: info message"
        exit 1
    fi
    local message=$1
    local verbose
    verbose=$(config verbose)

    if [ "${verbose}" == "true" ]; then
        log "${message}"
    fi
}

wait_jobs() {
    for job in $(jobs -p); do
        wait "${job}"
    done
}

topology() {
    if [ $# -ne 2 ]; then
        echo "usage: topology id n"
        exit 1
    fi
    local id=$1
    local n=$2
    # shellcheck disable=SC2086
    "${TOPOLOGY_FILE}" ${id} ${n} "${MACHINE_IPS_FILE}"
}
