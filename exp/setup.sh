#!/usr/bin/env bash
# shellcheck disable=SC2086

DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=util.sh
source "${DIR}/util.sh"

run_build() {
    # variables
    local branch
    local aws

    # get branch
    branch=$(config branch)
    aws="false"

    # build deps in each machines
    while IFS= read -r machine; do
        # copy build file to machine
        scp "${SSH_ARGS}" "${BUILD_FILE}" "${machine}:build.sh"

        # execute build in machine
        # shellcheck disable=SC2029
        ssh "${SSH_ARGS}" ${machine} "./build.sh ${branch} ${aws}" </dev/null &
    done <"${MACHINES_FILE}"

    # wait for all builds to complete
    wait_jobs
}

fetch_ips() {
    # variables
    local ip

    # drop current ips
    rm -f "${MACHINE_IPS_FILE}"

    # fetch ip from each machine
    while IFS= read -r machine; do

        # get machine ip
        # shellcheck disable=SC2029
        ip=$(ssh "${SSH_ARGS}" ${machine} hostname -I </dev/null | awk '{ print $1 }')

        echo "${machine} ${ip}" >>"${MACHINE_IPS_FILE}"
    done <"${MACHINES_FILE}"
}

run_build
info "all builds have been completed"

fetch_ips
info "all ips have been fetched"
cat "${MACHINE_IPS_FILE}"
