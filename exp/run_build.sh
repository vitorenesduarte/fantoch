#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=util.sh
source "${DIR}/util.sh"

# get branch
branch=$(config branch)

# build deps in each machines
while IFS= read -r machine; do
    # copy build file to machine
    scp "${SSH_ARGS}" "${BUILD_FILE}" "${machine}:build.sh"

    # execute build in machine
    # shellcheck disable=SC2029
    ssh "${SSH_ARGS}" "${machine}" "./build.sh ${branch}" </dev/null &
done <"${MACHINES_FILE}"

# wait for all builds to complete
wait_jobs

info "all builds have been completed"
