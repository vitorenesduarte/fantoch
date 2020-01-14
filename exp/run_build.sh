#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source "${DIR}/util.sh"

branch=$(config branch)
machines_file=machines
build_file=build.sh
ssh_args="-oStrictHostKeyChecking=no"

while IFS= read -r machine; do
    # copy build file to machine
    scp "${ssh_args}" "${DIR}/${build_file}" "${machine}:${build_file}"

    # execute build in machine
    ssh "${ssh_args}" "${machine}" "./${build_file} ${branch}" </dev/null &
done <"${DIR}/${machines_file}"

# wait for all builds to complete
wait_jobs
