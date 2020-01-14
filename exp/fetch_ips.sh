#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source "${DIR}/util.sh"

# drop current ips
rm -f "${MACHINE_IPS_FILE}"

# fetch ip from each machine
while IFS= read -r machine; do

    # get machine ip
    # shellcheck disable=SC2029
    ip=$(ssh "${SSH_ARGS}" "${machine}" hostname -I </dev/null | awk '{ print $1 }')

    echo "${machine} ${ip}" >>"${MACHINE_IPS_FILE}"
done <"${MACHINES_FILE}"
