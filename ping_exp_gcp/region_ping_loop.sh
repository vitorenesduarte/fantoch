#!/usr/bin/env bash

SECONDS=$1
OUTPUT=$2
HOSTS=hosts
TIMEOUT=0

zone() {
    local host=$1
    echo ${host} | awk -F'::' '{ print $1 }'
}

ip() {
    local host=$1
    echo ${host} | awk -F'::' '{ print $2 }'
}

file() {
    local host=$1
    local zone=$(zone ${host})
    echo "log-${zone}.dat"
}

rm -f *.dat

for host in $(cat ${HOSTS}); do
    IP=$(ip ${host})
    FILE=$(file ${host})

    ping -w ${SECONDS} -W ${TIMEOUT} ${IP} >>${FILE} &
done
wait

for host in $(cat ${HOSTS}); do
    ZONE=$(zone ${host})
    FILE=$(file ${host})
    STATS=$(grep min/avg/max ${FILE} | grep -Eo "([0-9\.]+/?){4}")
    echo ${STATS}:${ZONE}
done | sort -n >${OUTPUT}
