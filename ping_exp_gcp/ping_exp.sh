#!/usr/bin/env bash

REGIONS=regions
HOSTS=hosts
SCRIPT=region_ping_loop.sh

# get list of regions from gcloud and write them to ${REGIONS}
list_regions() {
    gcloud compute regions list --format="value(name)" >${REGIONS}
    local count=$(cat ${REGIONS} | wc -l | xargs)
    echo "found ${count} regions"
}

# get list of active instances and write them to ${HOSTS}
list_instances() {
    gcloud compute instances list | awk '{print $1"::"$5}' | grep -v "NAME::INTERNAL_IP" >${HOSTS}
    local count=$(cat ${HOSTS} | wc -l | xargs)
    echo "found ${count} hosts"
}

# get region name
zone() {
    local region=$1
    # -b zones just happen to exist in every region
    echo "${region}-b"
}

# create one f1-micro instance per region
create_instances() {
    for region in $(cat ${REGIONS}); do
        echo "creating ${region}"
        local zone=$(zone ${region})

        gcloud beta compute instances create ${region} \
            --zone=${zone} \
            --machine-type="f1-micro" \
            --image-project="ubuntu-os-cloud" \
            --image-family="ubuntu-1604-lts" \
            --boot-disk-size="10" \
            --boot-disk-type="pd-standard" \
            --boot-disk-device-name=${region} &

    done
    wait
    echo "all instances creating"
}

#  delete all instances
delete_instances() {
    for region in $(cat ${REGIONS}); do
        echo "deleting ${region}"
        local zone=$(zone ${region})

        gcloud -q compute instances delete ${region} --zone=${zone} &
    done
    wait
    echo "all instances deleted"
}

# run ping experiment during the number of seconds passed as argument
run_ping_exp() {
    local seconds=$1
    for region in $(cat ${REGIONS}); do
        echo "starting ping exp in ${region}"
        run_region_ping_exp ${region} ${seconds} &
    done
    wait
}

# run ping experiment in region passed as argument
run_region_ping_exp() {
    local region=$1
    local seconds=$2
    local zone=$(zone ${region})
    local output="output.log"

    # initial connect to instance
    local greeting=""
    while [ "${greeting}" != "hello" ]; do
        echo "trying to connect to ${region}..."
        greeting=$(gcloud compute ssh \
            --ssh-flag="-o ConnectTimeout=10" \
            --zone ${zone} \
            ${region} --command "echo hello" 2>&1)
    done
    echo "now connected to ${region}"

    # copy both ${HOSTS} and ${SCRIPT} files to running instance
    gcloud compute scp \
        --zone ${zone} \
        ${HOSTS} ${SCRIPT} ${region}:~/

    # run ${SCRIPT} in instance
    echo "ping exp started in ${region}"
    gcloud compute ssh \
        --ssh-flag="-o ConnectTimeout=10" \
        --zone ${zone} \
        ${region} --command "bash ${SCRIPT} ${seconds} ${output}"
    echo "ping exp ended in ${region}"

    # retrive ${OUTPUT}
    gcloud compute scp \
        --zone ${zone} \
        ${region}:~/${output} ${region}.dat
}

# exp config
# run the experiment for 1h unless the user passes a different number of seconds as the first argument
SECONDS=3600
if [ ! -z "$1" ]; then
    SECONDS=$1
fi

# remove previous files
rm -f *.dat

list_regions
create_instances
list_instances
run_ping_exp ${SECONDS}
delete_instances
