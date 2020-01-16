#!/usr/bin/env bash

# - needed for RUN_MODE="leak"
RUST_NIGHTLY="true"
# - needed for RUN_MODE="flamegraph"
FLAMEGRAPH="true"

if [ $# -ne 1 ]; then
    echo "usage: build.sh branch"
    exit 1
fi

# get branch
branch=$1

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# shellcheck disable=SC1090
source "${HOME}/.cargo/env"

# check for rust updates (in case it was already installed)
rustup update

if [ "${RUST_NIGHTLY}" == "true" ]; then
    # install nightly
    rustup toolchain install nightly
fi

if [ "${FLAMEGRAPH}" == "true" ]; then
    # install perf:
    # - this command seems to be debian specific
    sudo apt-get update
    sudo apt-get install -y perf-tools-unstable

    # give permissions to perf by setting "kernel.perf_event_paranoid = -1" in "/etc/sysctl.conf"
    # - first delete current setting, if any
    sudo sed -i '/^kernel.perf_event_paranoid.*/d' /etc/sysctl.conf
    # - then append correct setting
    echo "kernel.perf_event_paranoid = -1" | sudo tee -a /etc/sysctl.conf
    # - finally, reload system configuration so that changes take place
    sudo sysctl --system

    # install flamegraph
    sudo apt-get install -y linux-tools-common linux-tools-generic
    cargo install flamegraph
fi

# install dstat
sudo apt-get install -y dstat

# clone the repository if dir does not exist
if [[ ! -d planet_sim ]]; then
    git clone https://github.com/vitorenesduarte/planet_sim -b "${branch}"
fi

# pull recent changes in ${branch}
cd planet_sim/ || {
    echo "planet_sim/ directory must exist after clone"
    exit 1
}
git stash
git checkout "${branch}"
git pull
