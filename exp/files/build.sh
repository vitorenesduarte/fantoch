#!/usr/bin/env bash

# - needed for RUN_MODE="leak"
RUST_NIGHTLY="true"
# - needed for RUN_MODE="flamegraph"
FLAMEGRAPH="true"
# flag indicating whether we should just remove previous installations
NUKE_RUST="false"
NUKE_PLANET_SIM="false"

# maximum number of open files
MAX_OPEN_FILES=100000

if [ $# -ne 1 ]; then
    echo "usage: build.sh branch"
    exit 1
fi

# get branch
branch=$1

# maybe nuke previous stuff
if [ "${NUKE_RUST}" == "true" ]; then
    rm -rf .cargo/ .rustup/
fi
if [ "${NUKE_PLANET_SIM}" == "true" ]; then
    rm -rf .planet_sim/
fi

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

    # reload system configuration so that previous changes  take place
    sudo sysctl --system

    # install flamegraph
    sudo apt-get install -y linux-tools-common linux-tools-generic
    cargo install flamegraph
fi

# increase maximum number of open files by changing "/etc/security/limits.conf"
# - first delete current setting, if any
sudo sed -i '/.*soft.*nofile.*/d' /etc/security/limits.conf
sudo sed -i '/.*hard.*nofile.*/d' /etc/security/limits.conf
# - then append correct setting
echo "*                soft    nofile          ${MAX_OPEN_FILES}" | sudo tee -a /etc/security/limits.conf
echo "*                hard    nofile          ${MAX_OPEN_FILES}" | sudo tee -a /etc/security/limits.conf

# install dstat and lsof
sudo apt-get install -y dstat lsof

# clean up
sudo apt-get autoremove

# clone the repository if dir does not exist
if [[ ! -d planet_sim ]]; then
    git clone https://github.com/vitorenesduarte/planet_sim -b "${branch}"
fi

# pull recent changes in ${branch}
cd planet_sim/ || {
    echo "planet_sim/ directory must exist after clone"
    exit 1
}
# stash before checkout to make sure checkout will succeed
git stash
git checkout "${branch}"
git pull

# use nightly
rustup override set nightly
