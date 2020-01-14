#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "usage: build branch"
    exit 1
fi

# get branch
branch=$1

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# shellcheck disable=SC1090
source "${HOME}/.cargo/env"

# clone the repository if dir does not exist
if [[ ! -d planet_sim ]]; then
    git clone https://github.com/vitorenesduarte/planet_sim -b "${branch}"
fi

# pull recent changes in ${branch}
cd planet_sim/ || {
    echo "planet_sim/ directory must exist after clone"
    exit 1
}
git checkout "${branch}"
git pull

# build all the binaries in release mode for this architecture
cargo build --release --bins

# return machine IP
hostname -I | awk '{ print $1 }'
