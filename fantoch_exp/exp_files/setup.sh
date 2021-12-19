#!/usr/bin/env bash

RUST_TOOLCHAIN="1.57.0"
# RUST_TOOLCHAIN="nightly-2020-06-10"

# flag indicating whether we should just remove previous installations
NUKE_RUST="false"
NUKE_FANTOCH="false"
FANTOCH_PACKAGE="fantoch_ps"

# maximum number of open files
MAX_OPEN_FILES=100000
# maximum buffer sizes
MAX_SO_RCVBUF=$((10 * 1024 * 1024)) # 10mb
MAX_SO_SNDBUF=$((10 * 1024 * 1024)) # 10mb

setup() {
    local aws=$1
    local mode=$2 # possible values: release, flamegraph, heaptrack

    # cargo/deps requirements
    sudo apt-get update
    sudo apt-get install -y \
            build-essential \
            pkg-config \
            libssl-dev \
            git

    # install chrony if in aws
    if [ "${aws}" == "true" ]; then
        # see: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html
        sudo apt-get install chrony -y

        # set chrony server:
        # - first delete current setting, if any
        sudo sed -i '/^server 169.254.169.123 ' /etc/chrony/chrony.conf
        # - then append correct setting
        echo "server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4" | sudo tee -a /etc/chrony/chrony.conf

        # restart chrony daemon
        sudo /etc/init.d/chrony restart
    fi

    # maybe nuke previous stuff
    if [ "${NUKE_RUST}" == "true" ]; then
        rm -rf .cargo/ .rustup/
    fi
    if [ "${NUKE_FANTOCH}" == "true" ]; then
        rm -rf fantoch/
    fi

    # install rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --default-toolchain none --profile minimal
    # shellcheck disable=SC1090
    source "${HOME}/.cargo/env"

    # install toolchain
    rustup toolchain install ${RUST_TOOLCHAIN}
    rustup override set ${RUST_TOOLCHAIN}
    rustup update ${RUST_TOOLCHAIN}

    case ${mode} in
    "release")
        # nothing else to install
        ;;
    "flamegraph")
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

        # install deps
        sudo apt-get install -y linux-tools-common linux-tools-generic

        # additional requirement if on aws
        if [ "${aws}" == "true" ]; then
            sudo apt-get install linux-tools-5.3.0-1023-aws -y
        fi

        # install flamegraph
        cargo install flamegraph --version 0.4.0
        flamegraph --help
        ;;
    "heaptrack")
        # install heaptrack
        sudo apt install heaptrack -y
        ;;
    *)
        echo "invalid run mode: ${mode}"
        exit 1
    esac

    # increase maximum number of open files by changing "/etc/security/limits.conf"
    # - first delete current setting, if any
    sudo sed -i '/.*soft.*nofile.*/d' /etc/security/limits.conf
    sudo sed -i '/.*hard.*nofile.*/d' /etc/security/limits.conf
    # - then append correct setting
    echo "*                soft    nofile          ${MAX_OPEN_FILES}" | sudo tee -a /etc/security/limits.conf
    echo "*                hard    nofile          ${MAX_OPEN_FILES}" | sudo tee -a /etc/security/limits.conf

    # increase max size for SO_RCVBUF and SO_SNDBUF
    # - first delete current setting, if any
    sudo sed -i '/^net.core.rmem_max.*/d' /etc/sysctl.conf
    sudo sed -i '/^net.core.wmem_max.*/d' /etc/sysctl.conf
    # - then append correct setting
    echo "net.core.rmem_max = ${MAX_SO_RCVBUF}" | sudo tee -a /etc/sysctl.conf
    echo "net.core.wmem_max = ${MAX_SO_SNDBUF}" | sudo tee -a /etc/sysctl.conf

    # reload system configuration so that previous changes  take place
    sudo sysctl --system

    # install htop, dstat and lsof
    sudo apt-get install -y htop dstat lsof
    dstat --help
    lsof -h

    # clean up
    sudo apt-get autoremove -y
}

build_fantoch() {
    local branch=$1
    local features=$2 # comma-separated list of features

    # clone the repository if dir does not exist
    if [[ ! -d fantoch ]]; then
        until git clone https://github.com/vitorenesduarte/fantoch -b "${branch}"; do
            echo "git clone failed; trying again"
            rm -rf fantoch
        done
    fi

    # pull recent changes in ${branch}
    cd fantoch/ || {
        echo "fantoch/ directory must exist after clone"
        exit 1
    }
    # stash before checkout to make sure checkout will succeed
    git stash
    git pull
    git checkout "${branch}"

    # build all the binaries in release mode for maximum performance:
    # - build if features enabled if any features were defined
    cd "${FANTOCH_PACKAGE}"
    if [ "${features}" == "" ]; then
        RUSTFLAGS="-C target-cpu=native" cargo build --release --bins
    else
        RUSTFLAGS="-C target-cpu=native" cargo build --release --bins --features ${features}
    fi
}

if [[ $# < 3 || $# > 4 ]]; then
    echo "usage: setup.sh testbed mode branch (features)"
    exit 1
fi

testbed=$1
mode=$2
branch=$3
features=$4

case ${testbed} in
"aws")
    setup "true" ${mode}
    ;;
"baremetal")
    setup "false" ${mode}
    ;;
"local")
    # nothing to setup
    ;;
*)
    echo "invalid testbed: ${testbed}"
    exit 1
esac

# in all cases, build fantoch
build_fantoch ${branch} ${features}
