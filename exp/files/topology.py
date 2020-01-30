#!/usr/bin/env python3

import sys


def machines_and_ips(machine_ips_file):
    """ parses the file returning a list of tuples (machine, ip) """
    with open(machine_ips_file) as f:
        content = f.readlines()

    def parse(line):
        """ maps a line "machine ip" to (machine, ip) """
        parts = line.strip().split(" ")
        assert len(parts) == 2
        return (parts[0], parts[1])

    return [parse(line) for line in content]


def sorted_by_distance(id, n):
    """ returns processes (sorted by distance) that id should connect to """
    up_to_n = list(range(id, n + 1))
    before_id = list(range(1, id))
    return up_to_n + before_id


if len(sys.argv) != 6:
    print("usage: topology.py ttype index process_number client_machines_number machine_ips_file")
    sys.exit(1)

# get arguments:
# - two topology types are supported: process and client
ttype = sys.argv[1]
assert ttype == "process" or ttype == "client"
# - index represents the machine that will be assigned for this process of client;
# in case of `ttype == "process"` this represents the process id
index = int(sys.argv[2])
# - number of processes (and machines for those processes)
n = int(sys.argv[3])
# - number of machines where clients will run
client_machines_number = int(sys.argv[4])
# - file with all machines and their ips
machine_ips_file = sys.argv[5]

# get machines and ips list
data = machines_and_ips(machine_ips_file)

# the total number of machines should be:
# - at least as much as the sum of `n` and `client_machines_number`
assert len(data) >= n + client_machines_number

if ttype == "process":
    # index should be between 1 and n
    assert index >= 1 and index <= n

    # get my machine and ip
    (my_machine, my_ip) = data[index - 1]

    # compute sorted
    sorted = sorted_by_distance(index, n)

    # compute ip and ips to connect to (all but me)
    ips = []
    for p in list(range(index + 1, n + 1)) + list(range(1, index)):
        # find process ip
        (_, ip) = data[p - 1]
        ips.append(ip)

    sorted = ",".join(map(str, sorted))
    ips = ",".join(ips)
    print(my_machine, sorted, my_ip, ips)

elif ttype == "client":
    # index should be between 1 and `client_machines_number`
    assert index >= 1 and index <= client_machines_number

    # select client machine:
    # - we should skip the first `n` machines are those are assigned to processes
    (my_machine, _) = data[n + index - 1]

    # select the ip of the process machine to connect to:
    # - assuming there are as many processes as client machines,
    # we can select the process at this client's index
    # check that our assumption is true
    assert client_machines_number <= n
    (_, ip) = data[index - 1]
    # TODO make this more flexible
    print(my_machine, ip)

else:
    print("this case is impossible")
    sys.exit(1)
