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


if len(sys.argv) != 4:
    print("usage: topology.py id number_of_processes machine_ips_file")
    sys.exit(1)

# get number of processes and output file (the first argument is this script)
id = int(sys.argv[1])
n = int(sys.argv[2])
machine_ips_file = sys.argv[3]

# get machines and ips list
data = machines_and_ips(machine_ips_file)

# compute sorted
sorted = sorted_by_distance(id, n)

# compute ip and ips to connect to (all but me)
ips = []
for p in range(1, n + 1):
    # find process ip
    (machine, ip) = data[p - 1]
    if p == id:
        my_machine = machine
        my_ip = ip
    else:
        ips.append(ip)

sorted = ",".join(map(str, sorted))
ips = ",".join(ips)
print(my_machine, sorted, my_ip, ips)
