### This script parses the unordered output of parallel simulation and pretty-prints it.

import sys

if len(sys.argv) != 2:
    print("wrong number of arguments!")
    sys.exit()


data = []

with open(sys.argv[1]) as f:
    content = f.readlines()

    for line in content:
        line = line.strip()

        # extract header and histogram values
        parts = line.split("|")
        header = parts[0].strip()
        hist = parts[1].strip()

        # extract protocol name, n, f, and c
        parts = header.split(" ")
        assert(parts[1] == "n")
        assert(parts[4] == "f")
        assert(parts[7] == "c")
        protocol = parts[0]
        n = int(parts[3])
        f = int(parts[6])
        c = int(parts[9])

        # append in the order we want them sorted
        data.append((n, c, protocol, f, hist))

# sort data
data.sort()

last_n = 0
last_c = 0

for (n, c, protocol, f, hist) in data:
    if n != last_n:
        print()
        print()
        print("n = " + str(n))
    
    if c != last_c:
        print()
        print("c = " + str(c))

    print("{:<6}".format(protocol) + " f = " + str(f) + " | " + hist)

    last_n = n
    last_c = c
