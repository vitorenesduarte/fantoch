import matplotlib
import matplotlib.pyplot as plt
import numpy as np

file = open("out.dat", "r")

# parse data in file
for (id, line) in enumerate(file):
    min_n = 3
    max_n = 0
    # a2 and f2 are initialized with a latency 0 since for n = 3 we don't have a latency value
    data = {"a1": [], "a2": [0], "f1": [], "f2": [0], "e": [],
            "a1C": [], "a2C": [0], "f1C": [], "f2C": [0], "eC": []}

    # drop last " " and split by "|"
    for n_entry in line.strip()[:-1].split("|"):
        # now we have an entry like:
        # - "[n=3] a1=(330, 0.19, 52.91) f1=(442, 0.21, 75.15) e=(330, 0.19, 52.91)"

        # if we split by " " we can get n
        splitted = n_entry.strip().split(" ")
        n = int(splitted[0][1:-1].split("=")[1])

        # update max n
        max_n = max(max_n, n)

        # join the remaining back to have something like:
        # - "a1=(330, 0.19, 52.91) f1=(442, 0.21, 75.15) e=(330, 0.19, 52.91)"
        n_entry = " ".join(splitted[1:])

        # replace ") " with "|" and split by it
        for protocol_stats in n_entry.replace(") ", "|").split("|"):
            # now we have something like:
            # - "a1=(330, 0.19, 52.91"

            # if we split by "=(" we can get protocol name and its latency (1st entry in the tuple)
            splitted = protocol_stats.split("=(")
            protocol = splitted[0]
            latency = int(splitted[1].split(",")[0])

            # save it
            data[protocol].append(latency)

    # computes xs
    labels = list(range(min_n, max_n + 1, 2))
    xs = np.arange(len(labels))

    # style stuff
    width = 0.12
    ylimits = [0, 600]

    for (suffix, title) in [("", "everywhere"), ("C", "colocated")]:
        # create plot
        fig, ax = plt.subplots()

        ax.bar(xs - 4*width/2, data["a1" + suffix], width, label='Atlas f=1')
        ax.bar(xs - 2*width/2, data["a2" + suffix], width, label='Atlas f=2')
        ax.bar(xs, data["e" + suffix], width, label='EPaxos')
        ax.bar(xs + 2*width/2, data["f1" + suffix], width, label='FPaxos f=1')
        ax.bar(xs + 4*width/2, data["f2" + suffix], width, label='FPaxos f=2')

        # set title, y limits, legend and labels
        ax.set_title(title)
        ax.set_ylim(ylimits)
        ax.legend(loc='upper right', shadow=True)
        ax.set(xlabel='#sites', ylabel='(ms)')
        ax.grid()

        # tickcs
        ax.set_xticks(xs)
        ax.set_xticklabels(labels)

        # save figure in png
        fig.savefig(str(id) + title + ".png", dpi=400, format='png')
