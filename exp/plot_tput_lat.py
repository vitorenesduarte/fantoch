#!/usr/bin/env python3

import math
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

seq_inconsistent_lat = [201,280,453,784,1533,3078]
seq_inconsistent_tput = [238016,342857,423218,489379,500760,499025]

inconsistent_lat = [182,230,331,537,926,1698]
inconsistent_tput = [263254,416184,578894,715083,829373,904416]

seq_newt_lat_0 = [242,396,683,1341,2677,5364]
seq_newt_tput_0 = [197802,242016,281112,286282,286816,286353]

newt_lat_0 = [195,257,403,681,1247,2328]
newt_tput_0 = [246153,372574,476426,563600,615878,659793]

# newt_lat_2 = [274,361,527,897,1783,3536,7147]
# newt_tput_2 = [175182,265927,364096,427775,430654,434348,429790]

skip_newt_lat_0 = [203,262,396,662,1204,2231]
skip_newt_tput_0 = [235679,365482,484440,579476,637873,688274]

seq_atlas_lat_0 = [291,473,843,1638,3235,6496]
seq_atlas_tput_0 = [164948,202816,227667,234384,237354,236453]

atlas_lat_0 = [244,389,667,1227,2393,4772]
atlas_tput_0 = [196721,246575,287568,312788,320936,321877]

skip_atlas_lat_0 = [212,283,452,805,1504,2634]
skip_atlas_tput_0 = [226415,339222,424153,477018,510412,583069]

seq_fpaxos_lat = [312,488,931,1814,3642,7686]
seq_fpaxos_tput = [153846,196721,206156,211609,210853,199826]

fpaxos_lat = [284,381,601,1099,2107,4128]
fpaxos_tput = [168815,251968,319113,349408,364499,372032]

skip_fpaxos_lat = [262,347,582,1086,2139,4254]
skip_fpaxos_tput = [183206,276126,329519,353591,358990,361071]

fpaxos_1024_lat = [406,526,816,1421]
fpaxos_1024_tput = [118226,182393,235294,270232]

fpaxos_2048_lat = [464,669,1214]
fpaxos_2048_tput = [103448,143426,158154]

# w = 4
# h = 10
# d = 70
# fig = plt.figure(figsize=(w, h), dpi=d)
# ax = plt.axes()

fig, ax = plt.subplots()

# fig, ax = plt.subplots()
# ax.plot(ys, ys, ':r', label='system size')

plot_type = 3

if plot_type == 1:
    ax.plot(list(map(lambda v: v / 1000, seq_fpaxos_tput)), seq_fpaxos_lat, '.:r', label='sequential paxos')
    ax.plot(list(map(lambda v: v / 1000, fpaxos_tput)), fpaxos_lat, '.--r', label='paxos')

    ax.plot(list(map(lambda v: v / 1000, seq_atlas_tput_0)), seq_atlas_lat_0, '.:g', label='sequential atlas 0%')
    ax.plot(list(map(lambda v: v / 1000, atlas_tput_0)), atlas_lat_0, '.--g', label='atlas 0%')

    ax.plot(list(map(lambda v: v / 1000, seq_newt_tput_0)), seq_newt_lat_0, '.:k', label='sequential newt 0%')
    ax.plot(list(map(lambda v: v / 1000, newt_tput_0)), newt_lat_0, '.--k', label='newt 0%')

    ax.plot(list(map(lambda v: v / 1000, seq_inconsistent_tput)), seq_inconsistent_lat , '.:y', label='sequential inconsistent')
    ax.plot(list(map(lambda v: v / 1000, inconsistent_tput)), inconsistent_lat , '.--y', label='inconsistent')
    plt.xlim(0, 1000)
    plt.ylim(0, 8000)

elif plot_type == 2:
    ax.plot(list(map(lambda v: v / 1000, skip_fpaxos_tput)), skip_fpaxos_lat, '.:r', label='skip-exec paxos')
    ax.plot(list(map(lambda v: v / 1000, fpaxos_tput)), fpaxos_lat, '.--r', label='paxos')

    ax.plot(list(map(lambda v: v / 1000, skip_atlas_tput_0)), skip_atlas_lat_0, '.:g', label='skip-exec atlas 0%')
    ax.plot(list(map(lambda v: v / 1000, atlas_tput_0)), atlas_lat_0, '.--g', label='atlas 0%')

    ax.plot(list(map(lambda v: v / 1000, skip_newt_tput_0)), skip_newt_lat_0, '.:k', label='skip-exec newt 0%')
    ax.plot(list(map(lambda v: v / 1000, newt_tput_0)), newt_lat_0, '.--k', label='newt 0%')

    ax.plot(list(map(lambda v: v / 1000, inconsistent_tput)), inconsistent_lat , '.--y', label='inconsistent')
    plt.xlim(0, 1000)
    plt.ylim(0, 8000)

elif plot_type == 3:
    ax.plot(list(map(lambda v: v / 1000, fpaxos_tput)), fpaxos_lat, '.:r', label='paxos')
    ax.plot(list(map(lambda v: v / 1000, fpaxos_1024_tput)), fpaxos_1024_lat, '.--r', label='paxos (1KB)')
    ax.plot(list(map(lambda v: v / 1000, fpaxos_2048_tput)), fpaxos_2048_lat, '.-.r', label='paxos (2KB)')
    plt.xlim(0, 400)

# set ticks
# plt.xticks(xs)
# plt.yticks(list(range(1, max_y + 1)))

legend = ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), shadow=True, ncol=2)

ax.set(xlabel = "throughput (KOps/s)", ylabel='latency (microseconds)')
ax.grid()

fig.subplots_adjust(bottom=0.3)

fig.savefig("tput_lat.eps", format='eps')
