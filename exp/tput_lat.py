#!/usr/bin/env python3

import sys

N = 3

with open(".run_log") as f:
    content = f.readlines()

    index = 0
    while index < len(content):
        line = content[index]
        if line[0] == "C":
            # get number of clients
            clients = int(line[2:].strip())
            index += 1

            metrics = {}
            # read the next N lines
            for _ in range(N):
                line = content[index]
                parts = line.split()
                # skip the first part
                parts = parts[1:]
                for part in parts:
                    id_and_value = part.strip().split("=")
                    part_id = id_and_value[0]
                    part_value = id_and_value[1]

                    if not part_id in metrics:
                        metrics[part_id] = []

                    metrics[part_id].append(int(part_value))
                index += 1

            # show metric
            print()
            print("C=" + str(clients))
            for id in metrics:
                value_micros = sum(metrics[id]) / len(metrics[id])
                print(id, int(value_micros), "micros")

                if id == "avg":
                    value_millis = value_micros / 1000
                    tput = 1000 / value_millis * clients * N
                    print("tput", int(tput), "ops/s")
        else:
            print()
            print("-----------------------------")
            print(line)
            index += 1
