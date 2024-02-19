#!/usr/bin/env bash

PID_FILE=server.pid

if [ ! -f "${PID_FILE}" ]; then
    echo "Servers aren't running."
else
	while read pid; do
		if [ -z "${pid}" ]; then
      echo "Servers aren't running."
		else
			kill -15 "${pid}"
			echo "Server with PID ${pid} shutdown."
    	fi
	done < "${PID_FILE}"
	rm "${PID_FILE}"
fi
