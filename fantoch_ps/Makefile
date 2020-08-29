SIM_OUTPUT="A"

parse:
	rg "n = " $(SIM_OUTPUT) | rg -v region | sort > SIM
	python3 parse_sim.py SIM
	rm SIM

clean:
	rm -f *.log .metrics_* *.execution_log
