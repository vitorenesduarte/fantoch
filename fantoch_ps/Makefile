SIM_OUTPUT="A"

parse:
	grep "n = " "$(SIM_OUTPUT)" | grep -v "region" | sort > SIM
	python3 parse_sim.py SIM
	rm SIM

parse1:
	rg "n = " $(SIM_OUTPUT) | rg -v region | sort > SIM
	python3 parse_sim.py SIM
	rm SIM

clean:
	rm -f *.log .metrics_* *.execution_log
