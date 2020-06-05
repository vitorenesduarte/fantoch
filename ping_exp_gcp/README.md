### Ping experiment

Run `ping_exp.sh` to retrieve ping information (i.e. round-trip times between regions) from all regions in Google Cloud.

The experiment will run for 1 hour unless you pass the number of seconds as the first argument to `ping_exp.sh`. For example, `ping_exp.sh 60` will run the experiment for 1 minute.

This will produce `*.dat` files, as many as the number of regions in Google Cloud.
These files can then be used to update the current ping information in the [../latency_gcp](../latency_gcp) folder.
