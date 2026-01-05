#!/bin/bash
# assume proj path
PROJ_PATH="$HOME/workspace/uFS-sched"
PERF_PATH="$PROJ_PATH/cfs/lib/spdk/examples/nvme/perf/perf"

RESULTS_PATH="./results/dev_charact_randread.csv"

rm -rf "$RESULTS_PATH"
echo "size,qdepth,bandwidth,iops" >> "$RESULTS_PATH"
for sz in 4096 8192; do
	for qd in {1..32}; do
		# only do 100% random read for now
		res=$(sudo "$PERF_PATH" -q $qd -s $sz -w randread -c 1 -t 5 | grep Total)
		str_array=($res)
		echo "$sz,$qd,${str_array[3]},${str_array[2]}" >> "$RESULTS_PATH"
	done
done
