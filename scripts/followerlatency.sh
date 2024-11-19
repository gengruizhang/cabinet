#!/bin/bash

latencies=(100 300 500 700 900)
deviation=(10 10 10 10 10)

index=${1:-0}
interval=${2:-2}
totaltime=${3:-30}

len=${#latencies[@]}
loopnum=$(echo $totaltime / $interval | bc) 
ind=$(($index % $len))

sudo tc qdisc add dev ens3 root netem delay ${latencies[$ind]}ms ${deviation[$ind]}ms distribution normal

for (( i=0; i<${loopnum%.*}; i++)); do 
	ind=$(($index % $len))
	sudo tc qdisc change dev ens3 root netem delay ${latencies[$ind]}ms ${deviation[$ind]}ms distribution normal
	((index++))
	sleep $interval
done

sudo tc qdisc del dev ens3 root 
