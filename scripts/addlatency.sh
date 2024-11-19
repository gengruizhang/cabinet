#!/bin/bash

mode=$1
nodenum=${2:-50}
skewlat=(100 300 500 700 900)
skewdev=(10 10 10 10 10)
zonenum=5

while IFS= read -r line; do
	OIFS=$IFS; IFS=' '
	read -a ips <<< "$line"
	
	if [[ ${ips[0]} -eq 0 ]]; then continue; fi
        
	echo "add network latency on server ${ips[0]} -> id: ${ips[1]}"
        
	zone=$(( (${ips[0]}-1) * $zonenum / $nodenum ))
	echo "server ${ips[0]}'s zone is $zone"
	case $mode in 
		1 | normal)
			echo "normal distributed latency"
			ssh ${ips[1]} "sudo tc qdisc add dev ens3 root netem delay ${3:-100}ms ${4:-20}ms distribution normal" &
                        echo "server ${ips[0]} -> ${ips[1]} latency ${3:-100}ms added"
        		;;
		2 | skew) 
	 		echo "skew distributed latency"
			ssh ${ips[1]} "sudo tc qdisc add dev ens3 root netem delay ${skewlat[$zone]}ms ${skewdev[$zone]}ms distribution normal" &
			echo "server ${ips[0]} -> ${ips[1]} latency ${skewlat[$i]}ms added"
			;;
		3 | dynamic)
			echo "dynamically skew distributed latency"
			ssh ${ips[1]} "cd go/src/cabinet/scripts; ./followerlatency.sh $zone ${3:-2} ${4:-30}" &
			echo "server ${ips[0]} -> ${ips[1]} latency added; latencies change every ${3:-2}s"
			;;
		*)
			echo "1: normal distribution; 2: static skew distribution; 3: dynamic skew distribution"
			break
	esac

	if [[ ${ips[0]} -ge $nodenum-1 ]]; then break; fi
                        IFS=$OIFS
done < ../config/cluster_homo_new_$nodenum.conf
