#!/bin/bash

mode=$1
nodenum=${2:-50}

while IFS= read -r line; do
	OIFS=$IFS; IFS=' '
	read -a ips <<< "$line"
	
	if [[ ${ips[0]} -eq 0 ]]; then continue; fi
	
	echo "delete network latency on server ${ips[0]} -> id: ${ips[1]}"
	ssh ${ips[1]} "sudo tc qdisc del dev ens3 root" &
	if [ $mode == '3' ] || [ $mode == 'dynamic' ]; then
		ssh ${ips[1]} "kill -9 \$(ps aux | grep 'followerlatency.sh' | awk '{print \$2}'); sudo tc qdisc del dev ens3 root" &
	fi
	echo "server ${ips[0]} -> ${ips[1]} latency deleted"

	if [[ ${ips[0]} -ge $nodenum-1 ]]; then break; fi
	IFS=$OIFS
done < ../config/cluster_homo_new_$nodenum.conf
