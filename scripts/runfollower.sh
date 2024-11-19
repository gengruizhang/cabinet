#!/bin/bash

# read -p "---------- executing commands remotely? --------------"
while IFS= read -r line; do
    OIFS=$IFS; IFS=' '

    read -a ips <<< "$line"
    
    if [[ ${ips[0]} -eq 0 ]]; then continue; fi
    if [[ $2 -gt 0 ]]; then
		ep=true
	else
		ep=false
	fi

    echo "executing on server ${ips[0]} -> id: ${ips[1]}"
    ssh ${ips[1]} "cd ~/go/src/cabinet; ./cabinet -n=$1 -f=$2 -ep=$ep -et=2 -mode=1 -log=error -id=${ips[0]} -path=./config/cluster_heter_$1.conf -mload=${3:-a} -b=${4:-10000} > /dev/null" &

    if [[ ${ips[0]} -ge $1-1 ]]; then break; fi
    
    IFS=$OIFS
done < ../config/cluster_heter_$1.conf
