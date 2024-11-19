#!/bin/bash

# read -p "---------- executing commands remotely? --------------"
while IFS= read -r line; do
    OIFS=$IFS; IFS=' '

    read -a ips <<< "$line"
    
    if [[ ${ips[0]} -eq 0 ]]; then continue; fi

    echo "killing process on server ${ips[0]} -> id: ${ips[1]}"
    portnum=$(( 10000+${ips[0]} ))
    # ssh ${ips[1]} "kill -2 \$(ps aux | grep 'cabinet' | awk '{print \$2}')" &
    ssh ${ips[1]} "fuser -k -INT $portnum/tcp > /dev/null" &	
    echo "server ${ips[0]} -> ${ips[1]} is done"

    if [[ ${ips[0]} -ge ${1:-50}-1 ]]; then break; fi

    IFS=$OIFS
done < ../config/cluster_heter_${1:-50}.conf
