#!/bin/bash

read -p "---------- transfer files? --------------"

while IFS= read -r line; do
    OIFS=$IFS; IFS=' '
    read -a ips <<< "$line"
    
    echo "transferring to replica ${ips[0]} -> id: ${ips[1]}"
    scp -r ${1:-../../cabinet}  ${ips[1]}:~/go/src/${2:-}
    
    IFS=$OIFS
done < ${3:-../config/cluster_homo_3.conf}
