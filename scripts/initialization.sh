#!/bin/bash

while IFS= read -r line; do
	OIFS=$IFS; IFS=' '
	read -a ips <<< "$line"

	echo "ssh-keygen to server ${ips[0]} on host -> id: ${ips[1]}"
	ssh-keygen -R ${ips[1]}

	echo "ssh-keygen on server ${ips[0]}"
	ssh -T ${ips[1]} <<'EOL'
		cd go/src/cabinet
		while IFS= read -r sline; do
			OIFS=$IFS; IFS=' '
			read -a sips <<< "$sline"
			ssh-keygen -R ${sips[1]}
			IFS=$OIFS
		done < config/cluster_homo_3.conf
		sudo service mongod restart
EOL
	echo "server ${ips[0]} -> ${ips[1]} is done"
	IFS=$OIFS
done < ../config/cluster_homo_3.conf

sudo service mongod restart
