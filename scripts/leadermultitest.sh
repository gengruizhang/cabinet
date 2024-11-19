#!/bin/bash

wlist=(a b c d e f)
nlist=(100 50 20 10)
flist=(0.4 0.3 0.2 0.1 0)
blist=(10000 5000 2000 1000)
dllist=(none u100 u200 u500 u1000 skew dysk)
faultlist=(strict_strong strict_weak strict_random raft_strong)

dlnum=0
faultnum=0
for wl in ${wlist[@]}; do
	for n in ${nlist[@]}; do
		for fr in ${flist[@]}; do
			if (( $(echo "$fr > 0" | bc -l) )); then
				fc=$(echo $n*$fr | bc | awk '{printf("%.0f", $1)}')
				ep=true
			else
				fc=0
				ep=false
			fi
			for b in ${blist[@]}; do
				for t in {1..3}; do
					echo "===================================================================="
					echo "============  (n, f, wl, b, t) -> ($n, $fc, $wl, $b, $t)  ============"
					echo "===================================================================="

					# echo "add latencies: dynamic skew int 15s"
					# ./addlatency.sh 3 $n 15 1800
					# sleep 25

					echo "followers start"
					./runfollower.sh $n $fc $wl $b > /dev/null
					sleep 60

					echo "leader starts"
					cd ..
					./cabinet -n=$n -f=$fc -et=2 -cm=$(($faultnum+1)) -mode=1 -log=error -path=./config/cluster_heter_$n.conf -mload=$wl -b=$b -id=0 -ep=$ep -suffix="wl${wl}_delay_${dllist[$dlnum]}_fault_${faultlist[$faultnum]}_t${t}"  > /dev/null
					cd scripts
					echo "leader done"

					sleep 30
					# echo "delete latencies"
					# ./dellatency.sh 3 $n
					# sleep 30
					./killall.sh $n > /dev/null
					sleep 5
					./killall.sh $n > /dev/null
					sleep 5
					./killall.sh $n > /dev/null
					echo "test done"
					sleep 60
				done
				echo "===================================================================="
                                echo "======= done with (n, f, wl, b) -> ($n, $fc, $wl, $b)  ======="
                                echo "===================================================================="
				sleep 30
			done
                        echo "===================================================================="
                        echo "======== done with (n, f, wl) -> ($n, $fc, $wl)  ========"
                        echo "===================================================================="
			sleep 30
		done
		echo "===================================================================="
                echo "============== done with (n, wl) -> ($n, $wl)  =============="
                echo "===================================================================="
		sleep 60
	done
      	echo "===================================================================="
        echo "============== done with (wl) -> ($wl)  ================"
        echo "===================================================================="
        sleep 60
done

