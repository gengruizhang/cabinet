#!/bin/bash

while getopts :f:r:o: flag
do
    case $flag in
        f) file=${OPTARG};;
        r) rc=${OPTARG};;
        o) oc=${OPTARG};;
        # d) dist=${OPTARG};;
    esac
done

cd ./YCSB
echo "generating load data..."
./bin/ycsb load basic -P ../config/workload$file \
            -p recordcount=$rc \
            > ../workData/workload.dat

echo "generating run data..."
./bin/ycsb run basic -P ../config/workload$file \
            -p recordcount=$rc \
            -p operationcount=$oc \
            > ../workData/run_workload$file.dat
cd ..