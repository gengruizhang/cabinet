#!/bin/bash

YCSB_DIR=./YCSB
MVN_DIR=./apache-maven-3.5.4

mkdir -p ./workData

if [ ! -d $YCSB_DIR ];
then
    echo "YCSB does not exist, downloading..."
    git clone https://github.com/brianfrankcooper/YCSB.git
fi

MVN_VER=$(mvn -v)
if [[ ${MVN_VER:13:1} -ne 3 || ${MVN_VER:15:1} -gt 6 ]];
then
    echo "downgrading maven to 3.5.4..."
    if [ ! -d $MVN_DIR ];
    then
        echo "downloading maven 3.5.4..."
        wget --no-check-certificate "https://archive.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz"
        tar -xvf apache-maven-3.5.4-bin.tar.gz
	rm apache-maven-3.5.4-bin.tar.gz
    fi
    M2_HOME=$(pwd)/apache-maven-3.5.4
    PATH=$M2_HOME/bin:$PATH
    export PATH
fi
MVN_VER=$(mvn -v)
echo "current maven: "${MVN_VER:13:5}

if [ -d "./config" ];
then
    CONFIG_DIR=config
else
    CONFIG_DIR=YCSB/workloads
fi

cd $YCSB_DIR
echo "generating loading workload"
./bin/ycsb load basic -P "../$CONFIG_DIR/workloada" > ../workData/workload.dat
for i in a b c d e f
do
    echo "generating running workload$i..."
    ./bin/ycsb run basic -P "../$CONFIG_DIR/workload$i" > ../workData/run_workload$i.dat
done
cd ..
