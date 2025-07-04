package main

import (
	"cabinet/tpcc"
	"cabinet/tpcc/measurement"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"time"
)

const (
	Localhost = iota
	Distributed
)

const (
	PlainMsg = iota
	TPCC
	MongoDB
)

var numOfServers int
var threshold int
var quorum int
var myServerID int
var configPath string
var production bool
var logLevel string
var mode int
var evalType int

var batchsize int
var msgsize int

var ratioTryStep float64
var enablePriority bool

// Mongo DB input parameters
var mongoLoadType string
var mongoClientNum int

// crash test parameters
var crashTime int
var crashMode int

// suffix of files
var suffix string

func loadCommandLineInputs() {
	flag.IntVar(&numOfServers, "n", 10, "# of servers")

	flag.IntVar(&threshold, "t", 1, "# of quorum tolerated")
	// f = t + 1;
	quorum = threshold + 1

	flag.IntVar(&batchsize, "b", 1, "batch size")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")

	// Production mode stores log on disk at ./logs/
	flag.BoolVar(&production, "pd", false, "production mode?")
	flag.StringVar(&logLevel, "log", "debug", "trace, debug, info, warn, error, fatal, panic")
	flag.IntVar(&mode, "mode", 0, "0 -> localhost; 1 -> distributed")
	flag.IntVar(&evalType, "et", 0, "0 -> plain msg; 1 -> tpcc; 2 -> mongodb")

	flag.Float64Var(&ratioTryStep, "rstep", 0.001, "rate for trying qualified ratio")

	flag.BoolVar(&enablePriority, "ep", true, "true -> cabinet; false -> raft")

	// Plain message input parameters
	flag.IntVar(&msgsize, "ms", 512, "message size")

	// MongoDB input parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb load type")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# of mongodb clients")

	// TPC-C input parameters
	flag.StringVar(&tpcc.TpccConfig.Action, "action", "", "Action of TPCC benchmark")
	flag.IntVar(&tpcc.TpccConfig.Threads, "threads", 16, "Thread concurrency")
	flag.DurationVar(&tpcc.TpccConfig.OutputInterval, "interval", 10*time.Second, "Output interval time")
	flag.DurationVar(&tpcc.TpccConfig.PrepareRetryInterval, "retry-interval", 10*time.Second, "The interval for each retry")
	flag.DurationVar(&tpcc.TpccConfig.MaxMeasureLatency, "max-measure-latency", measurement.DefaultMaxLatency, "max measure latency in millisecond")
	flag.DurationVar(&tpcc.TpccConfig.TotalTime, "time", 1<<63-1, "Total execution time")
	tpcc.TpccConfig.Targets = append(tpcc.TpccConfig.Targets, "")

	// crash test parameters
	flag.IntVar(&crashTime, "ct", 20, "# of rounds before crash")
	flag.IntVar(&crashMode, "cm", 0, "0 -> no crash; 1 -> strong machines; 2 -> weak machines; 3 -> random machines")

	// suffix of files
	flag.StringVar(&suffix, "suffix", "xxx", "suffix of files")

	flag.Parse()

	log.Debugf("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n", numOfServers, myServerID)
}

func readTpccConfig() {
	// Read the configuration file
	data, err := ioutil.ReadFile("tpccConfig.json")
	if err != nil {
		fmt.Println("Error reading config file:", err)
		return
	}

	// Unmarshal the JSON data into the config struct
	err = json.Unmarshal(data, &tpcc.TpccConfig)
	if err != nil {
		fmt.Println("Error unmarshaling config:", err)
		return
	}
}
