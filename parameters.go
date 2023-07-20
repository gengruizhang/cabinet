package main

import "flag"

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
var faults int
var myServerID int
var configPath string
var production bool
var logLevel string
var mode int
var evalType int

var batchsize int
var msgsize int

var enablePriority bool

// Mongo DB input parameters
var mongoLoadType string
var mongoClientNum int

// suffix of files
var suffix string

func loadCommandLineInputs() {
	flag.IntVar(&numOfServers, "n", 5, "# of servers")
	flag.IntVar(&faults, "f", 3, "# of faults tolerated") // n= 10 f=10%n+1 -> 2; f=20%n+1 -> 3
	flag.IntVar(&batchsize, "b", 10000, "batch size")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")
	flag.BoolVar(&production, "pd", false, "production mode?")
	flag.StringVar(&logLevel, "log", "debug", "trace, debug, info, warn, error, fatal, panic")
	flag.IntVar(&mode, "mode", 0, "0 -> localhost; 1 -> distributed")
	flag.IntVar(&evalType, "et", 2, "0 -> plain msg; 1 -> tpcc; 2 -> mongodb")

	flag.BoolVar(&enablePriority, "ep", true, "true -> cabinet; false -> raft")

	// Plain message input parameters
	flag.IntVar(&msgsize, "ms", 512, "message size")

	// MongoDB input parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb load type")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# of mongodb clients")

	// TPC-C input parameters

	// suffix of files
	flag.StringVar(&suffix, "suffix", "xxx", "suffix of files")

	flag.Parse()

	log.Infof("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n", numOfServers, myServerID)
}
