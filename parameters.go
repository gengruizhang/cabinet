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

// Mongo DB input parameters
var mongoLoadType string
var mongoClientNum int

func loadCommandLineInputs() {
	flag.IntVar(&numOfServers, "n", 5, "# of servers")
	flag.IntVar(&faults, "f", 2, "# of faults tolerated")
	flag.IntVar(&batchsize, "b", 1, "batch size")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")
	flag.BoolVar(&production, "pd", false, "production mode?")
	flag.StringVar(&logLevel, "log", "debug", "trace, debug, info, warn, error, fatal, panic")
	flag.IntVar(&mode, "mode", 0, "0 -> localhost; 1 -> distributed")
	flag.IntVar(&evalType, "et", 0, "0 -> plain msg; 1 -> tpcc; 2 -> mongodb")

	// Plain message input parameters
	flag.IntVar(&msgsize, "ms", 512, "message size")

	// MongoDB input parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb load type")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# of mongodb clients")
	flag.Parse()

	log.Infof("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n", numOfServers, myServerID)
}
