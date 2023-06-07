package main

import "flag"

var numOfServers int
var faults int
var myServerID int
var configPath string
var production bool
var logLevel string

func loadCommandLineInputs() {
	flag.IntVar(&numOfServers, "n", 5, "# of servers")
	flag.IntVar(&faults, "f", 2, "# of faults tolerated")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")
	flag.BoolVar(&production, "pd", false, "production mode?")
	flag.StringVar(&logLevel, "log", "debug", "trace, debug, info, warn, error, fatal, panic")
	flag.Parse()

	log.Infof("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n", numOfServers, myServerID)
}
