package main

import (
	"cabinet/mongodb"
	"cabinet/smr"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

var log = logrus.New()
var mypriority = smr.NewServerPriority(-1, 0)
var mystate = smr.NewServerState()
var pscheme []priority

// Mongo DB variables
var mongoDbFollower *mongodb.MongoFollower
var runLocal bool = true

// Mongo DB input parameters
var loadType string = "a"
var clientNum int = 16

type serverID = int
type prioClock = int
type priority = float64

func init() {
	loadCommandLineInputs()
	setLogger(logLevel)

	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0)

	pscheme, mypriority.Majority = initPriorities(numOfServers, faults, 1)
}

func main() {
	fmt.Println("information board")
	fmt.Printf("priority scheme: %v\n", pscheme)
	fmt.Printf("majority: %v\n", mypriority.Majority)

	if myServerID == 0 {
		mypriority.PrioVal = pscheme[0] // leader has the highest priority
		establishRPCs()
		startSyncCabInstance()
	} else {
		// mongo DB clean up in case of ctrl+C
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c
			log.Debugf("clean up MongoDb follower")
			mongoDbFollower.CleanUp()
			os.Exit(1)
		}()

		runFollower()
		// tpccDependency()
		// mongoDependency()
	}
}
